"""Write-ahead log with group commit.

The WAL is a file of JSON lines. Three record kinds exist:
  {"op": "meta", "term", "vote"}            currentTerm / votedFor
  {"op": "entries", "index", "entries"}     entries placed at index (truncates the rest)
  {"op": "snapshot", "index", "term", "data"}
A {"op": "compact", "records": [...]} request replaces the whole file with `records`.
"""

import json
import os
import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any

from raftkv.log import Entry, RaftLog

Record = dict[str, Any]


@dataclass
class PersistentState:
    term: int = 0
    vote: int | None = None
    snapshot: Record | None = None
    log: RaftLog = field(default_factory=RaftLog)


def replay(records: list[Record]) -> PersistentState:
    """Rebuild durable state from WAL records, in order."""
    state = PersistentState()
    for record in records:
        if record["op"] == "meta":
            state.term, state.vote = record["term"], record["vote"]
        elif record["op"] == "snapshot":
            state.snapshot = record
            state.log = RaftLog(record["index"], record["term"])
        elif record["op"] == "entries":
            state.log.write(record["index"], [Entry(*e) for e in record["entries"]])
    return state


def encode(record: Record) -> bytes:
    return json.dumps(record, separators=(",", ":")).encode() + b"\n"


class Storage:
    """Durable WAL. A single writer thread batches queued writes into one fsync."""

    def __init__(self, path: str) -> None:
        self.path = path
        self.fsyncs = 0
        self._queue: queue.Queue = queue.Queue()
        self._file: Any = None

    def load(self) -> PersistentState:
        """Read the WAL, cutting off a torn last line left by a crash mid-write."""
        data = b""
        if os.path.exists(self.path):
            with open(self.path, "rb") as f:
                data = f.read()
        records, good = [], 0
        while (end := data.find(b"\n", good)) != -1:
            try:
                records.append(json.loads(data[good:end]))
            except ValueError:
                break
            good = end + 1
        with open(self.path, "ab") as f:
            f.truncate(good)
        return replay(records)

    def start(self) -> None:
        self._file = open(self.path, "ab")
        threading.Thread(target=self._run, name="wal-writer", daemon=True).start()

    def submit(self, records: list[Record], on_durable: Callable[[], None]) -> None:
        """Queue records; `on_durable` runs on the writer thread after fsync."""
        self._queue.put((records, on_durable))

    def stop(self) -> None:
        self._queue.put(None)

    def _run(self) -> None:
        while True:
            batch = [self._queue.get()]
            while not self._queue.empty():
                batch.append(self._queue.get_nowait())
            stop = None in batch
            batch = [item for item in batch if item is not None]
            self._write_batch(batch)
            for _, on_durable in batch:
                on_durable()
            if stop:
                self._file.close()
                return

    def _write_batch(self, batch: list[tuple[list[Record], Callable[[], None]]]) -> None:
        dirty = False
        for records, _ in batch:
            for record in records:
                if record["op"] == "compact":
                    self._rewrite(record["records"])
                    dirty = False
                else:
                    self._file.write(encode(record))
                    dirty = True
        if dirty:
            self._fsync(self._file)

    def _rewrite(self, records: list[Record]) -> None:
        # Write a temp file and rename it, so a crash leaves either the old or the new WAL.
        self._file.close()
        tmp = self.path + ".tmp"
        with open(tmp, "wb") as f:
            for record in records:
                f.write(encode(record))
            self._fsync(f)
        os.replace(tmp, self.path)
        self._file = open(self.path, "ab")

    def _fsync(self, f: Any) -> None:
        f.flush()
        os.fsync(f.fileno())
        self.fsyncs += 1
