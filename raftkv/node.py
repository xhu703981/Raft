"""Threaded runtime around RaftCore: one node = one process.

Threads in a node:
  - one accept thread, plus one thread per inbound connection
  - one sender thread per peer (transport.Peer)
  - one ticker thread that drives election and heartbeat timers
  - one WAL writer thread (storage.Storage) doing group commit
All Raft and state machine state sits behind the single `self.lock`.
"""

import itertools
import logging
import os
import random
import threading
import time

from raftkv.core import RaftCore, Ready
from raftkv.kv import KVStore
from raftkv.storage import Storage
from raftkv.transport import Address, Message, Peer, Server

RAFT_MESSAGES = {"vote", "vote_reply", "append", "append_reply", "snapshot"}
TICK_SECONDS = 0.01
REQUEST_TIMEOUT = 2.0
SNAPSHOT_EVERY = 1000  # log entries

log = logging.getLogger(__name__)


def now_ms() -> float:
    return time.monotonic() * 1000


class Node:
    def __init__(self, node_id: int, cluster: dict[int, Address], data_dir: str) -> None:
        os.makedirs(data_dir, exist_ok=True)
        self.storage = Storage(os.path.join(data_dir, "wal"))
        state = self.storage.load()
        peer_ids = [p for p in cluster if p != node_id]
        self.lock = threading.Lock()
        # Client handler threads wait on this for entries to apply or reads to finish.
        self.changed = threading.Condition(self.lock)
        self.core = RaftCore(node_id, peer_ids, state, now_ms(), random.Random())
        self.kv = KVStore()
        if state.snapshot:
            self.kv.restore(state.snapshot["data"])
        self.read_ids = itertools.count()
        self.read_results: dict[int, int | None] = {}
        self.peers = {p: Peer(cluster[p]) for p in peer_ids}
        self.server = Server(cluster[node_id], self.handle)
        self.role = self.core.role

    def start(self) -> None:
        self.storage.start()
        for peer in self.peers.values():
            peer.start()
        self.server.start()
        threading.Thread(target=self._tick_loop, name="ticker", daemon=True).start()

    # ---- requests -----------------------------------------------------------

    def handle(self, msg: Message) -> Message | None:
        kind = msg["type"]
        if kind in RAFT_MESSAGES:
            with self.lock:
                self.core.step(msg, now_ms())
                self._flush()
            return None
        if kind == "put":
            return self._write(["put", msg["key"], msg["value"]])
        if kind == "delete":
            return self._write(["delete", msg["key"]])
        if kind == "get":
            return self._read(msg["key"])
        if kind == "status":
            return self._status()
        return {"ok": False, "error": f"unknown request {kind}"}

    def _write(self, command: list) -> Message:
        with self.lock:
            index = self.core.propose(command)
            if index is None:
                return self._not_leader()
            term = self.core.term
            self._flush()
            applied = self.changed.wait_for(
                lambda: self.core.applied_index >= index, REQUEST_TIMEOUT
            )
            # If another leader overwrote our entry, a different term sits at `index`.
            ok = applied and self.core.log.term_at(index) == term
            return {"ok": ok, "leader": self.core.leader_id}

    def _read(self, key: str) -> Message:
        """Read-index: confirm leadership with a majority, then read once applied."""
        with self.lock:
            read_id = next(self.read_ids)
            if not self.core.read_index(read_id):
                return self._not_leader()
            self._flush()

            def ready() -> bool:
                if read_id not in self.read_results:
                    return False
                index = self.read_results[read_id]
                return index is None or self.core.applied_index >= index

            done = self.changed.wait_for(ready, REQUEST_TIMEOUT)
            index = self.read_results.pop(read_id, None)
            if not done or index is None:
                return self._not_leader()
            return {"ok": True, "value": self.kv.get(key)}

    def _not_leader(self) -> Message:
        return {"ok": False, "leader": self.core.leader_id}

    def _status(self) -> Message:
        with self.lock:
            core = self.core
            return {
                "id": core.id,
                "pid": os.getpid(),
                "role": core.role,
                "term": core.term,
                "leader": core.leader_id,
                "commit": core.commit_index,
                "fsyncs": self.storage.fsyncs,
            }

    # ---- driving the core ---------------------------------------------------

    def _tick_loop(self) -> None:
        while True:
            time.sleep(TICK_SECONDS)
            with self.lock:
                self.core.tick(now_ms())
                self._flush()

    def _flush(self) -> None:
        """Apply committed entries and hand pending writes to the WAL.

        Messages go out only once their writes are durable. Even message-only
        batches pass through the WAL queue, so a reply is never sent ahead of an
        earlier, still unsynced write it depends on. Caller holds the lock.
        """
        if self.core.role != self.role:
            self.role = self.core.role
            log.info("%s in term %s", self.role, self.core.term)
        self._apply()
        ready = self.core.take_ready()
        self.read_results.update(ready.reads)
        if ready.reads:
            self.changed.notify_all()
        if ready.writes or ready.messages:
            self.storage.submit(ready.writes, lambda: self._on_durable(ready))

    def _apply(self) -> None:
        restore, entries = self.core.take_committed()
        if restore:
            self.kv.restore(restore["data"])
        for _, entry in entries:
            self.kv.apply(entry.command)
        if restore or entries:
            self.changed.notify_all()
        log_size = len(self.core.log.entries)
        if log_size >= SNAPSHOT_EVERY and self.core.applied_index > self.core.log.base_index:
            self.core.compact(self.core.applied_index, self.kv.snapshot())

    def _on_durable(self, ready: Ready) -> None:
        if ready.mark:
            with self.lock:
                self.core.persisted(*ready.mark)
                self._flush()
        for msg in ready.messages:
            self.peers[msg["dst"]].send(msg)


def run_node(node_id: int, cluster: dict[int, Address], data_dir: str) -> None:
    """Process entry point."""
    logging.basicConfig(level=logging.INFO, format=f"%(asctime)s n{node_id} %(message)s")
    Node(node_id, cluster, data_dir).start()
    threading.Event().wait()
