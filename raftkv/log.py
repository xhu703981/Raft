"""In-memory Raft log whose prefix may have been replaced by a snapshot."""

from typing import Any, NamedTuple


class Entry(NamedTuple):
    term: int
    command: Any


class RaftLog:
    """Log entries after `base_index`, the last index covered by the snapshot.

    Indexes are 1-based as in the paper; index 0 with term 0 is the empty log.
    """

    def __init__(self, base_index: int = 0, base_term: int = 0) -> None:
        self.base_index = base_index
        self.base_term = base_term
        self.entries: list[Entry] = []

    @property
    def last_index(self) -> int:
        return self.base_index + len(self.entries)

    @property
    def last_term(self) -> int:
        return self.entries[-1].term if self.entries else self.base_term

    def term_at(self, index: int) -> int | None:
        """Term of the entry at `index`, or None if it is compacted away or missing."""
        if index == self.base_index:
            return self.base_term
        if self.base_index < index <= self.last_index:
            return self.entries[index - self.base_index - 1].term
        return None

    def get(self, index: int) -> Entry:
        return self.entries[index - self.base_index - 1]

    def slice(self, start: int, limit: int) -> list[Entry]:
        offset = start - self.base_index - 1
        return self.entries[offset : offset + limit]

    def append(self, entry: Entry) -> int:
        self.entries.append(entry)
        return self.last_index

    def write(self, index: int, entries: list[Entry]) -> None:
        """Place `entries` starting at `index`, discarding everything after them."""
        skip = max(0, self.base_index + 1 - index)
        index += skip
        del self.entries[index - self.base_index - 1 :]
        self.entries.extend(entries[skip:])

    def merge(self, prev_index: int, entries: list[Entry]) -> int | None:
        """Add a leader's entries after `prev_index`; return the first index that changed.

        Truncating only on a real term conflict matters: a delayed, shorter
        AppendEntries must not erase entries we already acknowledged.
        """
        for offset, entry in enumerate(entries):
            index = prev_index + 1 + offset
            if index > self.base_index and self.term_at(index) != entry.term:
                self.write(index, entries[offset:])
                return index
        return None

    def compact(self, index: int) -> None:
        """Drop entries up to and including `index` (now covered by a snapshot)."""
        term = self.term_at(index)
        assert term is not None
        del self.entries[: index - self.base_index]
        self.base_index, self.base_term = index, term

    def reset(self, index: int, term: int) -> None:
        self.base_index, self.base_term = index, term
        self.entries = []
