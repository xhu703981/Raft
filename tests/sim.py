"""Deterministic in-memory cluster for testing RaftCore.

Time is a logical millisecond counter. The network and the disks are queues
with seeded random delays, so a seed reproduces a run exactly. A crash throws
away the node object and anything it had not yet "fsynced"; a restart rebuilds
the node from its simulated disk with the same replay() the real WAL uses.
"""

import heapq
import random

from raftkv.core import LEADER, RaftCore, Ready
from raftkv.log import Entry
from raftkv.storage import Record, replay

TICK_MS = 10


class SimCluster:
    def __init__(self, size: int = 3, seed: int = 0) -> None:
        self.rng = random.Random(seed)
        self.ids = list(range(1, size + 1))
        self.now = 0
        self.nodes: dict[int, RaftCore | None] = {}
        self.disks: dict[int, list[Record]] = {i: [] for i in self.ids}
        self.disk_queue: dict[int, list[tuple[int, Ready]]] = {i: [] for i in self.ids}
        self.network: list[tuple[int, int, dict]] = []
        self.msg_seq = 0
        self.groups: list[set[int]] | None = None
        self.net_delay = (1, 5)
        self.disk_delay = (0, 2)
        self.drop_rate = 0.0
        self.leaders_by_term: dict[int, int] = {}
        # Every entry any node has ever seen committed, by index.
        self.committed: dict[int, Entry] = {}
        self.applied: dict[int, list[tuple[int, object]]] = {}
        for i in self.ids:
            self.start(i)

    # ---- faults -------------------------------------------------------------

    def start(self, i: int) -> None:
        peers = [p for p in self.ids if p != i]
        state = replay(self.disks[i])
        self.nodes[i] = RaftCore(i, peers, state, self.now, random.Random(self.rng.random()))
        self.applied[i] = []

    def crash(self, i: int) -> None:
        self.nodes[i] = None
        self.disk_queue[i] = []

    def partition(self, *groups: list[int]) -> None:
        self.groups = [set(g) for g in groups]

    def heal(self) -> None:
        self.groups = None

    def connected(self, a: int, b: int) -> bool:
        return self.groups is None or any(a in g and b in g for g in self.groups)

    # ---- driving ------------------------------------------------------------

    def run(self, ms: int) -> None:
        for _ in range(ms):
            self.now += 1
            self._deliver()
            self._sync_disks()
            if self.now % TICK_MS == 0:
                for i, node in self.nodes.items():
                    if node:
                        node.tick(self.now)
                        self._collect(i)

    def run_until(self, predicate, limit_ms: int = 5000) -> bool:
        for _ in range(limit_ms):
            if predicate():
                return True
            self.run(1)
        return predicate()

    def propose(self, command: object) -> int | None:
        leader = self.leader()
        if leader is None:
            return None
        index = leader.propose(command)
        self._collect(leader.id)
        return index

    def commands(self, i: int) -> list[object]:
        """Client commands node i has applied, in order (no-ops left out)."""
        return [c for _, c in self.applied[i] if c is not None]

    def leader(self) -> RaftCore | None:
        live = [n for n in self.nodes.values() if n and n.role == LEADER]
        return max(live, key=lambda n: n.term) if live else None

    def live(self) -> list[RaftCore]:
        return [n for n in self.nodes.values() if n]

    # ---- internals ----------------------------------------------------------

    def _deliver(self) -> None:
        while self.network and self.network[0][0] <= self.now:
            msg = heapq.heappop(self.network)[2]
            node = self.nodes[msg["dst"]]
            if node and self.connected(msg["src"], msg["dst"]):
                node.step(msg, self.now)
                self._collect(msg["dst"])

    def _collect(self, i: int) -> None:
        node = self.nodes[i]
        self._check(node)
        ready = node.take_ready()
        if ready.writes or ready.messages:
            queue = self.disk_queue[i]
            due = self.now + self.rng.randint(*self.disk_delay)
            if queue:
                due = max(due, queue[-1][0])  # the WAL is FIFO
            queue.append((due, ready))

    def _sync_disks(self) -> None:
        for i, queue in self.disk_queue.items():
            while queue and queue[0][0] <= self.now:
                ready = queue.pop(0)[1]
                self._persist(i, ready.writes)
                if ready.mark:
                    self.nodes[i].persisted(*ready.mark)
                    self._collect(i)
                for msg in ready.messages:
                    if self.rng.random() >= self.drop_rate:
                        self.msg_seq += 1
                        due = self.now + self.rng.randint(*self.net_delay)
                        heapq.heappush(self.network, (due, self.msg_seq, msg))

    def _persist(self, i: int, writes: list[Record]) -> None:
        for record in writes:
            if record["op"] == "compact":
                self.disks[i] = list(record["records"])
            else:
                self.disks[i].append(record)

    def _check(self, node: RaftCore) -> None:
        if node.role == LEADER and node.term not in self.leaders_by_term:
            self.leaders_by_term[node.term] = node.id
            self._check_leader_completeness(node)
        winner = self.leaders_by_term.get(node.term)
        assert node.role != LEADER or winner == node.id, f"two leaders in term {node.term}"
        for index, entry in node.take_committed():
            seen = self.committed.setdefault(index, entry)
            assert seen == entry, f"index {index} committed as {seen} and {entry}"
            self.applied[node.id].append((index, entry.command))

    def _check_leader_completeness(self, node: RaftCore) -> None:
        for index, entry in self.committed.items():
            if index > node.log.base_index:
                assert node.log.term_at(index) == entry.term, f"new leader lost {index}"
                assert node.log.get(index) == entry
