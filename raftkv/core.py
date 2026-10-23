"""Raft consensus (paper section 5) as a deterministic state machine.

RaftCore does no I/O and never reads a clock. The caller passes in messages
and the current time, then calls take_ready() to collect the records that must
be made durable and the messages that may be sent only after that. Because of
this split, the same code runs in the threaded server and in the simulated
fault-injection tests.
"""

import random
from dataclasses import dataclass
from typing import Any

from raftkv.log import Entry
from raftkv.storage import PersistentState, Record

Message = dict[str, Any]

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"
ELECTION_TIMEOUT_MS = (150, 300)
HEARTBEAT_MS = 50
MAX_BATCH = 100  # entries per AppendEntries


@dataclass
class Ready:
    writes: list[Record]
    messages: list[Message]
    # Once `writes` are durable, the log is durable up to this (index, term).
    mark: tuple[int, int] | None


class RaftCore:
    def __init__(
        self,
        node_id: int,
        peers: list[int],
        state: PersistentState,
        now: float,
        rng: random.Random,
    ) -> None:
        self.id = node_id
        self.peers = peers
        self.majority = (len(peers) + 1) // 2 + 1
        self.rng = rng
        self.term = state.term
        self.voted_for = state.vote
        self.log = state.log
        self.role = FOLLOWER
        self.leader_id: int | None = None
        self.votes: set[int] = set()
        self.commit_index = self.log.base_index
        self.applied_index = self.log.base_index
        # Everything loaded from disk is durable by definition.
        self.durable_index = self.log.last_index
        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}
        self.inflight: dict[int, bool] = {}
        self.writes: list[Record] = []
        self.messages: list[Message] = []
        self.election_deadline = 0.0
        self.heartbeat_due = 0.0
        self.reset_election_timer(now)

    # ---- inputs -------------------------------------------------------------

    def tick(self, now: float) -> None:
        if self.role == LEADER:
            if now >= self.heartbeat_due:
                self.broadcast_append(now)
        elif now >= self.election_deadline:
            self.start_election(now)

    def step(self, msg: Message, now: float) -> None:
        if msg["term"] > self.term:
            self.become_follower(msg["term"])
        handlers = {
            "vote": self.on_vote,
            "vote_reply": self.on_vote_reply,
            "append": self.on_append,
            "append_reply": self.on_append_reply,
        }
        handlers[msg["type"]](msg, now)

    def propose(self, command: Any) -> int | None:
        """Append a client command; returns its log index, or None if not leader."""
        if self.role != LEADER:
            return None
        index = self.log.append(Entry(self.term, command))
        self.persist_entries(index)
        for peer in self.peers:
            if not self.inflight[peer]:
                self.send_append(peer)
        return index

    def persisted(self, index: int, term: int) -> None:
        """The WAL has fsynced the log up to (index, term)."""
        # The term check skips marks for entries that were overwritten meanwhile.
        if self.log.term_at(index) == term and index > self.durable_index:
            self.durable_index = index
            if self.role == LEADER:
                self.advance_commit()

    # ---- outputs ------------------------------------------------------------

    def take_ready(self) -> Ready:
        mark = (self.log.last_index, self.log.last_term) if self.writes else None
        ready = Ready(self.writes, self.messages, mark)
        self.writes, self.messages = [], []
        return ready

    def take_committed(self) -> list[tuple[int, Entry]]:
        """Committed entries not yet handed to the state machine."""
        start = self.applied_index + 1
        entries = self.log.slice(start, self.commit_index - self.applied_index)
        self.applied_index = self.commit_index
        return list(enumerate(entries, start))

    # ---- elections ----------------------------------------------------------

    def reset_election_timer(self, now: float) -> None:
        # Randomized so that split votes do not repeat forever.
        self.election_deadline = now + self.rng.uniform(*ELECTION_TIMEOUT_MS)

    def start_election(self, now: float) -> None:
        self.role = CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.leader_id = None
        self.persist_meta()
        self.votes = {self.id}
        self.reset_election_timer(now)
        for peer in self.peers:
            self.send(peer, "vote", last_index=self.log.last_index, last_term=self.log.last_term)
        self.check_votes(now)

    def on_vote(self, msg: Message, now: float) -> None:
        mine = (self.log.last_term, self.log.last_index)
        up_to_date = (msg["last_term"], msg["last_index"]) >= mine
        granted = msg["term"] == self.term and self.voted_for in (None, msg["src"]) and up_to_date
        if granted:
            self.voted_for = msg["src"]
            self.persist_meta()
            self.reset_election_timer(now)
        self.send(msg["src"], "vote_reply", granted=granted)

    def on_vote_reply(self, msg: Message, now: float) -> None:
        if self.role == CANDIDATE and msg["term"] == self.term and msg["granted"]:
            self.votes.add(msg["src"])
            self.check_votes(now)

    def check_votes(self, now: float) -> None:
        if len(self.votes) >= self.majority:
            self.become_leader(now)

    def become_follower(self, term: int) -> None:
        if term > self.term:
            self.term = term
            self.voted_for = None
            self.leader_id = None
            self.persist_meta()
        self.role = FOLLOWER

    def become_leader(self, now: float) -> None:
        self.role = LEADER
        self.leader_id = self.id
        self.next_index = {p: self.log.last_index + 1 for p in self.peers}
        self.match_index = {p: 0 for p in self.peers}
        self.inflight = {p: False for p in self.peers}
        # A no-op from the new term lets earlier entries commit (paper 5.4.2).
        self.persist_entries(self.log.append(Entry(self.term, None)))
        self.broadcast_append(now)

    # ---- replication --------------------------------------------------------

    def broadcast_append(self, now: float) -> None:
        self.heartbeat_due = now + HEARTBEAT_MS
        for peer in self.peers:
            self.send_append(peer)

    def send_append(self, peer: int) -> None:
        prev = self.next_index[peer] - 1
        self.inflight[peer] = True
        self.send(
            peer,
            "append",
            prev_index=prev,
            prev_term=self.log.term_at(prev),
            entries=self.log.slice(prev + 1, MAX_BATCH),
            commit=self.commit_index,
        )

    def on_append(self, msg: Message, now: float) -> None:
        if msg["term"] < self.term:
            self.send(msg["src"], "append_reply", success=False, match=0)
            return
        # Same term: a candidate that hears from the winner steps down.
        self.become_follower(msg["term"])
        self.leader_id = msg["src"]
        self.reset_election_timer(now)

        prev = msg["prev_index"]
        if self.log.term_at(prev) != msg["prev_term"]:
            # Tell the leader where to retry: right before the mismatch.
            hint = min(prev - 1, self.log.last_index)
            self.send(msg["src"], "append_reply", success=False, match=hint)
            return
        entries = [Entry(*e) for e in msg["entries"]]
        changed = self.log.merge(prev, entries)
        if changed is not None:
            self.persist_entries(changed)
        match = prev + len(entries)
        # Only up to `match`: entries past it may still be stale leftovers.
        self.commit_index = max(self.commit_index, min(msg["commit"], match))
        self.send(msg["src"], "append_reply", success=True, match=match)

    def on_append_reply(self, msg: Message, now: float) -> None:
        if self.role != LEADER or msg["term"] != self.term:
            return
        peer = msg["src"]
        self.inflight[peer] = False
        if msg["success"]:
            self.match_index[peer] = max(self.match_index[peer], msg["match"])
            self.next_index[peer] = self.match_index[peer] + 1
            self.advance_commit()
            if self.next_index[peer] <= self.log.last_index:
                self.send_append(peer)
        else:
            self.next_index[peer] = max(1, msg["match"] + 1)
            self.send_append(peer)

    def advance_commit(self) -> None:
        for index in range(self.log.last_index, self.commit_index, -1):
            # Only current-term entries are committed by counting (paper 5.4.2);
            # older ones then commit indirectly as their prefix.
            if self.log.term_at(index) != self.term:
                return
            acks = (self.durable_index >= index) + sum(
                self.match_index[p] >= index for p in self.peers
            )
            if acks >= self.majority:
                self.commit_index = index
                return

    # ---- helpers ------------------------------------------------------------

    def send(self, dst: int, kind: str, **fields: Any) -> None:
        self.messages.append(
            {"type": kind, "term": self.term, "src": self.id, "dst": dst, **fields}
        )

    def persist_meta(self) -> None:
        self.writes.append({"op": "meta", "term": self.term, "vote": self.voted_for})

    def persist_entries(self, index: int) -> None:
        """Queue the log from `index` on for the WAL (this also truncates on replay)."""
        entries = self.log.slice(index, self.log.last_index)
        self.writes.append({"op": "entries", "index": index, "entries": entries})
        self.durable_index = min(self.durable_index, index - 1)
