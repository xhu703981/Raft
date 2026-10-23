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

from raftkv.storage import PersistentState, Record

Message = dict[str, Any]

FOLLOWER, CANDIDATE, LEADER = "follower", "candidate", "leader"
ELECTION_TIMEOUT_MS = (150, 300)
HEARTBEAT_MS = 50


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
        self.writes: list[Record] = []
        self.messages: list[Message] = []
        self.election_deadline = 0.0
        self.heartbeat_due = 0.0
        self.reset_election_timer(now)

    # ---- inputs -------------------------------------------------------------

    def tick(self, now: float) -> None:
        if self.role == LEADER:
            if now >= self.heartbeat_due:
                self.broadcast_heartbeat(now)
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

    def take_ready(self) -> Ready:
        mark = (self.log.last_index, self.log.last_term) if self.writes else None
        ready = Ready(self.writes, self.messages, mark)
        self.writes, self.messages = [], []
        return ready

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
            self.persist_meta()
        self.role = FOLLOWER

    def become_leader(self, now: float) -> None:
        self.role = LEADER
        self.leader_id = self.id
        self.broadcast_heartbeat(now)

    # ---- AppendEntries ------------------------------------------------------

    def broadcast_heartbeat(self, now: float) -> None:
        self.heartbeat_due = now + HEARTBEAT_MS
        for peer in self.peers:
            self.send(peer, "append")

    def on_append(self, msg: Message, now: float) -> None:
        if msg["term"] < self.term:
            self.send(msg["src"], "append_reply", success=False)
            return
        # Same term: a candidate that hears from the winner steps down.
        self.become_follower(msg["term"])
        self.leader_id = msg["src"]
        self.reset_election_timer(now)
        self.send(msg["src"], "append_reply", success=True)

    def on_append_reply(self, msg: Message, now: float) -> None:
        pass

    # ---- helpers ------------------------------------------------------------

    def send(self, dst: int, kind: str, **fields: Any) -> None:
        self.messages.append(
            {"type": kind, "term": self.term, "src": self.id, "dst": dst, **fields}
        )

    def persist_meta(self) -> None:
        self.writes.append({"op": "meta", "term": self.term, "vote": self.voted_for})
