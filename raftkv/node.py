"""Threaded runtime around RaftCore: one node = one process.

Threads in a node:
  - one accept thread, plus one thread per inbound connection
  - one sender thread per peer (transport.Peer)
  - one ticker thread that drives election and heartbeat timers
  - one WAL writer thread (storage.Storage) doing group commit
All Raft state sits behind `self.lock`.
"""

import logging
import os
import random
import threading
import time

from raftkv.core import RaftCore, Ready
from raftkv.storage import Storage
from raftkv.transport import Address, Message, Peer, Server

RAFT_MESSAGES = {"vote", "vote_reply", "append", "append_reply", "snapshot"}
TICK_SECONDS = 0.01

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
        self.core = RaftCore(node_id, peer_ids, state, now_ms(), random.Random())
        self.peers = {p: Peer(cluster[p]) for p in peer_ids}
        self.server = Server(cluster[node_id], self.handle)
        self.role = self.core.role

    def start(self) -> None:
        self.storage.start()
        for peer in self.peers.values():
            peer.start()
        self.server.start()
        threading.Thread(target=self._tick_loop, name="ticker", daemon=True).start()

    def handle(self, msg: Message) -> Message | None:
        if msg["type"] in RAFT_MESSAGES:
            with self.lock:
                self.core.step(msg, now_ms())
                self._flush()
            return None
        if msg["type"] == "status":
            return self.status()
        return {"ok": False, "error": f"unknown request {msg['type']}"}

    def status(self) -> Message:
        with self.lock:
            core = self.core
            return {"id": core.id, "role": core.role, "term": core.term, "leader": core.leader_id}

    def _tick_loop(self) -> None:
        while True:
            time.sleep(TICK_SECONDS)
            with self.lock:
                self.core.tick(now_ms())
                self._flush()

    def _flush(self) -> None:
        """Hand pending writes to the WAL; their messages go out only once durable.

        Even message-only batches pass through the WAL queue, so that a reply is
        never sent ahead of an earlier, still unsynced write it depends on.
        """
        if self.core.role != self.role:
            self.role = self.core.role
            log.info("%s in term %s", self.role, self.core.term)
        ready = self.core.take_ready()
        if ready.writes or ready.messages:
            self.storage.submit(ready.writes, lambda: self._on_durable(ready))

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
