"""Client API: finds the leader and retries until the request succeeds."""

import socket
import time
from typing import Any

from raftkv.transport import Address, Message, recv_msg, send_msg

RETRY_PAUSE = 0.05


class Client:
    def __init__(self, cluster: dict[int, Address], timeout: float = 10.0) -> None:
        self.cluster = cluster
        self.timeout = timeout
        self.leader = next(iter(cluster))
        self.socks: dict[int, socket.socket] = {}

    def get(self, key: str) -> str | None:
        return self._call({"type": "get", "key": key})["value"]

    def put(self, key: str, value: str) -> None:
        self._call({"type": "put", "key": key, "value": value})

    def delete(self, key: str) -> None:
        self._call({"type": "delete", "key": key})

    def close(self) -> None:
        for sock in self.socks.values():
            sock.close()
        self.socks.clear()

    def _call(self, request: Message) -> Message:
        # Retrying can apply a put/delete twice, which is harmless: both are idempotent.
        deadline = time.monotonic() + self.timeout
        while time.monotonic() < deadline:
            reply = self._send(self.leader, request)
            if reply and reply["ok"]:
                return reply
            hint = reply.get("leader") if reply else None
            if hint is not None and hint != self.leader:
                self.leader = hint
            else:
                self.leader = self._next_node(self.leader)
                time.sleep(RETRY_PAUSE)
        raise TimeoutError(f"no leader answered {request['type']} within {self.timeout}s")

    def _next_node(self, node: int) -> int:
        ids = list(self.cluster)
        return ids[(ids.index(node) + 1) % len(ids)]

    def _send(self, node: int, request: Message) -> dict[str, Any] | None:
        try:
            if node not in self.socks:
                self.socks[node] = socket.create_connection(self.cluster[node], timeout=3)
            send_msg(self.socks[node], request)
            reply = recv_msg(self.socks[node])
            if reply is None:
                raise OSError("connection closed")
            return reply
        except OSError:
            sock = self.socks.pop(node, None)
            if sock:
                sock.close()
            return None
