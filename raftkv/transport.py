"""TCP transport: each frame is a 4-byte big-endian length followed by a JSON body."""

import json
import logging
import queue
import socket
import struct
import threading
from collections.abc import Callable
from typing import Any

Message = dict[str, Any]
Address = tuple[str, int]
HEADER = struct.Struct(">I")

log = logging.getLogger(__name__)


def send_msg(sock: socket.socket, msg: Message) -> None:
    body = json.dumps(msg, separators=(",", ":")).encode()
    sock.sendall(HEADER.pack(len(body)) + body)


def recv_msg(sock: socket.socket) -> Message | None:
    """Read one frame; None means the peer closed the connection."""
    header = _recv_exact(sock, HEADER.size)
    if header is None:
        return None
    body = _recv_exact(sock, HEADER.unpack(header)[0])
    return None if body is None else json.loads(body)


def _recv_exact(sock: socket.socket, size: int) -> bytes | None:
    # TCP is a byte stream: one recv() may return part of a frame.
    buf = bytearray()
    while len(buf) < size:
        chunk = sock.recv(size - len(buf))
        if not chunk:
            return None
        buf += chunk
    return bytes(buf)


class Server:
    """Accepts connections; each connection is served by its own thread.

    `handler` returns a reply to send back, or None for one-way messages.
    """

    def __init__(self, addr: Address, handler: Callable[[Message], Message | None]) -> None:
        self.addr = addr
        self.handler = handler
        self.sock: socket.socket | None = None

    def start(self) -> None:
        self.sock = socket.create_server(self.addr)
        threading.Thread(target=self._accept, name="accept", daemon=True).start()

    def stop(self) -> None:
        if self.sock:
            self.sock.close()

    def _accept(self) -> None:
        assert self.sock is not None
        while True:
            try:
                conn, _ = self.sock.accept()
            except OSError:
                return
            threading.Thread(target=self._serve, args=(conn,), daemon=True).start()

    def _serve(self, conn: socket.socket) -> None:
        conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        try:
            with conn:
                while (msg := recv_msg(conn)) is not None:
                    reply = self.handler(msg)
                    if reply is not None:
                        send_msg(conn, reply)
        except OSError:
            pass


class Peer:
    """One-way channel to one peer, drained by a dedicated sender thread.

    Messages are dropped when the peer is unreachable. Raft tolerates loss, and
    the leader's next heartbeat carries whatever the peer still needs.
    """

    CONNECT_TIMEOUT = 0.2

    def __init__(self, addr: Address) -> None:
        self.addr = addr
        self.queue: queue.Queue[Message] = queue.Queue()
        self.sock: socket.socket | None = None

    def start(self) -> None:
        threading.Thread(target=self._run, name=f"peer-{self.addr[1]}", daemon=True).start()

    def send(self, msg: Message) -> None:
        self.queue.put(msg)

    def _run(self) -> None:
        while True:
            msg = self.queue.get()
            try:
                if self.sock is None:
                    self.sock = socket.create_connection(self.addr, self.CONNECT_TIMEOUT)
                    self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                send_msg(self.sock, msg)
            except OSError as exc:
                log.debug("send to %s failed: %s", self.addr, exc)
                self._reset()

    def _reset(self) -> None:
        if self.sock:
            self.sock.close()
        self.sock = None
        # Whatever piled up while the peer was down is stale; drop it.
        while not self.queue.empty():
            self.queue.get_nowait()
