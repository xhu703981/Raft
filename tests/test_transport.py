import queue
import socket
import threading
import time

from raftkv.transport import HEADER, Peer, Server, recv_msg, send_msg


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_frame_roundtrip():
    a, b = socket.socketpair()
    with a, b:
        send_msg(a, {"type": "x", "n": 1})
        send_msg(a, {"big": "y" * 1_000_000})
        assert recv_msg(b) == {"type": "x", "n": 1}
        assert len(recv_msg(b)["big"]) == 1_000_000


def test_frame_split_across_reads():
    a, b = socket.socketpair()
    body = b'{"k":"v"}'
    frame = HEADER.pack(len(body)) + body

    def dribble():
        for i in range(len(frame)):
            a.sendall(frame[i : i + 1])
            time.sleep(0.001)

    with a, b:
        threading.Thread(target=dribble).start()
        assert recv_msg(b) == {"k": "v"}


def test_recv_returns_none_on_close():
    a, b = socket.socketpair()
    a.close()
    with b:
        assert recv_msg(b) is None


def test_server_request_reply():
    addr = ("127.0.0.1", free_port())
    server = Server(addr, lambda msg: {"echo": msg["n"] + 1})
    server.start()
    try:
        with socket.create_connection(addr) as sock:
            for n in range(3):
                send_msg(sock, {"n": n})
                assert recv_msg(sock) == {"echo": n + 1}
    finally:
        server.stop()


def test_peer_delivers_and_reconnects():
    addr = ("127.0.0.1", free_port())
    received: queue.Queue = queue.Queue()

    def handler(msg):
        received.put(msg)

    server = Server(addr, handler)
    server.start()
    peer = Peer(addr)
    peer.start()
    peer.send({"n": 1})
    assert received.get(timeout=2) == {"n": 1}

    # Kill the connection from the peer's side; the next sends must reconnect.
    peer.sock.close()
    deadline = time.time() + 3
    while time.time() < deadline:
        peer.send({"n": 2})
        try:
            assert received.get(timeout=0.1) == {"n": 2}
            break
        except queue.Empty:
            pass
    else:
        raise AssertionError("peer never reconnected")
    server.stop()
