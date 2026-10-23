"""Helpers to run a local cluster, one multiprocessing.Process per node."""

import multiprocessing
import os
import socket
import time

from raftkv.node import run_node
from raftkv.transport import Address, Message, recv_msg, send_msg


def local_cluster(size: int, base_port: int) -> dict[int, Address]:
    return {i: ("127.0.0.1", base_port + i) for i in range(1, size + 1)}


def start_node(node_id: int, cluster: dict[int, Address], data_dir: str) -> multiprocessing.Process:
    path = os.path.join(data_dir, f"node{node_id}")
    proc = multiprocessing.Process(target=run_node, args=(node_id, cluster, path), daemon=True)
    proc.start()
    return proc


def status(addr: Address) -> Message | None:
    try:
        with socket.create_connection(addr, timeout=0.5) as sock:
            send_msg(sock, {"type": "status"})
            return recv_msg(sock)
    except OSError:
        return None


def wait_for_leader(cluster: dict[int, Address], timeout: float = 10.0) -> int:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        for addr in cluster.values():
            info = status(addr)
            if info and info["role"] == "leader":
                return info["id"]
        time.sleep(0.01)
    raise TimeoutError("no leader elected")
