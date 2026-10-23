"""End-to-end tests on real processes and real TCP."""

import socket
import time

import pytest

from raftkv.client import Client
from raftkv.cluster import local_cluster, start_node, status, wait_for_leader


def free_base_port() -> int:
    # Find three consecutive free ports above an ephemeral one.
    while True:
        with socket.socket() as s:
            s.bind(("127.0.0.1", 0))
            base = s.getsockname()[1]
        if base < 60000 and all(status(("127.0.0.1", base + i)) is None for i in (1, 2, 3)):
            return base


@pytest.fixture
def cluster(tmp_path):
    addrs = local_cluster(3, free_base_port())
    procs = {i: start_node(i, addrs, str(tmp_path)) for i in addrs}
    yield addrs, procs, str(tmp_path)
    for proc in procs.values():
        proc.kill()


def test_put_get_delete(cluster):
    addrs, _, _ = cluster
    wait_for_leader(addrs)
    client = Client(addrs)
    client.put("a", "1")
    client.put("b", "2")
    client.delete("a")
    assert client.get("a") is None
    assert client.get("b") == "2"
    client.close()


def test_survives_leader_kill_and_restart(cluster):
    addrs, procs, data_dir = cluster
    leader = wait_for_leader(addrs)
    client = Client(addrs)
    for n in range(20):
        client.put(f"k{n}", str(n))

    procs[leader].kill()
    procs[leader].join()
    client.put("after", "kill")  # retried until a new leader commits it
    assert wait_for_leader(addrs) != leader
    assert [client.get(f"k{n}") for n in range(20)] == [str(n) for n in range(20)]

    # The old leader rejoins from its WAL and catches up.
    procs[leader] = start_node(leader, addrs, data_dir)
    client.put("final", "x")
    target = status(addrs[client.leader])["commit"]
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        info = status(addrs[leader])
        if info and info["commit"] >= target:
            break
        time.sleep(0.05)
    else:
        raise AssertionError("restarted node did not catch up")
    client.close()
