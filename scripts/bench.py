"""Benchmark a fresh local 3-node cluster: write throughput and leader failover time.

python scripts/bench.py
"""

import argparse
import statistics
import sys
import tempfile
import threading
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from raftkv.client import Client  # noqa: E402
from raftkv.cluster import local_cluster, start_node, status, wait_for_leader  # noqa: E402


def write_load(cluster: dict, clients: int, seconds: float) -> None:
    """Each client thread does sequential puts; report throughput and latency."""
    latencies: list[float] = []
    lock = threading.Lock()
    stop_at = time.monotonic() + seconds

    def worker(n: int) -> None:
        client = Client(cluster)
        mine = []
        while time.monotonic() < stop_at:
            start = time.perf_counter()
            client.put(f"key{n}", "x" * 100)
            mine.append(time.perf_counter() - start)
        client.close()
        with lock:
            latencies.extend(mine)

    leader = cluster[wait_for_leader(cluster)]
    fsyncs_before = status(leader)["fsyncs"]
    threads = [threading.Thread(target=worker, args=(n,)) for n in range(clients)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    fsyncs = status(leader)["fsyncs"] - fsyncs_before

    latencies.sort()
    ms = [x * 1000 for x in latencies]
    print(
        f"{clients:>3} clients: {len(ms) / seconds:7.0f} writes/s  "
        f"p50 {ms[len(ms) // 2]:5.1f} ms  p99 {ms[int(len(ms) * 0.99)]:6.1f} ms  "
        f"{len(ms) / fsyncs:5.1f} writes per leader fsync"
    )


def failover(cluster: dict, procs: dict, data_dir: str, trials: int) -> None:
    """Kill the leader; time until a new leader exists and until a write succeeds."""
    elected, writable = [], []
    client = Client(cluster, timeout=30)
    for _ in range(trials):
        client.put("warmup", "x")
        old = wait_for_leader(cluster)
        procs[old].kill()
        start = time.monotonic()
        while True:
            infos = [status(addr) for i, addr in cluster.items() if i != old]
            if any(info and info["role"] == "leader" for info in infos):
                break
            time.sleep(0.005)
        elected.append(time.monotonic() - start)
        client.put("after-failover", "x")
        writable.append(time.monotonic() - start)
        procs[old].join()
        procs[old] = start_node(old, cluster, data_dir)
        time.sleep(1.0)  # let it rejoin before the next kill
    client.close()

    for name, xs in (("new leader elected", elected), ("first write accepted", writable)):
        ms = sorted(x * 1000 for x in xs)
        print(
            f"{name:>21}: median {statistics.median(ms):5.0f} ms  "
            f"min {ms[0]:5.0f} ms  max {ms[-1]:5.0f} ms  ({trials} kills)"
        )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-port", type=int, default=7100)
    parser.add_argument("--seconds", type=float, default=5)
    parser.add_argument("--failovers", type=int, default=10)
    args = parser.parse_args()

    data_dir = tempfile.mkdtemp(prefix="raftkv-bench-")
    cluster = local_cluster(3, args.base_port)
    procs = {i: start_node(i, cluster, data_dir) for i in cluster}
    try:
        wait_for_leader(cluster)
        print("write throughput (100-byte values, every write fsynced on a majority):")
        for clients in (1, 4, 16, 64):
            write_load(cluster, clients, args.seconds)
        print("leader failover:")
        failover(cluster, procs, data_dir, args.failovers)
    finally:
        for proc in procs.values():
            proc.kill()


if __name__ == "__main__":
    main()
