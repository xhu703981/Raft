"""Start a local cluster, one process per node. Ctrl+C stops it.

python scripts/cluster.py --size 3 --base-port 7000 --data data
"""

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from raftkv.cluster import local_cluster, start_node, wait_for_leader  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--size", type=int, default=3)
    parser.add_argument("--base-port", type=int, default=7000)
    parser.add_argument("--data", default="data")
    args = parser.parse_args()

    cluster = local_cluster(args.size, args.base_port)
    procs = [start_node(i, cluster, args.data) for i in cluster]
    print(f"nodes: {cluster}", flush=True)
    print(f"leader: node {wait_for_leader(cluster)}", flush=True)
    try:
        for proc in procs:
            proc.join()
    except KeyboardInterrupt:
        for proc in procs:
            proc.kill()


if __name__ == "__main__":
    main()
