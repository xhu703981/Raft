# RaftKV

A small Raft-replicated key-value store in pure Python (3.11+, standard library only).
It implements Raft §5 (election, log replication, commit rule), snapshots with
InstallSnapshot, a WAL with group commit, and read-index linearizable reads.
The code is about 850 lines, not counting tests.

## Architecture

```
          Client (get / put / delete)
                 | TCP
                 v
+------------------- node process -------------------+
|  connection threads --> RaftCore + KVStore         |
|                         (one threading.Lock)       |
|                              |                     |
|  WAL writer thread:  queue -> batch -> one fsync   |
|                              |                     |
|  per-peer sender threads ---------- TCP ---------> peers
|  ticker thread: election timeout / heartbeats      |
+----------------------------------------------------+
frame = 4-byte big-endian length + JSON body
```

`core.py` is the Raft logic as a pure state machine: messages and time come in,
and records to persist plus messages to send come out. `node.py` connects it to
threads, sockets and the disk.

## How to run

```
python scripts/cluster.py        # start a local 3-node cluster (ports 7001-7003)
pytest -q                        # run all tests
python scripts/bench.py          # run the benchmark
```

```python
from raftkv.client import Client
from raftkv.cluster import local_cluster

c = Client(local_cluster(3, 7000))
c.put("a", "1")
c.get("a")
c.delete("a")
```

## Tests

- Unit tests cover the log, the WAL and crash recovery, framing, elections,
  replication, snapshots and read-index.
- `test_cluster.py` runs real processes: it kills the leader, restarts it, and
  checks that it catches up.
- `test_fault_injection.py` runs 300 seeded random scenarios (about 3 s): leader
  kills, partitions, message loss, delay spikes, slow disks. It asserts that a
  committed entry is never lost. The scenarios run on a deterministic simulation
  of the same `RaftCore` (`tests/sim.py`), because real processes could not fit
  300 scenarios in 60 s or be reproduced from a seed.
- `tests/mutation_check.py` injects 7 classic Raft bugs. The fault suite catches
  6 of them. The 7th, "leader counts itself before fsync", cannot happen with
  more than one node in this design; a unit test catches it.

## Results

Measured on one laptop (Core Ultra 9 275HX, NVMe SSD, Windows 11), with the
3-node cluster and the client on the same machine:

| Clients | Writes/s | p50 | p99 | Writes per fsync |
|---:|---:|---:|---:|---:|
| 1 | 633 | 1.5 ms | 2.6 ms | 1.0 |
| 4 | 1433 | 2.7 ms | 4.5 ms | 1.8 |
| 16 | 2494 | 5.6 ms | 14.5 ms | 3.9 |
| 64 | 3518 | 16.4 ms | 40.9 ms | 8.3 |

Leader failover (10 kills): a new leader was elected in 438 ms (median,
range 187-1234 ms), and the first write was accepted at 523 ms (median).
Numbers vary by about 25% between runs.

## Design trade-offs

- **One lock per node** instead of fine-grained locks: simpler, at the cost of concurrency.
- **Raft messages are one-way**, and replies are messages too. No thread blocks
  waiting for a peer.
- **Every outgoing message goes through the WAL queue**, so nothing is sent
  before the writes it depends on are fsynced.
- **The WAL is JSON lines.** A snapshot rewrites the WAL with one atomic rename.
- **No pipelining**: one AppendEntries in flight per follower. Snapshots go out
  in one message.

## Why processes for nodes and threads for RPC?

Nodes are processes because killing a process is a real crash: only what was
fsynced survives. Each process also has its own GIL. Inside a node the work is
I/O-bound, and blocking threads are the simplest model for it. A dead peer only
blocks its own sender thread. asyncio would need an executor for fsync anyway,
and one blocking call would freeze the node's timers.

## What does the GIL do here?

Only one thread per node runs Python code at a time. The leader's CPU is the
throughput limit: about 125% of one core at 64 clients, while the followers sat
around 30%. Threads still help because socket I/O and `fsync` release the GIL.
While one fsync runs, other threads queue more writes, which is why group commit
reaches about 8 writes per fsync. The GIL does not make multi-step updates
atomic, so the lock is still needed.

## Known simplifications

- No membership changes, no PreVote/CheckQuorum.
- No client request deduplication: a retried write can be applied twice.
- Snapshots are not chunked, and compaction rewrites the whole WAL.
- No directory fsync after rename.
- The fault-injection tests run on the simulation, not on real sockets or threads.
- No backpressure, no TLS or authentication.
