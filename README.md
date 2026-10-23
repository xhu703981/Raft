# RaftKV

A Raft-replicated key-value store written from scratch in Python 3.11+, using
only the standard library. It implements Raft from section 5 of the paper (leader
election, log replication, the commit rule) plus snapshots/InstallSnapshot,
a write-ahead log with group commit, and linearizable reads via read-index.

The goal is readability, not production use: 844 lines of code (excluding
tests, comments and docstrings), short typed functions, and tests that try
hard to break it.

## Architecture

```
                 client.Client  (get / put / delete, follows leader hints)
                       |  request/reply over TCP
                       v
 +-------------------------------- node process ---------------------------------+
 |                                                                                |
 |  accept thread --> one thread per inbound connection (clients and peers)      |
 |                          |                       ^                             |
 |                          | step(msg)             | Condition.wait until        |
 |                          v                       | applied / read confirmed    |
 |   +--------------------- one threading.Lock ----------------------------+     |
 |   |  RaftCore  (core.py: pure state machine, no I/O, no clock)          |     |
 |   |    tick(now) . step(msg) . propose(cmd) . read_index(id)            |     |
 |   |    take_ready() -> records to persist + messages to send            |     |
 |   |    take_committed() -> entries / snapshot  ---------->  KVStore     |     |
 |   +---------------------------|-----------------------------  (kv.py)   |     |
 |                               v                                               |
 |   WAL writer thread:  queue.Queue --> write batch --> ONE fsync               |
 |                               | (only after the fsync)                        |
 |                               v                                               |
 |   one sender thread per peer (transport.Peer) ---- one-way frames ----> peers |
 |   ticker thread: core.tick() every 10 ms  (election timeout / heartbeats)     |
 +--------------------------------------------------------------------------------+

 wire format:  [4-byte big-endian length][JSON body]
 WAL format:   one JSON record per line: meta(term, vote) | entries(index, ...) | snapshot
```

| File | Code lines | What it does |
|---|---:|---|
| `raftkv/core.py` | 273 | Raft: elections, AppendEntries, commit rule, InstallSnapshot, read-index |
| `raftkv/node.py` | 136 | Threads, the lock, client request handling, applying to the state machine |
| `raftkv/storage.py` | 94 | WAL, crash recovery (torn-tail cut), group-commit writer thread |
| `raftkv/transport.py` | 85 | Length-prefixed framing, server, per-peer sender |
| `raftkv/client.py` | 51 | `Client.get/put/delete` with leader redirect and retry |
| `raftkv/log.py` | 49 | In-memory log with a compacted prefix |
| `raftkv/cluster.py` | 29 | Start node processes, query status |
| `raftkv/kv.py` | 18 | The state machine (a dict), knows nothing about Raft |
| `scripts/` | 109 | `cluster.py` (start a cluster), `bench.py` (benchmark) |

## How to run

No runtime dependencies. For development: `pip install pytest ruff`.

**Start a 3-node cluster** (one process per node, data in `./data`):

```
python scripts/cluster.py              # --size 5, --base-port 7000, --data data
```

**Talk to it** from another terminal:

```python
from raftkv.client import Client
from raftkv.cluster import local_cluster

c = Client(local_cluster(3, 7000))
c.put("lang", "python")
c.get("lang")  # 'python', a linearizable read through the leader
c.delete("lang")
```

**Tests** (345 tests, about 14 s; the 300 fault-injection scenarios take about 3 s):

```
pytest -q
pytest -q tests/test_fault_injection.py -k "seed_42]"   # replay one scenario
python tests/mutation_check.py                           # does the fault suite catch bugs?
```

**Benchmark** (starts its own fresh cluster on ports 7101-7103):

```
python scripts/bench.py
```

## Testing

| Test file | What it covers |
|---|---|
| `test_log.py` | append, term lookup, truncation only on a real conflict, compaction |
| `test_storage.py` | persist + recover, torn last line after a crash, group commit (100 writes, 1 fsync), WAL rewrite |
| `test_transport.py` | framing, frames split across reads, reconnect |
| `test_election.py` | one leader, election safety over 30 lossy seeds, re-election after a crash, minority partition, stale-log candidate rejected, vote survives restart |
| `test_replication.py` | replication, catch-up, isolated leader's entries overwritten, Figure 8 commit rule, leader counts itself only after fsync |
| `test_snapshot.py` | compaction, InstallSnapshot to a lagging follower, whole-cluster restart from snapshots |
| `test_read_index.py` | reads wait for a majority; a deposed leader cannot serve a stale read |
| `test_cluster.py` | real processes and TCP: put/get/delete, kill the leader, restart it, catch up |
| `test_fault_injection.py` | 300 seeded random scenarios, described below |

**Fault injection.** The 300 scenarios run on `tests/sim.py`, a deterministic
simulation that drives the same `RaftCore` used in production. It replaces
the network and disks with seeded queues and replaces time with a logical
millisecond counter. Each scenario picks 3 or 5 nodes, a message loss rate
(0/5/20%), a disk latency, a snapshot interval and a batch size, then for
4 simulated seconds:

- kills the leader or a random node (anything not yet fsynced is lost)
  and restarts it 20-400 ms later
- cuts the network into random partitions and heals them
- injects delay spikes of up to 300 ms
- keeps proposing writes

After that the network is healed, and every node must converge on exactly the
same history. The checks run on **every step**: at most one leader per term, a
committed index never changes, and a newly elected leader holds every entry
ever committed. In short: a committed entry is never lost.

Why a simulation instead of killing real processes? 300 real-process scenarios
cannot fit in 60 s with 150-300 ms election timeouts, and threads are not
reproducible from a seed. Here every failure replays exactly. Real
processes are still covered by `test_cluster.py`.

**Does the fault suite actually catch bugs?** `tests/mutation_check.py` injects
classic Raft bugs one at a time:

| Injected bug | Caught by the fault suite? |
|---|---|
| vote granted without the "log up-to-date" check | yes, seed 0 |
| leader commits old-term entries by counting (Figure 8) | yes, seed 67 |
| follower commit index not capped by the verified prefix | yes, seed 195 |
| follower truncates its log on every AppendEntries | yes, seed 36 |
| vote not persisted before replying | yes, seed 129 |
| messages sent before the WAL fsync (in the sim harness) | yes, seed 0 |
| leader counts itself before its own fsync | **no**, see below; caught by `test_replication.py` |

The last one survives the multi-node scenarios for a structural reason: all of
a leader's messages leave only after its WAL write is durable, so a follower
can never acknowledge an entry the leader has not yet fsynced. The explicit
`durable_index` check still matters for a single-node cluster, and a unit test
covers it.

## Benchmark results

Measured with `python scripts/bench.py` on a laptop (Intel Core Ultra 9 275HX,
Samsung NVMe SSD, Windows 11, Python 3.11.9). All three nodes and the load
generator run on the same machine. Every acknowledged write was fsynced on the
leader and on at least one follower.

| Concurrent clients | Writes/s | p50 latency | p99 latency | Writes per leader fsync |
|---:|---:|---:|---:|---:|
| 1 | 623 | 1.5 ms | 2.4 ms | 1.0 |
| 4 | 1175 | 3.1 ms | 6.7 ms | 1.7 |
| 16 | 2179 | 6.5 ms | 17.9 ms | 3.9 |
| 64 | 3292 | 17.5 ms | 42.1 ms | 8.5 |

Leader failover over 10 kills (the leader process is killed outright):

| | Median | Min | Max |
|---|---:|---:|---:|
| until a new leader is elected | 360 ms | 219 ms | 735 ms |
| until the first write is accepted | 446 ms | 297 ms | 829 ms |

Notes:
- These numbers come from one of three full runs. Across all three, 64-client
  throughput was 3292-4315 writes/s. The median election time was 234-438 ms,
  and the worst single failover took 1234 ms (several split votes in a row).
- Group commit works: at 64 clients, one fsync covers about 8.5 writes.
- Failover time is mostly the election timeout. A follower waits 150-300 ms
  without heartbeats before standing for election, and split votes (when two
  followers time out together) explain the long tail. The client then needs
  about one more round trip to find the new leader.
- At 64 clients the leader process used about 125% of one core, the followers
  about 30%, and the load generator 15%. The leader's CPU, not the disk, is the
  ceiling (see the GIL answer below).

## Design decisions and what they give up

- **A pure Raft core.** `RaftCore` does no I/O and reads no clock. Callers pass
  in messages and the time, then collect the records to persist and the
  messages to send. This is the only way to run the fault scenarios
  deterministically and fast. It costs an extra layer (`node.py`) that wires
  the core to threads, sockets and the disk.
- **One-way messages, not request/response RPC.** A reply is just another
  message. No thread ever blocks waiting for a peer, and a lost reply is the
  same as a lost request. This gives up the simplicity of
  "call a function, get a result".
- **Persist before replying, enforced by ordering.** Every batch of messages,
  even one with nothing to write, goes through the WAL queue and leaves only
  after that batch's fsync. A reply can never overtake an earlier write it
  depends on. The cost is that heartbeat replies may wait behind an fsync.
- **Group commit.** A single writer thread drains everything queued so far and
  does one fsync for the whole batch.
- **One lock per node.** No fine-grained locking. Client threads wait on a
  `Condition` built on the same lock.
- **The WAL is JSON lines.** It is easy to read with `cat`. Compaction rewrites
  the whole WAL as snapshot + term/vote + remaining entries into a temp file,
  then does one atomic `os.replace`. There is no separate snapshot file, so a
  snapshot and the log can never disagree after a crash. A torn last line is
  cut off on startup.
- **Replication flow control:** at most one AppendEntries is in flight per
  follower (no pipelining), with up to 100 entries per message. On a mismatch
  the follower suggests where to retry; there is no term-based fast backup.
- **The leader commits a no-op when elected.** Older-term entries are never
  committed by counting replicas (paper §5.4.2), and read-index needs a
  current-term entry committed first.
- **Read-index, not lease reads.** The leader records its commit index,
  confirms it is still leader through one heartbeat round to a majority, waits
  until that index is applied, then reads. Every read costs one round trip, and
  reads are not batched.

## Two questions

### 1. Why are nodes processes, and RPC handled by threads, instead of all threads or all asyncio?

**Nodes are processes** because a Raft node is a unit of failure. Killing a
process is a real crash: memory is gone, and only what was fsynced survives,
which is exactly what Raft's persistence rules are about. A thread cannot be
killed from outside in Python, so with an all-thread design every "crash" would
be cooperative and far less convincing. Processes also keep nodes from sharing
state by accident, and each one has its own GIL, so three nodes really run in
parallel on three cores.

**Inside a node, threads** fit because the work is I/O: blocking on sockets and
on fsync. With one thread per peer, a dead peer only stalls its own sender
(for example while a 200 ms connect times out). Heartbeats to the healthy
peers keep flowing. Blocking code also reads top to bottom, which suits a
project that should be easy to explain.

**Why not asyncio?** fsync has no async version, so it would need
`run_in_executor` (threads again). Any blocking call accidentally left in the
event loop would freeze the whole node, timers included, and cause spurious
elections. Every function on the path would have to be `async`. Asyncio would
scale better to many thousands of connections, but a 3-node cluster has a
handful. And because the Raft logic is a pure state machine, it does not care.
Swapping the runtime would mean rewriting `node.py` and `transport.py` only.

### 2. What does the GIL do here, and why does multithreading still help?

Within one node process, only one thread runs Python bytecode at a time. All
the CPU work is serialized: JSON encoding and decoding, Raft logic, applying
commands. That is the throughput ceiling. At 64 clients the leader used about
125% of one core, while the followers and the load generator were mostly idle.
More threads cannot raise this limit.

Multithreading still pays off because blocking I/O releases the GIL:
`socket.recv`, `sendall`, `os.fsync`, `time.sleep` and `Condition.wait` all let
other threads run. The ">100%" above is that overlap: syscall time running
alongside Python code on another thread. Group commit depends on it. While the
WAL thread sits in an fsync (without the GIL), client threads keep receiving
requests and appending to the queue, so the next fsync covers them all. That
is how 64 clients get 8.5 writes per fsync.

The GIL is not a substitute for the lock. It only keeps the interpreter's own
data structures consistent, and threads can switch between any two bytecodes.
A compound step like "check role, append to the log, update next_index" would
interleave without `self.lock`.

Across nodes the GIL does not matter: every process has its own.

## Known simplifications

- **No membership changes** (paper §6). The cluster is fixed at startup.
- **No PreVote and no CheckQuorum.** A node cut off from the cluster keeps
  starting elections and raising its term. When it rejoins, it forces the
  current leader to step down, an avoidable extra election. A leader stuck in a minority keeps
  believing it leads. Its writes and reads time out instead of returning wrong
  data, but its pending read requests pile up in memory.
- **No client sessions or deduplication.** A client retries after a timeout,
  so one put can be applied twice. Put and delete are idempotent, but a retried
  put can still overwrite a newer put from another client. Strict
  linearizability holds only for requests that are not retried.
- **Snapshots** hold the whole state in memory and go out as one message (no
  chunking). Compaction rewrites the entire WAL.
- **No directory fsync after rename.** On Linux, `os.replace` is not durable
  until the directory is fsynced. Windows has no equivalent call.
- **The fault-injection suite runs on the simulation.** It tests the real Raft
  logic, but not the threading, socket or file code. Those are covered by the
  unit tests and the real-process `test_cluster.py`, which kills only the
  leader. The simulation does not model torn writes; `test_storage.py` does.
- **Unbounded queues.** There is no backpressure: the WAL queue and the peer
  queues can grow without limit under overload.
- **Fixed timing.** Election timeout 150-300 ms, heartbeat 50 ms, tuned for
  localhost.
- **No security.** No TLS, no authentication, and JSON over plain TCP.
- **The benchmark runs on a single machine**, so network latency is close to
  zero.
- **Smaller than the target size.** The spec asked for 1000-1500 lines. All
  required features fit in 844, and I did not pad it.
