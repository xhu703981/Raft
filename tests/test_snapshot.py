import random

from sim import SimCluster

from raftkv.core import LEADER, RaftCore
from raftkv.log import Entry, RaftLog
from raftkv.storage import PersistentState


def elected(seed: int = 0, snapshot_every: int = 10) -> SimCluster:
    sim = SimCluster(3, seed)
    sim.snapshot_every = snapshot_every
    assert sim.run_until(lambda: sim.leader() is not None)
    return sim


def test_compaction_keeps_logs_short_and_state_intact():
    sim = elected()
    for n in range(60):
        sim.propose(n)
        sim.run(5)
    sim.run(300)
    for i in sim.ids:
        node = sim.nodes[i]
        assert node.log.base_index > 0
        assert len(node.log.entries) < 10
        assert sim.commands(i) == list(range(60))


def test_lagging_follower_gets_install_snapshot():
    sim = elected(seed=1)
    follower = next(n.id for n in sim.live() if n.role != LEADER)
    sim.crash(follower)
    for n in range(50):
        sim.propose(n)
        sim.run(5)
    sim.run(200)
    assert sim.leader().log.base_index > 0  # the follower's next entry is compacted away
    sim.start(follower)
    sim.run(1000)
    assert sim.nodes[follower].log.base_index > 0
    assert sim.commands(follower) == list(range(50))


def test_whole_cluster_restarts_from_compacted_disks():
    sim = elected(seed=2)
    for n in range(40):
        sim.propose(n)
        sim.run(5)
    sim.run(300)
    for i in sim.ids:
        sim.crash(i)
    for i in sim.ids:
        sim.start(i)
    sim.run(1000)
    sim.propose("after")
    sim.run(300)
    for i in sim.ids:
        assert sim.commands(i) == list(range(40)) + ["after"]


def follower_with_log(*terms: int) -> RaftCore:
    log = RaftLog()
    for t in terms:
        log.append(Entry(t, t))
    return RaftCore(1, [2, 3], PersistentState(term=3, log=log), 0, random.Random(0))


def snapshot_msg(index: int, snap_term: int) -> dict:
    return {"type": "snapshot", "src": 2, "dst": 1, "term": 3, "index": index,
            "snap_term": snap_term, "data": {"k": "v"}, "seq": 4}  # fmt: skip


def test_install_snapshot_keeps_matching_suffix():
    core = follower_with_log(1, 1, 2, 2)
    core.step(snapshot_msg(index=2, snap_term=1), now=0)
    assert (core.log.base_index, core.log.last_index) == (2, 4)
    restore, entries = core.take_committed()
    assert restore["data"] == {"k": "v"}
    assert entries == []


def test_install_snapshot_discards_conflicting_log():
    core = follower_with_log(1, 1, 2, 2)
    core.step(snapshot_msg(index=3, snap_term=3), now=0)
    assert (core.log.base_index, core.log.last_index) == (3, 3)
    ready = core.take_ready()
    assert ready.writes[-1]["op"] == "compact"
    assert ready.messages[-1] == {"type": "append_reply", "term": 3, "src": 1, "dst": 2,
                                  "success": True, "match": 3, "seq": 4}  # fmt: skip
