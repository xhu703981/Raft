from sim import SimCluster

from raftkv.core import LEADER
from raftkv.kv import KVStore


def elected(size: int = 3, seed: int = 0) -> SimCluster:
    sim = SimCluster(size, seed)
    assert sim.run_until(lambda: sim.leader() is not None)
    leader = sim.leader()
    # Wait for the leader's no-op to commit; read_index refuses until then.
    assert sim.run_until(lambda: leader.log.term_at(leader.commit_index) == leader.term)
    return sim


def test_read_completes_after_majority_ack():
    sim = elected()
    leader = sim.leader()
    sim.propose("x")
    assert leader.read_index(read_id=1)
    sim._collect(leader.id)
    assert 1 not in sim.reads  # needs a round trip first
    sim.run(50)
    assert sim.reads[1] >= leader.log.last_index - 1


def test_follower_refuses_reads():
    sim = elected()
    follower = next(n for n in sim.live() if n.role != LEADER)
    assert not follower.read_index(read_id=1)


def test_new_leader_refuses_reads_until_its_noop_commits():
    sim = SimCluster(3, seed=1)
    assert sim.run_until(lambda: sim.leader() is not None)
    leader = sim.leader()
    assert leader.commit_index < leader.log.last_index
    assert not leader.read_index(read_id=1)


def test_deposed_leader_cannot_serve_stale_reads():
    sim = elected(5, seed=2)
    old = sim.leader()
    others = [i for i in sim.ids if i != old.id]
    sim.partition([old.id], others)
    sim.run(1000)
    sim.propose("newer write")  # committed by the new leader on the other side
    sim.run(200)
    # The old leader still believes it leads, but no majority will ack its heartbeat.
    assert old.role == LEADER
    assert old.read_index(read_id=7)
    sim._collect(old.id)
    sim.run(500)
    assert 7 not in sim.reads
    sim.heal()
    sim.run(500)
    assert sim.reads[7] is None  # failed once it learned about the newer term


def test_kv_state_machine():
    kv = KVStore()
    kv.apply(None)
    kv.apply(["put", "a", "1"])
    kv.apply(["put", "b", "2"])
    kv.apply(["delete", "a"])
    kv.apply(["delete", "missing"])
    assert kv.get("a") is None
    assert kv.get("b") == "2"
    other = KVStore()
    other.restore(kv.snapshot())
    assert other.data == {"b": "2"}
