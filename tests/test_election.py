import random

from sim import SimCluster

from raftkv.core import CANDIDATE, FOLLOWER, LEADER, RaftCore
from raftkv.log import Entry, RaftLog
from raftkv.storage import PersistentState


def test_elects_exactly_one_leader():
    sim = SimCluster(3, seed=1)
    assert sim.run_until(lambda: sim.leader() is not None, 1000)
    leader = sim.leader()
    sim.run(500)  # heartbeats keep the same leader in place
    assert sim.leader() is leader
    for node in sim.live():
        assert node.term == leader.term
        assert node.leader_id == leader.id
        assert node.role == (LEADER if node is leader else FOLLOWER)


def test_election_safety_over_many_seeds():
    # SimCluster asserts "at most one leader per term" on every step.
    for seed in range(30):
        sim = SimCluster(5, seed=seed)
        sim.drop_rate = 0.2
        sim.run(2000)


def test_new_leader_after_crash():
    sim = SimCluster(3, seed=2)
    sim.run_until(lambda: sim.leader() is not None)
    old = sim.leader()
    sim.crash(old.id)
    assert sim.run_until(lambda: sim.leader() is not None, 1000)
    assert sim.leader().term > old.term


def test_minority_partition_cannot_elect():
    sim = SimCluster(5, seed=3)
    sim.run_until(lambda: sim.leader() is not None)
    old = sim.leader()
    others = [i for i in sim.ids if i != old.id]
    sim.partition([old.id, others[0]], others[1:])
    sim.run(2000)
    minority = [sim.nodes[old.id], sim.nodes[others[0]]]
    assert all(n.role != LEADER or n is old for n in minority)
    majority_leaders = [sim.nodes[i] for i in others[1:] if sim.nodes[i].role == LEADER]
    assert len(majority_leaders) == 1
    assert majority_leaders[0].term > old.term


def core_with_log(*terms: int, term: int = 0) -> RaftCore:
    log = RaftLog()
    for t in terms:
        log.append(Entry(t, None))
    state = PersistentState(term=term, log=log)
    return RaftCore(1, [2, 3], state, 0, random.Random(0))


def vote_request(src: int, term: int, last_index: int, last_term: int) -> dict:
    return {"type": "vote", "src": src, "dst": 1, "term": term,
            "last_index": last_index, "last_term": last_term}  # fmt: skip


def test_rejects_candidate_with_stale_log():
    core = core_with_log(1, 2, term=2)
    core.step(vote_request(2, 3, last_index=5, last_term=1), now=0)
    assert core.take_ready().messages[0]["granted"] is False
    # Same last term but shorter log is also stale.
    core.step(vote_request(3, 3, last_index=1, last_term=2), now=0)
    assert core.take_ready().messages[0]["granted"] is False


def test_grants_one_vote_per_term_and_persists_it():
    core = core_with_log(1, term=1)
    core.step(vote_request(2, 2, last_index=1, last_term=1), now=0)
    ready = core.take_ready()
    assert ready.messages[0]["granted"] is True
    assert {"op": "meta", "term": 2, "vote": 2} in ready.writes
    core.step(vote_request(3, 2, last_index=1, last_term=1), now=0)
    assert core.take_ready().messages[0]["granted"] is False


def test_vote_survives_restart():
    sim = SimCluster(3, seed=4)
    node = sim.nodes[1]
    node.step(vote_request(2, 7, last_index=0, last_term=0), now=0)
    sim._collect(1)
    sim.run(5)  # let the disk write land
    sim.crash(1)
    sim.start(1)
    restarted = sim.nodes[1]
    assert (restarted.term, restarted.voted_for) == (7, 2)
    restarted.step(vote_request(3, 7, last_index=0, last_term=0), now=0)
    assert restarted.take_ready().messages[0]["granted"] is False


def test_election_timeout_starts_candidacy():
    core = core_with_log()
    core.tick(now=149)
    assert core.role == FOLLOWER
    core.tick(now=301)
    assert core.role == CANDIDATE
    assert core.term == 1
    assert len(core.take_ready().messages) == 2
