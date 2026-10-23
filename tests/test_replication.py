import random

from sim import SimCluster

from raftkv.core import LEADER, RaftCore
from raftkv.log import Entry, RaftLog
from raftkv.storage import PersistentState


def elected(size: int = 3, seed: int = 0) -> SimCluster:
    sim = SimCluster(size, seed)
    assert sim.run_until(lambda: sim.leader() is not None)
    return sim


def test_replicates_to_all_nodes_in_order():
    sim = elected()
    for n in range(10):
        sim.propose(f"cmd{n}")
    sim.run(200)
    expected = [f"cmd{n}" for n in range(10)]
    for i in sim.ids:
        assert sim.commands(i) == expected


def test_lagging_follower_catches_up_after_restart():
    sim = elected(seed=1)
    follower = next(n.id for n in sim.live() if n.role != LEADER)
    sim.crash(follower)
    for n in range(250):  # more than one AppendEntries batch
        sim.propose(n)
    sim.run(200)
    sim.start(follower)
    sim.run(1000)
    assert sim.commands(follower) == list(range(250))


def test_uncommitted_entries_of_isolated_leader_are_replaced():
    sim = elected(5, seed=2)
    old = sim.leader()
    others = [i for i in sim.ids if i != old.id]
    sim.partition([old.id], others)
    old.propose("lost")  # can never reach a majority
    sim.run(1000)
    sim.propose("kept")
    sim.run(200)
    sim.heal()
    sim.run(1000)
    for i in sim.ids:
        assert sim.commands(i) == ["kept"]
    assert all(e.command != "lost" for e in sim.nodes[old.id].log.entries)


def append_reply(src: int, term: int, match: int) -> dict:
    return {"type": "append_reply", "src": src, "dst": 1, "term": term,
            "success": True, "match": match, "seq": 0}  # fmt: skip


def leader_with_log(*terms: int) -> RaftCore:
    log = RaftLog()
    for t in terms:
        log.append(Entry(t, None))
    core = RaftCore(1, [2, 3], PersistentState(term=max(terms), log=log), 0, random.Random(0))
    core.start_election(now=0)
    core.step({"type": "vote_reply", "src": 2, "dst": 1, "term": core.term, "granted": True}, 0)
    assert core.role == LEADER
    return core


def test_old_term_entry_is_not_committed_by_counting():
    # Paper figure 8: entry 2 (term 2) on a majority is still not safe to commit.
    core = leader_with_log(1, 2)
    core.persisted(3, core.term)  # the leader's own no-op is durable
    core.step(append_reply(2, core.term, match=2), now=0)
    assert core.commit_index == 0
    core.step(append_reply(2, core.term, match=3), now=0)
    assert core.commit_index == 3  # entries 1 and 2 commit with it


def test_leader_counts_itself_only_after_fsync():
    core = leader_with_log(1)
    index = core.propose("x")
    core.step(append_reply(2, core.term, match=index), now=0)
    assert core.commit_index < index
    core.persisted(index, core.term)
    assert core.commit_index == index


def test_follower_commit_is_capped_by_verified_prefix():
    log = RaftLog()
    for t in (1, 1, 1):
        log.append(Entry(t, "stale"))
    core = RaftCore(1, [2, 3], PersistentState(term=1, log=log), 0, random.Random(0))
    msg = {"type": "append", "src": 2, "dst": 1, "term": 1, "prev_index": 0, "prev_term": 0,
           "entries": [[1, "stale"]], "commit": 3, "seq": 0}  # fmt: skip
    core.step(msg, now=0)
    # Only index 1 is known to match the leader, so only it may commit.
    assert core.commit_index == 1


def test_mismatch_reply_hints_retry_point():
    core = RaftCore(1, [2, 3], PersistentState(term=1), 0, random.Random(0))
    msg = {"type": "append", "src": 2, "dst": 1, "term": 1, "prev_index": 7, "prev_term": 1,
           "entries": [], "commit": 0, "seq": 0}  # fmt: skip
    core.step(msg, now=0)
    reply = core.take_ready().messages[0]
    assert reply["success"] is False
    assert reply["match"] == 0
