"""Randomized fault injection on the simulated cluster.

Every scenario is fully determined by its seed; rerun one with
    pytest tests/test_fault_injection.py -k "seed_42]"

SimCluster checks on every step that
  - no term has two leaders,
  - an index, once committed anywhere, never holds a different entry,
  - a newly elected leader holds every committed entry (none is lost).
After the chaos, the network is healed and every node must converge on the
full committed history.
"""

import random

import pytest
from sim import SimCluster

from raftkv import core

SCENARIOS = 300
CHAOS_MS = 4000
STEP_MS = 20


def chaos(sim: SimCluster, rng: random.Random) -> None:
    """Random kills, partitions, delay spikes, and client writes."""
    proposed = 0
    restart_at: dict[int, int] = {}
    for _ in range(CHAOS_MS // STEP_MS):
        for _ in range(rng.randint(0, 3)):
            if sim.propose(f"cmd{proposed}") is not None:
                proposed += 1
        leader = sim.leader()
        roll = rng.random()
        if roll < 0.05 and leader:
            victim = leader.id
        elif roll < 0.10:
            victim = rng.choice(sim.ids)
        else:
            victim = None
        if victim is not None and sim.nodes[victim]:
            sim.crash(victim)
            restart_at[victim] = sim.now + rng.randint(20, 400)
        if 0.10 <= roll < 0.15:
            shuffled = rng.sample(sim.ids, len(sim.ids))
            cut = rng.randint(1, len(shuffled) - 1)
            sim.partition(shuffled[:cut], shuffled[cut:])
        elif 0.15 <= roll < 0.22:
            sim.heal()
        elif 0.22 <= roll < 0.27:
            sim.net_delay = (1, rng.choice([5, 20, 100, 300]))
        for node_id, due in list(restart_at.items()):
            if sim.now >= due:
                sim.start(node_id)
                del restart_at[node_id]
        sim.run(STEP_MS)
    for node_id in restart_at:
        sim.start(node_id)


def converge(sim: SimCluster) -> None:
    sim.heal()
    sim.drop_rate = 0.0
    sim.net_delay = (1, 5)

    def final_committed() -> bool:
        # Like a real client: an unacknowledged proposal may be lost, so retry it.
        if any(e.command == "final" for e in sim.committed.values()):
            return True
        leader = sim.leader()
        if leader and all(e.command != "final" for e in leader.log.entries):
            sim.propose("final")
        return False

    assert sim.run_until(final_committed, 5000), "nothing commits after heal"

    def all_applied() -> bool:
        leader = sim.leader()
        return leader is not None and all(
            n.applied_index == leader.log.last_index for n in sim.live()
        )

    assert sim.run_until(all_applied, 5000), "cluster did not converge"


@pytest.mark.parametrize("seed", range(SCENARIOS), ids=lambda s: f"seed_{s}")
def test_committed_entries_are_never_lost(seed: int, monkeypatch) -> None:
    rng = random.Random(seed)
    sim = SimCluster(rng.choice([3, 5]), seed)
    sim.drop_rate = rng.choice([0.0, 0.05, 0.2])
    # Slow disks widen the window in which a crash loses unsynced writes.
    sim.disk_delay = (0, rng.choice([0, 5, 20, 40]))
    sim.snapshot_every = rng.choice([None, 8, 30])
    # Small batches leave followers with stale suffixes past what was verified.
    monkeypatch.setattr(core, "MAX_BATCH", rng.choice([1, 4, 100]))

    chaos(sim, rng)
    converge(sim)

    history = [(i, sim.committed[i].command) for i in sorted(sim.committed)]
    assert [i for i, _ in history] == list(range(1, len(history) + 1))
    for node_id in sim.ids:
        assert sim.applied[node_id] == history, f"node {node_id} diverged"
    assert "final" in [command for _, command in history]
