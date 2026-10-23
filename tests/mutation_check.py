"""Inject known Raft bugs one at a time; check the fault-injection suite catches each.

Run from anywhere: python tests/mutation_check.py
"""

import os
import subprocess
import sys

os.chdir(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

MUTANTS = {
    "vote ignores log up-to-date check": (
        "raftkv/core.py",
        'up_to_date = (msg["last_term"], msg["last_index"]) >= mine',
        "up_to_date = True",
    ),
    "commit old-term entries by counting": (
        "raftkv/core.py",
        "            if self.log.term_at(index) != self.term:\n                return",
        "            if False:\n                return",
    ),
    "follower commit not capped by match": (
        "raftkv/core.py",
        'min(msg["commit"], match)',
        'min(msg["commit"], self.log.last_index)',
    ),
    "truncate log on every AppendEntries": (
        "raftkv/core.py",
        "        changed = self.log.merge(prev, entries)",
        "        self.log.write(prev + 1, entries)\n        changed = prev + 1",
    ),
    "leader counts itself before fsync": (
        "raftkv/core.py",
        "acks = (self.durable_index >= index) +",
        "acks = 1 +",
    ),
    "vote not persisted": (
        "raftkv/core.py",
        '            self.voted_for = msg["src"]\n            self.persist_meta()',
        '            self.voted_for = msg["src"]',
    ),
    "replies sent before fsync (sim harness)": (
        "tests/sim.py",
        "                ready = queue.pop(0)[1]\n",
        "                ready = queue.pop(0)[1]\n                ready.writes = []\n",
    ),
}

for name, (path, old, new) in MUTANTS.items():
    src = open(path, newline="").read()
    assert src.count(old) == 1, name
    open(path, "w", newline="").write(src.replace(old, new))
    try:
        cmd = [sys.executable, "-m", "pytest", "-q", "-x", "-p", "no:cacheprovider"]
        cmd.append("tests/test_fault_injection.py")
        out = subprocess.run(cmd, capture_output=True, text=True).stdout
    finally:
        open(path, "w", newline="").write(src)
    print(f"{name:42s} -> {out.strip().splitlines()[-1]}")
