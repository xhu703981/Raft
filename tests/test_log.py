from raftkv.log import Entry, RaftLog


def make_log(*terms: int) -> RaftLog:
    log = RaftLog()
    for i, term in enumerate(terms):
        log.append(Entry(term, f"c{i + 1}"))
    return log


def test_empty_log():
    log = RaftLog()
    assert log.last_index == 0
    assert log.last_term == 0
    assert log.term_at(0) == 0
    assert log.term_at(1) is None


def test_append_and_term_at():
    log = make_log(1, 1, 2)
    assert log.last_index == 3
    assert log.last_term == 2
    assert [log.term_at(i) for i in range(4)] == [0, 1, 1, 2]
    assert log.get(2) == Entry(1, "c2")
    assert log.slice(2, 10) == [Entry(1, "c2"), Entry(2, "c3")]


def test_merge_truncates_on_conflict():
    log = make_log(1, 1, 2, 2)
    changed = log.merge(2, [Entry(3, "x")])
    assert changed == 3
    assert log.last_index == 3
    assert log.get(3) == Entry(3, "x")


def test_merge_does_not_truncate_on_matching_stale_request():
    log = make_log(1, 1, 1, 1)
    # A delayed AppendEntries carrying only entry 2 must leave 3 and 4 alone.
    assert log.merge(1, [Entry(1, "c2")]) is None
    assert log.last_index == 4


def test_merge_appends_new_suffix():
    log = make_log(1)
    assert log.merge(1, [Entry(1, "a"), Entry(1, "b")]) == 2
    assert log.last_index == 3


def test_compact_keeps_suffix_and_term():
    log = make_log(1, 2, 3, 4)
    log.compact(2)
    assert log.base_index == 2
    assert log.base_term == 2
    assert log.term_at(1) is None
    assert log.term_at(2) == 2
    assert log.get(3) == Entry(3, "c3")
    assert log.last_index == 4


def test_write_skips_entries_inside_snapshot():
    log = make_log(1, 1, 1)
    log.compact(3)
    log.write(2, [Entry(1, "c2"), Entry(1, "c3"), Entry(2, "new")])
    assert log.last_index == 4
    assert log.get(4) == Entry(2, "new")


def test_merge_with_prev_inside_snapshot():
    log = make_log(1, 1, 1)
    log.compact(2)
    assert log.merge(1, [Entry(1, "c2"), Entry(1, "c3"), Entry(1, "c4")]) == 4
    assert log.last_index == 4
