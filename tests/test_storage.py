import threading

from raftkv.log import Entry
from raftkv.storage import Storage, encode


def write_and_wait(storage: Storage, records: list) -> None:
    done = threading.Event()
    storage.submit(records, done.set)
    assert done.wait(5)


def entries(index: int, *terms: int) -> dict:
    return {"op": "entries", "index": index, "entries": [[t, f"c{t}"] for t in terms]}


def test_fresh_storage_is_empty(tmp_path):
    state = Storage(str(tmp_path / "wal")).load()
    assert state.term == 0
    assert state.vote is None
    assert state.log.last_index == 0


def test_persist_and_recover(tmp_path):
    path = str(tmp_path / "wal")
    storage = Storage(path)
    storage.load()
    storage.start()
    write_and_wait(storage, [{"op": "meta", "term": 3, "vote": 2}, entries(1, 1, 1, 2)])
    write_and_wait(storage, [entries(3, 3)])  # overwrites index 3
    storage.stop()

    state = Storage(path).load()
    assert (state.term, state.vote) == (3, 2)
    assert state.log.last_index == 3
    assert state.log.get(3) == Entry(3, "c3")


def test_torn_tail_is_discarded(tmp_path):
    path = tmp_path / "wal"
    path.write_bytes(encode({"op": "meta", "term": 5, "vote": None}) + b'{"op":"ent')
    storage = Storage(str(path))
    state = storage.load()
    assert state.term == 5
    # The torn bytes are cut off so later appends start on a clean line.
    storage.start()
    write_and_wait(storage, [{"op": "meta", "term": 6, "vote": 1}])
    storage.stop()
    assert Storage(str(path)).load().term == 6


def test_group_commit_merges_fsyncs(tmp_path):
    storage = Storage(str(tmp_path / "wal"))
    storage.load()
    done = [threading.Event() for _ in range(100)]
    # Queue everything before the writer starts, so it must drain them as one batch.
    for i, event in enumerate(done):
        storage.submit([entries(i + 1, 1)], event.set)
    storage.start()
    assert all(event.wait(5) for event in done)
    assert storage.fsyncs == 1
    storage.stop()


def test_compaction_rewrites_wal(tmp_path):
    path = str(tmp_path / "wal")
    storage = Storage(path)
    storage.load()
    storage.start()
    write_and_wait(storage, [{"op": "meta", "term": 1, "vote": 1}, entries(1, 1, 1, 1, 1)])
    snapshot = {"op": "snapshot", "index": 3, "term": 1, "data": {"k": "v"}}
    compact = {
        "op": "compact",
        "records": [snapshot, {"op": "meta", "term": 1, "vote": 1}, entries(4, 1, 1)],
    }
    write_and_wait(storage, [compact])
    write_and_wait(storage, [entries(6, 2)])
    storage.stop()

    state = Storage(path).load()
    assert state.snapshot["data"] == {"k": "v"}
    assert state.log.base_index == 3
    assert state.log.last_index == 6
    assert state.log.term_at(6) == 2
