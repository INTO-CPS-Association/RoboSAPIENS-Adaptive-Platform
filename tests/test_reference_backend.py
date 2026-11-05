"""The classic journal store's publication, retry and lineage boundaries."""
import pytest
from test_observation import redis_config

from rpclpy.rpio_observation.core import (InMemoryJournalBackend, KnowledgeRef,
                                  KnowledgeVersion, ObservationError, WriteConflictError,
                                  WriteId, canonical_text, value_digest)
from rpclpy.rpio_observation.wire import knowledge_record, phase_record, tagged


def commit(backend, key, value, identity):
    return backend.commit_knowledge_write(
        key, value, WriteId(identity), value_digest(value),
        lambda ref, now: knowledge_record(ref, tagged(key, value), tagged("Node", {
            "run_id": "run", "node_id": "Setup"}), now))


def test_knowledge_retry_keeps_one_history_version():
    backend = InMemoryJournalBackend(store_epoch="E")
    first = commit(backend, "Reading", {"value": 3}, "W1")
    again = commit(backend, "Reading", {"value": 3}, "W1")
    assert first.created and not again.created and first.ref == again.ref
    assert backend.global_write_sequence == 1 and len(backend.knowledge_journal()) == 1
    with pytest.raises(WriteConflictError):
        commit(backend, "Reading", {"value": 4}, "W1")
    assert backend.read_latest("Reading").value == {"value": 3}


def test_failure_before_memory_publication_leaves_every_boundary_unchanged():
    def fail(*args):
        raise ObservationError("injected failure")
    backend = InMemoryJournalBackend(store_epoch="E", commit_fault=fail)
    with pytest.raises(ObservationError):
        commit(backend, "Reading", {"value": 3}, "W1")
    assert backend.global_write_sequence == 0
    assert backend.key_revision("Reading") == 0
    assert backend.read_latest("Reading") is None
    assert backend.knowledge_journal() == ()


def test_committed_history_and_snapshot_cannot_be_mutated():
    backend = InMemoryJournalBackend(store_epoch="E")
    value = {"values": [1, 2]}
    result = commit(backend, "Reading", value, "W1")
    value["values"].append(3)
    assert result.value == {"values": [1, 2]}
    with pytest.raises(TypeError):
        result.value["values"].append(4)
    assert backend.resolve(result.ref).value == {"values": [1, 2]}


def test_redis_wrong_type_and_epoch_replacement_refuse_without_partial_writes(redis_config):
    import redis
    from rpclpy.rpio_observation.redis_backend import RedisJournalBackend, StoreIntegrityError, StoreLineageError
    client = redis.Redis(decode_responses=True,
                         **{k: redis_config[k] for k in ("host", "port", "db")})
    ns = redis_config["namespace"]
    backend = RedisJournalBackend(client, key_prefix=ns, phase_stream=ns + ":phase",
                                  knowledge_stream=ns + ":knowledge", initial_epoch="E")
    commit(backend, "Reading", {"value": 3}, "W1")
    client.set(backend.keys["revisions"], "wrong type")
    before = {key: client.dump(key) for key in client.scan_iter(ns + ":*")}
    with pytest.raises(StoreIntegrityError):
        commit(backend, "Reading", {"value": 4}, "W2")
    assert {key: client.dump(key) for key in client.scan_iter(ns + ":*")} == before
    client.delete(backend.keys["revisions"])
    client.set(backend.keys["epoch"], "replacement")
    before = {key: client.dump(key) for key in client.scan_iter(ns + ":*")}
    with pytest.raises(StoreLineageError):
        commit(backend, "Reading", {"value": 4}, "W2")
    assert {key: client.dump(key) for key in client.scan_iter(ns + ":*")} == before

def write_phase(ref, now):
    return phase_record({"run_id": "run", "loop": ["test"], "role": "Monitor",
                         "activation_id": "A", "caused_by": {"$tag": "None"}},
                        tagged("Write", {"key": ref.key}), (ref,), now)


def test_atomic_write_rejects_invalid_phase_before_knowledge_publication():
    from rpclpy.rpio_observation import MemoryJournal, create_run
    backend = MemoryJournal()
    create_run("test", backend=backend, epoch="E")
    with pytest.raises(ObservationError):
        backend.commit("Reading", {"value": 1},
            lambda ref, now: knowledge_record(ref, tagged("Reading", {"value": 1}),
                                              tagged("External", {"name": "test"}), now),
            phase_builder=lambda ref, now: {"schema_version": 2, "$tag": "bad", "payload": {}})
    assert backend.read_latest("Reading") is None
    assert backend.records() == ("E", [])
    assert backend.global_write_sequence == 0


def test_memory_read_selection_and_admission_exclude_a_concurrent_write():
    import threading
    from rpclpy.rpio_observation import MemoryJournal, create_run
    backend = MemoryJournal()
    create_run("test", backend=backend, epoch="E")
    commit(backend, "Reading", {"value": 1}, "W1")
    selected, writer_attempted, writer_done = (threading.Event() for _ in range(3))
    def writer():
        assert selected.wait(3)
        writer_attempted.set()
        commit(backend, "Reading", {"value": 2}, "W2")
        writer_done.set()
    thread = threading.Thread(target=writer)
    thread.start()
    def build(snapshot, now):
        selected.set()
        assert writer_attempted.wait(3)
        assert not writer_done.is_set()
        return phase_record({}, tagged("Read", {"key": "Reading"}), (snapshot.ref,), now)
    snapshot, entry = backend.read_phase("Reading", build)
    thread.join(3)
    assert not thread.is_alive() and writer_done.is_set()
    assert snapshot.value == {"value": 1}
    assert entry.record["payload"]["knowledge"]["payload"]["global_write_sequence"] == 1
    assert backend.read_latest("Reading").value == {"value": 2}


def test_redis_read_reselects_if_latest_changes_before_admission(redis_config, monkeypatch):
    from rpclpy.rpio_observation import create_run
    from rpclpy.rpio_observation.journal import RedisJournal
    import rpclpy.rpio_observation.journal as journal
    cfg = {k: redis_config[k] for k in ("host", "port", "db")}
    ns = redis_config["namespace"]
    create_run(ns, redis_config=cfg, epoch="E")
    backend = RedisJournal(cfg, ns)
    commit(backend, "Reading", {"value": 1}, "W1")
    original = journal._record_template
    raced = []
    def racing_template(record, with_sequence):
        result = original(record, with_sequence)
        if not raced:
            raced.append(True)
            commit(backend, "Reading", {"value": 2}, "W2")
        return result
    monkeypatch.setattr(journal, "_record_template", racing_template)
    try:
        snapshot, entry = backend.read_phase("Reading", lambda selected, now:
            phase_record({}, tagged("Read", {"key": "Reading"}), (selected.ref,), now))
        assert snapshot.value == {"value": 2}
        assert entry.record["payload"]["knowledge"]["payload"]["global_write_sequence"] == 2
        assert len(backend.phase_journal()) == 1
    finally:
        backend.close()


def test_redis_full_phase_stream_refuses_atomic_write_without_partial_knowledge(redis_config):
    from rpclpy.rpio_observation import create_run
    from rpclpy.rpio_observation.journal import RedisJournal
    from rpclpy.rpio_observation.redis_backend import StoreIntegrityError
    cfg = {k: redis_config[k] for k in ("host", "port", "db")}
    ns = redis_config["namespace"]
    create_run(ns, redis_config=cfg, epoch="E")
    backend = RedisJournal(cfg, ns)
    backend.client.xadd(backend.phase_stream, {"record": "preexisting"},
                        id="18446744073709551615-18446744073709551615")
    before = {key: backend.client.dump(key) for key in backend.client.scan_iter(ns + ":*")}
    try:
        with pytest.raises(StoreIntegrityError):
            backend.commit("Reading", {"value": 1},
                lambda ref, now: knowledge_record(ref, tagged("Reading", {"value": 1}),
                                                  tagged("External", {"name": "test"}), now),
                phase_builder=write_phase)
        assert {key: backend.client.dump(key) for key in backend.client.scan_iter(ns + ":*")} == before
    finally:
        backend.close()


def test_redis_atomic_retry_keeps_version_and_only_adds_the_new_access(redis_config):
    from rpclpy.rpio_observation import create_run
    from rpclpy.rpio_observation.journal import RedisJournal
    cfg = {k: redis_config[k] for k in ("host", "port", "db")}
    ns = redis_config["namespace"]
    create_run(ns, redis_config=cfg, epoch="E")
    backend = RedisJournal(cfg, ns)
    def write(value):
        return backend.commit("Reading", value, lambda ref, now:
            knowledge_record(ref, tagged("Reading", value), tagged("External", {"name": "test"}), now),
            write_id=WriteId("same-operation"), phase_builder=write_phase)
    try:
        first = write({"value": 1})
        again = write({"value": 1})
        assert first.created and not again.created and first.ref == again.ref
        assert len(backend.knowledge_journal()) == 1
        assert len(backend.phase_journal()) == 2
        before = backend.records()
        with pytest.raises(WriteConflictError):
            write({"value": 2})
        assert backend.records() == before
        knowledge = backend.knowledge_journal()[0]
        phase = backend.phase_journal()[0]
        assert knowledge.journal_time == phase.journal_time
        assert knowledge.journal_clock == phase.journal_clock
        assert backend.entries()[0].record["$tag"] == "Knowledge"
    finally:
        backend.close()


def _owned_write(backend, key, value, owner):
    return backend.commit(key, {"value": value},
        lambda ref, now: knowledge_record(ref, tagged(key, {"value": value}),
            tagged("Node", {"run_id": "E", "name": owner}), now))


def _assert_current_selection_is_coherent_without_scan(backend, monkeypatch):
    import json
    for number in range(32):
        _owned_write(backend, f"Unrelated{number}", number, "Setup")
    selected = _owned_write(backend, "Reading", 1, "FirstWriter")
    def forbidden_scan():
        raise AssertionError("current must not enumerate the Knowledge history")
    monkeypatch.setattr(backend, "knowledge_journal", forbidden_scan)
    read_latest = backend.read_latest
    def racing_read(key):
        snapshot = read_latest(key)
        _owned_write(backend, "Reading", 2, "SecondWriter")
        return snapshot
    monkeypatch.setattr(backend, "read_latest", racing_read)
    current = json.loads(backend.current("Reading"))
    assert current["ref"]["global_write_sequence"] == selected.ref.global_write_sequence
    assert current["value"] == {"value": 1}
    assert current["written_by"]["payload"]["name"] == "FirstWriter"
    monkeypatch.setattr(backend, "read_latest", read_latest)
    newer = json.loads(backend.current("Reading"))
    assert newer["value"] == {"value": 2}
    assert newer["written_by"]["payload"]["name"] == "SecondWriter"
    assert backend.current("Absent") is None


def test_memory_current_keeps_selected_value_and_writer_without_history_scan(monkeypatch):
    from rpclpy.rpio_observation import MemoryJournal, create_run
    backend = MemoryJournal()
    create_run("current", backend=backend, epoch="E")
    _assert_current_selection_is_coherent_without_scan(backend, monkeypatch)


def test_redis_current_fetches_only_the_selected_history_row(redis_config, monkeypatch):
    from rpclpy.rpio_observation import create_run
    from rpclpy.rpio_observation.journal import RedisJournal
    cfg = {k: redis_config[k] for k in ("host", "port", "db")}
    create_run(redis_config["namespace"], redis_config=cfg, epoch="E")
    backend = RedisJournal(cfg, redis_config["namespace"])
    commands = []
    checked = backend._checked
    def inspect_commands(*items):
        commands.extend(items)
        return checked(*items)
    monkeypatch.setattr(backend, "_checked", inspect_commands)
    try:
        _assert_current_selection_is_coherent_without_scan(backend, monkeypatch)
        ranges = [command for command in commands if command[0] == "xrange"]
        assert len(ranges) == 2
        assert all(len(command) == 4 and command[2] == command[3] for command in ranges)
    finally:
        backend.close()


def test_redis_current_rechecks_history_and_epoch_after_prior_success(redis_config):
    from rpclpy.rpio_observation import create_run
    from rpclpy.rpio_observation.journal import RedisJournal
    from rpclpy.rpio_observation.redis_backend import StoreLineageError
    cfg = {k: redis_config[k] for k in ("host", "port", "db")}
    create_run(redis_config["namespace"], redis_config=cfg, epoch="E")
    backend = RedisJournal(cfg, redis_config["namespace"])
    try:
        written = _owned_write(backend, "Reading", 1, "Writer")
        assert backend.current("Reading") is not None
        backend.client.xdel(backend.knowledge_stream, written.entry.cursor)
        with pytest.raises(StoreLineageError, match="history entry.*missing"):
            backend.current("Reading")
        backend.client.set(backend.keys["epoch"], "replacement")
        with pytest.raises(StoreLineageError, match="store epoch"):
            backend.current("Reading")
    finally:
        backend.close()
