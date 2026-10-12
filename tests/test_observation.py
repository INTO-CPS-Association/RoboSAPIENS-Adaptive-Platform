"""Observation journal (classic reference journal): record shapes, activations, evidence,
failure policy, the memory and Redis backends, and dump.

Redis tests use a real server (RPIO_TEST_REDIS_URL, default redis://localhost:6379/0)
and are skipped when none answers.
"""
import json
import os
import pathlib
import threading
from urllib.parse import urlparse

import pytest

from observation_nominal import EPOCH, Analysis, Monitor, Reading, Settings, config, run_nominal, start_nominal
from rpclpy.CommunicationManager import LocalBus
from rpclpy.KnowledgeManager import reset_local_stores
from rpclpy.node import Node
from rpclpy.rpio_observation import MemoryJournal, ObservationError, create_run, dump, outcomes

FIXTURE = pathlib.Path(__file__).parent / "fixtures" / "journal-v2" / "nominal.jsonl"


# -- helpers ------------------------------------------------------------------------------
def normalise(lines):
    records = [json.loads(line) for line in lines]
    ids = {}
    for r in records:
        r["payload"]["arrival"]["journal_time"] = 0
        act = r["payload"].get("activation")
        if act is not None:
            ids.setdefault(act["activation_id"], f"A{len(ids) + 1}")
    text = json.dumps(records)
    for old, new in ids.items():
        text = text.replace(old, new)
    return json.loads(text)


def records(source, tmp_path, namespace="t"):
    path = tmp_path / "out.jsonl"
    dump(source, namespace, path)
    return [json.loads(line) for line in path.read_text().splitlines()]


def steps(recs, tag=None):
    return [r["payload"]["step"] for r in recs
            if r["$tag"] == "Phase" and (tag is None or r["payload"]["step"]["$tag"] == tag)]


def check_contiguous(recs):
    # Only committed writes allocate versions; Phase stream positions are separate.
    versions = [r["payload"]["version"]["ref"] for r in recs if r["$tag"] == "Knowledge"]
    assert [v["global_write_sequence"] for v in versions] == list(range(1, len(versions) + 1))
    assert {v["store_epoch"] for v in versions} == {EPOCH}
    assert all(r["schema_version"] == 2 for r in recs)


@pytest.fixture(autouse=True)
def _isolation():
    LocalBus.reset()
    yield
    LocalBus.reset()


@pytest.fixture
def redis_config():
    import redis
    url = urlparse(os.environ.get("RPIO_TEST_REDIS_URL", "redis://localhost:6379/0"))
    cfg = {"host": url.hostname, "port": url.port or 6379, "db": int(url.path.strip("/") or 0)}
    client = redis.Redis(decode_responses=True, **cfg)
    try:
        client.ping()
    except redis.ConnectionError:
        pytest.skip("no Redis server")
    namespace = f"rpio-test-{os.getpid()}-{id(cfg)}"
    cfg["namespace"] = namespace
    yield cfg
    keys = list(client.scan_iter(match=f"{namespace}:*"))
    if keys:
        client.delete(*keys)


def redis_run(cfg, **kw):
    redis = {k: cfg[k] for k in ("host", "port", "db")}
    return run_nominal(cfg["namespace"], redis_config=redis, **kw)


def redis_start(cfg, **kw):
    redis = {k: cfg[k] for k in ("host", "port", "db")}
    return start_nominal(cfg["namespace"], redis_config=redis, **kw)


# -- shapes -------------------------------------------------------------------------------
def test_nominal_run_matches_reference_fixture(tmp_path):
    nodes, source = run_nominal("nominal")
    assert not any(n.observation_failed for n in nodes)
    got = records(source, tmp_path)
    check_contiguous(got)
    assert normalise(json.dumps(r) for r in got) == normalise(FIXTURE.read_text().splitlines())


def test_record_shapes(tmp_path):
    _, source = run_nominal("t")
    recs = records(source, tmp_path)
    knowledge, start, reading, write, event, a_start, read, a_end, m_end = recs
    version = knowledge["payload"]["version"]
    assert version["ref"] == {"store_epoch": EPOCH, "global_write_sequence": 1, "key": "Settings"}
    assert version["written_by"] == {"$tag": "Node", "payload": {"run_id": EPOCH, "name": "Monitor"}}
    activation = start["payload"]["activation"]
    assert activation["run_id"] == EPOCH and activation["role"] == "Monitor"
    assert activation["loop"] == ["demo"] and activation["caused_by"] == {"$tag": "None"}
    assert isinstance(activation["activation_id"], str)
    assert start["payload"]["step"]["payload"]["trigger"] == {"$tag": "Received", "payload": {
        "kind": "message", "topic": "/sensor", "message": {"value": 3.0}}}
    committed = reading["payload"]["version"]
    ref = committed["ref"]
    assert ref["global_write_sequence"] == 2
    assert committed["value"] == {"$tag": "Reading", "payload": {"value": 3.0, "tcp_pose": None}}
    assert committed["written_by"] == {"$tag": "Activation", "payload": {
        "run_id": EPOCH, "activation_id": activation["activation_id"]}}
    assert write["payload"]["knowledge"] == event["payload"]["knowledge"] == read["payload"]["knowledge"] == {
        "$tag": "One", "payload": ref}
    assert event["payload"]["step"]["payload"]["name"] == "reading_recorded"
    assert event["payload"]["step"]["payload"]["payload"]["message"] == {"value": 3.0}
    assert a_end["payload"]["step"]["payload"]["outcome"] == {
        "$tag": "Analysed", "payload": {"$tag": "Nominal"}}
    assert m_end["payload"]["step"]["payload"]["outcome"] == {
        "$tag": "Monitored", "payload": {"observation": ref}}
    assert len([r for r in recs if r["$tag"] == "Knowledge"]) == 2


def test_nested_activations_and_cause(tmp_path):
    _, source = run_nominal("t")
    recs = records(source, tmp_path)
    starts = [r["payload"]["activation"] for r in recs
              if r["payload"].get("step", {}).get("$tag") == "Start"]
    assert [r["payload"].get("step", {}).get("$tag") for r in recs
            if r["payload"].get("step", {}).get("$tag") in ("Start", "End")] == ["Start", "Start", "End", "End"]
    assert starts[1]["caused_by"] == {"$tag": "Some", "payload": {
        "activation_id": starts[0]["activation_id"], "ordinal": 1}}


def test_two_triggers_number_emissions_per_activation(tmp_path):
    _, source = run_nominal("t", values=(1.0, 2.0))
    events = steps(records(source, tmp_path), "Event")
    assert [e["payload"]["emission"]["ordinal"] for e in events] == [1, 1]


# -- Knowledge access ---------------------------------------------------------------------
class Probe(Node):
    """Node whose callbacks run arbitrary test code."""

    def __init__(self, cfg, **kw):
        super().__init__(cfg, **kw)
        self.body = lambda message: outcomes.Nominal()
        self.register_message_callback("/probe", lambda message: self.body(message))


def probe(tmp_path_factory=None, *, start=True):
    LocalBus.reset()
    journal = MemoryJournal()
    create_run("t", backend=journal, epoch=EPOCH)
    node = Probe(config("t", {"knowledge_type": "local", "db": 9}), observation_backend=journal)
    if start:
        node.start()
    return node, journal


def trigger(node, journal, tmp_path, body):
    node.body = body
    LocalBus.get().publish("/probe", "{}")
    return records(journal, tmp_path)


def test_setup_write_and_read_before_start_record_only_the_write(tmp_path):
    node, journal = probe(start=False)
    node.write_knowledge(Settings())
    assert node.read_knowledge(Settings).threshold == 5.0
    assert [r["$tag"] for r in records(journal, tmp_path)] == ["Knowledge"]


def test_typed_access_outside_an_activation_raises_after_start():
    node, _ = probe()
    with pytest.raises(ObservationError):
        node.write_knowledge(Settings())
    with pytest.raises(ObservationError):
        node.read_knowledge(Settings)
    with pytest.raises(ObservationError):
        node.publish_event("e", {})
    assert node.observation_failed


def test_unobserved_typed_read_of_an_absent_key_is_none():
    reset_local_stores()
    cfg = config("t", {"knowledge_type": "local", "db": 9})
    del cfg["Observation_Config"]
    node = Node(cfg)
    assert node.read_knowledge(Settings) is None
    node.write_knowledge(Settings())
    assert node.read_knowledge(Settings).threshold == 5.0


def test_reads_return_fresh_objects_and_absent_keys_read_null(tmp_path):
    node, journal = probe()
    seen = []
    def body(message):
        seen.append(node.read_knowledge(Reading))
        node.write_knowledge(Reading(4.0))
        a, b = node.read_knowledge(Reading), node.read_knowledge(Reading)
        seen.extend([a, b])
        return outcomes.Monitored(b)
    recs = trigger(node, journal, tmp_path, body)
    assert seen[0] is None and seen[1] is not seen[2] and seen[1].value == 4.0
    reads = [r for r in recs if r["payload"].get("step", {}).get("$tag") == "Read"]
    assert [r["payload"]["knowledge"]["$tag"] for r in reads] == ["Nothing", "One", "One"]
    assert all(r["payload"]["step"]["payload"]["key"] == "Reading" for r in reads)


def test_evidence_resolves_the_latest_acquisition_of_that_object(tmp_path):
    node, journal = probe()
    def body(message):
        obj = Reading(1.0)
        node.write_knowledge(obj)
        obj.value = 2.0
        node.write_knowledge(obj)
        node.publish_event("e", {}, evidence={"r": obj})
        return outcomes.Monitored(obj)
    recs = trigger(node, journal, tmp_path, body)
    event = next(r for r in recs if r["payload"].get("step", {}).get("$tag") == "Event")
    ref = event["payload"]["knowledge"]["payload"]
    assert ref["global_write_sequence"] == 2
    version = next(r["payload"]["version"] for r in recs
                   if r["$tag"] == "Knowledge" and r["payload"]["version"]["ref"] == ref)
    assert version["value"]["payload"]["value"] == 2.0
    assert len([r for r in recs if r["$tag"] == "Knowledge"]) == 2


def test_unacquired_evidence_and_outcome_objects_are_refused(tmp_path):
    node, journal = probe()
    stranger = Reading(9.0)

    def publish(message):
        node.publish_event("e", {}, evidence={"r": stranger})

    with pytest.raises(ObservationError):
        trigger(node, journal, tmp_path, publish)
    assert node.observation_failed

    node, journal = probe()
    with pytest.raises(ObservationError):
        trigger(node, journal, tmp_path, lambda m: outcomes.Monitored(stranger))
    assert node.observation_failed


def test_non_outcome_return_is_an_observation_error(tmp_path):
    node, journal = probe()
    with pytest.raises(ObservationError):
        trigger(node, journal, tmp_path, lambda m: None)
    assert node.observation_failed


def test_callback_exception_is_journalled_and_reraised(tmp_path):
    node, journal = probe()

    def body(message):
        raise RuntimeError("boom")

    with pytest.raises(RuntimeError):
        trigger(node, journal, tmp_path, body)
    assert node.observation_failed
    end = steps(records(journal, tmp_path), "End")[0]
    assert end["payload"]["outcome"] == {"$tag": "Aborted", "payload": {
        "reason": {"$tag": "Failed", "payload": "callback_exception"}}}


def test_services_and_default_request_id(tmp_path):
    node, journal = probe()
    def body(message):
        rid = node.service_started("capture")
        node.service_completed("capture", rid, "NotDelivered")
        return outcomes.Nominal()
    recs = trigger(node, journal, tmp_path, body)
    act = recs[0]["payload"]["activation"]["activation_id"]
    started, completed = steps(recs, "ServiceStarted")[0], steps(recs, "ServiceCompleted")[0]
    assert started["payload"] == {"service": "capture", "request_id": f"capture/{EPOCH}/{act}"}
    assert completed["payload"]["outcome"] == {"$tag": "NotDelivered"}


def test_every_outcome_encodes_to_the_fixed_vocabulary():
    r = {"x": 1}
    resolve = lambda obj: obj           # noqa: E731
    cases = [
        (outcomes.Monitored(r), {"$tag": "Monitored", "payload": {"observation": r}}),
        (outcomes.Anomaly(r), {"$tag": "Analysed", "payload": {
            "$tag": "Anomaly", "payload": {"evidence": r}}}),
        (outcomes.NoPlan(), {"$tag": "Planned", "payload": {"$tag": "NoPlan"}}),
        (outcomes.Generated(r), {"$tag": "Planned", "payload": {
            "$tag": "Generated", "payload": {"plan": r}}}),
        (outcomes.Rejected(r, r), {"$tag": "Legitimised", "payload": {
            "$tag": "Rejected", "payload": {"plan": r, "legitimate_result": r}}}),
        (outcomes.Applied(r, r), {"$tag": "Executed", "payload": {
            "$tag": "Applied", "payload": {"plan": r, "effect": r}}}),
        (outcomes.NotApplied(r, outcomes.BudgetExhausted()), {"$tag": "Executed", "payload": {
            "$tag": "NotApplied", "payload": {"plan": r, "reason": {"$tag": "BudgetExhausted"}}}}),
        (outcomes.Aborted("why"), {"$tag": "Aborted", "payload": {
            "reason": {"$tag": "Failed", "payload": "why"}}}),
    ]
    for outcome, wire in cases:
        assert outcome.wire(resolve) == wire
    with pytest.raises(TypeError):
        outcomes.Applied(r)


# -- memory backend and dump --------------------------------------------------------------
def test_create_run_refuses_an_existing_run():
    journal = MemoryJournal()
    create_run("t", backend=journal)
    with pytest.raises(ObservationError):
        create_run("t", backend=journal)


def test_memory_admission_is_contiguous_under_threads(tmp_path):
    journal = MemoryJournal()
    create_run("t", backend=journal, epoch=EPOCH)
    node = Monitor(config("t", {"knowledge_type": "local", "db": 3}), observation_backend=journal)
    node.start()
    threads = [threading.Thread(target=lambda: [LocalBus.get().publish(
        "/sensor", json.dumps({"value": 1.0})) for _ in range(20)]) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    recs = records(journal, tmp_path)
    check_contiguous(recs)
    assert len(steps(recs, "Start")) == len(steps(recs, "End")) == 80


def test_dump_reports_epoch_count_and_hash(tmp_path):
    import hashlib
    _, source = run_nominal("t")
    path = tmp_path / "j.jsonl"
    summary = dump(source, "t", path)
    assert summary == {"epoch": EPOCH, "through": 9, "count": 9,
                       "sha256": hashlib.sha256(path.read_bytes()).hexdigest()}


# -- Redis --------------------------------------------------------------------------------
def test_redis_run_equals_memory_run(redis_config, tmp_path):
    nodes, source = redis_run(redis_config)
    assert not any(n.observation_failed for n in nodes)
    redis_records = records(source, tmp_path, redis_config["namespace"])
    from rpclpy.rpio_observation.journal import RedisJournal
    reader = RedisJournal(source, redis_config["namespace"])
    try:
        assert {e.journal_clock for e in reader.entries()} == {"redis.server.time"}
    finally:
        reader.close()
    _, memory = run_nominal("t")
    assert normalise(json.dumps(r) for r in redis_records) == normalise(
        json.dumps(r) for r in records(memory, tmp_path))


def test_redis_layout_counts_writes_separately_from_stream_cursors(redis_config):
    import redis
    redis_run(redis_config)
    client = redis.Redis(decode_responses=True, **{k: redis_config[k] for k in ("host", "port", "db")})
    ns = redis_config["namespace"]
    assert len(client.xrange(ns + ":phase")) == 7
    assert len(client.xrange(ns + ":knowledge")) == 2
    assert json.loads(client.hget(ns + ":latest", "Reading"))["global_write_sequence"] == 2
    assert client.get(ns + ":seq") == "2"


def test_redis_create_run_refuses_an_existing_namespace(redis_config):
    cfg = {k: redis_config[k] for k in ("host", "port", "db")}
    create_run(redis_config["namespace"], redis_config=cfg)
    with pytest.raises(ObservationError):
        create_run(redis_config["namespace"], redis_config=cfg)


def test_redis_data_cannot_collide_with_placeholders(redis_config, tmp_path):
    """Application text that looks like a placeholder, a Lua pattern or a format is stored verbatim."""
    nasty = "SEQ TIME READ %d %1 \x01SEQ\x01 \x01READ\x01 \u0001 \n"

    class Note(Node):
        def __init__(self, cfg, **kw):
            super().__init__(cfg, **kw)
            self.register_message_callback("/note", self.on_note)

        def on_note(self, message):
            note = Reading(nasty)
            self.write_knowledge(note)
            return outcomes.Monitored(self.read_knowledge(Reading))

    nodes, source = redis_start(redis_config, node_classes=(Note,))
    LocalBus.get().publish("/note", json.dumps({"text": nasty}))
    nodes[0].shutdown()
    recs = records(source, tmp_path, redis_config["namespace"])
    check_contiguous(recs)
    assert steps(recs, "Start")[0]["payload"]["trigger"]["payload"]["message"] == {"text": nasty}
    assert next(r["payload"]["version"]["value"]["payload"]["value"] for r in recs if r["$tag"] == "Knowledge") == nasty


def test_redis_concurrent_admission_is_contiguous(redis_config, tmp_path):
    nodes, source = redis_start(redis_config)
    threads = [threading.Thread(target=lambda: [LocalBus.get().publish(
        "/sensor", json.dumps({"value": 1.0})) for _ in range(10)]) for _ in range(4)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    [n.shutdown() for n in nodes]
    recs = records(source, tmp_path, redis_config["namespace"])
    check_contiguous(recs)
    assert len(steps(recs, "Start")) == 80

def test_application_codec_checks_slots_without_replacing_acquired_versions(tmp_path):
    from rpclpy.rpio_observation.wire import JsonCodec
    node, journal = probe()
    class Codec(JsonCodec):
        def validate_evidence(self, kind, topic, slots):
            if set(slots) != {"reading"} or slots["reading"]["key"] != "Reading":
                raise ValueError("undeclared evidence slot")
        def knowledge(self, key, value):
            return {"$tag": key, "payload": {"typed_value": value["value"]}}
    node._observation.codec = Codec()
    def body(message):
        obj = Reading(2.0)
        node.write_knowledge(obj)
        node.publish_event("e", {"value": 2.0}, evidence={"wrong": obj})
        return outcomes.Nominal()
    with pytest.raises(ObservationError, match="undeclared evidence slot"):
        trigger(node, journal, tmp_path, body)
    assert node.observation_failed
    recs = records(journal, tmp_path)
    assert not steps(recs, "Event")
    committed = next(r["payload"]["version"] for r in recs if r["$tag"] == "Knowledge")
    assert committed["value"] == {"$tag": "Reading", "payload": {"typed_value": 2.0}}
    # Type representation is separate from the authoritative raw application value.
    assert journal.read_latest("Reading").value["value"] == 2.0


def test_publication_transport_failure_keeps_intent_and_explicit_abort(tmp_path, monkeypatch):
    node, journal = probe()
    def fail_transport(topic, message):
        raise RuntimeError("transport unavailable")
    monkeypatch.setattr(node.event_manager, "publish", fail_transport)
    def body(message):
        obj = Reading(2.0)
        node.write_knowledge(obj)
        node.publish_event("e", {}, evidence={"reading": obj})
        return outcomes.Monitored(obj)
    with pytest.raises(RuntimeError, match="transport unavailable"):
        trigger(node, journal, tmp_path, body)
    recs = records(journal, tmp_path)
    assert node.observation_failed
    assert len(steps(recs, "Event")) == 1
    assert steps(recs, "End")[0]["payload"]["outcome"]["$tag"] == "Aborted"
    assert len(steps(recs, "Start")) == 1  # no receiving callback was invented
