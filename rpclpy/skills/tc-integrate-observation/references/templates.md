# Templates (generic; fill the `<...>` from the use case's .dsrv)

## 1. observation_schema.json

```json
{
  "schema_version": 2,
  "knowledge": {
    "<KnowledgeClass>": {"<field>": "Int", "<optional_field>": "Option<Float>", "<list>": "List<Float>"}
  },
  "message": {"<id_field>": "Option<Str>", "reason": "Option<Str>"},
  "trigger": {"<id_field>": "Option<Str>", "<request_field>": "Option<Any>"}
}
```

Types are DSRV: `Int Float Bool Str Any Option<T> List<T>`. `message` = business fields of events the
nodes send; `trigger` = fields of the message that starts a callback. Provenance is added by rpclpy.

## 2. observation_codec.py

```python
import json
from pathlib import Path

SCHEMA = json.loads(Path(__file__).with_name("observation_schema.json").read_text())
EVENTS = {"<topic>": "<DsrvEventTag>"}          # from the spec's event names
SLOTS = {"<slot>": "<KnowledgeClass>"}           # evidence slot -> Knowledge key
REQUEST_TOPICS = {"<external_request_topic>": "<RequestTag>"}  # optional: first Monitor trigger


def tag(name, payload=...):
    return {"$tag": name, **({"payload": payload} if payload is not ... else {})}


def option(v):
    return tag("None") if v is None else tag("Some", v)


def _fields(value, declared):
    return {n: option(value.get(n)) if t.startswith("Option<") else value[n]
            for n, t in declared.items()}


class Codec:
    def knowledge(self, key, value):
        return tag(key, {**value, **_fields(value, SCHEMA["knowledge"][key])})

    def trigger(self, kind, topic, message):
        if isinstance(message, str):
            try:
                message = json.loads(message)
            except ValueError:
                return tag("Unrecognized", message)
        if topic in REQUEST_TOPICS:
            return tag(REQUEST_TOPICS[topic], _fields(message or {}, SCHEMA["trigger"]))
        if topic in EVENTS:
            return tag(EVENTS[topic], self.emitted(kind, topic, message or {}))
        return tag("Unrecognized", json.dumps(message, sort_keys=True))

    def emitted(self, kind, topic, message):
        if topic not in EVENTS:
            raise ValueError(f"undeclared publication {topic!r}")
        return {"detail": tag(EVENTS[topic]), "message": _fields(message, SCHEMA["message"])}

    def outcome(self, wire):
        return wire

    def validate_evidence(self, kind, topic, slots):
        for name, ref in slots.items():
            if SLOTS.get(name) != ref["key"]:
                raise ValueError(f"{topic}: undeclared or mistyped evidence slot {name!r}")


CODEC = Codec()
```

If an event needs extra detail (e.g. an abort reason), put it in that event's `tag(..., {...})` inside
`emitted`, exactly as the spec's event type declares it. If the spec gives one evidence slot different
Knowledge keys per topic, make `validate_evidence` look the expected key up per topic.

## 3. runtime.py

```python
import functools
import threading
from rpclpy.node import Node

LOCK = threading.RLock()   # share one lock if several nodes run in one process


def safe_callback(fn):
    @functools.wraps(fn)
    def wrapper(self, msg):
        try:
            with LOCK:
                return fn(self, msg)
        except Exception:
            self.callback_failed = True
            self.logger.exception("%s.%s failed", type(self).__name__, fn.__name__)
            if self._observation is not None:
                raise          # rpclpy journals Aborted and sets observation_failed
    return wrapper


class MapleNode(Node):
    callback_failed = False
```

Node edits: `class X(MapleNode)`; `def __init__(self, config=None, verbose=True, **options):` calling
`super().__init__(config=config, verbose=verbose, **options)`; `@safe_callback` directly on the callback
(under `@timeit_callback` if the code uses it).

## 4. observation_trial.py (core)

```python
import copy, json, pathlib, uuid
from rpio_observation import MemoryJournal, create_run, dump

ROLES = ("<Role1>", "<Role2>")   # node class names = config sections "<Role>_Config"


class TrialFailed(RuntimeError):
    pass


def fresh_namespace(prefix="trial"):
    return f"{prefix}-{uuid.uuid4().hex[:12]}"


def trial_config(config, *, namespace):
    from observation_codec import CODEC
    tpl = dict(config.get("Observation_Config") or {})
    out = copy.deepcopy(dict(config))
    out.pop("Observation_Config", None)
    for role in ROLES:
        sec = out[f"{role}_Config"]
        sec["Observation_Config"] = {**copy.deepcopy(tpl), "namespace": namespace, "codec": CODEC}
        sec["Knowledge_Config"] = {"knowledge_type": "local", "db": 0}
        for k in ("Event_Manager_Config", "Message_Manager_Config"):
            sec[k] = {"protocol": "local", "bus": namespace}
    return out


class ObservedTrial:
    def __init__(self, config, namespace, epoch, memory, out, name):
        self.config, self.namespace, self.epoch = config, namespace, epoch
        self.memory, self.out, self.name, self.nodes = memory, pathlib.Path(out), name, []

    @classmethod
    def open(cls, config, out, name="trial"):
        pathlib.Path(out).mkdir(parents=True, exist_ok=True)
        ns = config[f"{ROLES[0]}_Config"]["Observation_Config"]["namespace"]
        mem = MemoryJournal()
        return cls(config, ns, create_run(ns, backend=mem), mem, out, name)   # BEFORE any node

    def build(self, build_nodes):
        self.nodes = list(build_nodes(self.config, observation_backend=self.memory))
        return self.nodes

    def failures(self):
        return [(type(n).__name__, "observation failed" if n.observation_failed else "callback raised")
                for n in self.nodes if n.observation_failed or getattr(n, "callback_failed", False)]

    def finish(self):
        for n in self.nodes:
            n.shutdown()
        bad = self.failures()
        if bad:
            raise TrialFailed(f"trial failed, no journal written: {bad}")
        jl = self.out / f"{self.name}.jsonl"
        s = dump(self.memory, self.namespace, jl)
        rec = {"namespace": self.namespace, "epoch": s["epoch"], "journal_file": jl.name,
               "record_count": s["count"], "sha256": s["sha256"],
               "journal_clock": "python:time.time_ns"}
        (self.out / f"{self.name}.trial.json").write_text(json.dumps(rec, indent=2) + "\n")
        return rec
```

This is the `local` (memory) transport: enough to get a journal for replay. For Redis captures set
`Knowledge_Config.knowledge_type: redis`, `Event_Manager_Config` / `Message_Manager_Config` `protocol: redis`,
call `create_run(namespace, redis_config={host, port, db})`, `dump(redis_config, ...)`, and use
`journal_clock: redis.server.time`. Refuse a Redis server where another deployment already subscribes to the
loop's channels. `build_nodes` must construct and register all nodes first, then start them, and shut down
already-built nodes if construction fails.

Entry point: add `--observe OUT` to the existing runner:
`trial = ObservedTrial.open(trial_config(cfg, namespace=fresh_namespace()), OUT)`,
`trial.build(build_nodes)`, run the normal workload, then `trial.finish()`.
