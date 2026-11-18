---
name: tc-integrate-observation
description: Make an rpclpy MAPLE-K use case observable by the Trustworthiness Checker (TC), driven by the use case's own .dsrv spec. Reads the .dsrv and the node code, then makes the smallest code changes (callback outcomes, evidence on publish_event, Observation_Config, schema JSON, codec, trial runner). Use when asked to "integrate TC", "hook up the trustworthiness checker", or "make this use case observable" and a .dsrv file exists.
---

# Integrate TC into a use case (spec-driven, minimal diff)

The `.dsrv` says what must be observable. The node code says what can be. Change only the gap between them.
Never copy from, or point at, any other use-case repo. Everything needed is in this skill and
`references/templates.md`. Library: `rpio_observation` (ships in the RoboSAPIENS Adaptive Platform's
`rpclpy`). If these notes and the installed library disagree, the installed library wins: read its
`outcomes.py` / `core.py` / `journal.py` and adapt. Do not invent API.

Observation is opt-in per node (`Observation_Config` in its config). Unobserved nodes ignore every new call,
so the change must keep existing behaviour and tests green.

## Step 1: Read the spec (`<usecase>.dsrv`, plus `.lola` if present)

Build a table from the `.dsrv`. Report it to the user before editing.

| Take from the spec | Where to look | Becomes |
|---|---|---|
| Event topics | `in`/`out` streams or `event` strings (e.g. `plan_validated`) | `EVENTS` in the codec; `publish_event` topics |
| Knowledge keys + fields | `Struct<...>` types, one per `in` stream | `observation_schema.json` `knowledge` |
| Field types | `Int Float Bool Str Any Option<T> List<T>`; sentinels like `-1` mean `Option` | schema types |
| Evidence a property needs | which Knowledge a property reads when an event fires | `evidence={slot: obj}` and `SLOTS` |
| Services / deadlines | names like `SV-xx`, `deadline`, response budgets | `service_started/completed` |
| Correlation ids | `run_id`, `cycle_id`, ... compared across events | keep as business fields in payload and Knowledge |

If the spec uses a different shape for something (e.g. a field name), the **spec wins**: rename in the schema,
not in the spec. Do not edit the user's properties.

## Step 2: Read the code

Find: `class X(Node)` nodes, each `register_event_callback`, `read_knowledge` / `write_knowledge`,
every `publish_event`, every early `return`, the node config YAML, and the entry point.
Map each callback to a MAPLE-K role (Monitor, Analysis, Plan, Legitimate, Execute) and to the spec topics
it consumes and emits. Anything the spec mentions that the code never does is a **gap to report**, not
something to invent.

Check `python -c "import rpio_observation"`. If it fails, stop and tell the user to upgrade rpclpy or put
the Adaptive Platform on `PYTHONPATH` (`git submodule update --init`). Do not vendor the library.

## Step 3: Minimal edits (in this order)

1. **`observation_schema.json`** (new). Shape and rules in `references/templates.md` section 1. One entry per
   Knowledge class, `cls.name` equal to the key; every field the code writes is declared; nullable means
   `Option<..>`.
2. **`observation_codec.py`** (new). Template section 2. Fill only `EVENTS` and `SLOTS` from the spec table.
3. **`runtime.py`** (new, tiny). Template section 3: `MapleNode` + `safe_callback` (observed: re-raise;
   unobserved: log and keep listening). Then in the nodes: change the base class `Node` to `MapleNode`, add
   `@safe_callback` under the existing decorators, and accept `**options` in `__init__` and pass it to
   `super()`. This is the only structural change.
4. **Each callback**: return an outcome from `rpio_observation.outcomes` on **every** path, naming the
   Knowledge objects acquired in that activation.
   `Monitored(obs)`, `Nominal()`, `Anomaly(evidence)`, `Generated(plan)`, `NoPlan()`,
   `Accepted(plan, result)`, `Rejected(plan, result)`, `Applied(plan, effect)`,
   `NotApplied(plan, reason)`, `Aborted(reason)`.
5. **Each publish**: `publish_event(topic, payload)` becomes
   `publish_event(topic, payload, evidence={slot: obj})`. The objects must have been read or written in
   this callback; slot names must match `SLOTS`. Evidence = what the spec reads when that event fires.
6. **Services**: add `service_started(id)` / `service_completed(id, outcome=...)` only if the spec budgets
   them, at the provider boundary, only on the branch taken.
7. **Typed Knowledge only**: `read_knowledge(Cls)` / `write_knowledge(obj)`. Raw string access is not
   observed. `read_knowledge` can return `None`: guard it.
8. **Config YAML**: add a template `Observation_Config: {loop: [<usecase>]}` (keys: `role`, `loop`,
   `producer` only). Add any newly published topic to the node's `eventOut`. Use bare topic names if the
   spec does.
9. **`observation_trial.py`** (new) and an entry point flag, template section 4. Protocol order: fresh
   namespace, `create_run` **before any node exists**, build and start nodes, run workload, shutdown, fail
   on `observation_failed` or `callback_failed`, then dump `out/<name>.jsonl` + `<name>.trial.json`.
   A failed trial writes no journal.

Prefer additive edits. Do not refactor business logic, rename classes, or reformat files.

## Step 4: Connect the spec to the journal

Keep the user's `.dsrv` properties untouched. Add a thin layer beside them (new files):
- declarations generated from `observation_schema.json` (Knowledge `Struct`s, `Union`, accessors),
- a clock file: label `"redis.server.time"` for Redis captures, `"python:time.time_ns"` for memory.

Where the TC checker, its journal library modules or the DSRV generator live is the RV team's decision.
**Ask the user**; do not guess a path or copy from another repo.

Replay config:

```json
{"sources":{"journal":{"kind":"rpio-observation-clock-file","variable":"journal_input",
 "journal_clock":"redis.server.time","files":["trial.jsonl"],"clock_values":[]}}}
```

```sh
trustworthiness_checker <root>.dsrv --input-config trial.json --output-stdout \
  --semantics gradual-typed-untimed --execution-policy synchronous
```

Live: source `rpio-observation-clock` with `phase_stream`, `knowledge_stream`, clock label and period.

## Step 5: Verify

- Run the existing tests before and after; report any difference.
- Memory trial: one nominal and one anomaly workload; `finish()` succeeds; every callback has Start/End;
  every written key has a Knowledge record; evidence passes `validate_evidence`.
- A callback that raises gives `TrialFailed` and no journal.
- Nodes built without `Observation_Config` behave as before.
- Replay the journal through TC if the binary is available. If not, say it was not run.

## Pitfalls

- Early `return` with no outcome: End without outcome, TC protocol finding. Check every branch.
- Evidence object not touched in this activation: rpclpy refuses it.
- Field written but not in schema, or `None` in a non-`Option` field: codec error.
- `create_run` after node construction: constructor writes are lost; startup looks incomplete.
- Reused namespace: rejected. Always generate a fresh one.
- Unobserved node + uncaught exception: the listener thread dies silently (hence `safe_callback`).
- Windows: use `127.0.0.1`, never `localhost` (about 21 s IPv6 stall). Redis 7 required.
- Redis-transport trials need a Redis no other deployment subscribes to.

## Report back

Spec-to-code table, files added/changed, outcome per callback, evidence slots, services marked, gaps
(spec asks for something the code cannot give), tests run and results, anything unverified. Ask before
committing.
