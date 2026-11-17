---
name: integrate-tc-observation
description: Integrate the Trustworthiness Checker (TC) into an rpclpy / RoboSAPIENS MAPLE-K repository that already has trustworthiness specs (.dsrv/.lola) but no observation. Adds rpio_observation (Observation_Config, outcomes, evidence slots, service boundaries, schema, codec, trial harness) so TC can replay or live-check the journal. Use when asked to "integrate TC", "add rpio_observation", or "make a project observable by the trustworthiness checker".
---

# Integrate TC via rpio_observation

Goal: take a repo that has a MAPLE-K loop on `rpclpy` plus trust specs, and make its nodes emit the
classic Phase + Knowledge journals that TC consumes. Start from the working templates in `assets/` (`observation_codec.py`, `observation_trial.py`,
`runtime.py`, `observation_schema.json`; exercised by the rpclpy test-suite). The ScrewDetection repo is a
larger real-world example (`managing_system/nodes.py`, `correlation.py`, `docs/observation-configuration.md`). Library:
https://github.com/INTO-CPS-Association/RoboSAPIENS-Adaptive-Platform (`rpclpy/rpio_observation`, a git
submodule; README at https://github.com/INTO-CPS-Association/rpio-observation).

Do not invent API. Where this skill and the installed library disagree, read the installed
`rpio_observation` sources (`core.py observation.py outcomes.py journal.py wire.py`) and follow them.
Read `references/rpio_observation.md` (library facts and limits) before changing application code.

## Model in one paragraph

Observation is opt-in per node: only a node whose config has `Observation_Config` is observed. Observed
nodes journal (a) each callback as Start/End Phase records, (b) typed Knowledge reads/writes with exact
versions, (c) each `publish_event` (with the evidence objects cited), (d) `service_started/completed`
markers. Immutable Knowledge values go to a separate Knowledge stream; Phase records cite them by ref.
The callback's **return value** is its End outcome. TC then reads the journal (live via
`rpio-observation-clock`, or `*.jsonl` replay via `rpio-observation-clock-file`) and the DSRV specs
check properties. Unobserved nodes ignore all of these calls, so the change is behaviour-preserving.

## Procedure

### 0. Survey (read-only; report findings before editing)
- Find the MAPLE-K nodes (`class X(Node)`), their `register_event_callback`s, `read_knowledge`/`write_knowledge`
  use, `publish_event` calls, config YAML, and the spec files (`*.dsrv`, `*.lola`, property catalogue).
- Check the installed rpclpy: `pip show rpclpy`; `python -c "import rpio_observation"`. If missing, the
  platform repo must be on `PYTHONPATH` with submodules (`git submodule update --init`) or rpclpy upgraded
  to the latest version. Don't hand-install into the repo; tell the user.
- Extract from the specs: Knowledge keys + fields they read, event topics, evidence each property needs,
  service/deadline names. The specs define what must be observable; the nodes define what can be.
- Confirm Redis 7 availability for Redis transport (see Pitfalls for the Windows host note).

### 1. Types: `observation_schema.json` (template: `assets/observation_schema.json`)
`{"schema_version": 2, "knowledge": {Key: {field: type}}, "message": {...}, "trigger": {...}}`.
Types are DSRV syntax: `Int Float Bool Str Any Option<T> List<T>`. One entry per Knowledge class
(`cls.name` must equal the key). Every field the code writes must be declared; optional fields must
be `Option<...>`. Message/trigger carry business fields only (ids, reason); observation provenance is
added by rpclpy.

### 2. Codec: `observation_codec.py` (template: `assets/observation_codec.py`)
The codec is optional: without one rpclpy uses a default `JsonCodec` (untyped tagged JSON). Add the typed
codec when the DSRV specs need typed Knowledge/Option fields.
Class with `knowledge(key, value)`, `trigger(kind, topic, message)`, `emitted(kind, topic, message)`,
`outcome(wire)`, `validate_evidence(kind, topic, slots)`; expose `CODEC = ...`. Declare closed
vocabularies: `EVENTS` (topic -> DSRV event tag) and `SLOTS` (evidence name -> Knowledge key).
`Option` fields encode as tagged `{"$tag":"Some","payload":v}` / `{"$tag":"None"}`. Start from the template
codec; it computes no verdicts.

### 3. Failure policy: `runtime.py` (template: `assets/runtime.py`)
`MapleNode(Node)` with `callback_failed = False`, and `safe_callback` that sets the flag and **re-raises
when `self._observation is not None`** (rpclpy then journals an `Aborted` End and sets
`observation_failed`); unobserved nodes log and keep listening (rpclpy listener threads die on uncaught
exceptions). Wrap callbacks with a shared `RLock` if several nodes share a process.

### 4. Nodes
- Subclass `MapleNode`; constructor `__init__(self, config=None, verbose=True, **options)` ->
  `super().__init__(config=config, verbose=verbose, **options)` (options carries `observation_backend`).
- Each callback **returns an outcome** from `rpio_observation.outcomes`, naming the *acquired* Knowledge
  objects: `Monitored(obs)`, `Nominal()`, `Anomaly(evidence)`, `Generated(plan)`, `NoPlan()`,
  `Accepted(plan, result)`, `Rejected(plan, result)`, `Applied(plan, effect)`, `NotApplied(plan, reason)`,
  `Aborted(reason | BudgetExhausted())`. Every early `return` needs one (this is where most bugs hide).
- Replace `publish_event(event_key=X)` with `publish_event(X, payload, evidence={slot: obj})`; objects in
  `evidence` must have been read or written *in this activation*. Slot names must match the codec `SLOTS`
  and what the specs' evidence checks require.
- Put `service_started(id)` / `service_completed(id, outcome="Delivered"|"NotDelivered")` at the provider
  boundaries the specs budget (see their SV-/deadline definitions); branch-conditional services only on
  the taken branch.
- Knowledge classes need a class attribute `name` equal to the key; `write_knowledge(obj)` journals `obj.__dict__`.
- Use only typed `read_knowledge(Cls)` / `write_knowledge(obj)`; raw-string access is not observed.
  After `start()`, typed reads/writes/publishes **outside a callback** (timers, background threads) raise
  `ObservationError` and set `observation_failed`; do them inside a registered callback. Before `start()`
  (constructors) writes are journalled and reads are not.
  `read_knowledge` may return `None` for a never-written key: handle it.
- Correlation ids (run/cycle/episode/attempt) are application data placed in Knowledge and payloads; keep
  them separate from observation identities (see ScrewDetection `correlation.py`). Needed only if specs match
  causes by id.
- `build_nodes(config, **node_options)`: construct and register all nodes first, then start all; shut down
  already-built nodes if construction fails.
- Initial Knowledge written in constructors is journalled only if the run exists first (step 5).

### 5. Config and trial harness
- `config.yaml`: add `Observation_Config: {loop: [<name>]}` as a **template**. Library keys: `namespace`
  (required, set per trial), `loop`, `role` and `producer` (both default to the class name), `run_id`
  (defaults to the epoch), `codec` (set per trial). Knowledge must be `redis`, or `local` with
  `observation_backend=MemoryJournal()`; anything else raises `ObservationError`. Remove leading `/` from event topics if the
  specs use bare names. Add any new topics nodes now publish (e.g. completion/abort events) to `eventOut`.
- `observation_trial.py`: start from `assets/observation_trial.py` (`trial_config`, `ObservedTrial.open/build/finish`,
  `TrialFailed`). Order is the protocol: fresh namespace -> `create_run(...)` **before any node exists** ->
  build/start nodes -> workload -> `shutdown` -> fail if any node has `observation_failed` or
  `callback_failed` -> `dump(...)` to `out/<name>.jsonl` + `<name>.trial.json` (namespace, epoch, count,
  sha256, journal clock). A failed trial must write no journal.
- Transports the library supports: memory (`local` Knowledge + shared `MemoryJournal`, pass
  `observation_backend=`) and Redis (`knowledge_type: redis`; the journal lives in the same Redis under the
  namespace). Event/message buses are separate (`local` or Redis pub/sub) and configured as usual.
- Add an entry point (e.g. `simulation.py --observe OUT [--redis HOST:PORT]`).

### 6. Specs: connect to TC
- Specs must read the journal: `in journal_input`, adapter/`lib` modules, declarations generated from
  `observation_schema.json`. Don't rewrite the user's properties; if they use the older event/latest-value
  contract, keep them and add journal-based roots alongside (ScrewDetection keeps originals byte-for-byte
  and adds `screw-detection/`).
- Set `observation_clock.dsrv` label: `"redis.server.time"` for Redis captures,
  `"python:time.time_ns"` for memory captures.
- Replay config:
  `{"sources":{"journal":{"kind":"rpio-observation-clock-file","variable":"journal_input","journal_clock":"redis.server.time","files":["trial.jsonl"],"clock_values":[]}}}`
  run `trustworthiness_checker <root>.dsrv --input-config trial.json --output-stdout --semantics gradual-typed-untimed --execution-policy synchronous`.
  Live: `rpio-observation-clock` with `phase_stream`, `knowledge_stream`, clock label and period.
- The checker CLI flags and clock kinds above belong to the TC project and may change; confirm them
  against its docs. The TC checker comes from the RV team; if the DSRV generator / adapters aren't in the repo, ask the user
  where TC lives rather than guessing. WSL note: redis may be shared with Windows (mirrored networking).

### 7. Tests and verification
- Memory trial test: build trial, run one nominal workload and one anomaly workload, assert
  `finish()` succeeds, journal has Start/End per callback, Knowledge records for every written key,
  evidence slots pass `validate_evidence`.
- Failure test: a callback that raises -> `TrialFailed`, no journal.
- Unobserved test: nodes built without `Observation_Config` behave exactly as before (run existing tests).
- Run existing test-suite before and after; report any behaviour diffs.
- Replay the dumped journal through TC if the binary is available; otherwise say it was not run.

### 8. Docs
Add `docs/observation-configuration.md` (files table, config, trial usage, services table, test commands)
modelled on the ScrewDetection example. State clearly what is **not** observed (raw `read`/`write`, sensor drivers,
bridges).

## Pitfalls
- Outcome missing on an early return -> End record without outcome, TC `missing_publication`/protocol finding.
- Evidence object not read/written in this activation -> rpclpy refuses it.
- Writing a Knowledge field not in the schema, or non-`Option` field set to `None` -> codec error.
- `create_run` after node construction -> constructor writes not journalled; startup looks incomplete.
- Reusing a namespace -> rejected (epoch bound to namespace); always generate a fresh one.
- Uncaught exception in an unobserved node silently kills its listener thread.
- Windows: `127.0.0.1` not `localhost` (21 s IPv6 stall).
- A callback returning anything but an `outcomes` object (including `None`) -> `ObservationError`, trial failed.

## Report back
Summarise: files added/changed, outcomes per callback, evidence slots, services marked, tests run and
results, and anything unverifiable (e.g. TC binary not run). Ask before committing.
