# rpio_observation reference

Inspect the installed library (`rpclpy/rpio_observation`) before changing an application; it is
authoritative over any example here.

| Module | Defines |
|---|---|
| `core.py` | immutable Knowledge store, versions/refs, journal entry types |
| `observation.py` | `NodeObservation`: callback wrapping, activations, read/write/publish/service |
| `outcomes.py` | outcome vocabulary a callback must return |
| `journal.py` | `MemoryJournal`, `RedisJournal`, `create_run`, `dump` |
| `wire.py` | record shapes and the default `JsonCodec` |

## Facts verified against the library
- `Observation_Config` keys: `namespace` (required), `loop`, `role`, `producer`, `run_id`, `codec`.
- A codec provides `knowledge`, `trigger`, `emitted`, `outcome` and optionally `validate_evidence`.
  A Knowledge value must be tagged with its key.
- Outcome constructors take Knowledge objects acquired in the activation: `Monitored(obs)`, `Nominal()`,
  `Anomaly(evidence)`, `Generated(plan)`, `NoPlan()`, `Accepted(plan, result)`, `Rejected(plan, result)`,
  `Applied(plan, effect)`, `NotApplied(plan, reason)`, `Aborted(reason)`. `reason` is a `str` or
  `BudgetExhausted()`.
- An exception in an observed callback journals `Aborted("callback_exception")`, sets `observation_failed`
  and propagates.
- `create_run(namespace, backend=... | redis_config=...)` must precede node construction; it returns the epoch.
- `dump(source, namespace, path)` returns `{epoch, through, count, sha256}`.

## Not part of the library (application code, see `assets/`)
`callback_failed`, `MapleNode`, the trial harness, the typed codec and `observation_schema.json`.

Validate a local memory trial before adding Redis or invoking the Trustworthiness Checker.
