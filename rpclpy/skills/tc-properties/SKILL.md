---
name: tc-properties
description: Suggest runtime trustworthiness properties for a system and write them as a DSRV specification for the RoboSapiens trustworthiness checker (TC checker), with an offline trace proving each property fires, validated against the real checker. Use this whenever the user wants to come up with, suggest, review, add or write TC properties, runtime verification or monitoring properties, invariants, safety properties or deadlines for a MAPLE-K, MAPE-K, RAP or rpclpy system, or wants to write or fix a .dsrv spec, even if they only ask "what should we monitor?". To wire a finished spec into a running version of the code, use tc-integrate instead.
---

# Suggest and write TC properties

Read the user's code, propose properties worth monitoring, and write the chosen ones in DSRV with a trace that proves
each one fires. This skill produces the specification. `tc-integrate` connects it to a code version.

Read `references/dsrv_language.md` before writing DSRV, and `references/property_catalog.md` before suggesting.

Step 5 needs a built checker from https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker. Say so early if the
user has none, since everything before that step works without it.

## Step 1: Learn what the system does and what can be seen

Read the code the user points to. If they give none, ask which version to work from. Note these, with `file:line`:

- **Observables**: every topic (`publish_event`, `publish_message`) with its payload fields and units, and every
  knowledge class with its fields. Note what is computed but never leaves the process.
- **Rules already in the code**: thresholds, window sizes, limits, clamps, retries, validation, unit conversions.
- **The adaptation protocol**: stages, the order they run in, back-edges, flags and counters.
- **Known weak spots**: `README`, field notes, `TODO`s, and any existing `.dsrv` spec.

If a spec already exists, read it first and suggest what it lacks instead of starting over.

## Step 2: Suggest properties, and let the user choose

Walk the catalog for this system: before, during and after adaptation, plus timing. Present a table, and don't write
any DSRV yet, because a property set is a judgment about what matters and the user knows that better:

| # | Property (plain language) | Why it matters | Based on | Observable today? | False-alarm risk |
|---|---|---|---|---|---|
| 1 | Pressure stays within sensor range | A bad reading drives a wrong retune | `pump.py:12` | yes, `PumpPressure` | low |

- **Based on** must cite real code or be marked as an assumption, so numbers can be challenged.
- **Observable today?** must be honest. If the value never reaches the bus, say so and name the publish that would be
  needed, which is a code change for `tc-integrate`. Never write a property that silently waits on a stream nothing
  publishes, because it looks exactly like one that passes.
- Recommend a set of 5 to 8, and say which you would cut first. Point out any gaps you noticed, especially after-adaptation
  properties, which are the ones most often missing.

If the user asked you to go ahead without stopping, choose the recommended set and list the assumptions in the report.

## Step 3: Write the spec

Write `<name>.dsrv`, typically in `trustworthiness_specs/`, and don't overwrite an existing spec unless asked. Section 8 of the
language reference shows the layout.

- Put a header comment with the properties in plain language and the run commands.
- One `out` per property, `: Bool`, true means **violated**, with a trailing `// C1 short label`. The dashboard builds its
  rows from these comments. Declarations sharing a comment become one row.
- Finish with an aggregate `out violation: Bool`, the `||` of every property.
- Make every parameter an `aux` with a comment naming the code constant it mirrors, so drift can be spotted later.
- Treat ticks as "the previous message", not as time. Section 3 of the language reference explains why, and it's the
  biggest difference between a spec that passes offline and one that works live.

## Step 4: Write a trace that proves it

Write `<name>.input` covering a normal episode that trips each property, and state the verdicts at the top:

```text
// expect: v_range @ 2
// expect: v_sequence @ 9
```

Trip each property alone, on its own tick, with the other properties false, so a pass shows that this property caught this
fault. Include a quiet stretch and a complete legitimate episode too: a property that fires during correct behaviour would
false-alarm in production, and only a clean episode reveals that.

Start the trace from the system's real startup state, taken from what its nodes seed: empty lists, unset values, and no status
yet if there's a warm-up. Startup is where crashes and false alarms hide, and a trace that begins with healthy data skips it.

## Step 5: Validate

```bash
python ${CLAUDE_SKILL_DIR}/scripts/validate_dsrv.py <name>.dsrv --trace <name>.input
```

This lints for constructs that hang or crash the monitor, then runs the real checker and compares every output against
your expectations, so an output that can never fire fails. Fix errors and rerun until it passes. It finds the checker from
`--checker`, then the `TC_CHECKER` environment variable, then a `trustworthiness_checker/` folder above the spec. On Windows, a Linux
build found that way runs through WSL. If there is no build, it tells you how to make one. `--lint-only` needs no checker.

When a verdict is wrong, work out which side is wrong before editing. A mismatch can mean a bad trace, or a real bug in the
property. Don't adjust expectations to match the output without being able to explain why they were wrong.

## Step 6: Report

```
## Properties
<table: id, output, plain-language meaning, what it is based on>

## Validation
Lint: <errors, warnings, and why any warning is acceptable>
Trace: <PASS/FAIL, ticks, which tick trips which property>
Not verified: offline only. Live behaviour needs tc-integrate.

## Assumptions to check
<thresholds and deadlines not taken from the code or from measurements>

## Left out, and why
<properties considered and dropped, or those needing new published data>
```

A passing trace shows the spec is well-formed and each property reacts to the fault you wrote. It doesn't show the system
really publishes those streams. Say that, and offer `tc-integrate` as the next step.
