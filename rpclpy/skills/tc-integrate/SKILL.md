---
name: tc-integrate
description: Integrate a DSRV trustworthiness specification with a specific version of the user's code, so the RoboSapiens trustworthiness checker (TC checker) really monitors it. Finds where spec and code have drifted, wires up the tc_bridge, Redis channel map, clock and dashboard, then proves it end to end with the real checker on a throwaway Redis. Use this whenever the user wants to connect, wire, hook up, run or integrate TC properties or a .dsrv spec with their code, MAPLE-K loop, managed system or a new code version, when code changed and the spec may be stale, or when the checker's outputs or dashboard panel are silent, frozen or gray. To invent or write the properties themselves, use tc-properties first.
---

# Integrate a TC spec with a code version

Connect a DSRV spec to the exact code version the user gives you, and prove the properties are live. Being connected is
easy to get wrong without noticing, since a property that isn't receiving data looks the same as one that passes. The goal
is evidence that each property can fire on this code.

Read `references/integration.md` first. This skill uses `validate_dsrv.py` from the `tc-properties` skill, which sits next to it.

Steps 2 and 3 work on the code alone. Step 4 needs a built checker (section 7 of the reference says how to get one) and `redis-server`.
The property panel needs `rpclpy>=1.0.4` with the `dashboard` extra. If the system's nodes have `Observation_Config`, read section 8
first, because the bridge can't read their knowledge.

## Step 1: Pin down the version and the spec

Establish exactly which files make up the version: its nodes, managed system(s), message classes. If the user names a commit or
branch, read that with `git show <rev>:<path>` or a worktree, not the working tree, and say which one you used. Ask if it's
unclear, because checking the wrong version gives a confident wrong answer.

If there's no spec yet, offer `tc-properties` first.

## Step 2: Find the drift

```bash
python ${CLAUDE_SKILL_DIR}/scripts/check_tc_integration.py --spec <spec>.dsrv --routes <spec>_redis_input.json5 \
    --bridge <bridge>.py --code <node_file>.py --code <managed_system>.py --messages <messages>.py \
    --param <aux_name>=<Class>.<constant>
```

It follows each stream from spec through channel map and bridge to the code that produces it. Pass every backend with `--code`,
and `--external-topic` for anything published from outside those files. If there's no bridge or channel map yet, create them
first (Step 3) and then run this.

Also check by hand what it can't see, reading the spec's property header against the code:

- **Protocol**: do the spec's phase machine and back-edges match this version's stages and event order?
- **Meaning**: is `List.get(status, 3)` still the same quantity in this version? Are units the same?
- **Parameters**: for any reported under I16, find the code constant by reading.

## Step 3: Make the changes

Apply the smallest set that connects the spec. `references/integration.md` section 4 lists what has to change together.

- **Bridge**: extend the existing one, or start from `assets/tc_bridge_template.py`. Each spec input needs an `EVENTS` schema or a
  `KNOWLEDGE` projection. Schema values set the types (`0.0` is Float) and list lengths.
- **Guard**: if the existing bridge forwards payloads unchecked (I12), bring over `_conform`, and add `tc:wire_errors` with its
  property. One bad message can otherwise permanently and silently disable properties. This changes a file the user owns, so
  describe why when you do it.
- **Channel map**: `<spec>_redis_input.json5`, every stream on a `tc:` channel.
- **Startup**: run the bridge exactly once, usually from the loop's `main()`, and point `tc_spec_file` at the spec.
- **Missing data**: if a property needs a value that never reaches the bus, add the publish in the managed system, and mirror it in
  every other backend. Message classes are part of the wire format, so check all readers.

Fix a mismatch on the side that's wrong. If the code changed on purpose, update the spec, and if the code has a bug, say so
instead of loosening the property until it passes. A property that fires on real code may be doing its job.

Don't change adaptation logic to satisfy the monitor without the user's agreement.

## Step 4: Verify

Rerun Step 2 until there are no errors, and run the offline trace:

```bash
python ${CLAUDE_SKILL_DIR}/../tc-properties/scripts/validate_dsrv.py <spec>.dsrv --trace <spec>.input
```

Then test live. Offline can't catch a stream that isn't flowing, a tick-timing bug or a silent panic, so a spec isn't integrated
until this passes. Write `scenario.py` from `assets/scenario_template.py`, with the startup state, a healthy stretch that must stay quiet and one
real fault per property. Copy payloads from what this code sends, or better, run the real nodes in `setup`, so the scenario tests the code and not your
assumptions about it.

**Begin at real startup.** Seed knowledge exactly as this version's nodes do, including empty lists and `None`s, and hold that state through any
warm-up before the first status. Skipping this hid a real failure in one system: its spec stopped two outputs for good on the empty list its
Monitor seeded, and any test seeded with healthy data passed. Make the timing faithful too, so a deadline that alarms during warm-up shows up here.

```bash
# Linux, or inside WSL on Windows (from Git Bash, prefix with MSYS_NO_PATHCONV=1)
python3 ${CLAUDE_SKILL_DIR}/scripts/live_check.py --spec <spec>.dsrv --routes <spec>_redis_input.json5 \
    --bridge <bridge>.py --scenario scenario.py --repo <repo_root> --checker <checker_binary>
```

It starts a private Redis on its own port, so it can't disturb a running system. It fails if an output goes silent, the checker panics,
an expected violation doesn't fire, or a healthy stretch raises a false alarm.

If the bridge has the guard, add one malformed message per schema to the scenario, and confirm all properties keep updating afterwards.

## Step 5: Report

```
## Integration against <version>
Changed: <file: what and why>
Drift found: <mismatch, and which side was fixed>

## Verification
Static: <errors, warnings, and why any warning is acceptable>
Offline trace: <PASS/FAIL>
Live: <PASS/FAIL, outputs updating, which faults fired, malformed messages survived>
Not verified: <e.g. the real managed system, real timing, unmapped parameters>

## For you to decide
<likely bugs found in the code, parameters or deadlines to confirm>

## To run it
<the exact commands for this system>
```

State plainly what the live test used. A scenario that replays payloads shows the monitor chain works. It doesn't show that the real
system sends those payloads, unless the real nodes were running. Deadlines are also only meaningful against real durations, so point out any
that were never measured.
