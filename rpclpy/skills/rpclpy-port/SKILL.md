---
name: rpclpy-port
description: Port any existing code (a script, algorithm, controller, ML pipeline, ROS node or notebook) into rpclpy Monitor, Analysis, Plan, Legitimate and Execute nodes that share state through rpclpy knowledge, so it integrates with the RAP / RoboSapiens MAPLE-K architecture, then verify the generated nodes do not violate the rpclpy APIs with a static checker and a no-Redis smoke test. Use this whenever the user wants to port, convert, wrap, migrate or refactor code into MAPLE-K, MAPE-K, RAP or rpclpy nodes, make code self-adaptive with an adaptation loop, or add a new Node, even if they just say "integrate this with RAP". Also use it to audit existing rpclpy nodes for API violations.
---

# Port code to rpclpy MAPLE-K nodes

Turn the user's code into five rpclpy nodes coupled only by events and shared knowledge, then prove the result
respects the rpclpy API. Porting is a design job: the hard part is deciding which code belongs to which stage
and what state must become shared knowledge. Verification is the second half of the work, and a port that has
not passed it is not finished.

Read `references/rpclpy_api.md` before generating code. Start from `assets/node_template.py` and
`assets/messages_template.py`. The scripts need rpclpy and PyYAML, so run them with the Python of the user's project.

## Step 1: Understand the source code

Read all of it, including what it imports from the user's own project, then write down:

- **Inputs**: where data comes from and at what rate. This becomes the topic Monitor subscribes to.
- **State**: every variable that outlives one iteration: globals, `self.` attributes, loop accumulators, files.
- **Decisions**: where the code detects that something is wrong.
- **Actions**: what it changes in the outside world.
- **Blocking work**: training, long loops, sleeps, network waits.

If the code has no separable input, or no decision and action, say so. A pure function has nothing to adapt:
offer to wrap it as a single stage with `--partial` instead of inventing a loop around it.

## Step 2: Map it to the stages and confirm

| Stage | Owns | Must not |
|---|---|---|
| Monitor | ingesting input, appending history, seeding knowledge | decide anything |
| Analysis | anomaly or condition detection, recording what it found | choose a response |
| Plan | choosing or computing a candidate response, often slow | change the live system |
| Legitimate | validating the candidate; re-planning if rejected | apply it |
| Execute | applying the validated change to the system | judge it |

Show the user this mapping before generating code: which source function goes to which stage, the knowledge
classes, and the topics. Source code rarely has a Legitimate stage. If nothing validates the plan, propose a real
check and ask about it, because a Legitimate that always approves gives false assurance. A wrong stage split is
expensive to fix after generation, so this is the one place worth stopping to ask.

If the user asked you to go ahead without stopping, make the calls yourself and list them in the final report.

## Step 3: Generate

Write new files next to the user's code, and don't overwrite existing files unless asked:

- `<name>_messages.py`: one class per piece of shared state, following the template. These must stay in their own
  module because of how pickle resolves classes.
- `<name>_maple_k.py`: the five nodes and `main()`.
- `<name>_config.yaml`: `python ${CLAUDE_SKILL_DIR}/scripts/gen_config.py <name>_config.yaml`.

When converting:

- Move the user's algorithms over unchanged and call them from callbacks. Restructure the plumbing around their
  logic, not the logic itself, so the port does not change behaviour. Pure helpers can stay in their own module.
- Turn each piece of cross-stage state into a knowledge class, and seed it in `Monitor.__init__`.
- Replace `while True` loops with event-driven callbacks; the loop body becomes the Monitor callback.
- Move slow work to a thread started from the callback.
- Convert numpy to lists and floats wherever values enter a published message.
- Prefix knowledge names if the loop may share a Redis with another system, since keys are global to the db.
- If the user wants observed nodes (`Observation_Config`), read section 7 of the reference first, because the template's shapes
  change.
- Wherever the source's behaviour was ambiguous, leave a comment and report it instead of guessing silently.

## Step 4: Verify the API contract

Run the static checker, passing topics that come from outside the loop:

```bash
python ${CLAUDE_SKILL_DIR}/scripts/check_rpclpy_api.py <name>_maple_k.py --messages <name>_messages.py \
    --config <name>_config.yaml --external-topic <input_topic>
```

Fix every ERROR. Treat L01 and K08 as errors too in generated code. Read the remaining warnings and either fix
them or be ready to justify them in the report. Rule meanings are in `references/rpclpy_api.md`.

Then run the nodes for real on the in-process bus, which needs no Redis:

```bash
python ${CLAUDE_SKILL_DIR}/scripts/smoke_test_local.py <name>_maple_k.py --config <name>_config.yaml \
    --trigger <input_topic> --payload fixture.json --count 20 \
    --expect new_data,anomaly,new_plan,isLegit
```

Write `fixture.json` from the source code's real input format, with values that actually trigger an anomaly, so
the whole chain runs and not just Monitor. If `--expect` cannot be met, the fixture or the port is wrong, so
investigate before loosening it. Use `--stub <attr>` for module-level clients such as a socketio `sio`, and stub
or shrink slow work like model training rather than skipping the stage.

Fix what these find and rerun both until they pass. If the port changes logic to get there, tell the user.

The smoke test uses `local` transports, which are more forgiving than Redis in one way (see "The string form does
not encode" in the reference) and synchronous. If Redis is running and the user wants more confidence, offer a
live run too.

## Step 5: Report

```
## Port summary
Stage mapping: <table: stage, source function(s), callback>
Knowledge: <class: what it holds, who writes, who reads>
Topics: <chain>

## Verification
Static checker: <N errors, N warnings, and why each remaining warning is acceptable>
Smoke test: <PASS/FAIL, the topics that fired, what the fixture exercised>
Not verified: <e.g. live Redis, real hardware, stubbed components>

## Decisions for you
<behaviour changes, invented Legitimate checks, ambiguities>
```

State plainly anything stubbed or not run. A pass on a stubbed Plan does not show that the real training works.

## Auditing existing nodes

If the user only wants existing nodes checked, run Step 4 on them and report findings without changing anything,
unless they ask for fixes. Add `--partial` if the file holds only some stages.
