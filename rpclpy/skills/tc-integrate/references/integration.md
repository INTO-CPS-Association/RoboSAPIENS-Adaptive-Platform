# Connecting a DSRV spec to a running system

The path a value takes, what can break it, and how to run each piece. Rule ids match `check_tc_integration.py`.
The behaviour described here was observed by running the compiled checker against a real Redis.

## Contents
1. The data path
2. Why the bridge exists
3. Silent failure on bad data
4. What must change together
5. Running it
6. Rule index
7. Getting the checker
8. Observed nodes

## 1. The data path

```
Managed system / nodes
  | publish_event, publish_message: JSON on a pub/sub channel
  | write_knowledge: pickle in a Redis key
  v
tc_bridge.py -- polls keys, caches events, checks payloads, republishes all of it at a fixed rate
  | tc:<Name> channels, plus tc:t and tc:wire_errors
  v
*_redis_input.json5 -- maps each spec input to a tc: channel
  v
trustworthiness_checker <spec>.dsrv --input-redis-file ... --redis-output
  | one Redis channel per output, named after the output, payload true or false
  v
rpclpy dashboard panel, built from the spec's `out <name>: Bool  // label` lines
```

## 2. Why the bridge exists

- **Knowledge is out of reach.** The checker reads JSON. rpclpy stores pickle. The checker's newer `redis-knowledge`
  input does not help: its docs state that Python pickle is unsupported, and it needs keyspace notifications turned on.
- **There is no clock.** Deadline properties need `t`, and rpclpy never publishes time.
- **Rare events would starve the monitor.** So the bridge republishes the newest value of everything each period, and
  specs detect a real event by its `uid` changing.

Run exactly one bridge. Two double every message.

Two messages on one channel inside a bridge period coalesce, since only the newest is republished, and the spec sees just the
last. The default of 2 Hz suits a 1 Hz loop. Raise the rate if the code publishes faster.

List `EVENTS` **in loop order**. A fast loop can run several stages inside one bridge period, and the bridge republishes in
dict order, so that order is what the spec's phase machine sees. One real loop ran anomaly, plan and approval within one
period and stayed clean for this reason.

## 3. Silent failure on bad data

This is the most important thing to know about a live monitor. A malformed value makes the checker panic. On builds before
`v0.2.0` the panic stays **inside one stream**: the process keeps running and most outputs keep updating, but some stop for good.
On `v0.2.0` and `main`, a `null` value made the whole process exit with code 101, and an int on a `Float` field is accepted. The
table was measured on an older build with a real seven-output spec, with one fresh checker for each case, one bad message, then a
real fault:

| Bad message | Panics | Outputs still updating | Real fault detected? |
|---|---|---|---|
| none (control) | 0 | 7 of 7 | yes |
| ints in a `List<Float>` | 4 | 4 of 7 | **no** |
| int on a `Float` field | 1 | 5 of 7 | **no** |
| list too short | 3 | 4 of 7 | **no** |
| missing field | 1 | 5 of 7 | **no** |
| `null` value | 1 | 5 of 7 | **no** |

The aggregate `violation` was among those lost, so nothing looks wrong unless you notice the outputs that stopped.

The spec can do this to itself with perfectly valid data. `if` evaluates both branches, so `if n == 0 then 0.0 else
List.get(l, n - 1)` panics on an empty list, and Monitors commonly seed lists as empty. Lint rule D15 flags this, and the fix is
in the language reference, section 5.

Defences, all needed:

1. **Guard in the bridge.** `_conform` in `assets/tc_bridge_template.py` fits each payload to its schema, turning an int into a
   float, and rejects anything else while keeping the last good value. Without it an integer-valued number from one code path
   can blind the monitor. Python only writes `5.0` for a real float, so `int` values and `numpy` conversions are the usual cause.
2. **Count rejections.** A rejected message would otherwise hide the very fault it carries. The bridge publishes
   `tc:wire_errors`. Route it, and add the wire integrity property from the catalog, so bad data appears as a violation.
3. **Test for silence.** `live_check.py` fails if any output stops updating. An offline trace can't show this.

The dashboard shows a stale row as gray instead of green after `TC_STALE_SEC`, so gray in production means a stopped output,
and it deserves investigating rather than ignoring.

## 4. What must change together

| If this changes in the code | Also change |
|---|---|
| A topic name | the bridge's `EVENTS`, the channel map, the spec input name, and any other backend publishing it |
| A payload field or its type | the bridge schema, the spec's `Struct<...>`, the traces |
| Length or order of a list | the bridge schema length, `List.get` indexes in the spec |
| A knowledge class's `name` or attribute | the bridge's `KNOWLEDGE` |
| A threshold or window | the matching `aux` parameter |
| A stage or back-edge in the loop | the phase state machine in the spec |

If several managed systems or backends feed the same spec, pass each of them with `--code`.

## 5. Running it

Offline, the first check after any edit:

```bash
python <tc-properties skill>/scripts/validate_dsrv.py <spec>.dsrv --trace <spec>.input
```

Live, with the system running. On Windows the checker runs inside WSL:

```bash
cd <checker checkout>
cargo run --profile dev-fast -- <project>/trustworthiness_specs/<spec>.dsrv \
    --input-redis-file <project>/trustworthiness_specs/<spec>_redis_input.json5 \
    --redis-output
```

- `--redis-port N` applies to the route file too.
- `--output-stdout` prints rows instead of publishing them. The dashboard needs `--redis-output`.
- Don't add `--redis-input` beside `--input-redis-file`: the checker refuses to start.
- Route values are plain strings, `stream: "channel"`. The `{ topic: "..." }` form is a parse error.

Starting the bridge in the loop's `main()`:

```python
try:
    from trustworthiness_specs.tc_bridge import run_bridge
    threading.Thread(target=run_bridge, name='TCBridge', daemon=True).start()
except Exception as e:
    print(f"TC bridge not started ({e}); run tc_bridge.py separately if needed.")
```

Showing the properties on the dashboard, which needs `rpclpy>=1.0.4` and `pip install rpclpy[dashboard]`:

```python
threading.Thread(target=run_dashboard, kwargs={'host': '127.0.0.1', 'port': 8050, 'debug': False,
                 'start_trust': True, 'tc_spec_file': 'trustworthiness_specs/<spec>.dsrv'}, daemon=True).start()
```

## 6. Rule index

| Rule | Sev. | Meaning |
|---|---|---|
| I01 | E | spec input without a route, or a route in the rejected object form |
| I02 | W | route matching no input |
| I03 | W | route to a bare channel, not through the bridge |
| I04 | E | route to a channel the bridge never publishes |
| I05 | E | the bridge mirrors a topic no code publishes |
| I06 | E | spec reads a field missing from the bridge payload |
| I07 | E | field type differs between spec and bridge payload |
| I08 | E | `List.get` past the bridge's default list |
| I09 | E | spec reads a field this code's real messages lack |
| I10 | E | the bridge reads a knowledge key no class uses |
| I11 | E | the bridge projects an attribute the code never sets |
| I12 | W | the bridge has no payload guard |
| I13 | W | bridge started in several places, or in none |
| I14 | W | dashboard configured with a different spec, or none |
| I15 | E / W | spec parameter differs from the code / the code has several values |
| I16 | W | no code constant found for a parameter |
| I17 | W | the bridge reads knowledge from an observed system, where it isn't stored in plain keys |

## 7. Getting the checker

The checker is a separate Rust project: https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker

```bash
git clone https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker.git
cd robosapiens-trustworthiness-checker
cargo build --profile dev-fast     # target/dev-fast/trustworthiness_checker
```

Pass that path with `--checker`, or set `TC_CHECKER`. A checkout in a `trustworthiness_checker/` folder above the spec is found
automatically, and on Windows a Linux build found that way runs through WSL. `live_check.py` also needs `redis-server` and
redis-py, so on Windows run it inside WSL.

## 8. Observed nodes

With `Observation_Config`, typed knowledge is committed as JSON to a journal under `{namespace}:latest`, not pickled under its
plain key. The bridge's `KNOWLEDGE` table reads plain keys, so it logs `key not in Redis yet` and those streams never update, while
properties that depend on them stay silent. `EVENTS` still work, since events travel on the normal channels, and their extra
observation stamps are dropped by `_conform`.

For an observed system, monitor through events, or feed the checker from the journal streams (`{namespace}:phase` and
`{namespace}:knowledge`), which needs a codec matching the spec. See the
[rpio-observation README](https://github.com/INTO-CPS-Association/rpio-observation#readme). `check_tc_integration.py --config` warns (I17)
when a bridge reads knowledge from such a system.
