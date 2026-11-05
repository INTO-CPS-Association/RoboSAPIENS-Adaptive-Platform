# DSRV for the trustworthiness checker

The grammar is `src/lang/dsrv/syntax/lalr.lalrpop` in the
[checker repository](https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker). If this file and the grammar
disagree, the grammar wins. Behaviour differs between checker versions, so this file teaches the forms that run on all of them.
Version notes compare a build from August 2026 ("older"), `v0.2.0` and `main` at `6e263c1`.

## Contents
1. Program shape
2. Expressions and built-ins
3. What a tick is
4. Things that break the monitor
5. Idioms
6. Trace format
7. Lint rule index
8. Spec layout

## 1. Program shape

```dsrv
in t: Float                                    // input streams
in Status: Struct<uid: Str, value: Float, ...> // `...` allows extra fields on the wire
aux limit: Float                               // internal stream, not emitted
out v_range: Bool                              // C1 value in range    <- the dashboard label
limit = 10.0
v_range = abs(Status.value) > limit
```

A property output is `true` when **violated**, so everything green means all false. Types: `Int Float Str Bool Unit
Any List<T> Map<T> Struct<...>`, tuples `(A, B)` and functions `(A -> B)`.

## 2. Expressions and built-ins

| Kind | Forms |
|---|---|
| Arithmetic | `+ - * / %`, unary `-` |
| Comparison | `< <= > >= ==`. Write `!(a == b)`: `!=` is a parse error on older builds |
| Boolean | `&& || => !`. `not`, `and` and `or` exist from `v0.2.0` only |
| Strings | `++` |
| Conditional | `if c then a else b` |
| History | `x[n]`, the value n ticks back; field access `x[1].uid` |
| Missing values | `default(x, fallback)`, `is_defined(x)`, `init(a, b)`, `latch(a, b)`, `when(x)`, `update(a, b)` |
| Lists | `List.get(l, i) List.len(l) List.head(l) List.tail(l) List.append(l, x) List.concat(a, b)`, `[a, b]` |
| Higher-order | `List.map(f, l) List.filter(f, l) List.fold(f, init, l)`, lambda `\x: Int -> x + 1` |
| Maps, structs | `s.field`, `Map.get(m, "k") Map.has_key(m, "k") Map.insert(m, "k", v) Map.remove(m, "k")` |
| Math | `abs sin cos tan` |
| Comments | `// line` and `(* block *)` |

Write floats with digits on both sides of the dot: `0.5`, `5.0`, `1.0e3`. `.5` is rejected everywhere, `5.` is rejected from
`v0.2.0`, and `1e3` is rejected on older builds.

## 3. What a tick is

This differs between offline and live runs, and most subtle bugs come from it.

- **Offline**, assignments under one timestamp form one tick, so streams change together.
- **Live on Redis**, each incoming message advances the monitor separately. With 8 inputs at a 5 Hz bridge rate we
  measured about 29 output rows per second for each output, not 5.

Consequences:

- **Ticks are not time.** Use deadlines on the `t` stream. Never count rows as seconds.
- **History means the previous row.** `x[1]` is usually the same value republished, not the last real sample.
- **Republishing must not look like change.** Detect events by a changed `uid`, and growth by a changed length. Never
  accumulate on every row.
- **Do not rely on two streams changing on the same tick.** It passes offline and fails live. Latch the first event and
  compare it when the second arrives, or use deadlines.
- A violation may be true for a single row. That is normal, and the aggregate reports it just as briefly.

## 4. Things that break the monitor

| Cause | What happens | Rule |
|---|---|---|
| `out` with no input dependency, e.g. `out k: Float` / `k = 1.0` | Older builds: infinite generator that never exits and floods `--redis-output`. `v0.2.0` and `main`: finite | D07 |
| Lambda body reads an outer stream | Older builds: hangs with no output. Not confirmed on `v0.2.0` or `main` | D08 |
| `out` or `aux` with no equation | Older builds: panic. `v0.2.0` and `main`: startup error | D02 |
| `Float` compared with an Int literal, `f > 1` | Older builds: panic. `v0.2.0` and `main`: works | D12 |
| `x[1]` without `default` | Undefined on the first rows, printed `Deferred` on older builds and `⊥` from `v0.2.0` | D11 |
| Int on the wire for a `Float` input | Older builds: panic. `v0.2.0` and `main`: accepted | see below |
| `List.get` past the end, missing struct field, `null` | Panic on all versions | see below |
| `if n == 0 then 0.0 else List.get(l, n - 1)` on an empty list | Panic: **`if` evaluates both branches**, so it can't guard anything | D15 |

**Panics on live data stop monitoring.** On older builds the process stays alive after a panic and only some outputs stop:
a single bad message stopped 2 or 3 of 7 outputs, including the aggregate, and a later real violation went undetected. On `v0.2.0`
and `main` a `null` value made the process exit with code 101. The cause lies on the wire rather than in the spec, so `tc-integrate`
handles it, but keep it in mind:

- Index lists only at positions the publisher always provides.
- Declare `Float` only for values that are always floats on the wire. In JSON `5` is an Int and `5.0` is a Float.

Offline quirks that can mislead you:

- On older builds, an output that depends only on history (`p = a[1] > 0`) emits n extra rows after the trace ends. The
  validator ignores rows past the last tick.
- On older builds, the row number in `name[row]` is shared across all outputs. The validator counts rows per name.
- Values print as `Bool(true)` on older builds and `true` from `v0.2.0`. The validator accepts both.

## 5. Idioms

```dsrv
// Event pulse: a uid change means the event fired on this row.
fired = default(!(Event.uid == default(Event[1].uid, "")), false)

// Falling edge of a knowledge flag.
fell = default(default(Flag[1].on, false) && !Flag.on, false)

// Growth guard: only count when a list really grew.
len_now = List.len(L.items)
grew = len_now > default(len_now[1], 0)

// Latch the time an event last happened.
last_t = if fired then t else default(last_t[1], 0.0)

// Last element of a list, safe on an empty one. An `if` around List.get is not safe.
latest = List.fold(\acc: Float, x: Float -> x, 0.0, L.items)
```

Knowledge lists are often **empty at startup**, because a Monitor seeds them as `[]`. A trace that starts with a populated
list won't reveal a panic on the empty one, so begin traces from the state the real node seeds.

Apply `x[n]` only to a stream name. `List.len(l)[1]` does not parse on any version, and `main` also refuses `(a + 1.0)[1]`. Give
the value a name, as above.

Parameters go in `aux`, and only `aux`, with a comment naming the code constant they mirror:

```dsrv
aux threshold: Float   // = threshold in Analysis.analyse
threshold = 2.0
```

Keep lambdas out unless you need them. If you do, they must use only their own parameters and literals.

## 6. Trace format

```text
// expect: v_range @ 2
0: t = 0.0
   Status = {"uid": "s0", "value": 1.0}
1: t = 1.0
   Status = {"uid": "s1", "value": 1.0}
```

- Each stamp is one tick, stamps must not decrease, and values are JSON.
- Give **every input on every tick**. A missing one does not produce a clean gap.
- Write `1.0`, not `1`, for Float streams.
- Unfired events use an empty uid: `Event = {"uid": ""}`.
- Comments are `//` and go above the first tick.

## 7. Lint rule index

| Rule | Sev. | Meaning |
|---|---|---|
| D01 | W | `!=` used: a parse error on older builds |
| D02 | E | `out` or `aux` without an equation |
| D03 / D04 | E | equation for an undeclared name / for an input |
| D05 | E | duplicate declaration or equation |
| D06 | E | reads an undeclared stream |
| D07 | W | `out` depends on no input: floods on older builds |
| D08 | E | lambda captures an outer stream |
| D10 | W | `out` not `: Bool`, or no dashboard label |
| D11 | W | history without `default` |
| D12 | W | Float mixed with an Int literal: panics on older builds |
| D13 | W | no `violation` aggregate, or it omits a property |
| D14 | W | input or aux never read |
| D15 | W | `List.get` with a computed index |
| D16 | W | float with no digits after the point: rejected from `v0.2.0` |
| D17 | W | `[n]` applied to something other than a stream name: refused on `main` |

## 8. Spec layout

```dsrv
// C1: pressure stays within the sensor's range.
// Run: python validate_dsrv.py pump.dsrv --trace pump.input

in t: Float
in PumpStatus: Struct<uid: Str, pressure: Float, ...>

aux pressure_max: Float   // = MAX_PRESSURE in pump.py
aux status_fired: Bool

out v_range: Bool         // C1 pressure in range
out violation: Bool

pressure_max = 10.0
status_fired = default(!(PumpStatus.uid == default(PumpStatus[1].uid, "")), false)
v_range = status_fired && PumpStatus.pressure > pressure_max
violation = v_range
```
