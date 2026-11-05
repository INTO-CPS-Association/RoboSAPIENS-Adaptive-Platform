# Property catalog

Ways of finding properties for a self-adaptive system, with a DSRV pattern for each. Adapt them; don't paste them
all. A property earns its place when its violation means something a person would act on, and when it could fail
without the code already failing some other way.

Each pattern assumes pulses (`x_fired`) and `t` from `dsrv_language.md`, section 5.

## Contents
1. Before adaptation: is the input trustworthy?
2. Deciding to adapt: is the decision sound?
3. During adaptation: does the loop follow its protocol?
4. After adaptation: did it work?
5. Timing
6. Monitoring the monitor
7. What to leave out

## 1. Before adaptation: is the input trustworthy?

**Physical envelope.** A reading outside what the hardware can produce. Look for: limits in the code, units, datasheets,
clamps, and validation that exists in one place only.

```dsrv
v_range = status_fired && (Status.value < value_min || Status.value > value_max)
```

**Units and frames.** Values that are valid numbers but in the wrong unit. These do the most silent damage, since nothing
crashes. Look for: `deg2rad`, conversions, and comments stating units. A heading of 350 can only be degrees if the code
expects radians.

```dsrv
v_units = status_fired && (Status.heading < 0.0 || Status.heading > two_pi)
```

**Frozen sensor.** The same value repeating while uids advance. Needs real noise in the signal, or it false-alarms.

```dsrv
same = status_fired && Status.value == default(value_prev[1], Status.value + 1.0)
value_prev = Status.value
stuck = if same then default(stuck[1], 0) + 1 else if status_fired then 0 else default(stuck[1], 0)
v_frozen = stuck >= stuck_limit
```

**Implausible jump.** A change between samples too large to be physical. Compare only on a real sample.

## 2. Deciding to adapt: is the decision sound?

**Trigger soundness.** The detector fired, so its condition must really hold. Re-derive the condition independently from
knowledge. Look for: thresholds and window sizes in the Analysis code.

```dsrv
v_anomaly_unsound = anomaly_fired && default(phase[1], 0) == 0
                 && !(len_now > window && run_above >= window)
```

Exclude paths that raise the event legitimately without the condition, such as a re-plan from Legitimate.

**Missed trigger.** The condition held but no anomaly followed within a deadline. This catches a detector that has
stopped working, and its soundness counterpart cannot.

**Baseline sanity.** A reference value, such as `Score0`, that is set once must not change, and must be set before use.

## 3. During adaptation: does the loop follow its protocol?

**Stage ordering.** A state machine over stages. Allow the real back-edges, such as Legitimate re-emitting `anomaly`.

```dsrv
// 0 idle | 1 planning | 2 judging | 3 executing
phase = if anomaly_fired then 1
        else if plan_fired then 2
        else if legit_fired then 3
        else if done then 0
        else default(phase[1], 0)
v_sequence = (plan_fired && !(default(phase[1], 0) == 1))
          || (legit_fired && !(default(phase[1], 0) == 2))
```

**Suppression.** Something that must stay quiet during an episode. Read the flag from the *previous* row, because the
event that sets the flag can land on the same one.

```dsrv
v_silent = data_fired && default(InPlanning[1].in_planning, false)
```

**Unauthorised change.** Live configuration changes only following an approval. This is one of the most valuable
properties, because it holds no matter what went wrong upstream.

```dsrv
gain_changed = !(Gain.gain == default(gain_prev[1], Gain.gain))
gain_prev = Gain.gain
approved = if legit_fired then true else if gain_changed then false else default(approved[1], false)
v_unauthorised = gain_changed && !default(approved[1], false)
```

**Validation bound.** What Legitimate approved really satisfies the safety rule. Re-state the rule independently.

```dsrv
v_unsafe_approved = legit_fired && (Candidate.gain < gain_min || Candidate.gain > gain_max)
```

**Bounded re-planning.** A retry counter stays under a limit, so the loop can't retrain forever.

```dsrv
v_replanning = Replan.replanning_counter > replan_limit
```

## 4. After adaptation: did it work?

Often missing, and the question people care about most. Self-adaptation that never improves anything can pass every
protocol property.

**Recovery.** After Execute, the score must return under its threshold within a deadline.

```dsrv
applied_t = if done then t else default(applied_t[1], -1.0)
v_no_recovery = applied_t >= 0.0 && (t - applied_t) > recovery_deadline && latest_ratio > threshold
```

**No regression.** The metric after adapting is no worse than before it.

**State reset.** Episode flags cleared: `InPlanning` false, counters zeroed. Stale flags block the next episode.

## 5. Timing

Use `t`, never row counts. Choose deadlines from measured durations, not guesses, and state in the report which were
measured. A slow training step will false-alarm against a tight deadline.

```dsrv
status_t = if status_fired then t else default(status_t[1], t)       // the clock starts at the first row
v_timing = ((t - status_t) > status_deadline)                        // input stopped
        || (phase == 1 && (t - phase_entered_t) > plan_deadline)     // stage overran
        || (!(phase == 0) && (t - episode_start_t) > episode_deadline)
```

**Check startup.** If the system publishes nothing for a warm-up period, a freshness deadline alarms the whole time unless
you allow for it. Decide what you want and say so. `default(status_t[1], t)` above stays quiet until the first message, so
it can't detect a system that never starts. For that, add a separate, longer startup deadline.

## 6. Monitoring the monitor

Offer these when integrating, since a dead monitor looks like a healthy one.

**Wire integrity.** Messages the bridge rejected as malformed.

```dsrv
v_wire = wire_errors > default(wire_errors[1], 0)
```

**Clock advancing.** `t` going backwards means two bridges or a restart.

```dsrv
v_clock = t < default(t[1], 0.0)
```

## 7. What to leave out

- **Restating an assertion.** If the code already crashes on the condition, a property adds little.
- **Properties on values that never reach the bus.** Either propose publishing them, as a code change, or drop the
  property. Don't write one that will sit silent forever, since it looks identical to one that passes.
- **Thresholds nobody can justify.** Say where each number came from. If it came from nowhere, mark it as an assumption.
- **Too many outputs.** One real project replaced a 21-property spec with 6 because nobody could read it. Aim for 5 to 8,
  group related checks into one output, and let the user ask for more.
