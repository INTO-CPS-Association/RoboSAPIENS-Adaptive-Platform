"""Scenario for live_check.py: a healthy stretch, then one real fault per property.

Base it on what this code version really sends. A scenario with made-up payloads only proves
the spec matches itself.
"""


def good(value=1.0):
    return {"value": value}  # TODO: the real status payload, with floats written as 1.0


def setup(ctx):
    """Runs before the bridge starts. Seed EXACTLY what the real nodes' __init__ seed, including
    empty lists and None, because startup is where specs panic. Do not seed healthy data here."""
    # from TODO_messages import TODOHistory
    # history = TODOHistory()
    # history.samples = []
    # ctx.write_knowledge(history)


def run(ctx):
    # The real warm-up: seeded knowledge, and no status yet if the system takes time to start.
    ctx.sleep(4.0)

    # A healthy stretch must stay quiet, or the property would false-alarm in production.
    start = ctx.mark()
    for _ in range(10):
        ctx.publish("TODO_status", good())
        ctx.sleep(0.3)
    ctx.expect_clean(start)

    # Each fault must make its property fire.
    mark = ctx.mark()
    ctx.publish("TODO_status", good(value=1e6))
    ctx.expect_true("v_range", mark)

    # Recover, so later faults are tested from a healthy state.
    for _ in range(3):
        ctx.publish("TODO_status", good())
        ctx.sleep(0.3)
