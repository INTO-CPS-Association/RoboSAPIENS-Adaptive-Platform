"""Knowledge -> JSON bridge and clock for the trustworthiness checker.

    python tc_bridge.py --port 6379

Edit EVENTS and KNOWLEDGE for your system; the rest is generic. references/integration.md
explains the reasons in full:

  * The checker reads JSON from pub/sub. rpclpy keeps knowledge in Redis keys as pickle.
  * rpclpy publishes no clock, and deadline properties need one.
  * Each input should tick steadily, so the newest value of every stream is republished each
    period. Specs detect a real event by its uid changing.
  * One malformed value (an int for a Float, a null, a short list, a missing field) leaves the
    checker alive with some properties permanently stopped. So every payload is fitted to its
    schema here, and rejected if it can't be. tc:wire_errors counts rejections, so a spec can
    report them and bad data is not simply hidden.
"""

import argparse
import json
import math
import os
import pickle
import sys
import threading
import time

import redis

# Knowledge values are pickled instances, so unpickling must be able to import their module.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Pub/sub channel -> schema, which doubles as the payload sent until the first real message.
# Types come from the values: 0.0 is a Float, 0 an Int. A list's length is its minimum length,
# so give it as many items as the spec indexes. uid "" keeps *_fired pulses false.
EVENTS = {
    "TODO_status": {"uid": "", "value": 0.0, "readings": [0.0] * 6},
    "new_data": {"uid": ""},
    "anomaly": {"uid": ""},
    "new_plan": {"uid": ""},
    "isLegit": {"uid": ""},
}

# Knowledge key (the class's `name`) -> (stream name, projector to a small JSON object).
KNOWLEDGE = {
    "TODOFlag": ("TODOFlag", lambda o: {"on": bool(getattr(o, "on", False))}),
    "TODOValue": ("TODOValue", lambda o: {"value": _f(getattr(o, "value", None))}),
}


def _f(v, missing=-1.0):
    """None/unparseable -> sentinel float."""
    try:
        v = float(v) if v is not None else missing
        return v if math.isfinite(v) else missing
    except (TypeError, ValueError):
        return missing


def _i(v, missing=-1):
    """None/unparseable -> sentinel int."""
    try:
        return int(v) if v is not None else missing
    except (TypeError, ValueError):
        return missing


def _fit(value, schema, path):
    """value coerced to the type of schema, or ValueError."""
    if isinstance(schema, bool):
        if isinstance(value, bool):
            return value
    elif isinstance(schema, float):
        if isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value):
            return float(value)
    elif isinstance(schema, int):
        if isinstance(value, int) and not isinstance(value, bool):
            return value
    elif isinstance(schema, str):
        if isinstance(value, str):
            return value
    elif isinstance(schema, list):
        if isinstance(value, list) and len(value) >= len(schema):
            return [_fit(v, schema[0], f"{path}[{i}]") for i, v in enumerate(value)] if schema else value
    elif isinstance(schema, dict):
        if isinstance(value, dict):
            return _conform(value, schema, f"{path}.")
    raise ValueError(f"{path}: expected {type(schema).__name__}, got {value!r}"[:200])


def _conform(payload, schema, prefix=""):
    """Only the schema's fields, each with the right type. Extra fields are dropped, so a NaN
    in a field no spec reads can't reach the checker's JSON parser."""
    out = {}
    for field, expected in schema.items():
        if field not in payload:
            raise ValueError(f"{prefix}{field}: missing")
        out[field] = _fit(payload[field], expected, f"{prefix}{field}")
    return out


def _event_listener(r, cache, lock, stop, errors):
    """Cache the newest valid payload of every mirrored channel."""
    pubsub = r.pubsub(ignore_subscribe_messages=True)
    pubsub.subscribe(*EVENTS.keys())
    while not stop.is_set():
        msg = pubsub.get_message(timeout=0.5)
        if not msg or msg.get("type") != "message":
            continue
        chan = msg.get("channel")
        if isinstance(chan, bytes):
            chan = chan.decode("utf-8", errors="ignore")
        try:
            payload = _conform(json.loads(msg.get("data")), EVENTS[chan])
        except Exception as exc:
            with lock:
                errors["count"] += 1
            print(f"tc_bridge: rejected a message on {chan!r}: {exc}")
            continue
        with lock:
            cache[chan] = payload
    pubsub.close()


def run_bridge(host="127.0.0.1", port=6379, db=0, rate=2.0, stop=None):
    """Importable, so a MAPLE-K main can run it in a daemon thread. Run exactly one per system:
    two bridges double every tc:* message."""
    r = redis.Redis(host=host, port=port, db=db)
    period = 1.0 / rate
    t0 = time.monotonic()
    warned = set()
    if stop is None:
        stop = threading.Event()

    cache = {chan: dict(default) for chan, default in EVENTS.items()}
    errors = {"count": 0}
    lock = threading.Lock()
    threading.Thread(target=_event_listener, args=(redis.Redis(host=host, port=port, db=db), cache, lock, stop, errors),
                     daemon=True).start()

    print(f"tc_bridge: {host}:{port} db{db} at {rate} Hz.")
    try:
        while not stop.is_set():
            r.publish("tc:t", json.dumps(round(time.monotonic() - t0, 3)))
            with lock:
                snapshot = dict(cache)
                wire_errors = errors["count"]
            r.publish("tc:wire_errors", json.dumps(wire_errors))
            for chan, payload in snapshot.items():
                r.publish(f"tc:{chan}", json.dumps(payload))

            for key, (stream, project) in KNOWLEDGE.items():
                raw = r.get(key)
                if raw is None:
                    if key not in warned:
                        print(f"tc_bridge: key {key!r} not in Redis yet")
                        warned.add(key)
                    continue
                try:
                    r.publish(f"tc:{stream}", json.dumps(project(pickle.loads(raw))))
                except Exception as exc:
                    if key not in warned:
                        print(f"tc_bridge: cannot project {key!r}: {exc}")
                        warned.add(key)
            time.sleep(period)
    finally:
        stop.set()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--port", type=int, default=6379)
    ap.add_argument("--db", type=int, default=0)
    ap.add_argument("--rate", type=float, default=2.0, help="publish rate in Hz")
    args = ap.parse_args()
    try:
        run_bridge(host=args.host, port=args.port, db=args.db, rate=args.rate)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
