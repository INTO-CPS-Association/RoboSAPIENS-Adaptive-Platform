"""Application codec for rpio_observation (template: edit EVENTS, SLOTS and observation_schema.json).

The codec only *encodes* application values into the typed wire vocabulary the DSRV specs read.
It computes no verdicts and cannot substitute other Knowledge versions (rpclpy picks the refs).

Wire conventions used here:
  Knowledge value   {"$tag": <Key>, "payload": {field: encoded}}
  Option<T>         {"$tag": "Some", "payload": v} | {"$tag": "None"}
  trigger           {"$tag": "Received", "payload": {"kind", "topic", "message"}}
"""
import json
import pathlib

SCHEMA = json.loads((pathlib.Path(__file__).with_name("observation_schema.json")).read_text(encoding="utf-8"))

# Closed vocabularies. Names must match what the specs expect.
EVENTS = {"reading_recorded": "ReadingRecorded"}   # published topic -> DSRV event tag
SLOTS = {"reading": "Reading"}                      # evidence slot  -> required Knowledge key


def _option(value):
    return {"$tag": "None"} if value is None else {"$tag": "Some", "payload": value}


def _encode(type_name, value):
    if type_name.startswith("Option<"):
        return _option(value)
    return value


class Codec:
    def knowledge(self, key, value):
        fields = SCHEMA["knowledge"].get(key)
        if fields is None:
            raise ValueError(f"Knowledge {key!r} is not declared in observation_schema.json")
        extra = set(value) - set(fields)
        if extra:
            raise ValueError(f"{key}: undeclared fields {sorted(extra)}")
        payload = {}
        for name, type_name in fields.items():
            if name not in value:
                raise ValueError(f"{key}.{name} missing")
            if value[name] is None and not type_name.startswith("Option<"):
                raise ValueError(f"{key}.{name} is None but declared {type_name}")
            payload[name] = _encode(type_name, value[name])
        return {"$tag": key, "payload": payload}

    def trigger(self, kind, topic, message):
        return {"$tag": "Received", "payload": {"kind": kind, "topic": topic, "message": message}}

    def emitted(self, kind, topic, message):
        if kind == "event" and topic not in EVENTS:
            raise ValueError(f"event {topic!r} is not in EVENTS")
        return {"kind": kind, "topic": topic, "message": message}

    def outcome(self, wire):
        return wire

    def validate_evidence(self, kind, topic, slots):
        for name, ref in slots.items():
            if name not in SLOTS:
                raise ValueError(f"evidence slot {name!r} is not in SLOTS")
            if ref["key"] != SLOTS[name]:
                raise ValueError(f"slot {name!r} must cite {SLOTS[name]}, not {ref['key']}")


CODEC = Codec()
