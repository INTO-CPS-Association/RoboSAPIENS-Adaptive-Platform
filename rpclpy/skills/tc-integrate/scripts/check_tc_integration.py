#!/usr/bin/env python
"""Check that a DSRV spec really connects to this version of the code.

Follows every stream the spec reads, back through the channel map and the bridge to the code
that produces it, and reports where the chain is broken or will silently stop working.

    python check_tc_integration.py --spec s.dsrv --routes s_redis_input.json5 \
        --bridge tc_bridge.py --code my_maple_k.py --code managed_system.py --messages my_messages.py \
        --param anomaly_threshold=Analysis.threshold

Exit code: 0 clean (warnings allowed), 1 errors found, 2 could not run.
Rule meanings are in references/integration.md.
"""
import argparse
import ast
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "tc-properties", "scripts"))
try:
    from validate_dsrv import Spec, strip_comments
except ImportError:
    print("needs the tc-properties skill next to this one (scripts/validate_dsrv.py)", file=sys.stderr)
    sys.exit(2)

ERROR, WARNING = "ERROR", "WARNING"
ENVELOPE = {"uid", "timestamp"}  # rpclpy adds these to every published message
PROJECTOR_TYPES = {"_f": 0.0, "_i": 0, "bool": False, "len": 0, "_flatten": [0.0], "float": 0.0, "int": 0}


def read_routes(path):
    with open(path, encoding="utf-8") as f:
        text = strip_comments(f.read())
    routes = dict(re.findall(r'([A-Za-z_]\w*)\s*:\s*"([^"]*)"', text))
    objects = re.findall(r"([A-Za-z_]\w*)\s*:\s*\{", text)
    return routes, objects


class Bridge:
    """EVENTS and KNOWLEDGE tables of a bridge, read without running it."""

    def __init__(self, path):
        with open(path, encoding="utf-8") as f:
            self.source = f.read()
        self.events, self.knowledge = {}, {}  # channel -> schema; key -> (stream, {field: sample}, {field: attr})
        self.guarded = "_conform" in self.source
        for node in ast.parse(self.source).body:
            if isinstance(node, ast.Assign) and isinstance(node.targets[0], ast.Name):
                if node.targets[0].id == "EVENTS":
                    self.events = self._events(node.value)
                elif node.targets[0].id == "KNOWLEDGE":
                    self.knowledge = self._knowledge(node.value)
        self.channels = re.findall(r'["\']tc:(\w+)["\']', self.source)

    @staticmethod
    def _events(node):
        out = {}
        for key, value in zip(node.keys, node.values):
            schema = {}
            for k, v in zip(value.keys, value.values):
                try:
                    schema[k.value] = ast.literal_eval(v)
                except ValueError:  # [0.0] * 6
                    if isinstance(v, ast.BinOp) and isinstance(v.op, ast.Mult):
                        schema[k.value] = ast.literal_eval(v.left) * ast.literal_eval(v.right)
                    else:
                        schema[k.value] = None
            out[key.value] = schema
        return out

    @staticmethod
    def _knowledge(node):
        out = {}
        for key, value in zip(node.keys, node.values):
            stream, projector = value.elts
            fields, attrs = {}, {}
            if isinstance(projector, ast.Lambda) and isinstance(projector.body, ast.Dict):
                for k, v in zip(projector.body.keys, projector.body.values):
                    fname = getattr(getattr(v, "func", None), "id", None)
                    fields[k.value] = PROJECTOR_TYPES.get(fname)
                    for sub in ast.walk(v):
                        if isinstance(sub, ast.Call) and getattr(sub.func, "id", "") == "getattr":
                            attrs[k.value] = sub.args[1].value
            out[key.value] = (stream.value, fields, attrs)
        return out


class Code:
    """What a code version publishes, and the knowledge classes it defines."""

    def __init__(self, code_paths, message_paths):
        self.published = {}  # topic -> set of payload fields, or None when unknown
        self.classes = {}  # class name -> {key, attrs, topic}
        self.set_attrs = set()
        self.constants = {}  # (class, name) -> value
        self.sources = {}
        trees = []
        for path in list(message_paths) + list(code_paths):
            with open(path, encoding="utf-8") as f:
                source = f.read()
            self.sources[path] = source
            tree = ast.parse(source)
            trees.append(tree)
            self._classes(tree)
        for tree in trees:
            self._walk(tree)

    def _classes(self, tree):
        for cls in [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]:
            attrs, key, topic = set(), None, None
            for sub in ast.walk(cls):
                if isinstance(sub, ast.Assign):
                    for t in sub.targets:
                        if isinstance(t, ast.Attribute) and getattr(t.value, "id", "") == "self":
                            attrs.add(t.attr)
                            if isinstance(sub.value, ast.Constant) and isinstance(sub.value.value, str):
                                key = sub.value.value if t.attr == "name" else key
                                topic = sub.value.value if t.attr == "topic" else topic
            if "name" in attrs and key:
                self.classes[cls.name] = dict(key=key, attrs=attrs, topic=topic)

    def _walk(self, tree):
        for scope in [n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)]:
            for sub in ast.walk(scope):
                if isinstance(sub, ast.Assign) and isinstance(sub.value, ast.Constant) and isinstance(sub.value.value, (int, float)) \
                        and not isinstance(sub.value.value, bool):
                    for t in sub.targets:
                        if isinstance(t, ast.Name):
                            self.constants.setdefault((scope.name, t.id), sub.value.value)
        for sub in ast.walk(tree):
            if isinstance(sub, ast.Assign):
                for t in sub.targets:
                    if isinstance(t, ast.Attribute):
                        self.set_attrs.add(t.attr)
                    elif isinstance(t, ast.Name) and isinstance(sub.value, ast.Constant) and isinstance(sub.value.value, (int, float)) \
                            and not isinstance(sub.value.value, bool):
                        self.constants.setdefault((None, t.id), sub.value.value)
            if not (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)):
                continue
            if sub.func.attr not in ("publish_event", "publish_message"):
                continue
            param = "event_key" if sub.func.attr == "publish_event" else "message_key"
            key = sub.args[0] if sub.args else next((k.value for k in sub.keywords if k.arg == param), None)
            message = sub.args[1] if len(sub.args) > 1 else next((k.value for k in sub.keywords if k.arg == "message"), None)
            if isinstance(key, ast.Constant):
                fields = set(ENVELOPE)
                if isinstance(message, ast.Dict) and all(isinstance(k, ast.Constant) for k in message.keys):
                    fields |= {k.value for k in message.keys}
                elif message is not None:
                    fields = None
                self._publish(key.value, fields)
            else:
                # publish_message(obj): resolve the class from `obj = Cls()` or the name itself.
                name = getattr(key, "id", None) or getattr(getattr(key, "func", None), "id", None)
                cls = self.classes.get(name) or self.classes.get(self._assigned_class(tree, name))
                if cls:
                    self._publish(cls["topic"] or cls["key"], set(cls["attrs"]) | ENVELOPE)

    @staticmethod
    def _assigned_class(tree, var):
        for sub in ast.walk(tree):
            if isinstance(sub, ast.Assign) and isinstance(sub.value, ast.Call) and isinstance(sub.value.func, ast.Name):
                if any(getattr(t, "id", None) == var for t in sub.targets):
                    return sub.value.func.id
        return None

    def _publish(self, topic, fields):
        if fields is None or self.published.get(topic, set()) is None:
            self.published[topic] = None
        else:
            self.published[topic] = self.published.get(topic, set()) | fields


def field_uses(spec, stream):
    """Fields of `stream` the equations read, and the largest constant List.get index for each."""
    fields, indexes = set(), {}
    aliases = {}
    pattern = re.compile(rf"(?<![\w.]){re.escape(stream)}(?:\[\d+\])?\.([A-Za-z_]\w*)")
    for expr, _ in spec.defs.values():
        fields.update(pattern.findall(expr))
        for field, index in re.findall(rf"List\.get\(\s*{re.escape(stream)}(?:\[\d+\])?\.([A-Za-z_]\w*)\s*,\s*(\d+)\s*\)", expr):
            indexes[field] = max(indexes.get(field, -1), int(index))
    return fields, indexes


def declared_kind(typ):
    typ = (typ or "").strip()
    if typ == "Float":
        return float
    if typ == "Int":
        return int
    if typ == "Bool":
        return bool
    if typ == "Str":
        return str
    return list if typ.startswith("List<") else None


class Report:
    def __init__(self):
        self.items = []

    def add(self, rule, severity, message):
        self.items.append((rule, severity, message))


def check(spec, routes, route_objects, bridge, code, args, report):
    external = set(args.external_topic)

    for name in route_objects:
        report.add("I01", ERROR, f"route for '{name}' uses the object form, which the checker rejects: write {name}: \"channel\"")
    for name in spec.inputs:
        if name not in routes and name not in route_objects:
            report.add("I01", ERROR, f"input '{name}' has no route in the channel map, so it never gets a value")
    for name in routes:
        if name not in spec.inputs:
            report.add("I02", WARNING, f"route '{name}' matches no input of the spec")

    schema_of = {}
    for stream, channel in routes.items():
        if stream not in spec.inputs:
            continue
        if not channel.startswith("tc:"):
            report.add("I03", WARNING, f"'{stream}' reads the bare channel '{channel}': it only ticks when the event fires, "
                                       "which starves the monitor. Route it through the bridge")
            continue
        base = channel[3:]
        if base in bridge.events:
            schema_of[stream] = ("event", base, bridge.events[base])
        else:
            match = next(((k, v) for k, v in bridge.knowledge.items() if v[0] == base), None)
            if match:
                schema_of[stream] = ("knowledge", match[0], match[1])
            elif base not in bridge.channels:
                report.add("I04", ERROR, f"'{stream}' is routed to '{channel}', which the bridge never publishes")

    # Is each mirrored event really published by this code version?
    for stream, (kind, base, schema) in schema_of.items():
        if kind != "event" or base in external:
            continue
        if base not in code.published:
            near = [t for t in code.published if t.lower() == base.lower()]
            hint = f" The code publishes '{near[0]}'." if near else ""
            report.add("I05", ERROR, f"the bridge mirrors '{base}' for '{stream}', but no code publishes it, so this stream "
                                     f"never changes and its properties can't fire.{hint} If it comes from outside, pass --external-topic {base}")

    # Fields the spec reads, against the bridge schema and against what is really sent.
    for stream, (kind, base, info) in schema_of.items():
        used, indexes = field_uses(spec, stream)
        declared = spec.struct_fields(stream)
        schema = info if kind == "event" else info[1]
        for field in sorted(used):
            if field not in schema:
                report.add("I06", ERROR, f"the spec reads {stream}.{field}, but the bridge's payload for '{base}' has no '{field}': "
                                         "this panics and stops the property")
                continue
            sample = schema[field]
            want = declared_kind(declared.get(field))
            if want and sample is not None and not (isinstance(sample, want) and (want is not int or not isinstance(sample, bool))):
                report.add("I07", ERROR, f"{stream}.{field} is declared {declared.get(field)}, but the bridge sends {sample!r} "
                                         f"({type(sample).__name__}): a type mismatch panics. Use 0.0 for Float")
        for field, index in indexes.items():
            sample = schema.get(field)
            if isinstance(sample, list) and index >= len(sample) and kind == "event":
                report.add("I08", ERROR, f"the spec reads {stream}.{field} at index {index}, but the bridge's payload has only "
                                         f"{len(sample)} items before the first message: out of range panics")
        if kind == "event" and base in code.published and code.published[base] is not None:
            missing = sorted(f for f in used if f not in code.published[base])
            if missing:
                report.add("I09", ERROR, f"the spec reads {stream}.{', '.join(missing)}, but this code's '{base}' messages have only "
                                         f"{sorted(code.published[base] - ENVELOPE) or 'uid/timestamp'}")
        if kind == "knowledge":
            cls = next((c for c in code.classes.values() if c["key"] == base), None)
            if cls is None:
                near = [n for n, c in code.classes.items() if n == base]
                hint = f" Class {near[0]} stores under key '{code.classes[near[0]]['key']}'." if near else ""
                report.add("I10", ERROR, f"the bridge reads knowledge key '{base}' for '{stream}', but no class has that name.{hint}")
                continue
            for field, attr in info[2].items():
                if attr not in cls["attrs"] and attr not in code.set_attrs:
                    report.add("I11", ERROR, f"the bridge projects {base}.{attr}, which this code never sets: getattr's default "
                                             "would be sent forever, and the property goes quiet without any error")

    if schema_of and not bridge.guarded and any(k == "event" for k, _, _ in schema_of.values()):
        report.add("I12", WARNING, "the bridge forwards event payloads unchecked: one int for a Float, a null or a short list "
                                   "permanently stops some properties while the checker stays alive. Add the _conform guard from "
                                   "assets/tc_bridge_template.py")

    starts = [p for p, s in code.sources.items() if re.search(r"\brun_bridge\b", s)]
    if len(starts) > 1:
        report.add("I13", WARNING, f"run_bridge appears in {len(starts)} files; two running bridges double every tc:* message: {starts}")
    elif not starts:
        report.add("I13", WARNING, "no code starts run_bridge, so the bridge has to run as its own process")

    spec_base = os.path.basename(spec.path)
    specs_named = {s for src in code.sources.values() for s in re.findall(r"tc_spec_file['\"]?\s*[:=]\s*['\"]([^'\"]+)", src)}
    if specs_named and not any(os.path.basename(s) == spec_base for s in specs_named):
        report.add("I14", WARNING, f"the dashboard is configured with {sorted(specs_named)}, not {spec_base}, so its panel shows other properties")
    elif "run_dashboard" in "".join(code.sources.values()) and not specs_named:
        report.add("I14", WARNING, "run_dashboard is called with no tc_spec_file, so the property panel stays empty")

    observed = "Observation_Config" in "".join(code.sources.values())
    if args.config and not observed:
        with open(args.config, encoding="utf-8") as f:
            observed = "Observation_Config" in f.read()
    if observed and bridge.knowledge:
        report.add("I17", WARNING, f"the bridge reads knowledge keys {sorted(bridge.knowledge)}, but observed nodes commit typed knowledge "
                                   "to a journal, not to plain keys, so these streams would never update. See references/integration.md, section 8")

    check_params(spec, code, args, report)


def check_params(spec, code, args, report):
    """Compare spec parameters with the code constants they mirror."""
    mapping = dict(item.split("=", 1) for item in args.param)
    unmatched = []
    for name, (expr, _) in spec.defs.items():
        if name not in spec.auxes:
            continue
        literal = re.fullmatch(r"\s*(-?\d+(?:\.\d*)?)\s*", expr)
        if not literal:
            continue
        value = float(literal.group(1))
        target = mapping.get(name, name)
        cls, _, var = target.rpartition(".")
        found = [(c, v) for (c, n), v in code.constants.items() if n == var and (not cls or c == cls)]
        if not found:
            unmatched.append(name)
        elif len({v for _, v in found}) > 1:
            where = ", ".join(f"{c or 'module'}={v}" for c, v in found)
            report.add("I15", WARNING, f"parameter {name} = {value:g}: '{var}' has different values in the code ({where}). "
                                       f"Say which with --param {name}=Class.{var}")
        elif float(found[0][1]) != value:
            report.add("I15", ERROR, f"parameter {name} = {value:g} in the spec, but {target} = {found[0][1]} in this code version")
    if unmatched:
        report.add("I16", WARNING, "no code constant found for these parameters, so check them by hand or map them with --param: "
                                   + ", ".join(unmatched))


def main(argv=None):
    p = argparse.ArgumentParser(description="Check a DSRV spec against a code version.")
    p.add_argument("--spec", required=True)
    p.add_argument("--routes", required=True, help="the *_redis_input.json5 channel map")
    p.add_argument("--bridge", required=True)
    p.add_argument("--code", action="append", required=True, help="module of this code version; repeat")
    p.add_argument("--messages", action="append", default=[], help="module defining the message and knowledge classes")
    p.add_argument("--external-topic", action="append", default=[], help="topic published by something outside --code")
    p.add_argument("--config", help="the nodes' YAML config, to detect Observation_Config")
    p.add_argument("--param", action="append", default=[], metavar="AUX=[Class.]constant", help="the code constant a spec parameter mirrors")
    args = p.parse_args(argv)
    try:
        spec = Spec(args.spec)
        routes, objects = read_routes(args.routes)
        bridge = Bridge(args.bridge)
        code = Code(args.code, args.messages)
    except (OSError, SyntaxError) as e:
        print(f"cannot run: {e}", file=sys.stderr)
        return 2

    report = Report()
    check(spec, routes, objects, bridge, code, args, report)
    report.items.sort(key=lambda i: (i[1] != ERROR, i[0]))
    for rule, severity, message in report.items:
        print(f"{severity} [{rule}] {message}")
    errors = sum(i[1] == ERROR for i in report.items)
    print(f"\n{errors} error(s), {len(report.items) - errors} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
