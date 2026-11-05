#!/usr/bin/env python
"""Static API-contract checker for rpclpy MAPLE-K nodes.

Parses the generated node module(s) and reports every way they violate the
rpclpy API, without importing or running them. Signatures are read from the
rpclpy sources themselves, so the checker follows the framework if it changes.

    python check_rpclpy_api.py my_maple_k.py --messages my_messages.py \
        --config my_config.yaml --external-topic SensorReading

Exit code: 0 clean (warnings allowed), 1 errors found, 2 could not run.
Rule ids and the reason behind each one are in references/rpclpy_api.md.
"""
import argparse
import ast
import importlib.util
import json
import os
import re
import sys

try:
    import yaml
except ImportError:
    yaml = None

ERROR, WARNING = "ERROR", "WARNING"

MAPLE_ROLES = ["Monitor", "Analysis", "Plan", "Legitimate", "Execute"]
COMM_PROTOCOLS = {"mqtt", "rabbitmq", "kafka", "redis", "zenoh", "websocket", "tcp", "tcp/ip", "local"}
KNOWLEDGE_BACKENDS = {"redis", "memcached", "ignite", "hazelcast", "tarantool", "aerospike", "kafka", "sqlite", "local"}
LOGGER_TYPES = {"terminal", "file", "mqtt", "redis"}
CC_MARKER = re.compile(r"<!--\s*cc_(\w+)\s+(START|END)\s*--!>")
LINK_COMMENT = re.compile(r"#\s*LINK\s*<(\w+)>\s*(\S+)")
# (config key, required sub-keys)
NODE_SECTIONS = {
    "Logger_Config": ("log_level", "format", "logger_type"),
    "Event_Manager_Config": ("protocol",),
    "Message_Manager_Config": ("protocol",),
    "Knowledge_Config": ("knowledge_type",),
}


class Finding:
    def __init__(self, rule, severity, path, line, message):
        self.rule, self.severity, self.path, self.line, self.message = rule, severity, path, line, message

    def as_dict(self):
        return dict(rule=self.rule, severity=self.severity, file=self.path, line=self.line, message=self.message)

    def __str__(self):
        return f"{self.path}:{self.line}: {self.severity} [{self.rule}] {self.message}"


# ---------------------------------------------------------------------------
# Reading the real API out of the rpclpy sources
# ---------------------------------------------------------------------------
class Signature:
    """Positional/keyword shape of a method, minus self."""

    def __init__(self, fn):
        args = fn.args.posonlyargs + fn.args.args
        self.names = [a.arg for a in args][1:]
        self.required = len(self.names) - len(fn.args.defaults)
        self.kwonly = [a.arg for a in fn.args.kwonlyargs]
        self.required_kwonly = [a.arg for a, d in zip(fn.args.kwonlyargs, fn.args.kw_defaults) if d is None]
        self.star_args = fn.args.vararg is not None
        self.star_kwargs = fn.args.kwarg is not None

    def violation(self, call):
        """Return why `call` cannot bind to this signature, or None."""
        if any(isinstance(a, ast.Starred) for a in call.args) or any(k.arg is None for k in call.keywords):
            return None  # *args/**kwargs: not decidable statically
        if len(call.args) > len(self.names) and not self.star_args:
            return f"takes {len(self.names)} argument(s), got {len(call.args)} positional"
        bound = set(self.names[:len(call.args)])
        for kw in call.keywords:
            if kw.arg not in self.names and kw.arg not in self.kwonly and not self.star_kwargs:
                return f"has no parameter '{kw.arg}' (accepts: {', '.join(self.names + self.kwonly) or 'none'})"
            if kw.arg in bound:
                return f"got multiple values for '{kw.arg}'"
            bound.add(kw.arg)
        missing = [n for n in self.names[:self.required] + self.required_kwonly if n not in bound]
        if missing:
            return f"missing required argument(s): {', '.join(missing)}"
        return None


def public_api(path, class_name):
    with open(path, encoding="utf-8") as f:
        tree = ast.parse(f.read())
    methods, attrs = {}, set()
    for node in tree.body:
        if isinstance(node, ast.ClassDef) and node.name == class_name:
            for item in node.body:
                if isinstance(item, ast.FunctionDef):
                    methods[item.name] = Signature(item)
                    for sub in ast.walk(item):
                        if (isinstance(sub, ast.Attribute) and isinstance(sub.ctx, ast.Store)
                                and isinstance(sub.value, ast.Name) and sub.value.id == "self"):
                            attrs.add(sub.attr)
    if not methods:
        raise RuntimeError(f"class {class_name} not found in {path}")
    return methods, attrs


class RpclpyApi:
    def __init__(self, root):
        self.node_methods, self.node_attrs = public_api(os.path.join(root, "node.py"), "Node")
        self.knowledge, _ = public_api(os.path.join(root, "KnowledgeManager.py"), "KnowledgeManager")
        self.comm, _ = public_api(os.path.join(root, "CommunicationManager.py"), "CommunicationManager")

    def member(self, attr):
        return {"knowledge": ("KnowledgeManager", self.knowledge),
                "event_manager": ("CommunicationManager", self.comm),
                "message_manager": ("CommunicationManager", self.comm)}.get(attr)


# ---------------------------------------------------------------------------
# Model of the code under test
# ---------------------------------------------------------------------------
class KnowledgeClass:
    def __init__(self, node, path, nested=False):
        self.node, self.path, self.nested = node, path, nested
        self.name = node.name
        self.attrs, self.key, self.topic = set(), None, None
        self.required_init_args = 0
        for item in node.body:
            if isinstance(item, ast.FunctionDef):
                self.attrs.add(item.name)
                if item.name == "__init__":
                    a = item.args
                    self.required_init_args = len(a.posonlyargs + a.args) - 1 - len(a.defaults)
                    self.required_init_args += sum(d is None for d in a.kw_defaults)
                    for sub in ast.walk(item):
                        if (isinstance(sub, ast.Assign)):
                            for t in sub.targets:
                                self._record(t, sub.value)
            elif isinstance(item, ast.Assign):
                for t in item.targets:
                    if isinstance(t, ast.Name):
                        self.attrs.add(t.id)
                        self._record_value(t.id, item.value)

    def _record(self, target, value):
        if isinstance(target, ast.Attribute) and isinstance(target.value, ast.Name) and target.value.id == "self":
            self.attrs.add(target.attr)
            self._record_value(target.attr, value)

    def _record_value(self, attr, value):
        if isinstance(value, ast.Constant) and isinstance(value.value, str):
            if attr == "name":
                self.key = value.value
            elif attr == "topic":
                self.topic = value.value

    @property
    def data_attrs(self):
        return {a for a in self.attrs if not a.startswith("__")}


class Module:
    def __init__(self, path):
        self.path = path
        with open(path, encoding="utf-8") as f:
            self.source = f.read()
        self.tree = ast.parse(self.source, filename=path)
        self.lines = self.source.splitlines()


def base_names(cls):
    out = []
    for b in cls.bases:
        if isinstance(b, ast.Name):
            out.append(b.id)
        elif isinstance(b, ast.Attribute):
            out.append(b.attr)
    return out


def is_self_attr(node, attr=None):
    return (isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "self"
            and (attr is None or node.attr == attr))


def arg_of(call, position, keyword):
    """The argument bound to a parameter, by position or by keyword."""
    if len(call.args) > position:
        return call.args[position]
    for kw in call.keywords:
        if kw.arg == keyword:
            return kw.value
    return None


def const_str(node):
    return node.value if isinstance(node, ast.Constant) and isinstance(node.value, str) else None


class Checker:
    def __init__(self, modules, message_modules, api, config=None, config_path=None,
                 external_topics=(), sink_topics=(), external_knowledge=(), partial=False):
        self.modules, self.message_modules, self.api = modules, message_modules, api
        self.config, self.config_path = config, config_path
        self.external_topics, self.sink_topics = set(external_topics), set(sink_topics)
        self.external_knowledge = set(external_knowledge)
        self.partial = partial
        self.findings = []
        self.kclasses = {}  # class name -> KnowledgeClass
        self.nodes = {}  # class name -> (ast.ClassDef, Module)
        self.subscribed = {}  # topic -> [(class, path, line)]
        self.published = {}  # topic -> [(class, path, line)]
        self.reads, self.writes, self.init_writes = {}, {}, set()
        self.str_keys = {}  # string knowledge key -> (path, line)
        self.used_sections = {}  # config section -> (path, line)
        self.node_sections = {}  # node class -> config section it is constructed with

    def add(self, rule, severity, path, line, message):
        self.findings.append(Finding(rule, severity, path, line, message))

    # -- discovery ----------------------------------------------------------
    def discover(self):
        for mod in self.modules:
            parents = {}
            for cls in [n for n in ast.walk(mod.tree) if isinstance(n, ast.ClassDef)]:
                parents[cls.name] = base_names(cls)
            for cls in [n for n in mod.tree.body if isinstance(n, ast.ClassDef)]:
                seen, stack = set(), base_names(cls)
                while stack:
                    b = stack.pop()
                    if b == "Node":
                        self.nodes[cls.name] = (cls, mod)
                        break
                    if b not in seen:
                        seen.add(b)
                        stack.extend(parents.get(b, []))
        for mod in self.message_modules + self.modules:
            top = set(id(n) for n in mod.tree.body)
            for cls in [n for n in ast.walk(mod.tree) if isinstance(n, ast.ClassDef)]:
                if cls.name in self.nodes or cls.name in self.kclasses:
                    continue
                kc = KnowledgeClass(cls, mod.path, nested=id(cls) not in top)
                if "name" in kc.attrs:
                    self.kclasses[cls.name] = kc
                    kc.in_node_module = mod in self.modules and mod not in self.message_modules

    # -- rules --------------------------------------------------------------
    def check_knowledge_classes(self):
        keys = {}
        for kc in self.kclasses.values():
            line = kc.node.lineno
            if kc.required_init_args:
                self.add("K01", ERROR, kc.path, line,
                         f"{kc.name}.__init__ needs arguments, but read_knowledge({kc.name}) calls {kc.name}() with none")
            if kc.key is None:
                self.add("K02", ERROR, kc.path, line,
                         f"{kc.name}.name must be a string literal set in __init__: it is the knowledge key")
            else:
                if kc.key in keys:
                    self.add("K03", ERROR, kc.path, line,
                             f"{kc.name} and {keys[kc.key]} share the knowledge key '{kc.key}' and would overwrite each other")
                keys[kc.key] = kc.name
                if kc.key != kc.name:
                    self.add("K04", WARNING, kc.path, line,
                             f"{kc.name}.name is '{kc.key}': the knowledge key differs from the class name")
            for attr in ("uid", "timestamp"):
                if attr not in kc.attrs:
                    self.add("K05", WARNING, kc.path, line, f"{kc.name} does not declare '{attr}', which rpclpy stamps on write")
            if kc.nested:
                self.add("K06", ERROR, kc.path, line, f"{kc.name} is defined inside a function or class, so it cannot be pickled")
            elif getattr(kc, "in_node_module", False):
                self.add("K07", WARNING, kc.path, line,
                         f"{kc.name} is defined in the node script: pickled as __main__.{kc.name} when that script is run, "
                         "other processes cannot load it. Move it to a messages module")

    def check_node_class(self, name, cls, mod):
        methods = {i.name: i for i in cls.body if isinstance(i, ast.FunctionDef)}
        own_attrs = set()
        for sub in ast.walk(cls):
            if isinstance(sub, ast.Attribute) and isinstance(sub.ctx, ast.Store) and is_self_attr(sub):
                own_attrs.add(sub.attr)

        init = methods.get("__init__")
        if init is not None:
            calls_super = any(isinstance(c, ast.Call) and isinstance(c.func, ast.Attribute) and c.func.attr == "__init__"
                              for c in ast.walk(init))
            if not calls_super:
                self.add("N01", ERROR, mod.path, init.lineno, f"{name}.__init__ never calls super().__init__(config=...)")
            args = init.args.args
            defaults = dict(zip([a.arg for a in args][-len(init.args.defaults):], init.args.defaults)) if init.args.defaults else {}
            if const_str(defaults.get("config")) is not None:
                self.add("N02", WARNING, mod.path, init.lineno,
                         f"{name}.__init__ defaults config to a string, but Node needs a config dict; omit the default")

        for mname, fn in methods.items():
            self.check_function(name, mname, fn, mod, methods, own_attrs)

        if "register_callbacks" not in methods:
            self.add("N03", WARNING, mod.path, cls.lineno, f"{name} has no register_callbacks(): it can never receive an event")
        else:
            self.check_callbacks(name, methods, mod)

    def check_function(self, cname, mname, fn, mod, methods, own_attrs):
        var_types = {}  # variable -> knowledge class name
        written_vars, mutated = set(), {}
        publishes, writes = [], []
        in_init = mname == "__init__"

        for sub in sorted((n for n in ast.walk(fn) if hasattr(n, "lineno")), key=lambda n: (n.lineno, n.col_offset)):
            if isinstance(sub, ast.Assign) and len(sub.targets) == 1 and isinstance(sub.targets[0], ast.Name):
                t = self.instance_type(sub.value)
                if t:
                    var_types[sub.targets[0].id] = t
                else:
                    var_types.pop(sub.targets[0].id, None)
            if isinstance(sub, ast.Global):
                self.add("A01", ERROR, mod.path, sub.lineno,
                         f"{cname}.{mname} uses `global {', '.join(sub.names)}`: nodes may only share state through knowledge")
            if isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Name) and sub.value.id in var_types:
                self.check_attr(var_types[sub.value.id], sub, mod)
                if isinstance(sub.ctx, ast.Store):
                    mutated.setdefault(sub.value.id, sub.lineno)
            if isinstance(sub, ast.Attribute) and isinstance(sub.value, ast.Call):
                t = self.instance_type(sub.value)
                if t:
                    self.check_attr(t, sub, mod)
            if isinstance(sub, ast.Attribute) and isinstance(sub.ctx, ast.Store) and is_self_attr(sub) and not in_init:
                self.add("A02", WARNING, mod.path, sub.lineno,
                         f"{cname}.{mname} stores self.{sub.attr}: if other nodes need this, it belongs in knowledge")
            if isinstance(sub, ast.Attribute) and isinstance(sub.ctx, ast.Load) and is_self_attr(sub):
                if (sub.attr not in methods and sub.attr not in own_attrs and sub.attr not in self.api.node_attrs
                        and sub.attr not in self.api.node_methods and not self.inherits_extra(cname)):
                    self.add("N04", ERROR, mod.path, sub.lineno,
                             f"self.{sub.attr} does not exist on {cname} or on rpclpy Node")
            if isinstance(sub, ast.Call):
                self.check_call(cname, mname, sub, mod, var_types, written_vars, publishes, writes, in_init)

        for var, line in mutated.items():
            if var in var_types and var not in written_vars and self.is_read_var(fn, var):
                self.add("K08", WARNING, mod.path, line,
                         f"{cname}.{mname} changes `{var}` read from knowledge but never writes it back: read_knowledge returns a copy")

        if publishes and writes:
            first_publish = min(publishes)
            for line, target in writes:
                if line > first_publish and same_branch(fn, first_publish, line):
                    self.add("L01", WARNING, mod.path, line,
                             f"{cname}.{mname} writes {target} after publishing an event at line {first_publish}: "
                             "a subscriber can read the old value. Write first, then publish")

    def inherits_extra(self, cname):
        cls, _ = self.nodes[cname]
        return any(b != "Node" and b not in self.nodes for b in base_names(cls))

    def is_read_var(self, fn, var):
        for sub in ast.walk(fn):
            if (isinstance(sub, ast.Assign) and any(isinstance(t, ast.Name) and t.id == var for t in sub.targets)
                    and self.is_read_call(sub.value)):
                return True
        return False

    def is_read_call(self, node):
        return (isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute)
                and node.func.attr in ("read_knowledge", "read_knowledge_as_object")
                and isinstance(node.func.value, ast.Name) and node.func.value.id == "self")

    def instance_type(self, value):
        """Knowledge class of an expression, if it is `Cls()` or `self.read_knowledge(Cls)`."""
        if isinstance(value, ast.Call):
            if isinstance(value.func, ast.Name) and value.func.id in self.kclasses:
                return value.func.id
            if self.is_read_call(value):
                key = arg_of(value, 0, "key") or arg_of(value, 0, "cls")  # read_knowledge(key) / read_knowledge_as_object(cls)
                if isinstance(key, ast.Name) and key.id in self.kclasses:
                    return key.id
        return None

    def check_attr(self, kclass, attr_node, mod):
        kc = self.kclasses[kclass]
        if attr_node.attr not in kc.attrs and attr_node.attr not in ("__dict__", "__class__"):
            verb = "sets" if isinstance(attr_node.ctx, ast.Store) else "reads"
            self.add("K09", WARNING, mod.path, attr_node.lineno,
                     f"code {verb} .{attr_node.attr} but {kclass} does not declare it "
                     f"(declares: {', '.join(sorted(a for a in kc.data_attrs if a not in ('name', 'uid', 'timestamp')))})")

    def check_call(self, cname, mname, call, mod, var_types, written_vars, publishes, writes, in_init):
        func = call.func
        if isinstance(func, ast.Name):
            if func.id in self.nodes:
                self.add("A03", ERROR, mod.path, call.lineno,
                         f"{cname}.{mname} creates a {func.id} node: nodes must be coupled only by events and knowledge")
            return
        if not isinstance(func, ast.Attribute):
            return
        # direct use of a client library from a node
        if isinstance(func.value, ast.Name) and func.value.id == "redis" and func.attr in ("Redis", "StrictRedis"):
            self.add("A04", WARNING, mod.path, call.lineno,
                     f"{cname}.{mname} opens its own Redis client: use read_knowledge/write_knowledge/publish_event")
            return

        # self.<method>(...) on Node
        if is_self_attr(func) and func.attr in self.api.node_methods:
            cls, _ = self.nodes[cname]
            overridden = any(isinstance(i, ast.FunctionDef) and i.name == func.attr for i in cls.body)
            if not overridden:
                why = self.api.node_methods[func.attr].violation(call)
                if why:
                    self.add("N05", ERROR, mod.path, call.lineno, f"Node.{func.attr} {why}")
                    return
            self.check_node_api_call(cname, mname, func.attr, call, mod, var_types, written_vars, publishes, writes, in_init)
            return

        # self.knowledge.x(...) / self.event_manager.x(...)
        if is_self_attr(func.value) and self.api.member(func.value.attr):
            owner, methods = self.api.member(func.value.attr)
            attr = f"self.{func.value.attr}.{func.attr}"
            if func.attr not in methods:
                self.add("N06", ERROR, mod.path, call.lineno, f"{attr}: {owner} has no method '{func.attr}'")
                return
            why = methods[func.attr].violation(call)
            if why:
                self.add("N05", ERROR, mod.path, call.lineno, f"{owner}.{func.attr} {why}")
                return
            if func.value.attr == "knowledge" and func.attr in ("read", "write"):
                self.add("N07", WARNING, mod.path, call.lineno,
                         f"{attr} bypasses the Node wrapper and its serialization: use {func.attr}_knowledge")
            if func.value.attr in ("event_manager", "message_manager") and func.attr in ("publish", "subscribe"):
                self.add("N07", WARNING, mod.path, call.lineno,
                         f"{attr} bypasses the Node wrapper (no uid/timestamp envelope): use publish_event/register_event_callback")

    def check_node_api_call(self, cname, mname, method, call, mod, var_types, written_vars, publishes, writes, in_init):
        if method == "read_knowledge":
            key = arg_of(call, 0, "key")
            s = const_str(key)
            if s is not None:
                self.str_keys.setdefault(s, (mod.path, call.lineno))
            elif isinstance(key, ast.Call):
                self.add("K10", ERROR, mod.path, call.lineno,
                         "read_knowledge takes the class, not an instance: pass Cls rather than Cls()")
            elif isinstance(key, ast.Name):
                if key.id in self.kclasses:
                    self.reads.setdefault(key.id, (mod.path, call.lineno))
                elif key.id in var_types:
                    self.add("K10", ERROR, mod.path, call.lineno,
                             f"read_knowledge({key.id}): {key.id} is an instance, pass the class {var_types[key.id]}")
                elif key.id[:1].isupper():  # lowercase names are plain variables, undecidable here
                    self.add("K11", ERROR, mod.path, call.lineno, f"read_knowledge({key.id}): no knowledge class named {key.id}")
        elif method == "write_knowledge":
            key, value = arg_of(call, 0, "key"), arg_of(call, 1, "value")
            s = const_str(key)
            if s is not None:
                self.str_keys.setdefault(s, (mod.path, call.lineno))
                writes.append((call.lineno, f"'{s}'"))
                if value is None or (isinstance(value, ast.Constant) and value.value is None):
                    self.add("K12", ERROR, mod.path, call.lineno,
                             f"write_knowledge('{s}') has no value; the Redis backend rejects None")
                elif isinstance(value, (ast.Dict, ast.List, ast.Tuple, ast.Set, ast.ListComp, ast.DictComp)):
                    self.add("K12", ERROR, mod.path, call.lineno,
                             f"write_knowledge('{s}', <{type(value).__name__.lower()}>): the string-key form does no JSON encoding "
                             "and Redis rejects containers. Use a knowledge class, or json.dumps the value")
            elif isinstance(key, ast.Name) and key.id in self.kclasses:
                self.add("K13", ERROR, mod.path, call.lineno,
                         f"write_knowledge({key.id}) passes the class: pass an instance, because `name` is an instance attribute")
            else:
                target = None
                if isinstance(key, ast.Name):
                    target = var_types.get(key.id)
                    written_vars.add(key.id)
                elif isinstance(key, ast.Call):
                    target = self.instance_type(key)
                if target:
                    self.writes.setdefault(target, (mod.path, call.lineno))
                    if in_init:
                        self.init_writes.add(target)
                    writes.append((call.lineno, target))
                elif isinstance(key, ast.Name):
                    writes.append((call.lineno, key.id))
        elif method in ("publish_event", "publish_message"):
            self.check_publish(cname, method, call, mod, var_types, publishes)
        elif method in ("register_event_callback", "register_message_callback"):
            topic = const_str(arg_of(call, 0, "event_key"))
            if topic is not None:
                self.subscribed.setdefault(topic, []).append((cname, mod.path, call.lineno, method))
                self.check_link_comment(mod, call.lineno, topic)

    def check_publish(self, cname, method, call, mod, var_types, publishes):
        param = "event_key" if method == "publish_event" else "message_key"
        key, message = arg_of(call, 0, param), arg_of(call, 1, "message")
        publishes.append(call.lineno)
        s = const_str(key)
        if s is not None:
            self.published.setdefault(s, []).append((cname, mod.path, call.lineno, method))
            self.check_link_comment(mod, call.lineno, s)
            if message is not None and isinstance(message, (ast.Constant, ast.List, ast.Tuple, ast.Set, ast.JoinedStr)) \
                    and not (isinstance(message, ast.Constant) and message.value is None):
                self.add("E01", ERROR, mod.path, call.lineno, f"{method} message must be a dict; it is merged with uid/timestamp and JSON-encoded")
            return
        kclass = None
        if isinstance(key, ast.Name):
            if key.id in self.kclasses:
                kclass = key.id
            elif key.id in var_types:
                kclass = var_types[key.id]
        elif isinstance(key, ast.Call):
            kclass = self.instance_type(key)
            if method == "publish_event":
                self.add("E02", ERROR, mod.path, call.lineno, "publish_event takes the class (it calls event_key()); an instance is not callable")
        if kclass is None:
            return
        kc = self.kclasses[kclass]
        if method == "publish_event" and isinstance(key, ast.Name) and key.id not in self.kclasses:
            self.add("E02", ERROR, mod.path, call.lineno, f"publish_event({key.id}): it calls event_key(), so pass the class {kclass}, not an instance")
        if method == "publish_event" and "topic" not in kc.attrs:
            self.add("E03", ERROR, mod.path, call.lineno, f"publish_event({kclass}) reads .topic, which {kclass} does not declare")
        topic = kc.topic or kc.key
        if topic:
            self.published.setdefault(topic, []).append((cname, mod.path, call.lineno, method))
        hidden = sorted(a for a in kc.attrs - {"name", "uid", "timestamp", "topic"} if a.startswith("_") and not a.startswith("__"))
        if hidden:
            self.add("E04", WARNING, mod.path, call.lineno,
                     f"{kclass} is published as json.dumps(__dict__), so subscribers see keys {hidden} with the underscore")

    def check_link_comment(self, mod, line, topic):
        text = mod.lines[line - 1] if line <= len(mod.lines) else ""
        m = LINK_COMMENT.search(text)
        if m and m.group(2) != topic:
            self.add("M01", WARNING, mod.path, line, f"# LINK comment says '{m.group(2)}' but the topic is '{topic}'")

    def check_callbacks(self, name, methods, mod):
        for sub in ast.walk(methods["register_callbacks"]):
            if not (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                    and sub.func.attr in ("register_event_callback", "register_message_callback")):
                continue
            cb = arg_of(sub, 1, "callback")
            if not is_self_attr(cb):
                continue
            fn = methods.get(cb.attr)
            if fn is None:
                self.add("C01", ERROR, mod.path, sub.lineno, f"callback self.{cb.attr} is not a method of {name}")
                continue
            required = len(fn.args.args) - 1 - len(fn.args.defaults)
            if not fn.args.vararg and (len(fn.args.args) < 2 or required > 1):
                self.add("C02", ERROR, mod.path, fn.lineno,
                         f"callback {name}.{cb.attr} must take exactly one payload argument: (self, msg)")
            self.check_callback_body(name, cb.attr, fn, methods, mod)

    def check_callback_body(self, name, cbname, fn, methods, mod):
        decorated = {f for f in methods if has_timeit(methods[f])}
        targets = thread_targets(fn)
        if cbname not in decorated and not any(t in decorated for t in targets):
            self.add("C03", WARNING, mod.path, fn.lineno,
                     f"{name}.{cbname} has no @timeit_callback: the dashboard Gantt chart reads the timing it records")
        for sub in ast.walk(fn):
            if isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute):
                # A join/wait with positional arguments is os.path.join or str.join, not a thread.
                blocking = ((isinstance(sub.func.value, ast.Name) and sub.func.value.id == "time" and sub.func.attr == "sleep")
                            or sub.func.attr == "backward"
                            or (sub.func.attr in ("join", "wait") and not sub.args))
                if blocking:
                    self.add("C04", WARNING, mod.path, sub.lineno,
                             f"{name}.{cbname} blocks in .{sub.func.attr}(): a callback runs on the node's only listener thread, "
                             "so its events queue up. Move long work to a thread, as Plan does")

    # -- whole-program rules -------------------------------------------------
    def check_knowledge_seeding(self):
        for cname, (path, line) in self.reads.items():
            if cname in self.external_knowledge:
                continue
            if cname not in self.writes:
                self.add("K14", ERROR, path, line,
                         f"{cname} is read but never written: read_knowledge returns None, so using the result raises and ends the listener")
            elif cname not in self.init_writes:
                self.add("K15", WARNING, path, line,
                         f"{cname} is not seeded in any node __init__: a read before the first write returns None, and using it raises")
        for s, (path, line) in self.str_keys.items():
            for kc in self.kclasses.values():
                if kc.key == s:
                    self.add("K16", ERROR, path, line,
                             f"'{s}' is used as a string key but {kc.name} stores pickled bytes under the same key")

    def check_topology(self):
        for topic, subs in self.subscribed.items():
            if topic not in self.published and topic not in self.external_topics:
                _, path, line, _ = subs[0]
                self.add("T01", ERROR, path, line,
                         f"topic '{topic}' is subscribed but nothing publishes it. If the managed system does, pass --external-topic {topic}")
        for topic, pubs in self.published.items():
            if topic not in self.subscribed and topic not in self.sink_topics:
                _, path, line, _ = pubs[0]
                self.add("T02", WARNING, path, line,
                         f"topic '{topic}' is published but nothing subscribes. If an outside consumer does, pass --sink-topic {topic}")
        if self.partial:
            return
        anchor = next(iter(self.modules)).path
        roles = {}
        for role in MAPLE_ROLES:
            match = next((c for c in self.nodes if role.lower() in c.lower()), None)
            if match is None:
                self.add("T03", ERROR, anchor, 1, f"no node class for the {role} stage (use --partial if this is intended)")
            roles[role] = match
        for a, b in zip(MAPLE_ROLES, MAPLE_ROLES[1:]):
            if not (roles[a] and roles[b]):
                continue
            a_out = {t for t, ps in self.published.items() if any(p[0] == roles[a] for p in ps)}
            b_in = {t for t, ss in self.subscribed.items() if any(s[0] == roles[b] for s in ss)}
            if not a_out & b_in:
                self.add("T04", ERROR, self.nodes[roles[b]][1].path, self.nodes[roles[b]][0].lineno,
                         f"the loop is broken between {a} and {b}: {roles[a]} publishes {sorted(a_out) or 'nothing'}, "
                         f"{roles[b]} subscribes to {sorted(b_in) or 'nothing'}")

    def check_lifecycle(self):
        for mod in self.modules:
            for scope in [mod.tree] + [n for n in ast.walk(mod.tree) if isinstance(n, ast.FunctionDef)]:
                body = scope.body
                instances = {}
                for stmt in walk_shallow(body):
                    if (isinstance(stmt, ast.Assign) and isinstance(stmt.value, ast.Call)
                            and isinstance(stmt.value.func, ast.Name) and stmt.value.func.id in self.nodes
                            and len(stmt.targets) == 1 and isinstance(stmt.targets[0], ast.Name)):
                        instances[stmt.targets[0].id] = (stmt.value.func.id, stmt.lineno)
                        self.check_instantiation(stmt.value, mod)
                    elif (isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call)
                            and isinstance(stmt.value.func, ast.Name) and stmt.value.func.id in self.nodes):
                        self.check_instantiation(stmt.value, mod)
                for var, (cname, line) in instances.items():
                    reg = first_method_call(body, var, "register_callbacks")
                    start = first_method_call(body, var, "start")
                    has_reg = any(isinstance(i, ast.FunctionDef) and i.name == "register_callbacks" for i in self.nodes[cname][0].body)
                    if has_reg and reg is None:
                        self.add("N08", ERROR, mod.path, line, f"{var} = {cname}(...): register_callbacks() is never called")
                    if start is None:
                        self.add("N09", ERROR, mod.path, line, f"{var} = {cname}(...): start() is never called, so it cannot publish or receive")
                    if reg is not None and start is not None and start < reg:
                        self.add("N10", ERROR, mod.path, start,
                                 f"{var}.start() runs before register_callbacks(): the Redis listener only starts if topics "
                                 "are already registered, so this node would never receive anything")

    def check_instantiation(self, call, mod):
        cname = call.func.id
        cfg = arg_of(call, 0, "config")
        if cfg is None:
            self.add("N11", ERROR, mod.path, call.lineno, f"{cname}() needs a config dict: {cname}(config['{cname}_Config'])")
        elif const_str(cfg) is not None:
            self.add("N11", ERROR, mod.path, call.lineno,
                     f"{cname}('{cfg.value}') passes a filename, but Node needs a dict: load the YAML and pass config['{cname}_Config']")
        elif isinstance(cfg, ast.Subscript) and self.config is not None:
            section = const_str(cfg.slice)
            if section is not None:
                self.used_sections.setdefault(section, (mod.path, call.lineno))
                self.node_sections[cname] = section

    def check_config(self):
        if self.config is None:
            return
        cpath = self.config_path
        sections = {}
        for section, (path, line) in self.used_sections.items():
            cfg = self.config.get(section)
            if cfg is None:
                self.add("Y01", ERROR, path, line, f"config section '{section}' is not in {cpath}")
                continue
            sections[section] = cfg
            for key, subkeys in NODE_SECTIONS.items():
                if key not in cfg:
                    self.add("Y02", ERROR, cpath, 1, f"{section} is missing {key}")
                    continue
                for sub in subkeys:
                    if sub not in cfg[key]:
                        self.add("Y02", ERROR, cpath, 1, f"{section}.{key} is missing '{sub}'")
            self.check_config_values(section, cfg, cpath)
        self.check_shared_infrastructure(sections, cpath)

    def check_config_values(self, section, cfg, cpath):
        for key in ("Event_Manager_Config", "Message_Manager_Config"):
            proto = (cfg.get(key) or {}).get("protocol")
            if proto is not None and proto not in COMM_PROTOCOLS:
                self.add("Y03", ERROR, cpath, 1, f"{section}.{key}: unsupported protocol '{proto}'")
        backend = (cfg.get("Knowledge_Config") or {}).get("knowledge_type")
        if backend is not None and backend not in KNOWLEDGE_BACKENDS:
            self.add("Y03", ERROR, cpath, 1, f"{section}.Knowledge_Config: unsupported knowledge_type '{backend}'")
        logger = cfg.get("Logger_Config") or {}
        ltype = str(logger.get("logger_type", "terminal")).lower()
        if ltype not in LOGGER_TYPES:
            self.add("Y03", ERROR, cpath, 1, f"{section}.Logger_Config: unsupported logger_type '{ltype}'")
        if ltype == "redis":
            component = section.replace("_Config", "")
            channel = (logger.get("redis") or {}).get("channel")
            if channel != f"{component}:logs":
                self.add("Y04", WARNING, cpath, 1, f"{section} logs to '{channel}'; the dashboard expects '{component}:logs'")

    def check_shared_infrastructure(self, sections, cpath):
        def endpoint(c):
            return (c or {}).get("knowledge_type"), (c or {}).get("host"), (c or {}).get("port"), (c or {}).get("db")

        stores = {s: endpoint(c.get("Knowledge_Config")) for s, c in sections.items()}
        if len(set(stores.values())) > 1:
            self.add("Y05", ERROR, cpath, 1, f"nodes do not share one knowledge store, so they cannot see each other's data: {stores}")

        def transport(c):
            return (c or {}).get("protocol"), (c or {}).get("host"), (c or {}).get("port"), (c or {}).get("bus")

        event_planes = {transport(c.get("Event_Manager_Config")) for c in sections.values()}
        message_planes = {transport(c.get("Message_Manager_Config")) for c in sections.values()}
        if len(event_planes) > 1:
            self.add("Y06", ERROR, cpath, 1, f"nodes use different event transports and cannot reach each other: {sorted(map(str, event_planes))}")
        # A message published on one plane reaches an event subscriber only if both planes are one transport.
        for topic, subs in self.subscribed.items():
            for pub in self.published.get(topic, []):
                sub_method = {s[3] for s in subs}
                crosses = (pub[3] == "publish_message" and "register_message_callback" not in sub_method) or \
                          (pub[3] == "publish_event" and "register_event_callback" not in sub_method)
                if crosses and event_planes != message_planes:
                    self.add("Y07", ERROR, pub[1], pub[2],
                             f"topic '{topic}' crosses from the {pub[3].split('_')[1]} plane to the other one, "
                             "but the two planes use different transports in the config")

    def check_observation(self):
        """Rules for nodes whose config section has Observation_Config."""
        if not isinstance(self.config, dict):
            return
        access = {"read_knowledge", "write_knowledge", "publish_event", "publish_message",
                  "service_started", "service_completed"}
        for name, (cls, mod) in self.nodes.items():
            section = self.config.get(self.node_sections.get(name, f"{name}_Config"))
            if not isinstance(section, dict) or "Observation_Config" not in section:
                continue
            methods = {i.name: i for i in cls.body if isinstance(i, ast.FunctionDef)}
            seen = set()
            for sub in ast.walk(cls):
                if not (isinstance(sub, ast.Call) and isinstance(sub.func, ast.Attribute)
                        and sub.func.attr in ("register_event_callback", "register_message_callback")):
                    continue
                cb = arg_of(sub, 1, "callback")
                fn = methods.get(cb.attr) if is_self_attr(cb) else None
                if fn is None or fn.name in seen:
                    continue
                seen.add(fn.name)
                if not any(isinstance(r, ast.Return) and r.value is not None for r in ast.walk(fn)):
                    self.add("O01", ERROR, mod.path, fn.lineno,
                             f"{name}.{fn.name} is observed, so it must return an rpclpy.rpio_observation.outcomes object; "
                             "returning None fails the activation")
                for target in thread_targets(fn):
                    worker = methods.get(target)
                    if worker is not None and any(isinstance(c, ast.Call) and is_self_attr(c.func) and c.func.attr in access
                                                  for c in ast.walk(worker)):
                        self.add("O02", ERROR, mod.path, worker.lineno,
                                 f"{name}.{target} runs on a thread started by the observed callback {fn.name}, but typed knowledge "
                                 "access and publishing raise outside the callback's own thread. Do this work in the callback")

    def check_markers(self, mod):
        open_markers = []
        for i, text in enumerate(mod.lines, 1):
            for m in CC_MARKER.finditer(text):
                name, kind = m.group(1), m.group(2)
                if kind == "START":
                    open_markers.append((name, i))
                elif open_markers and open_markers[-1][0] == name:
                    open_markers.pop()
                else:
                    self.add("M02", WARNING, mod.path, i, f"cc_{name} END has no matching START")
        for name, line in open_markers:
            self.add("M02", WARNING, mod.path, line, f"cc_{name} START has no matching END")

    def run(self):
        self.discover()
        if not self.nodes:
            anchor = self.modules[0].path if self.modules else "?"
            self.add("N00", ERROR, anchor, 1, "no class derives from rpclpy Node")
            return self.findings
        self.check_knowledge_classes()
        for name, (cls, mod) in self.nodes.items():
            self.check_node_class(name, cls, mod)
        for mod in self.modules:
            self.check_markers(mod)
        self.check_knowledge_seeding()
        self.check_topology()
        self.check_lifecycle()
        self.check_config()
        self.check_observation()
        return self.findings


# ---------------------------------------------------------------------------
# AST helpers
# ---------------------------------------------------------------------------
def has_timeit(fn):
    for d in fn.decorator_list:
        name = d.id if isinstance(d, ast.Name) else getattr(d, "attr", None)
        if name == "timeit_callback":
            return True
    return False


def thread_targets(fn):
    """Names of self.<method>s this function hands to a Thread/Process."""
    out = []
    for sub in ast.walk(fn):
        if isinstance(sub, ast.Call):
            for kw in sub.keywords:
                if kw.arg == "target" and is_self_attr(kw.value):
                    out.append(kw.value.attr)
    return out


def walk_shallow(body):
    """Statements in a scope, entering if/try/with/for but not nested defs or classes."""
    stack = list(reversed(body))
    while stack:
        stmt = stack.pop()
        yield stmt
        if isinstance(stmt, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        children = []
        for field in ("body", "orelse", "finalbody"):
            children.extend(getattr(stmt, field, []) or [])
        for h in getattr(stmt, "handlers", []) or []:
            children.extend(h.body)
        stack.extend(reversed(children))


def first_method_call(body, var, method):
    lines = []
    for stmt in walk_shallow(body):
        if isinstance(stmt, ast.Expr) and isinstance(stmt.value, ast.Call):
            f = stmt.value.func
            if isinstance(f, ast.Attribute) and f.attr == method and isinstance(f.value, ast.Name) and f.value.id == var:
                lines.append(stmt.lineno)
    return min(lines) if lines else None


def same_branch(fn, line_a, line_b):
    """False when the two lines sit in opposite arms of the same if/else."""
    for sub in ast.walk(fn):
        if isinstance(sub, ast.If) and sub.orelse:
            in_body = lambda ln: any(s.lineno <= ln <= s.end_lineno for s in sub.body)
            in_else = lambda ln: any(s.lineno <= ln <= s.end_lineno for s in sub.orelse)
            if (in_body(line_a) and in_else(line_b)) or (in_else(line_a) and in_body(line_b)):
                return False
    return True


def find_rpclpy(start):
    d = os.path.abspath(start)
    while True:
        candidate = os.path.join(d, "rpclpy")
        if os.path.isfile(os.path.join(candidate, "node.py")):
            return candidate
        parent = os.path.dirname(d)
        if parent == d:
            return None
        d = parent


def installed_rpclpy():
    """The rpclpy this Python would import, located without importing it."""
    spec = importlib.util.find_spec("rpclpy")
    for location in (spec.submodule_search_locations or []) if spec else []:
        if os.path.isfile(os.path.join(location, "node.py")):
            return location
    return None


def main(argv=None):
    p = argparse.ArgumentParser(description="Check rpclpy MAPLE-K nodes against the rpclpy API.")
    p.add_argument("files", nargs="+", help="module(s) that define the Node subclasses")
    p.add_argument("--messages", action="append", default=[], help="module(s) that define the knowledge/message classes")
    p.add_argument("--config", help="YAML config the nodes are sliced from")
    p.add_argument("--rpclpy", help="path to the rpclpy package (default: search upward from the first file, then the installed package)")
    p.add_argument("--external-topic", action="append", default=[], help="topic published from outside the loop, e.g. by the managed system")
    p.add_argument("--sink-topic", action="append", default=[], help="topic consumed outside the loop")
    p.add_argument("--external-knowledge", action="append", default=[], help="knowledge class written from outside the loop")
    p.add_argument("--partial", action="store_true", help="not all five MAPLE-K stages are present")
    p.add_argument("--json", action="store_true", help="print findings as JSON")
    args = p.parse_args(argv)

    root = (args.rpclpy or find_rpclpy(os.path.dirname(os.path.abspath(args.files[0])))
            or find_rpclpy(os.getcwd()) or installed_rpclpy())
    if not root:
        print("cannot find the rpclpy package: run this with the Python that has rpclpy installed, or pass --rpclpy",
              file=sys.stderr)
        return 2
    print(f"checking against rpclpy at {root}", file=sys.stderr)
    try:
        api = RpclpyApi(root)
        modules = [Module(f) for f in args.files]
        message_modules = [Module(f) for f in args.messages]
    except (OSError, SyntaxError, RuntimeError) as e:
        print(f"cannot run: {e}", file=sys.stderr)
        return 2

    config = None
    if args.config:
        if yaml is None:
            print("PyYAML is needed for --config", file=sys.stderr)
            return 2
        with open(args.config, encoding="utf-8") as f:
            config = yaml.safe_load(f)

    findings = Checker(modules, message_modules, api, config, args.config, args.external_topic,
                       args.sink_topic, args.external_knowledge, args.partial).run()
    findings.sort(key=lambda x: (x.severity != ERROR, x.path, x.line))
    errors = sum(f.severity == ERROR for f in findings)
    warnings = len(findings) - errors
    if args.json:
        print(json.dumps({"errors": errors, "warnings": warnings, "findings": [f.as_dict() for f in findings]}, indent=2))
    else:
        for f in findings:
            print(f)
        print(f"\n{errors} error(s), {warnings} warning(s)")
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
