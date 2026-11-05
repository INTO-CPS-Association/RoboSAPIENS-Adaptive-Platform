#!/usr/bin/env python
"""Run generated rpclpy nodes for real, with no Redis, on the in-process bus.

Imports the node module, rewrites its config to the `local` transports, wires
every node up the way main() does, injects trigger messages and records which
topics fire and which callbacks raise. It catches what static checks cannot:
runtime type errors, non-JSON payloads, unpicklable knowledge, wrong data flow.

    python smoke_test_local.py my_maple_k.py --config my_config.yaml \
        --trigger SensorReading --payload fixture.json --count 20 \
        --expect new_data,anomaly,new_plan,isLegit --stub sio

Exit code: 0 pass, 1 fail, 2 could not run.
"""
import argparse
import copy
import importlib.util
import json
import os
import sys
import threading
import time
import traceback
import uuid
from datetime import datetime


class Stub:
    """Swallows any attribute access or call, for module-level clients like `sio`."""

    def __getattr__(self, _):
        return self

    def __call__(self, *a, **k):
        return self


def localize(section):
    """Same node config, with every external service swapped for an in-process one."""
    cfg = copy.deepcopy(section)
    cfg.pop("Observation_Config", None)  # needs create_run and a journal, which this test does not exercise
    cfg["Event_Manager_Config"] = {"protocol": "local"}
    cfg["Message_Manager_Config"] = {"protocol": "local"}
    cfg["Knowledge_Config"] = {"knowledge_type": "local", "db": 0}
    cfg["Logger_Config"] = dict(cfg.get("Logger_Config") or {}, logger_type="terminal", log_level="WARNING",
                                format=(cfg.get("Logger_Config") or {}).get("format", "%(name)s %(levelname)s %(message)s"))
    return cfg


def main(argv=None):
    p = argparse.ArgumentParser(description="Smoke-test rpclpy nodes on the local bus.")
    p.add_argument("module", help="the generated node module")
    p.add_argument("--config", required=True, help="YAML with one <Class>_Config section per node")
    p.add_argument("--trigger", required=True, help="topic to inject on, normally the first node's input")
    p.add_argument("--payload", help="JSON file: one object, or a list of objects to play in order")
    p.add_argument("--count", type=int, default=1, help="how many trigger messages to send")
    p.add_argument("--expect", default="", help="comma-separated topics that must fire, in this order")
    p.add_argument("--stub", action="append", default=[], help="module attribute to replace with a no-op, e.g. sio")
    p.add_argument("--node", action="append", default=[], metavar="CLASS=SECTION", help="config section for a node, if not <Class>_Config")
    p.add_argument("--timeout", type=float, default=30, help="seconds to wait for background threads and expected topics")
    p.add_argument("--repo", default=os.getcwd(), help="directory holding the messages module, and rpclpy if it is not installed (default: cwd)")
    args = p.parse_args(argv)

    sys.path.insert(0, os.path.abspath(args.repo))
    sys.path.insert(0, os.path.dirname(os.path.abspath(args.module)))
    try:
        import yaml
        from rpclpy.node import Node
        from rpclpy.CommunicationManager import LocalBus
        from rpclpy.KnowledgeManager import reset_local_stores
    except ImportError as e:
        print(f"cannot run: {e}. This needs rpclpy>=1.0.2 and PyYAML in this Python", file=sys.stderr)
        return 2

    with open(args.config, encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if any(isinstance(s, dict) and "Observation_Config" in s for s in config.values()):
        print("note: Observation_Config removed for this run, so journals, outcomes and evidence are not exercised")
    if args.payload:
        with open(args.payload, encoding="utf-8") as f:
            payloads = json.load(f)
        payloads = payloads if isinstance(payloads, list) else [payloads]
    else:
        payloads = [{}]

    # Registered in sys.modules under its own name so pickle can find its classes.
    name = os.path.splitext(os.path.basename(args.module))[0]
    spec = importlib.util.spec_from_file_location(name, args.module)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    except Exception:
        print("FAIL importing the module:\n" + traceback.format_exc())
        return 1
    for attr in args.stub:
        setattr(module, attr, Stub())

    node_classes = {n: c for n, c in vars(module).items()
                    if isinstance(c, type) and issubclass(c, Node) and c is not Node and c.__module__ == name}
    if not node_classes:
        print(f"FAIL: {args.module} defines no Node subclass")
        return 1
    overrides = dict(item.split("=", 1) for item in args.node)

    reset_local_stores()
    LocalBus.reset()
    trace, failures, fired = [], [], {}
    lock = threading.Lock()

    def thread_hook(info):
        with lock:
            failures.append(f"thread {info.thread.name}: " + "".join(
                traceback.format_exception(info.exc_type, info.exc_value, info.exc_traceback)))

    threading.excepthook = thread_hook

    # Every delivery is recorded and isolated, so one raising callback is reported rather than aborting the chain.
    def guard(topic, callback):
        def guarded(message):
            with lock:
                fired[topic] = fired.get(topic, 0) + 1
            try:
                return callback(message)
            except Exception:
                with lock:
                    failures.append(f"callback on '{topic}': " + traceback.format_exc())
        return guarded

    original_publish = LocalBus.publish

    def recording_publish(self, topic, message):
        try:
            json.loads(message)
        except (TypeError, ValueError):
            with lock:
                failures.append(f"topic '{topic}' carried a non-JSON payload: {message!r}")
        with lock:
            trace.append(topic)
        return original_publish(self, topic, message)

    LocalBus.publish = recording_publish

    nodes = {}
    for cname, cls in node_classes.items():
        section = overrides.get(cname, f"{cname}_Config")
        if section not in config:
            print(f"FAIL: no config section '{section}' for {cname} (use --node {cname}=SECTION)")
            return 1
        try:
            node = cls(localize(config[section]))
            if hasattr(node, "register_callbacks"):
                node.register_callbacks()
            for mgr in (node.event_manager, node.message_manager):
                mgr.event_callbacks = {t: guard(t, cb) for t, cb in mgr.event_callbacks.items()}
            node.start()
            nodes[cname] = node
        except Exception:
            print(f"FAIL constructing {cname}:\n" + traceback.format_exc())
            return 1

    print(f"nodes up: {', '.join(nodes)}")
    bus = LocalBus.get()
    for i in range(args.count):
        body = dict(payloads[i % len(payloads)])
        body.setdefault("uid", uuid.uuid4().hex[:8])
        body.setdefault("timestamp", datetime.now().isoformat())
        bus.publish(args.trigger, json.dumps(body))

    expected = [t.strip() for t in args.expect.split(",") if t.strip()]
    deadline = time.time() + args.timeout
    while time.time() < deadline:
        with lock:
            done = all(t in trace for t in expected)
        background = [t for t in threading.enumerate() if t is not threading.main_thread() and t.is_alive()]
        if done and not background:
            break
        time.sleep(0.05)
    still_running = [t.name for t in threading.enumerate() if t is not threading.main_thread() and t.is_alive()]

    print("topic counts: " + (", ".join(f"{t}={trace.count(t)}" for t in dict.fromkeys(trace)) or "none"))
    print("delivered to a callback: " + (", ".join(f"{t}={n}" for t, n in fired.items()) or "none"))

    ok = True
    position = -1
    for topic in expected:
        later = [i for i, t in enumerate(trace) if t == topic and i > position]
        if not later:
            print(f"FAIL: expected topic '{topic}' did not fire in order")
            ok = False
        else:
            position = later[0]
            if topic not in fired:
                print(f"FAIL: expected topic '{topic}' was published but no callback received it")
                ok = False
    silent = [t for t in dict.fromkeys(trace) if t not in fired and t not in expected]
    if silent:
        print(f"note: published with no subscriber: {', '.join(silent)}")
    if still_running:
        print(f"FAIL: threads still running after {args.timeout}s: {', '.join(still_running)}")
        ok = False
    if failures:
        ok = False
        print(f"FAIL: {len(failures)} exception(s). On Redis, an exception in a callback kills that node's listener for good.")
        for failure in failures[:5]:
            print(failure)
        if len(failures) > 5:
            print(f"... and {len(failures) - 5} more")

    print("PASS" if ok else "FAILED")
    sys.stdout.flush()
    # Daemon threads left over must not hold the verdict hostage.
    os._exit(0 if ok else 1)


if __name__ == "__main__":
    sys.exit(main())
