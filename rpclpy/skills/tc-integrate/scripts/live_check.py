#!/usr/bin/env python
"""End-to-end test: real bridge, real checker and a scenario, on a throwaway Redis.

Starts its own Redis on a private port, so a running system's data is untouched. It fails if
any output stops updating, which is how a checker panic looks on a live run: the process stays
up while some properties quietly stop.

    python3 live_check.py --spec s.dsrv --routes s_redis_input.json5 --bridge tc_bridge.py \
        --scenario scenario.py --repo /path/to/repo

The scenario is a Python file with `def run(ctx):`. See assets/scenario_template.py.
On Windows, run this inside WSL, where the checker binary and redis-server live.

Exit code: 0 pass, 1 fail, 2 could not run.
"""
import argparse
import importlib.util
import json
import os
import pickle
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time

try:
    import redis
except ImportError:
    print("needs redis-py: pip install redis", file=sys.stderr)
    sys.exit(2)


class Context:
    """What a scenario can do."""

    def __init__(self, port, outputs):
        self.port = port
        self.client = redis.Redis(port=port, socket_connect_timeout=1)
        self.outputs = outputs
        self.rows, self.lock = [], threading.Lock()
        self.expectations = []
        self.clean_windows = []
        self._seq = 0
        self.notes = []

    def publish(self, topic, payload):
        """Publish on the bus as rpclpy would. Adds uid and timestamp unless given."""
        self._seq += 1
        body = dict(payload)
        body.setdefault("uid", f"sc{self._seq}")
        body.setdefault("timestamp", "scenario")
        self.client.publish(topic, json.dumps(body))

    def publish_raw(self, topic, text):
        """Publish exact text, for testing malformed messages."""
        self.client.publish(topic, text)

    def write_knowledge(self, obj):
        """Store an instance the way Node.write_knowledge does."""
        self.client.set(obj.name, pickle.dumps(obj))

    def sleep(self, seconds):
        time.sleep(seconds)

    def mark(self):
        with self.lock:
            return len(self.rows)

    def expect_true(self, output, since, within=3.0):
        """Wait until `output` is true on a row after the `since` mark."""
        deadline = time.time() + within
        while time.time() < deadline:
            with self.lock:
                hit = any(c == output and v == "true" for c, v in self.rows[since:])
            if hit:
                self.expectations.append((output, True, ""))
                return True
            time.sleep(0.05)
        self.expectations.append((output, False, f"not true within {within}s"))
        return False

    def expect_clean(self, since, ignore=()):
        """No property has been true since the mark."""
        with self.lock:
            bad = sorted({c for c, v in self.rows[since:] if v == "true" and c not in ignore})
        self.clean_windows.append(("clean window", not bad, f"true: {bad}" if bad else ""))
        return not bad


def port_free(port):
    # On mirrored WSL networking a closed port hangs instead of refusing, so this needs a timeout.
    with socket.socket() as s:
        s.settimeout(1.0)
        return s.connect_ex(("127.0.0.1", port)) != 0


def find_binary(repo):
    for profile in ("dev-fast", "debug", "release"):
        path = os.path.join(repo, "trustworthiness_checker", "target", profile, "trustworthiness_checker")
        if os.path.isfile(path):
            return path
    return None


def runtime_args(binary):
    """From v0.2.0 the default runtime published no outputs on live Redis input, so ask for async where it can be chosen."""
    try:
        text = subprocess.run([binary, "--help"], capture_output=True, text=True, timeout=20).stdout
    except (OSError, subprocess.SubprocessError):
        return []
    return ["--runtime", "async"] if "--runtime" in text else []


def output_names(spec):
    with open(spec, encoding="utf-8") as f:
        return re.findall(r"^\s*out\s+([A-Za-z_]\w*)\s*:\s*Bool\b", f.read(), flags=re.M)


def main(argv=None):
    p = argparse.ArgumentParser(description="Live end-to-end test of a spec, bridge and checker.")
    p.add_argument("--spec", required=True)
    p.add_argument("--routes", required=True)
    p.add_argument("--bridge", required=True)
    p.add_argument("--scenario", help="file with run(ctx); without it, this just checks a quiet system stays live")
    p.add_argument("--repo", default=os.getcwd(), help="project root: holds the importable messages module, and rpclpy if it is not installed")
    p.add_argument("--checker", help="checker binary (default: $TC_CHECKER, else a build in <repo>/trustworthiness_checker)")
    p.add_argument("--port", type=int, default=6392, help="private Redis port")
    p.add_argument("--rate", type=float, default=5.0, help="bridge rate in Hz")
    p.add_argument("--settle", type=float, default=2.0, help="seconds to run before the scenario")
    p.add_argument("--tail", type=float, default=3.0, help="seconds after the scenario in which every output must still update")
    args = p.parse_args(argv)

    binary = args.checker or os.environ.get("TC_CHECKER") or find_binary(args.repo)
    if not binary:
        print("cannot find a built checker. Get it from "
              "https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker\n"
              "Build it with: cargo build --profile dev-fast\n"
              "Then pass --checker /path/to/target/dev-fast/trustworthiness_checker, or set TC_CHECKER to that path.", file=sys.stderr)
        return 2
    if not shutil.which("redis-server"):
        print("redis-server is not on PATH", file=sys.stderr)
        return 2
    if not port_free(args.port):
        print(f"port {args.port} is in use. Pick another with --port; this test will not share a Redis", file=sys.stderr)
        return 2
    outputs = output_names(args.spec)
    if not outputs:
        print(f"{args.spec} declares no `out <name>: Bool`", file=sys.stderr)
        return 2

    sys.path.insert(0, os.path.abspath(args.repo))
    workdir = tempfile.mkdtemp(prefix="tc_live_")
    checker_log = os.path.join(workdir, "checker.log")
    bridge_log = os.path.join(workdir, "bridge.log")
    env = dict(os.environ, PYTHONPATH=os.path.abspath(args.repo) + os.pathsep + os.environ.get("PYTHONPATH", ""))
    procs = {}
    failures = []
    try:
        procs["redis"] = subprocess.Popen(["redis-server", "--port", str(args.port), "--save", "", "--appendonly", "no"],
                                          stdout=subprocess.DEVNULL, cwd=workdir)
        ctx = Context(args.port, outputs)
        for _ in range(50):
            try:
                ctx.client.ping()
                break
            except redis.RedisError:
                time.sleep(0.1)
        else:
            print(f"redis-server did not come up on port {args.port}", file=sys.stderr)
            return 2

        def sniff():
            ps = redis.Redis(port=args.port).pubsub(ignore_subscribe_messages=True)
            ps.subscribe(*outputs)
            try:
                for m in ps.listen():
                    with ctx.lock:
                        ctx.rows.append((m["channel"].decode(), m["data"].decode(errors="replace").strip().lower()))
            except redis.RedisError:
                pass  # the private Redis is shut down at the end of the test

        threading.Thread(target=sniff, daemon=True).start()

        scenario = None
        if args.scenario:
            spec = importlib.util.spec_from_file_location("tc_scenario", args.scenario)
            scenario = importlib.util.module_from_spec(spec)
            try:
                spec.loader.exec_module(scenario)
                if hasattr(scenario, "setup"):
                    scenario.setup(ctx)  # seed knowledge before the bridge reads it
            except Exception as e:
                print(f"the scenario's setup raised {type(e).__name__}: {e}", file=sys.stderr)
                return 2

        with open(bridge_log, "w") as log:
            procs["bridge"] = subprocess.Popen([sys.executable, "-u", os.path.abspath(args.bridge), "--port", str(args.port), "--rate", str(args.rate)],
                                               stdout=log, stderr=subprocess.STDOUT, cwd=args.repo, env=env)
        time.sleep(0.4)
        with open(checker_log, "w") as log:
            procs["checker"] = subprocess.Popen([binary, os.path.abspath(args.spec), "--input-redis-file", os.path.abspath(args.routes),
                                                 "--redis-output", "--redis-port", str(args.port)] + runtime_args(binary),
                                                stdout=log, stderr=subprocess.STDOUT, cwd=args.repo)
        time.sleep(args.settle)
        started = ctx.mark()
        if started == 0:
            failures.append(f"no output in the first {args.settle}s. Is a stream missing a value? Check the bridge log")

        if scenario:
            try:
                scenario.run(ctx)
            except Exception as e:
                failures.append(f"the scenario raised {type(e).__name__}: {e}")

        tail = ctx.mark()
        ctx.sleep(args.tail)
        with ctx.lock:
            total, tail_rows = list(ctx.rows), ctx.rows[tail:]
        counts = {o: sum(1 for c, _ in total if c == o) for o in outputs}
        tail_counts = {o: sum(1 for c, _ in tail_rows if c == o) for o in outputs}
        trues = {o: sum(1 for c, v in total if c == o and v == "true") for o in outputs}

        print(f"{'output':32s}{'rows':>7s}{'in tail':>9s}{'true':>6s}")
        for o in outputs:
            print(f"{o:32s}{counts[o]:7d}{tail_counts[o]:9d}{trues[o]:6d}")

        silent = [o for o in outputs if tail_counts[o] == 0]
        if silent:
            failures.append(f"these outputs stopped updating, so they can no longer detect anything: {silent}")
        with open(checker_log, errors="replace") as f:
            log_text = f.read()
        panics = re.findall(r"panicked at [^\n]*\n([^\n]*)", log_text)
        if panics:
            failures.append(f"{len(panics)} checker panic(s), first: {panics[0][:160]}")
        if procs["checker"].poll() is not None:
            failures.append(f"the checker exited with code {procs['checker'].returncode}")
        if procs["bridge"].poll() is not None:
            failures.append(f"the bridge exited with code {procs['bridge'].returncode}")
        with open(bridge_log, errors="replace") as f:
            bridge_text = f.read()
        rejected = bridge_text.count("rejected a message")
        if rejected:
            print(f"bridge rejected {rejected} malformed message(s)")
        for key in re.findall(r"key '(\w+)' not in Redis yet", bridge_text):
            print(f"note: knowledge key {key} was not in Redis when the bridge started")
        for label, ok, detail in ctx.expectations:
            print(f"expect {label}: {'ok' if ok else 'FAILED, ' + detail}")
            if not ok:
                failures.append(f"expected {label} to fire: {detail}")
        for label, ok, detail in ctx.clean_windows:
            print(f"{label}: {'ok' if ok else 'FAILED, ' + detail}")
            if not ok:
                failures.append(f"false alarm in a healthy window: {detail}")
    finally:
        for name in ("checker", "bridge", "redis"):
            if name in procs and procs[name].poll() is None:
                procs[name].terminate()
                try:
                    procs[name].wait(timeout=5)
                except subprocess.TimeoutExpired:
                    procs[name].kill()
    for failure in failures:
        print(f"FAIL: {failure}")
    if failures:
        print(f"logs: {workdir}")
    print("PASS" if not failures else "FAILED")
    sys.stdout.flush()
    # A scenario may leave daemon node threads behind; they must not hold the verdict.
    os._exit(0 if not failures else 1)


if __name__ == "__main__":
    main()
