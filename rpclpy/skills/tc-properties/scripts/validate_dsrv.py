#!/usr/bin/env python
"""Validate a DSRV spec: lint it, run it in the real checker, compare verdicts.

    python validate_dsrv.py spec.dsrv --trace spec.input

The trace declares what it should trip, in comments:

    // expect: v_wind_check @ 4
    // expect: v_timing @ 11, 12

With a trace, every Bool output must be true exactly on its expected ticks and
false everywhere else, so a property that can never fire fails. `violation` is
derived from the other expectations if the trace does not list it.

Exit code: 0 pass, 1 fail, 2 could not run. Rule meanings are in
references/dsrv_language.md.
"""
import argparse
import json
import os
import re
import shlex
import shutil
import subprocess
import sys

ERROR, WARNING = "ERROR", "WARNING"
CHECKER_REPO = "https://github.com/INTO-CPS-Association/robosapiens-trustworthiness-checker"
KEYWORDS = {
    "not", "and", "or", "as", "match", "matches", "trunc", "floor", "ceil", "round", "language", "use",
    "if", "then", "else", "true", "false", "default", "is_defined", "when", "latch", "init", "update", "defer",
    "dynamic", "eval", "fix", "partial", "abs", "sin", "cos", "tan", "dist", "monitored_at",
    "List", "Map", "Tuple", "Struct", "Int", "Float", "Str", "Bool", "Unit", "Any", "Expr",
}
IDENT = re.compile(r"(?<![\w.])([A-Za-z_]\w*)")
DECL = re.compile(r"^\s*(in|out|aux|var)\s+([A-Za-z_]\w*)\s*(?::\s*(.*?))?\s*$")
ASSIGN = re.compile(r"^\s*([A-Za-z_]\w*)\s*=(?![=>])(.*)$")
EXPECT = re.compile(r"//[ \t]*expect:[ \t]*([A-Za-z_]\w*)[ \t]*@[ \t]*([\d, \t]+)")
ROW = re.compile(r"^([A-Za-z_]\w*)\[(\d+)\]\s*=\s*(.*)$")
# Checker versions print values differently: `Bool(true)` and `Deferred` before v0.2.0, `true` and a bottom sign since.
TRUE_VALUES = {"Bool(true)", "true"}
DEFERRED_VALUES = {"Deferred", "\u22a5"}
COMPARE = r"(?:<=|>=|==|<|>|\+|-|\*|/)"


def strip_comments(text):
    """Blank out comments, keeping line numbers. String contents are kept apart by blank_strings."""
    text = re.sub(r"\(\*.*?\*\)", lambda m: "\n" * m.group(0).count("\n"), text, flags=re.S)
    return re.sub(r"//[^\n]*", "", text)


def blank_strings(text):
    return re.sub(r'"(?:[^"\\\n]|\\.)*"', '""', text)


class Spec:
    """Declarations and equations of a .dsrv file. Shared with tc-integrate."""

    def __init__(self, path):
        self.path = path
        with open(path, encoding="utf-8") as f:
            self.raw = f.read()
        self.raw_lines = self.raw.splitlines()
        self.inputs, self.outputs, self.auxes = {}, {}, {}  # name -> (type, line)
        self.defs = {}  # name -> (expression, line)
        self.problems = []  # (rule, severity, line, message)
        self.clean = blank_strings(strip_comments(self.raw))
        current = None
        for number, line in enumerate(self.clean.splitlines(), 1):
            if not line.strip():
                continue
            decl = DECL.match(line)
            if decl:
                kind, name, typ = decl.groups()
                table = {"in": self.inputs, "out": self.outputs}.get(kind, self.auxes)
                if name in self.inputs or name in self.outputs or name in self.auxes:
                    self.problems.append(("D05", ERROR, number, f"'{name}' is declared twice"))
                # One-line form: `out y: Int = x` is a declaration and its equation.
                inline = re.match(r"(.*?)\s*=(?![=>])(.*)$", typ or "")
                current = None
                if inline:
                    typ = inline.group(1)
                    self.defs[name] = (inline.group(2), number)
                    current = name
                table[name] = (typ, number)
                continue
            assign = ASSIGN.match(line)
            if assign:
                name, expr = assign.groups()
                if name in self.defs:
                    self.problems.append(("D05", ERROR, number, f"'{name}' has two definitions"))
                self.defs[name] = (expr, number)
                current = name
            elif current:
                expr, start = self.defs[current]
                self.defs[current] = (expr + "\n" + line, start)

    def references(self, expr):
        """Stream names an expression reads, lambda parameters included."""
        return {name for name in IDENT.findall(expr) if name not in KEYWORDS}

    def input_dependent(self):
        """Names whose value depends, transitively, on some input."""
        known = set(self.inputs)
        changed = True
        while changed:
            changed = False
            for name, (expr, _) in self.defs.items():
                if name not in known and self.references(expr) & known:
                    known.add(name)
                    changed = True
        return known

    def struct_fields(self, name):
        """{field: type} of a `Struct<...>` input."""
        typ = (self.inputs.get(name) or (None,))[0] or ""
        match = re.match(r"Struct<(.*)>$", typ.strip())
        fields = {}
        if match:
            depth, part = 0, ""
            for ch in match.group(1) + ",":
                if ch == "," and depth == 0:
                    if ":" in part:
                        key, value = part.split(":", 1)
                        fields[key.strip()] = value.strip()
                    part = ""
                    continue
                depth += (ch == "<") - (ch == ">")
                part += ch
        return fields


def lambdas(expr):
    r"""Yield (params, body) for each `\a: T, b: T -> body`."""
    for match in re.finditer(r"\\", expr):
        arrow = expr.find("->", match.end())
        if arrow < 0:
            continue
        params = re.findall(r"([A-Za-z_]\w*)\s*:", expr[match.end():arrow])
        depth, end = 0, len(expr)
        for i in range(arrow + 2, len(expr)):
            if expr[i] in "([{":
                depth += 1
            elif expr[i] in ")]}":
                if depth == 0:
                    end = i
                    break
                depth -= 1
            elif expr[i] == "," and depth == 0:
                end = i
                break
        yield params, expr[arrow + 2:end]


def list_get_indexes(expr):
    """The index argument of each List.get(list, index)."""
    for match in re.finditer(r"List\.get\(", expr):
        depth, comma = 0, None
        for i in range(match.end(), len(expr)):
            if expr[i] in "([{":
                depth += 1
            elif expr[i] in ")]}":
                if depth == 0:
                    if comma is not None:
                        yield expr[comma + 1:i]
                    break
                depth -= 1
            elif expr[i] == "," and depth == 0 and comma is None:
                comma = i


def lint(spec):
    problems = list(spec.problems)
    declared = {**spec.inputs, **spec.outputs, **spec.auxes}

    if re.search(r"!=", spec.clean):
        line = spec.clean[:spec.clean.index("!=")].count("\n") + 1
        problems.append(("D01", WARNING, line, "`!=` is a parse error on older checker builds: write !(a == b) to run on all of them"))

    for name, (_, line) in {**spec.outputs, **spec.auxes}.items():
        if name not in spec.defs:
            problems.append(("D02", ERROR, line, f"'{name}' is declared but has no equation: the checker panics"))
    for name, (expr, line) in spec.defs.items():
        if name not in declared:
            problems.append(("D03", ERROR, line, f"'{name}' has an equation but no declaration"))
        if name in spec.inputs:
            problems.append(("D04", ERROR, line, f"input '{name}' cannot have an equation"))
        params = {p for ps, _ in lambdas(expr) for p in ps}
        for ref in sorted(spec.references(expr) - set(declared) - params):
            problems.append(("D06", ERROR, line, f"'{name}' reads undeclared stream '{ref}'"))
        for ps, body in lambdas(expr):
            captured = spec.references(body) - set(ps)
            if captured:
                problems.append(("D08", ERROR, line,
                                 f"lambda in '{name}' captures {sorted(captured)}: the checker hangs with no output. "
                                 "Inline the value as a literal"))
        if re.search(r"(?<![\w.])\d+\.(?![\w.])", expr):
            problems.append(("D16", WARNING, line, f"'{name}' has a float with no digits after the point: newer checker builds "
                                                   "reject `5.`, so write 5.0"))
        if re.search(r"(?:\)|\d|\])\s*\[\d+\]", expr):
            problems.append(("D17", WARNING, line, f"'{name}' applies [n] to something other than a stream name: newer checker builds "
                                                   "refuse this, so define the expression as a stream and index that"))
        for match in re.finditer(r"([A-Za-z_]\w*)\[\d+\]", expr):
            if not re.search(r"default\(\s*$", expr[:match.start()]):
                problems.append(("D11", WARNING, line,
                                 f"{match.group(0)} in '{name}' has no default(...): the first rows are Deferred, not a verdict"))
        for index in list_get_indexes(expr):
            if not re.fullmatch(r"\s*\d+\s*", index):
                problems.append(("D15", WARNING, line,
                                 f"List.get with computed index `{index.strip()}` in '{name}': an out-of-range index panics and "
                                 "permanently stops this output, and an `if` around it does not help because both branches "
                                 "are evaluated. For the last element use a fold"))

    dependent = spec.input_dependent()
    for name, (_, line) in spec.outputs.items():
        if name in spec.defs and name not in dependent:
            problems.append(("D07", WARNING, line,
                             f"out '{name}' depends on no input: checker builds before v0.2.0 flood output and never terminate. Declare it aux"))

    floats = {n for n, (t, _) in declared.items() if t == "Float"}
    for stream in spec.inputs:
        for field, typ in spec.struct_fields(stream).items():
            if typ == "Float":
                floats.add(f"{stream}.{field}")
    for name, (expr, line) in spec.defs.items():
        for var in floats:
            v = re.escape(var)
            if (re.search(rf"(?<![\w.]){v}\s*{COMPARE}\s*-?\d+(?![\d.]|\w)", expr)
                    or re.search(rf"(?<![\w.])-?\d+\s*{COMPARE}\s*{v}(?![\w.])", expr)
                    or re.search(rf"default\(\s*{v}\[\d+\]\s*,\s*-?\d+\s*\)", expr)):
                problems.append(("D12", WARNING, line,
                                 f"'{name}' mixes Float {var} with an Int literal: checker builds before v0.2.0 panic at runtime. Write 1.0, not 1"))

    labels = {}
    for name, (typ, line) in spec.outputs.items():
        if typ != "Bool":
            problems.append(("D10", WARNING, line, f"out '{name}' is not `: Bool`, so the dashboard panel ignores it"))
        match = re.search(r"//\s*(.*\S)", spec.raw_lines[line - 1])
        if not match:
            problems.append(("D10", WARNING, line, f"out '{name}' has no trailing // label for the dashboard"))
        else:
            labels[name] = match.group(1)

    bools = [n for n, (t, _) in spec.outputs.items() if t == "Bool"]
    if "violation" in spec.defs:
        missing = set(bools) - {"violation"} - spec.references(spec.defs["violation"][0])
        if missing:
            problems.append(("D13", WARNING, spec.defs["violation"][1], f"`violation` leaves out {sorted(missing)}"))
    elif len(bools) > 1:
        problems.append(("D13", WARNING, 1, "no aggregate `violation` output"))

    used = set()
    for expr, _ in spec.defs.values():
        used |= spec.references(expr)
    for name, (_, line) in {**spec.inputs, **spec.auxes}.items():
        if name not in used:
            problems.append(("D14", WARNING, line, f"'{name}' is never read"))
    return sorted(problems, key=lambda p: (p[1] != ERROR, p[2]))


# ---------------------------------------------------------------------------
# Running the real checker
# ---------------------------------------------------------------------------
def repo_root(start):
    d = os.path.abspath(start)
    while True:
        if os.path.isdir(os.path.join(d, "trustworthiness_checker", "src")):
            return d
        parent = os.path.dirname(d)
        if parent == d:
            return None
        d = parent


def wsl_path(path):
    path = os.path.abspath(path).replace("\\", "/")
    return f"/mnt/{path[0].lower()}{path[2:]}" if re.match(r"[A-Za-z]:", path) else path


def checker_command(explicit, start):
    """argv prefix that runs the checker, and how paths must be written for it."""
    explicit = explicit or os.environ.get("TC_CHECKER")
    if explicit:
        command = shlex.split(explicit, posix=os.name != "nt")
        through_wsl = os.path.basename(command[0]).lower().startswith("wsl")
        return command, wsl_path if through_wsl else os.path.abspath
    root = repo_root(start) or repo_root(os.getcwd())
    binary = None
    if root:
        for profile in ("dev-fast", "debug", "release"):
            for name in ("trustworthiness_checker", "trustworthiness_checker.exe"):
                candidate = os.path.join(root, "trustworthiness_checker", "target", profile, name)
                if os.path.isfile(candidate):
                    binary = candidate
                    break
            if binary:
                break
    if binary is None:
        return None, None
    # The grammar moved between checker versions, so a missing file is not an error.
    for parts in (("syntax", "lalr.lalrpop"), ("lalr.lalrpop",)):
        grammar = os.path.join(root, "trustworthiness_checker", "src", "lang", "dsrv", *parts)
        if os.path.isfile(grammar):
            if os.path.getmtime(grammar) > os.path.getmtime(binary):
                print("note: the DSRV grammar is newer than the compiled checker; rebuild with cargo build --profile dev-fast")
            break
    # Without .exe the target directory holds a Linux binary, built inside WSL.
    if os.name == "nt" and not binary.endswith(".exe") and shutil.which("wsl.exe"):
        return ["wsl.exe", "--", wsl_path(binary)], wsl_path
    return [binary], os.path.abspath


def read_trace(path):
    with open(path, encoding="utf-8") as f:
        text = f.read()
    expected = {}
    for name, ticks in EXPECT.findall(text):
        expected.setdefault(name, set()).update(int(t) for t in re.findall(r"\d+", ticks))
    stamps = [int(s) for s in re.findall(r"^(\d+):", strip_comments(text), flags=re.M)]
    return expected, stamps


def run_trace(spec, trace, command, to_path, timeout):
    """Return failure messages, printing what each output did."""
    expected, stamps = read_trace(trace)
    failures = []
    if stamps != sorted(stamps):
        failures.append("trace timestamps decrease")
    ticks = len(set(stamps))
    try:
        proc = subprocess.run(command + [to_path(spec.path), "--input-file", to_path(trace)],
                              capture_output=True, timeout=timeout)
    except subprocess.TimeoutExpired:
        return [f"the checker did not finish in {timeout}s. Likely a constant `out` (D07) or a capturing lambda (D08)"]
    stdout = proc.stdout.decode("utf-8", "replace")
    stderr = proc.stderr.decode("utf-8", "replace")
    for line in (stderr + stdout).splitlines():
        if "panicked at" in line:
            at = (stderr + stdout).split(line, 1)[1].strip().splitlines()
            failures.append(f"checker panic: {at[0] if at else line}")
    if proc.returncode != 0 and not failures:
        detail = [l for l in stderr.splitlines() if l.strip() and "systemd" not in l]
        failures.append(f"checker exited {proc.returncode}: {' '.join(detail)[:600]}")
    if failures:
        return failures

    # Rows share one counter across outputs, so the k-th row of a name is its tick k.
    rows = {}
    for line in stdout.splitlines():
        match = ROW.match(line.strip())
        if match:
            rows.setdefault(match.group(1), []).append(match.group(3).strip())

    bools = [n for n, (t, _) in spec.outputs.items() if t == "Bool"]
    if "violation" in bools and "violation" not in expected and expected:
        expected["violation"] = set().union(*expected.values())
    print(f"trace: {ticks} ticks")
    for name in spec.outputs:
        values = rows.get(name, [])[:ticks]
        true_at = [i for i, v in enumerate(values) if v in TRUE_VALUES]
        deferred = sum(v in DEFERRED_VALUES for v in values)
        extra = f", {deferred} Deferred" if deferred else ""
        print(f"  {name}: {len(values)} rows, true at {true_at or 'no tick'}{extra}")
        if len(values) < ticks:
            failures.append(f"{name} produced {len(values)} rows for {ticks} ticks: some inputs are missing from the trace")
        if name not in bools:
            continue
        if expected:
            want = sorted(expected.get(name, ()))
            if name not in expected:
                failures.append(f"{name}: no `// expect:` line, so nothing shows this property can fire")
            elif true_at != want:
                failures.append(f"{name}: expected true at {want}, got {true_at}")
        elif not true_at:
            failures.append(f"{name} never fired on this trace")
    for name in expected:
        if name not in spec.outputs:
            failures.append(f"`// expect:` names '{name}', which is not an output")
    return failures


def main(argv=None):
    p = argparse.ArgumentParser(description="Lint a DSRV spec and run it in the real checker.")
    p.add_argument("spec")
    p.add_argument("--trace", help="offline .input trace; `// expect: name @ tick` comments state the verdicts")
    p.add_argument("--checker", help="command that runs the checker (default: $TC_CHECKER, else a build in a trustworthiness_checker/ folder above the spec)")
    p.add_argument("--timeout", type=int, default=30)
    p.add_argument("--lint-only", action="store_true")
    p.add_argument("--json", action="store_true", help="print lint findings as JSON")
    args = p.parse_args(argv)
    try:
        spec = Spec(args.spec)
    except OSError as e:
        print(f"cannot read spec: {e}", file=sys.stderr)
        return 2

    problems = lint(spec)
    if args.json:
        print(json.dumps([dict(rule=r, severity=s, line=l, message=m) for r, s, l, m in problems], indent=2))
    else:
        for rule, severity, line, message in problems:
            print(f"{args.spec}:{line}: {severity} [{rule}] {message}")
    errors = sum(p[1] == ERROR for p in problems)
    print(f"lint: {errors} error(s), {len(problems) - errors} warning(s)")
    if errors:
        return 1  # the checker would panic or hang on these, so running it adds nothing
    if args.lint_only:
        return 0

    command, to_path = checker_command(args.checker, os.path.dirname(os.path.abspath(args.spec)))
    if command is None:
        print(f"cannot find a built checker. Get it from {CHECKER_REPO}\n"
              "Build it with: cargo build --profile dev-fast\n"
              "Then pass --checker /path/to/target/dev-fast/trustworthiness_checker, or set TC_CHECKER to that path.\n"
              "Use --lint-only to lint without it.", file=sys.stderr)
        return 2
    if not args.trace:
        print("no --trace: the spec was linted but not run, so its verdicts are unproven")
        return 0
    failures = run_trace(spec, args.trace, command, to_path, args.timeout)
    for failure in failures:
        print(f"FAIL: {failure}")
    print("PASS" if not failures else "FAILED")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
