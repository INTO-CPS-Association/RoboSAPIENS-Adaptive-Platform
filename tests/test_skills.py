"""The bundled Claude Code skills: packaging, the install command, and whether
the skills' own tools still agree with rpclpy.

The checker and smoke-test cases run the real scripts on a filled-in copy of the
skill's templates, so a change to the Node API that breaks them fails here.
"""
import importlib
import json
import os
import pathlib
import re
import subprocess
import sys

import pytest
import yaml

from rpclpy import skills

REPO = pathlib.Path(__file__).resolve().parents[1]
SKILLS = REPO / "rpclpy" / "skills"
PORT = SKILLS / "rpclpy-port"
NAMES = ["rpclpy-port", "tc-integrate", "tc-properties"]


def run(script, *args, cwd):
    env = dict(os.environ, PYTHONPATH=str(REPO), PYTHONDONTWRITEBYTECODE="1")
    return subprocess.run([sys.executable, str(script), *map(str, args)], cwd=cwd, env=env,
                          capture_output=True, text=True, timeout=120)


# ---------------------------------------------------------------------------
# What ships
# ---------------------------------------------------------------------------
def test_all_skills_are_bundled():
    assert sorted(skills.bundled()) == NAMES


@pytest.mark.parametrize("name", NAMES)
def test_front_matter_is_valid(name):
    text = (SKILLS / name / "SKILL.md").read_text(encoding="utf-8")
    front = yaml.safe_load(re.match(r"---\n(.*?)\n---\n", text, re.S).group(1))
    assert front["name"] == name
    assert front["description"]


@pytest.mark.parametrize("name", NAMES)
def test_every_file_a_skill_names_exists(name):
    text = (SKILLS / name / "SKILL.md").read_text(encoding="utf-8")
    named = set(re.findall(r"\b((?:references|assets|scripts)/[\w.]+\.\w+)", text))
    assert named
    for rel in named:
        assert (SKILLS / name / rel).is_file() or (SKILLS / "tc-properties" / rel).is_file(), rel


def test_no_references_to_a_private_repo():
    pattern = re.compile(r"ship.navigation|ksim|k_sim|CLAUDE\.md|mape_config|this repo\b|\.claude/skills", re.I)
    offenders = []
    for path in SKILLS.rglob("*"):
        if path.suffix in (".md", ".py") and path.parent != SKILLS:
            for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                if pattern.search(line):
                    offenders.append(f"{path.relative_to(SKILLS)}:{number}: {line.strip()[:80]}")
    assert not offenders, "\n".join(offenders)


# ---------------------------------------------------------------------------
# rpclpy-skills install
# ---------------------------------------------------------------------------
def test_install_copies_everything_with_a_version_stamp(tmp_path):
    assert skills.install(cwd=tmp_path) == 0
    for name in NAMES:
        installed = tmp_path / ".claude" / "skills" / name
        assert (installed / "SKILL.md").is_file()
        assert (installed / skills.STAMP).read_text().strip() == skills._version()
    assert (tmp_path / ".claude/skills/rpclpy-port/scripts/check_rpclpy_api.py").is_file()


def test_install_refuses_to_overwrite_and_changes_nothing(tmp_path, capsys):
    mine = tmp_path / ".claude" / "skills" / "tc-integrate" / "SKILL.md"
    mine.parent.mkdir(parents=True)
    mine.write_text("my edits")
    assert skills.install(cwd=tmp_path) == 1
    assert mine.read_text() == "my edits"
    assert not (tmp_path / ".claude" / "skills" / "rpclpy-port").exists()
    assert "--force" in capsys.readouterr().err


def test_install_force_replaces(tmp_path):
    mine = tmp_path / ".claude" / "skills" / "rpclpy-port" / "SKILL.md"
    mine.parent.mkdir(parents=True)
    mine.write_text("my edits")
    leftover = mine.parent / "old_file.py"
    leftover.write_text("stale")
    assert skills.install(["rpclpy-port"], force=True, cwd=tmp_path) == 0
    assert "my edits" not in mine.read_text()
    assert not leftover.exists()


def test_install_brings_the_skill_tc_integrate_imports(tmp_path):
    assert skills.install(["tc-integrate"], cwd=tmp_path) == 0
    target = tmp_path / ".claude" / "skills"
    assert sorted(p.name for p in target.iterdir()) == ["tc-integrate", "tc-properties"]
    result = run(target / "tc-integrate" / "scripts" / "check_tc_integration.py", "--help", cwd=tmp_path)
    assert result.returncode == 0, result.stderr


def test_install_user_goes_to_the_home_directory(tmp_path, monkeypatch):
    monkeypatch.setattr(pathlib.Path, "home", classmethod(lambda cls: tmp_path))
    assert skills.main(["install", "--user", "rpclpy-port"]) == 0
    assert (tmp_path / ".claude" / "skills" / "rpclpy-port" / "SKILL.md").is_file()


def test_install_rejects_an_unknown_skill(tmp_path):
    assert skills.install(["nope"], cwd=tmp_path) == 2
    assert not (tmp_path / ".claude").exists()


def test_list_names_every_skill(capsys):
    assert skills.main(["list"]) == 0
    out = capsys.readouterr().out
    assert all(name in out for name in NAMES)


# ---------------------------------------------------------------------------
# The port skill's tools, on its own templates
# ---------------------------------------------------------------------------
@pytest.fixture
def port(tmp_path):
    """The skill's templates with every TODO filled in."""
    messages = (PORT / "assets" / "messages_template.py").read_text(encoding="utf-8")
    (tmp_path / "demo_messages.py").write_text(messages.replace("TODO", "Demo"), encoding="utf-8")
    node = (PORT / "assets" / "node_template.py").read_text(encoding="utf-8")
    for old, new in [("TODO_messages", "demo_messages"), ("TODO_config.yaml", "demo_config.yaml"),
                     ("TODO_input_topic", "SensorReading"), ('reading["TODO_field"]', 'reading["value"]'),
                     ("TODO_detect(samples)", "samples[-1] > 10.0"), ("TODO_make_plan(finding)", "'slow_down'"),
                     ("TODO_is_safe(action)", "action == 'slow_down'"), ("TODO_apply(action)", "print(action)"),
                     ("TODOHistory", "DemoHistory"), ("TODOFinding", "DemoFinding"), ("TODOPlan", "DemoPlan")]:
        node = node.replace(old, new)
    assert "TODO_" not in node
    (tmp_path / "demo_maple_k.py").write_text(node, encoding="utf-8")
    result = run(PORT / "scripts" / "gen_config.py", "demo_config.yaml", cwd=tmp_path)
    assert result.returncode == 0, result.stderr
    (tmp_path / "fixture.json").write_text(json.dumps({"value": 42.0}), encoding="utf-8")
    return tmp_path


def check(directory, *extra):
    return run(PORT / "scripts" / "check_rpclpy_api.py", "demo_maple_k.py", "--messages", "demo_messages.py",
               "--config", "demo_config.yaml", "--external-topic", "SensorReading", "--rpclpy", REPO / "rpclpy",
               "--json", *extra, cwd=directory)


def findings(result):
    return {f["rule"] for f in json.loads(result.stdout)["findings"]}


def edit_node(directory, old, new):
    path = directory / "demo_maple_k.py"
    text = path.read_text(encoding="utf-8")
    assert old in text
    path.write_text(text.replace(old, new, 1), encoding="utf-8")


def test_filled_in_template_passes_the_checker(port):
    result = check(port)
    assert result.returncode == 0, result.stdout
    assert json.loads(result.stdout)["errors"] == 0


def test_filled_in_template_passes_the_smoke_test(port):
    result = run(PORT / "scripts" / "smoke_test_local.py", "demo_maple_k.py", "--config", "demo_config.yaml",
                 "--trigger", "SensorReading", "--payload", "fixture.json", "--count", 3,
                 "--expect", "new_data,anomaly,new_plan,isLegit", "--timeout", 20, cwd=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS" in result.stdout


def test_checker_finds_rpclpy_from_a_project_with_no_source_tree(port):
    """A pip user's project: nothing named rpclpy above it, so it must use the installed package."""
    result = run(PORT / "scripts" / "check_rpclpy_api.py", "demo_maple_k.py", "--messages", "demo_messages.py",
                 "--external-topic", "SensorReading", cwd=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "checking against rpclpy at" in result.stderr


def test_checker_accepts_the_keyword_only_evidence_argument(port):
    edit_node(port, "self.publish_event(event_key='new_data')",
              "self.publish_event(event_key='new_data', evidence={'history': _history})")
    assert "N05" not in findings(check(port))


def test_checker_still_rejects_an_unknown_keyword(port):
    edit_node(port, "self.publish_event(event_key='new_data')",
              "self.publish_event(event_key='new_data', nonsense=1)")
    assert "N05" in findings(check(port))


def test_checker_flags_reading_with_an_instance(port):
    edit_node(port, "self.read_knowledge(DemoHistory).samples", "self.read_knowledge(DemoHistory()).samples")
    result = check(port)
    assert result.returncode == 1
    assert "K10" in findings(result)


def test_observed_config_triggers_the_observation_rules(port):
    path = port / "demo_config.yaml"
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    for section in config.values():
        section["Observation_Config"] = {"namespace": "trial"}
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    result = check(port)
    assert result.returncode == 1
    assert {"O01", "O02"} <= findings(result)


def test_plain_config_does_not_trigger_the_observation_rules(port):
    assert not {"O01", "O02"} & findings(check(port))


def test_smoke_test_runs_an_observed_config_as_plain_and_says_so(port):
    path = port / "demo_config.yaml"
    config = yaml.safe_load(path.read_text(encoding="utf-8"))
    for section in config.values():
        section["Observation_Config"] = {"namespace": "trial"}
    path.write_text(yaml.safe_dump(config), encoding="utf-8")
    result = run(PORT / "scripts" / "smoke_test_local.py", "demo_maple_k.py", "--config", "demo_config.yaml",
                 "--trigger", "SensorReading", "--payload", "fixture.json",
                 "--expect", "new_data,anomaly,new_plan,isLegit", "--timeout", 20, cwd=port)
    assert result.returncode == 0, result.stdout + result.stderr
    assert "not exercised" in result.stdout


# ---------------------------------------------------------------------------
# The DSRV lint (no checker needed)
# ---------------------------------------------------------------------------
def lint(tmp_path, spec):
    path = tmp_path / "s.dsrv"
    path.write_text(spec, encoding="utf-8")
    script = SKILLS / "tc-properties" / "scripts" / "validate_dsrv.py"
    result = run(script, path, "--lint-only", "--json", cwd=tmp_path)
    return {(p["rule"], p["severity"]) for p in json.loads(result.stdout.split("\nlint:")[0])}


def test_lint_treats_not_equal_as_a_portability_warning(tmp_path):
    assert ("D01", "WARNING") in lint(tmp_path, "in a: Float\nout v: Bool // C1\nv = a != 1.0\n")


def test_lint_warns_about_constructs_only_some_checker_builds_accept(tmp_path):
    problems = lint(tmp_path, "in a: Float\nout v: Bool // C1\nv = default((a + 1.0)[1], 5.) > 1.0\n")
    assert {("D16", "WARNING"), ("D17", "WARNING")} <= problems


def test_lint_accepts_the_one_line_definition_form(tmp_path):
    problems = lint(tmp_path, "in a: Float\nout v: Bool = a > 1.0 // C1\n")
    assert not {rule for rule, severity in problems if severity == "ERROR"}


def test_lint_is_clean_on_words_from_newer_checker_grammars(tmp_path):
    problems = lint(tmp_path, "in a: Float\nout v: Bool // C1\nv = not (floor(a) > 1.0) and true\n")
    assert not {rule for rule, severity in problems if severity == "ERROR"}


def test_missing_checker_error_says_where_to_get_it(tmp_path):
    spec = tmp_path / "s.dsrv"
    spec.write_text("in a: Float\nout v: Bool // C1\nv = a > 1.0\n", encoding="utf-8")
    env = {k: v for k, v in os.environ.items() if k != "TC_CHECKER"}
    result = subprocess.run([sys.executable, str(SKILLS / "tc-properties" / "scripts" / "validate_dsrv.py"), str(spec)],
                            cwd=tmp_path, env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode == 2
    assert "robosapiens-trustworthiness-checker" in result.stderr


# ---------------------------------------------------------------------------
# The dashboard property panel
# ---------------------------------------------------------------------------
@pytest.fixture
def dashboard(fake_server):
    pytest.importorskip("dash")
    sys.modules.pop("rpclpy.DashboardApp", None)
    module = importlib.import_module("rpclpy.DashboardApp")
    yield module
    sys.modules.pop("rpclpy.DashboardApp", None)


def test_panel_rows_come_from_the_spec_and_comments_group_them(dashboard, tmp_path):
    spec = tmp_path / "s.dsrv"
    spec.write_text("in a: Float\n"
                    "out v_one: Bool     // C1 first\n"
                    "out v_two: Bool     // C2 shared\n"
                    "out v_three: Bool   // C2 shared\n"
                    "out v_bare: Bool\n"
                    "out count: Int      // not a property\n", encoding="utf-8")
    assert dashboard._parse_dsrv_bool_outputs(spec) == {
        "C1 first": ["v_one"], "C2 shared": ["v_two", "v_three"], "v_bare": ["v_bare"]}


def test_panel_state_goes_gray_when_a_signal_stops(dashboard):
    state = dashboard._tc_property_state
    assert state(["v"], {}, {}, now=100.0, stale_sec=2.0)[:2] == ("WAITING", None)
    assert state(["v"], {"v": False}, {"v": 99.5}, now=100.0, stale_sec=2.0)[:2] == ("OK", False)
    assert state(["v"], {"v": True}, {"v": 99.5}, now=100.0, stale_sec=2.0)[:2] == ("VIOLATED", True)
    assert state(["v"], {"v": True}, {"v": 90.0}, now=100.0, stale_sec=2.0)[:2] == ("STALE", None)


def test_run_dashboard_still_works_with_the_old_arguments(dashboard, monkeypatch):
    calls = []
    monkeypatch.setattr(dashboard.app, "run", lambda **kw: calls.append(kw))
    dashboard.run_dashboard(host="127.0.0.1", port=8050, debug=False, start_trust=False)
    assert calls == [{"host": "127.0.0.1", "port": 8050, "debug": False}]
    assert not dashboard._tc_config["properties"]
