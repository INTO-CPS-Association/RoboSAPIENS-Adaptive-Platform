"""Claude Code skills bundled with rpclpy, and the command that installs them.

Claude Code finds skills in `.claude/skills` and `~/.claude/skills`, not in
site-packages, and a wheel cannot run anything at install time. So the skills
ship here as package data, and `rpio-skills install` copies them into place.

    rpio-skills list
    rpio-skills install                 # into ./.claude/skills
    rpio-skills install --user          # into ~/.claude/skills
    rpio-skills install tc-integrate --force
"""
import argparse
import pathlib
import shutil
import subprocess
import sys

STAMP = ".rpclpy-version"
# Skills that import files from another skill.
REQUIRES = {"tc-integrate": ["tc-properties"]}


def bundled():
    """{skill name: its directory} for every skill shipped with this rpclpy."""
    here = pathlib.Path(__file__).resolve().parent
    return {p.parent.name: p.parent for p in sorted(here.glob("*/SKILL.md"))}


def _version():
    from rpclpy import __version__
    return __version__


def _description(skill_dir):
    """The first sentence of the skill's front matter description."""
    for line in (skill_dir / "SKILL.md").read_text(encoding="utf-8").splitlines():
        if line.startswith("description:"):
            return line.split(":", 1)[1].strip().split(". ")[0]
    return ""


def _with_dependencies(names):
    selected = []
    for name in names:
        for item in [name] + REQUIRES.get(name, []):
            if item not in selected:
                selected.append(item)
    return selected


def _warn_if_ignored(target):
    """Warn when git would keep the installed skills away from the rest of the team."""
    try:
        ignored = subprocess.run(["git", "check-ignore", "-q", str(target)], cwd=target.parent,
                                 capture_output=True, timeout=10).returncode == 0
    except (OSError, subprocess.SubprocessError):
        return
    if ignored:
        print(f"note: {target} is ignored by git, so teammates won't get these skills from the repository. "
              "Each of them can run this command too.")


def install(names=None, user=False, force=False, cwd=None):
    """Copy skills into place. Returns the process exit code."""
    skills = bundled()
    selected = _with_dependencies(names or list(skills))
    unknown = [n for n in selected if n not in skills]
    if unknown:
        print(f"unknown skill(s): {', '.join(unknown)}. Available: {', '.join(skills)}", file=sys.stderr)
        return 2

    base = pathlib.Path.home() if user else pathlib.Path(cwd or pathlib.Path.cwd())
    target = base / ".claude" / "skills"
    version = _version()

    # Decide everything first, so a refusal leaves nothing half installed.
    blocked = [n for n in selected if (target / n).exists()]
    if blocked and not force:
        for name in blocked:
            stamp = target / name / STAMP
            origin = f"from rpclpy {stamp.read_text(encoding='utf-8').strip()}" if stamp.exists() else "not installed by this command"
            print(f"{target / name} already exists ({origin})", file=sys.stderr)
        print(f"Nothing was changed. Pass --force to replace with the versions from rpclpy {version}.", file=sys.stderr)
        return 1

    target.mkdir(parents=True, exist_ok=True)
    for name in selected:
        destination = target / name
        if destination.exists():
            shutil.rmtree(destination)
        shutil.copytree(skills[name], destination, ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        (destination / STAMP).write_text(version + "\n", encoding="utf-8")
        print(f"installed {name} -> {destination}")

    if not user:
        _warn_if_ignored(target)
    print(f"Done: rpclpy {version}. Start a new Claude Code session if these don't appear. Re-run with --force after upgrading rpclpy.")
    return 0


def main(argv=None):
    parser = argparse.ArgumentParser(prog="rpio-skills", description="Install the Claude Code skills bundled with rpclpy.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("list", help="show the bundled skills")
    add = commands.add_parser("install", help="copy skills to ./.claude/skills, or ~/.claude/skills with --user")
    add.add_argument("names", nargs="*", help="skills to install (default: all)")
    add.add_argument("--user", action="store_true", help="install for all your projects, in ~/.claude/skills")
    add.add_argument("--force", action="store_true", help="replace skills that are already installed")
    args = parser.parse_args(argv)

    if args.command == "list":
        print(f"Skills in rpclpy {_version()}:")
        for name, path in bundled().items():
            print(f"  {name}: {_description(path)}")
        return 0
    return install(args.names, user=args.user, force=args.force)
