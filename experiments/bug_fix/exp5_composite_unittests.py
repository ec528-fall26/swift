#!/usr/bin/env python3
"""exp5 -- run Swift's composite-builder unit tests on a Linux development machine.

What this measures
------------------
Whether Swift's own composite ring builder tests are green on the tree you are
about to change, or have changed. The target is

    test/unit/common/ring/test_composite_builder.py

which exercises compose, save, load, the persisted metadata format
(check_composite_meta), relative-path saving, save errors and the cooperative
builder. Run it on the pre-fix and on the patched tree: a test that fails on
both is a pre-existing failure, not a regression the composite fix caused.

Where it runs
-------------
The EC528 layout is fork-as-both: this repo **is** a fork of Swift, so it
contains `swift/`, `test/unit/`, and `experiments/` in a single tree, and no
`--swift-src` is needed. `--swift-src` and `$SWIFT_SRC` select another
checkout, e.g. a pre-fix clone for the before/after comparison.

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp5_composite_unittests.py
    echo "exit=$?"

Against a separate pre-fix checkout:

    python3 experiments/exp5_composite_unittests.py --swift-src /path/to/prefix/swift

Setup (system packages, the XFS scratch filesystem and the virtualenv) is
automated by `experiments/setup.sh`.

Why not on the host
-------------------
Linux only.  Swift declares ``Operating System :: POSIX :: Linux``; the suite
imports ``fcntl``, ``grp`` and ``pwd``, and needs libc ``getifaddrs``.  On a
Windows or macOS host the script stops early and says so, instead of producing a
confusing half-result.

Exit codes
----------
    0   tests ran, everything passed          -> green
    1   tests ran, and something failed       -> code/test problem
    77  could not run (wrong host, no tree,
        deps missing, abort, timeout)         -> NOT a pass

A 1 is a statement about the code; a 77 is a statement about the environment,
and must never be recorded as either a pass or a failure.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_SKIP = 77

TEST_REL = Path("test") / "unit" / "common" / "ring" / "test_composite_builder.py"

# test/unit/__init__.py imports these at module scope. A missing one aborts
# collection, which otherwise reads as a broken tree rather than an
# unprovisioned machine.
RUNTIME_REQUIREMENTS = ("pbr", "eventlet", "xattr", "pyeclib")

SUMMARY_PREFIXES = (
    "passed", "failed", "error", "errors", "skipped",
    "xfailed", "xpassed", "warning", "warnings", "deselected",
)


# --------------------------------------------------------------------------
# output helpers -- every line is written to be read by a stranger
# --------------------------------------------------------------------------

def banner(text: str) -> None:
    print(f"\n=== {text} ===")


def say(text: str = "") -> None:
    print(text, flush=True)


def finish(code: int, headline: str) -> int:
    say(f"\nRESULT: {headline}")
    return code


# --------------------------------------------------------------------------
# environment
# --------------------------------------------------------------------------

def describe_environment() -> dict:
    """Facts a different machine needs in order to be compared, not assumed."""
    return {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "python_exe": sys.executable,
        "cwd": str(Path.cwd()),
        "hostname": platform.node(),
    }


def script_root() -> Path:
    """This repo's root -- the experiments/ -> .. hop."""
    return Path(__file__).resolve().parents[1]


def find_swift_src(explicit: str | None) -> Path | None:
    """The Swift checkout: --swift-src, then $SWIFT_SRC, then this repo.

    An explicit path is authoritative: if it is wrong we report that, rather
    than quietly substituting a different tree and describing the wrong code.
    """
    raw = explicit or os.environ.get("SWIFT_SRC") or str(script_root())
    try:
        path = Path(raw).expanduser().resolve()
    except (OSError, RuntimeError):
        return None
    return path if (path / TEST_REL).is_file() else None


def detect_revision(src: Path) -> dict:
    """Commit / branch / dirty flag, so a run is attributable to a revision."""
    info = {"commit": None, "branch": None, "dirty": None}
    if not shutil.which("git"):
        return info

    def git(*args: str) -> str | None:
        try:
            out = subprocess.run(["git", "-C", str(src), *args],
                                 capture_output=True, text=True, timeout=30,
                                 check=False)
        except (OSError, subprocess.SubprocessError):
            return None
        return out.stdout.strip() if out.returncode == 0 else None

    info["commit"] = git("rev-parse", "HEAD")
    info["branch"] = git("rev-parse", "--abbrev-ref", "HEAD")
    porcelain = git("status", "--porcelain", "--", "swift", "test")
    if porcelain is not None:
        info["dirty"] = bool(porcelain)
    return info


def check_runtime_deps() -> list[tuple[str, str]]:
    """Return [(module, error)] for each Swift runtime dep that won't import."""
    import importlib
    missing = []
    for name in RUNTIME_REQUIREMENTS:
        try:
            importlib.import_module(name)
        except Exception as exc:
            missing.append((name, f"{type(exc).__name__}: {exc}"))
    return missing


# --------------------------------------------------------------------------
# running the tests
# --------------------------------------------------------------------------

def build_cmd(src: Path, tmp: Path, extra: list[str]) -> list[str]:
    """pytest on the one file, with the JUnit report outside the checkout."""
    return [
        sys.executable, "-m", "pytest",
        str(src / TEST_REL),
        "--junitxml", str(tmp / "exp5.xml"),
        "-p", "no:cacheprovider",
        *extra,
    ]


def parse_junit(path: Path) -> dict | None:
    """Summarise a JUnit XML report with the stdlib only."""
    if not path.is_file():
        return None
    import xml.etree.ElementTree as ET

    try:
        root = ET.parse(path).getroot()
    except ET.ParseError:
        return None

    suites = [root] if root.tag == "testsuite" else list(root.iter("testsuite"))
    if not suites:
        return None

    totals = {}
    for key in ("tests", "failures", "errors", "skipped"):
        totals[key] = sum(int(float(s.get(key, 0) or 0)) for s in suites)

    totals["worst"] = []
    for case in root.iter("testcase"):
        for child in case:
            if child.tag in ("failure", "error"):
                totals["worst"].append(
                    f"{case.get('classname', '?')}::{case.get('name', '?')}"
                )
                break
    totals["worst"] = totals["worst"][:10]
    return totals


def collection_aborted(output: str, totals: dict | None) -> bool:
    """True when pytest never actually ran a test."""
    if "Interrupted:" in output or "errors during collection" in output:
        return True
    if not totals:
        return False
    return totals["tests"] == 0


def run_with_timeout(cmd: list[str], src: Path,
                     timeout: int) -> tuple[int, str, str]:
    """Run under a hard wall-clock bound. Returns (rc, output, skip_reason)."""
    say(f"\n$ {' '.join(cmd)}")
    say(f"  (working dir: {src})")
    say(f"  (timeout: {timeout}s -- the script always terminates)\n")

    try:
        proc = subprocess.Popen(
            cmd, cwd=str(src), stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, errors="replace",
            start_new_session=True,
        )
    except OSError as exc:
        return EXIT_SKIP, "", f"could not start pytest: {exc}"

    try:
        output, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        except (OSError, ProcessLookupError):
            proc.kill()
        output, _ = proc.communicate()
        say(output or "")
        say(f"\n!! exceeded the {timeout}s budget and was killed.")
        return EXIT_SKIP, output or "", f"exceeded {timeout}s budget"

    say(output or "")
    return proc.returncode, output or "", ""


def print_pytest_summary(output: str) -> None:
    """Echo pytest's summary block -- it is the primary evidence."""
    picked = [
        ln.rstrip() for ln in output.splitlines()
        if ln.startswith(SUMMARY_PREFIXES) and "=" in ln
    ]
    if picked:
        banner("pytest summary")
        for ln in picked:
            say(ln)


def _write_json(path: str | None, report: dict) -> None:
    if not path:
        return
    out = Path(path).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    say(f"\n  wrote {out}")


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Swift's composite ring builder unit tests.",
    )
    parser.add_argument("--swift-src", default=None,
                        help="Swift checkout to test (default: SWIFT_SRC or this repo)")
    parser.add_argument("--timeout", type=int, default=600,
                        help="hard wall-clock budget in seconds (default: 600)")
    parser.add_argument("--results-json", default=None,
                        help="also write environment + totals to this JSON path")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the command that would run, then stop")
    parser.add_argument("pytest_args", nargs="*",
                        help="extra arguments passed through to pytest")
    args = parser.parse_args(argv)

    say("Measuring: Swift composite ring builder unit tests on the tree")
    say(f"           {TEST_REL.as_posix()}")

    env = describe_environment()
    banner("environment")
    for key in ("hostname", "platform", "python", "python_exe", "cwd"):
        say(f"  {key:<12} {env[key]}")

    # -- host guard ---------------------------------------------------------
    if not sys.platform.startswith("linux"):
        say("")
        say("  This script must run on Linux.  Swift declares")
        say("  'Operating System :: POSIX :: Linux', and its suite imports fcntl,")
        say("  grp and pwd -- none of which exist on Windows or macOS.")
        say("")
        say("  On a Linux box (the authoritative environment):")
        say(f"    cd {script_root()}")
        say("    python3 experiments/exp5_composite_unittests.py")
        return finish(EXIT_SKIP, "SKIP (not a Linux machine)")
    if args.timeout <= 0:
        return finish(EXIT_SKIP, "SKIP (timeout must be positive)")

    # -- locate the tree ----------------------------------------------------
    src = find_swift_src(args.swift_src)
    banner("swift checkout")
    if src is None:
        say(f"  NOT FOUND -- no tree containing {TEST_REL.as_posix()}.")
        say("  Pass --swift-src PATH, or set $SWIFT_SRC, if the tree is elsewhere.")
        return finish(EXIT_SKIP, "SKIP (no Swift checkout found)")

    rev = detect_revision(src)
    say(f"  source       {src}")
    say(f"  branch       {rev['branch'] or 'n/a'}")
    say(f"  commit       {rev['commit'] or 'n/a'}")
    if rev["dirty"]:
        say("  swift/ + test/ MODIFIED -- this run describes your working tree,")
        say("                       not the commit above")
    elif rev["dirty"] is False:
        say("  working tree clean")

    # -- dependencies -------------------------------------------------------
    missing_deps = check_runtime_deps()
    banner("dependencies")
    for name in RUNTIME_REQUIREMENTS:
        hit = next((err for dep, err in missing_deps if dep == name), None)
        say(f"  {name:<10} {'MISSING -- ' + hit if hit else 'OK'}")
    if missing_deps:
        say("")
        say("  These import before any composite test can be collected.  Run")
        say("  experiments/setup.sh, activate .venv, then re-run this script.")
        return finish(EXIT_SKIP, "SKIP (missing import-time dependencies)")

    # -- dry run ------------------------------------------------------------
    if args.dry_run:
        with tempfile.TemporaryDirectory(prefix="exp5-") as tmpdir:
            cmd = build_cmd(src, Path(tmpdir), list(args.pytest_args))
        banner("dry run -- nothing executed")
        say(f"  command {' '.join(cmd)}")
        return finish(EXIT_OK, "DRY RUN (no tests executed)")

    # -- run ----------------------------------------------------------------
    with tempfile.TemporaryDirectory(prefix="exp5-") as tmpdir:
        tmp = Path(tmpdir)
        cmd = build_cmd(src, tmp, list(args.pytest_args))
        started = time.monotonic()
        rc, output, skip_reason = run_with_timeout(cmd, src, args.timeout)
        elapsed = time.monotonic() - started
        totals = parse_junit(tmp / "exp5.xml")

    print_pytest_summary(output)

    banner("result")
    say(f"  elapsed      {elapsed:.1f}s")
    if totals:
        say(f"  collected    {totals['tests']}")
        say(f"  failed       {totals['failures']}")
        say(f"  errors       {totals['errors']}")
        say(f"  skipped      {totals['skipped']}")
        if totals["worst"]:
            say("  first failing tests:")
            for name in totals["worst"]:
                say(f"    - {name}")

    report = {
        "environment": env,
        "swift": rev,
        "target": TEST_REL.as_posix(),
        "elapsed_s": round(elapsed, 1),
        "pytest_returncode": rc,
        "totals": totals,
    }

    if skip_reason:
        report["result"] = "SKIP"
        report["reason"] = skip_reason
        _write_json(args.results_json, report)
        return finish(EXIT_SKIP, f"SKIP ({skip_reason})")

    if collection_aborted(output, totals):
        report["result"] = "SKIP"
        report["reason"] = "collection aborted"
        _write_json(args.results_json, report)
        say("")
        say("  Nothing ran: collection was interrupted, so this says nothing about")
        say("  the tree either way.  An import error in composite_builder.py (for")
        say("  example a TabError) also lands here -- see the traceback above.")
        return finish(EXIT_SKIP, "SKIP (collection aborted)")

    broke = rc != 0 or (totals and (totals["failures"] or totals["errors"]))
    if broke:
        report["result"] = "FAIL"
        _write_json(args.results_json, report)
        say("")
        say("  The composite tests ran and are not green.  Compare these named")
        say("  failures with a run on the pre-fix tree before crediting or")
        say("  blaming the composite fix.")
        return finish(EXIT_FAIL, "FAIL (composite unit tests are not green)")

    report["result"] = "PASS"
    _write_json(args.results_json, report)
    say("")
    say("  Composite builder unit tests are green on this revision.")
    return finish(EXIT_OK, "PASS (composite unit tests green)")


if __name__ == "__main__":
    sys.exit(main())
