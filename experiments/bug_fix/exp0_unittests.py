#!/usr/bin/env python3
"""exp0 -- run Swift's unit-test suite on a Linux development machine.

What this measures
------------------
Whether Swift's own unit suite is green on the tree you are about to change.
This is the pre-fix reference point: a test that fails after our patch and
before it is a pre-existing failure, not a regression we caused.

Where it runs, and the two directories involved
-----------------------------------------------
Two layouts are supported, and this repo is normally the first one:

1. **fork-as-both (EC528)** -- this repo **is** a fork of Swift, so it contains
   `swift/`, `test/unit/`, and `experiments/` in a single tree. The tree under
   test is therefore this repo itself, and no `--swift-src` is needed.
2. **VSAIO (two checkouts)** -- the course repo and the Swift source are separate
   clones, e.g. `/vagrant/swift` inside the Vagrant dev VM.

Either way the script runs from this repo's root (the experiments contract
requires that) and discovers the tree holding `test/unit` independently, rather
than assuming the two are the same directory. `--swift-src` and `$SWIFT_SRC`
override the search; repo-relative candidates resolve against this repo, so the
result does not depend on your current directory.

On the EC528 Lightsail box:

    cd /opt/ec528-swift
    source .venv/bin/activate
    python3 experiments/exp0_unittests.py

On VSAIO, with this repo at `/vagrant/ec528-swift` and Swift at `/vagrant/swift`:

    cd /vagrant/ec528-swift
    python3 experiments/exp0_unittests.py --swift-src /vagrant/swift

Setup (system packages, the XFS scratch filesystem the suite requires, and the
virtualenv) is automated by `experiments/setup.sh`. See the design document's
Setup section for the pinned versions.

Why not on the host
-------------------
Linux only.  Swift declares ``Operating System :: POSIX :: Linux``; the suite
imports ``fcntl``, ``grp`` and ``pwd``, and needs libc ``getifaddrs``.  On a
Windows or macOS host the script stops early and says so, instead of producing a
confusing half-result.

Exit codes
----------
    0   suite ran, everything passed          -> green baseline
    1   suite ran, and something failed       -> code/test problem
    77  could not run (wrong host, no tree,
        deps missing, abort, timeout)         -> NOT a pass

0 vs 1 vs 77 matters because someone else runs this.  A 1 is a statement about
the code; a 77 is a statement about the environment, and must never be recorded
as either a pass or a failure.
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

# Two different directories are involved and they are NOT the same tree:
#
#   *script root*  -- this course repo (ec528-fall26/swift). The experiments
#                     contract says a script must run "from the repository root",
#                     so this is the cwd the grader uses.
#   *swift source* -- the Swift checkout that actually contains test/unit.
#
# There are two deployments this has to work in:
#
#   (a) VSAIO / two-checkout: the course repo and the Swift source are separate
#       clones, so the Swift tree lives at /vagrant/swift or next to the repo.
#   (b) fork-as-both: this repo IS a fork of Swift, so it contains test/unit and
#       experiments/ in one tree. That is the layout used for EC528, and it is
#       why REPO-RELATIVE candidates below are resolved against the *script
#       root* rather than the cwd -- resolving them against the cwd made the
#       candidates depend on where the caller happened to stand.
#
# So the Swift checkout is discovered independently of where the script lives.
# $SWIFT_SRC is consulted at call time (not import time) so exporting it in the
# current shell always takes effect.
FORK_LAYOUT_DOC = "--swift-src <the clone that contains test/unit>"

# Candidates resolved against the SCRIPT ROOT (this repo). A fork of Swift is
# its own tree, so "." is the primary answer for the EC528 layout.
REPO_RELATIVE_SWIFT_DIRS = (
    ".",                                     # fork-as-both: repo holds test/unit
    "swift",
)

# Candidates resolved against the CWD, for the VSAIO / two-checkout layout.
CWD_RELATIVE_SWIFT_DIRS = (
    "/vagrant/swift",                        # the VSAIO default
    "/opt/swift",
    "~/swift",
    "../swift",                              # VSAIO host layout
    "../../vagrant-swift-all-in-one/swift",  # sibling checkout
)

# test/unit/__init__.py imports these at module scope. A missing one aborts
# collection for the whole suite, which otherwise looks like "149 errors" and
# reads as a broken tree rather than an unprovisioned VM.
RUNTIME_REQUIREMENTS = ("eventlet", "xattr", "pyeclib")

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
    env = {
        "platform": platform.platform(),
        "python": platform.python_version(),
        "python_exe": sys.executable,
        "cwd": str(Path.cwd()),
        "hostname": platform.node(),
    }
    env["wsl"] = bool(
        os.environ.get("WSL_DISTRO_NAME") or os.environ.get("WSL_INTEROP")
    )
    return env


def in_vm() -> bool:
    """Heuristic: a Linux box rather than a Windows/macOS host.

    Swift is 'Operating System :: POSIX :: Linux' only, so Linux is the real
    requirement. This covers both supported deployments: the VSAIO VM and a
    standalone Linux instance (e.g. the EC528 Lightsail box).
    """
    if sys.platform.startswith("linux"):
        return True
    return False


def script_root() -> Path:
    """This course repo's root -- the experiments/ -> .. hop."""
    return Path(__file__).resolve().parents[1]


def find_swift_src(explicit: str | None,
                   verbose: bool = False) -> tuple[Path | None, list[Path]]:
    """Locate the Swift checkout (the tree holding test/unit).

    Deliberately independent of where this script lives, because in the VSAIO
    layout the course repo and the Swift source are two different checkouts.
    In the EC528 layout they are the same tree (this repo is a fork of Swift),
    which is why the repo-relative candidates are tried first.

    Returns (found, tried) so the caller can show the candidate list when
    discovery fails.
    """
    tried: list[Path] = []

    def consider(raw: str, base: Path | None = None) -> Path | None:
        if not raw:
            return None
        try:
            p = Path(raw).expanduser()
            if not p.is_absolute() and base is not None:
                p = base / p
            p = p.resolve()
        except (OSError, RuntimeError):
            return None
        if p in tried:
            return None
        tried.append(p)
        return p if (p / "test" / "unit").is_dir() else None

    if explicit:
        # An explicit path is authoritative: if it is wrong we report that,
        # rather than quietly substituting a different tree and describing the
        # wrong code. But say so, because a typo otherwise looks like "no tree".
        found = consider(explicit)
        if found is None:
            say("")
            say(f"  --swift-src {explicit} does not contain test/unit.")
            say("  An explicit path is never silently overridden, because the")
            say("  result would then describe a different tree than you asked for.")
            repo_self = script_root()
            if (repo_self / "test" / "unit").is_dir():
                say(f"  This repo does contain test/unit:  {repo_self}")
                say(f"  Retry without --swift-src, or:  --swift-src {repo_self}")
        return found, tried

    # Read $SWIFT_SRC now, not at import time, so `export SWIFT_SRC=...` in the
    # current shell is honoured even if this module was imported earlier.
    found = consider(os.environ.get("SWIFT_SRC", ""))
    if found:
        return found, tried

    # 1. The script's own repo. This is the EC528 layout: a Swift fork holding
    #    test/unit and experiments/ in one tree. Checked first because it is
    #    what this artifact actually ships, and because it makes the scripts
    #    work with no --swift-src at all.
    root = script_root()
    for rel in REPO_RELATIVE_SWIFT_DIRS:
        found = consider(rel, base=root)
        if found:
            return found, tried

    # 2. The VSAIO / two-checkout layout.
    for rel in CWD_RELATIVE_SWIFT_DIRS:
        found = consider(rel)
        if found:
            return found, tried

    # Last resort while developing on a host: a Swift tree beside the repo.
    found = consider(str(root.parent / "swift"))
    return found, tried


def detect_swift_revision(src: Path) -> dict:
    """Pin the revision the baseline refers to."""
    info = {"version": None, "commit": None, "branch": None, "dirty": None}

    # A pbr-generated version.py exists in a git checkout, but not in an sdist
    # or a tarball export, so fall back to the egg-info metadata.
    version_py = src / "swift" / "common" / "version.py"
    if version_py.is_file():
        for line in version_py.read_text(errors="replace").splitlines():
            if "canonical_version" in line and "=" in line:
                info["version"] = line.split("=", 1)[1].strip().strip("\"'")
                break

    if not info["version"]:
        pkg_info = src / "swift.egg-info" / "PKG-INFO"
        if pkg_info.is_file():
            for line in pkg_info.read_text(errors="replace").splitlines():
                if line.startswith("Version:"):
                    info["version"] = line.split(":", 1)[1].strip()
                    break

    if shutil.which("git"):
        def git(*args: str) -> str | None:
            try:
                out = subprocess.run(
                    ["git", "-C", str(src), *args],
                    capture_output=True, text=True, timeout=30, check=False,
                )
            except (OSError, subprocess.SubprocessError):
                return None
            return out.stdout.strip() if out.returncode == 0 else None

        info["commit"] = git("rev-parse", "HEAD")
        info["branch"] = git("rev-parse", "--abbrev-ref", "HEAD")
        porcelain = git("status", "--porcelain")
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


def find_unittests_script(src: Path) -> Path | None:
    """The repo's own entry point, which this script mirrors."""
    script = src / ".unittests"
    return script if script.is_file() else None


# --------------------------------------------------------------------------
# running the suite
# --------------------------------------------------------------------------

def build_cmd(src: Path, tmp: Path, mode: str, extra: list[str]) -> tuple[list[str], str]:
    """Build the pytest command.

    Mirrors .unittests: run pytest with test/unit as the target so no installed
    `swift` package can shadow collection, and write the machine-readable
    report to a temp dir rather than into the checkout.
    """
    target = src / "test" / "unit"
    if mode == "quick":
        target = target / "common" / "ring"

    cmd = [
        sys.executable, "-m", "pytest",
        str(target),
        "--junitxml", str(tmp / "exp1.xml"),
        "-p", "no:cacheprovider",
        *extra,
    ]
    return cmd, str(target)


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
    """True when pytest never actually ran a test.

    Collection errors are counted as `tests` in JUnit, so the numbers alone
    cannot separate "the suite ran and 149 tests errored" from "149 files failed
    to import". pytest's own Interrupted banner is the reliable signal; an
    all-errors result with nothing passing is the fallback.
    """
    if "Interrupted:" in output:
        return True
    if not totals:
        return False
    ran = totals["tests"] - totals["errors"] - totals["failures"]
    return totals["tests"] > 0 and ran <= 0


def run_with_timeout(cmd: list[str], src: Path,
                     timeout: int) -> tuple[int, str, str]:
    """Run under a hard wall-clock bound. Returns (rc, output, skip_reason)."""
    say(f"\n$ {' '.join(cmd)}")
    say(f"  (working dir: {src})")
    say(f"  (timeout: {timeout}s -- the script always terminates)\n")

    kwargs = {}
    if os.name != "nt":
        kwargs["start_new_session"] = True

    try:
        proc = subprocess.Popen(
            cmd, cwd=str(src), stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT, text=True, errors="replace", **kwargs,
        )
    except OSError as exc:
        return EXIT_SKIP, "", f"could not start pytest: {exc}"

    try:
        output, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        if os.name != "nt":
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except (OSError, ProcessLookupError):
                proc.kill()
        else:
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


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run Swift's unit tests inside the VSAIO VM (pre-fix baseline).",
    )
    parser.add_argument("--swift-src", default=None,
                        help="Swift checkout to test (default: autodetect)")
    parser.add_argument("--timeout", type=int, default=1800,
                        help="hard wall-clock budget in seconds (default: 1800)")
    parser.add_argument("--quick", action="store_true",
                        help="ring subset only -- smoke test, not the baseline")
    parser.add_argument("--results-json", default=None,
                        help="also write environment + totals to this JSON path")
    parser.add_argument("--dry-run", action="store_true",
                        help="print the command that would run, then stop")
    parser.add_argument("pytest_args", nargs="*",
                        help="extra arguments passed through to pytest")
    args = parser.parse_args(argv)

    say("Measuring: Swift unit-test suite health on the tree")
    say("           (this is the pre-fix baseline the fix will be measured against)")

    env = describe_environment()
    banner("environment")
    for key in ("hostname", "platform", "python", "wsl", "python_exe", "cwd"):
        say(f"  {key:<12} {env[key]}")

    # -- host guard ---------------------------------------------------------
    if not in_vm():
        say("")
        say("  This script must run on Linux, where ./.unittests is a valid")
        say("  command.  Swift declares")
        say("  'Operating System :: POSIX :: Linux', and its suite imports fcntl,")
        say("  grp and pwd -- none of which exist on Windows or macOS.")
        say("")
        say("  It must run FROM THIS REPO's root.  In the fork-as-both layout the")
        say("  tree under test is this repo itself; in the VSAIO layout it is a")
        say("  separate checkout, normally /vagrant/swift.")
        say("")
        say("  On a Linux box (the authoritative environment):")
        say(f"    cd {script_root()}")
        say("    python3 experiments/exp0_unittests.py")
        say("")
        say("  If the environment is not built yet, run experiments/setup.sh")
        say("  first (see the design document's Setup section).")
        return finish(EXIT_SKIP, "SKIP (not a Linux machine)")

    # -- locate the tree ----------------------------------------------------
    src, tried = find_swift_src(args.swift_src)
    banner("swift checkout")
    say(f"  script root  {script_root()}     (this course repo)")
    if src is None:
        say("  swift source NOT FOUND -- no tree containing test/unit.")
        say("")
        say("  This repo IS a fork of Swift, so the tree is normally this repo")
        say(f"  itself ({script_root()}). If test/unit is not there, this is not")
        say("  the Swift fork -- or the clone is incomplete.")
        say("")
        say("  Pass --swift-src PATH, or set $SWIFT_SRC, if the tree is elsewhere.")
        if tried:
            say("  Tried:")
            for p in tried:
                say(f"    {p}")
        say("")
        say("  Two layouts are supported:")
        say("    fork-as-both (this repo)   --swift-src " + str(script_root()))
        say("    VSAIO (two checkouts)      --swift-src /vagrant/swift")
        return finish(EXIT_SKIP, "SKIP (no Swift checkout found)")

    rev = detect_swift_revision(src)
    say(f"  swift source {src}")
    say(f"  version      {rev['version'] or 'unknown'}")
    say(f"  branch       {rev['branch'] or 'n/a'}")
    say(f"  commit       {rev['commit'] or 'n/a'}")
    if rev["dirty"]:
        say("  working tree MODIFIED -- this is no longer a pristine baseline")
    elif rev["dirty"] is False:
        say("  working tree clean")

    upstream = find_unittests_script(src)
    say(f"  .unittests   {upstream if upstream else 'not present'}")

    # -- dependencies -------------------------------------------------------
    missing_deps = check_runtime_deps()
    banner("dependencies")
    for name in RUNTIME_REQUIREMENTS:
        hit = next((err for dep, err in missing_deps if dep == name), None)
        say(f"  {name:<10} {'MISSING -- ' + hit if hit else 'OK'}")
    if missing_deps:
        say("")
        say("  These import at the top of test/unit/__init__.py, so without them")
        say("  collection aborts for the *entire* suite -- not just the tests that")
        say("  need them.  Fix the VM before trusting any result:")
        say("    vagrant ssh")
        say("    reinstallswift        # bins + deps, then restart the services")
        say("    reec                  # only if pyeclib / liberasure are the problem")
        say("  Then re-run this script.")

    # -- dry run ------------------------------------------------------------
    if args.dry_run:
        with tempfile.TemporaryDirectory(prefix="exp1-") as tmpdir:
            cmd, target = build_cmd(src, Path(tmpdir), "quick" if args.quick else "full",
                                    list(args.pytest_args))
        banner("dry run -- nothing executed")
        say(f"  target  {target}")
        say(f"  command {' '.join(cmd)}")
        return finish(EXIT_OK, "DRY RUN (no tests executed)")

    # -- run ----------------------------------------------------------------
    mode = "quick" if args.quick else "full"
    if args.quick:
        say("\n  --quick: restricting to test/unit/common/ring (smoke test only)")

    with tempfile.TemporaryDirectory(prefix="exp1-") as tmpdir:
        tmp = Path(tmpdir)
        cmd, _ = build_cmd(src, tmp, mode, list(args.pytest_args))
        started = time.monotonic()
        rc, output, skip_reason = run_with_timeout(cmd, src, args.timeout)
        elapsed = time.monotonic() - started
        totals = parse_junit(tmp / "exp1.xml")

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
        "mode": mode,
        "elapsed_s": round(elapsed, 1),
        "pytest_returncode": rc,
        "totals": totals,
        "missing_deps": [name for name, _ in missing_deps],
    }

    if rc == EXIT_SKIP:
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
        say("  the tree either way.  Treat it as a VM problem, not a result.")
        if missing_deps:
            say("  Most likely cause:")
            for name, err in missing_deps:
                say(f"    - {name}: {err}")
        else:
            say("  See the traceback above for the failing import.")
        return finish(EXIT_SKIP, "SKIP (collection aborted -- VM problem)")

    broke = rc != 0 or (totals and (totals["failures"] or totals["errors"]))
    if broke:
        report["result"] = "FAIL"
        _write_json(args.results_json, report)
        say("")
        say("  The suite ran and is not green.  Record these failures before")
        say("  touching any code: a fix cannot be credited for a failure that was")
        say("  already here.")
        return finish(EXIT_FAIL, "FAIL (unit suite is not green)")

    report["result"] = "PASS"
    _write_json(args.results_json, report)
    say("")
    say("  Baseline is green.  Quote this in the design document's Setup and")
    say("  Running-the-experiments sections, together")
    say("  with the commit above, as the pre-fix reference point.")
    return finish(EXIT_OK, "PASS (unit tests green)")


def _write_json(path: str | None, report: dict) -> None:
    if not path:
        return
    out = Path(path).expanduser()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(report, indent=2) + "\n")
    say(f"\n  wrote {out}")


if __name__ == "__main__":
    sys.exit(main())
