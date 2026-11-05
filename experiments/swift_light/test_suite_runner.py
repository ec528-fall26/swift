#!/usr/bin/env python3
"""Shared runner for the experiments that execute the swift_light test suites.

``exp3`` runs the per-component unit tests and ``exp4`` runs the end-to-end
service integration tests.  The two experiments differ only in which
directories they discover, so the discovery rules, the prerequisite checks and
the 0/1/77 result convention live here once.

This module is a helper, not an experiment: it has no ``main`` of its own.
"""
from __future__ import annotations

import argparse
import socket
import sys
import tempfile
import unittest
from pathlib import Path

EXIT_OK = 0
"""Every test ran and passed."""

EXIT_FAIL = 1
"""A test failed, errored, or was skipped."""

EXIT_SKIP = 77
"""The suite could not be attempted (missing tests, temp dir, or loopback)."""

TESTS_ROOT = Path(__file__).resolve().parents[2] / "src" / "test"
"""The suite root.  Discovery uses it as the top-level directory, so the tests
import ``functional_test_support`` and ``swift_light.*`` exactly as they do
under pytest or ``python -m unittest``."""


def run(argv=None, *, number, description, directories, needs_loopback=True):
    """Discover and run ``directories`` under the suite root.

    Returns the experiment exit code: ``0`` when every test ran and passed,
    ``1`` when one failed, errored or was skipped, and ``77`` when the suite
    could not be attempted at all.
    """
    args = _parser(number, description).parse_args(argv)
    prefix = f"[exp{number}]"
    print(f"{prefix} {description}", flush=True)
    print(f"{prefix} tests     {TESTS_ROOT}", flush=True)

    try:
        selected = _check_temp_dir(args.temp_dir)
        if needs_loopback:
            _check_loopback()
    except OSError as exc:
        print(f"{prefix} cannot attempt the experiment: {exc}", file=sys.stderr)
        hint = "use --temp-dir with a writable directory"
        if needs_loopback:
            hint += " and allow localhost connections"
        print(f"{prefix} {hint}", file=sys.stderr)
        return EXIT_SKIP

    suite = _discover(directories, prefix)
    if suite is None:
        return EXIT_SKIP
    count = suite.countTestCases()
    if count == 0:
        print(f"{prefix} cannot attempt the experiment: no tests found", file=sys.stderr)
        return EXIT_SKIP

    # Point the tests' temporary directories at the checked location.
    tempfile.tempdir = str(selected)
    print(f"{prefix} temporary {selected}", flush=True)
    print(f"{prefix} running   {count} tests", flush=True)

    result = unittest.TextTestRunner(verbosity=2).run(suite)
    ok = result.wasSuccessful() and result.testsRun > 0 and not result.skipped
    print(
        f"{prefix} {'OK' if ok else 'FAIL'}: {result.testsRun} tests; "
        f"{len(result.failures)} failures, {len(result.errors)} errors, "
        f"{len(result.skipped)} skipped",
        flush=True,
    )
    print("RESULT: PASS" if ok else "RESULT: FAIL", flush=True)
    return EXIT_OK if ok else EXIT_FAIL


# ---------------------------------------------------------------------------
# Command line, prerequisites, discovery
# ---------------------------------------------------------------------------


def _parser(number, description):
    """The command-line grammar both experiments share."""
    parser = argparse.ArgumentParser(prog=f"exp{number}", description=description)
    parser.add_argument(
        "--temp-dir",
        metavar="PATH",
        help="existing writable directory for the tests' temporary state "
        "(default: the OS temporary directory)",
    )
    return parser


def _check_temp_dir(value):
    """Return the selected temporary directory after proving it is writable."""
    selected = Path(value).resolve() if value else Path(tempfile.gettempdir())
    if not selected.is_dir():
        raise OSError(f"temporary directory does not exist: {selected}")
    with tempfile.TemporaryDirectory(
        prefix="swift-light-suite-check-", dir=selected
    ) as probe:
        check = Path(probe) / "check"
        check.write_bytes(b"check")
        if check.read_bytes() != b"check":
            raise OSError("temporary file read-back failed")
    return selected


def _check_loopback():
    """Prove localhost sockets work before the tests start binding ports."""
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        with socket.create_connection(listener.getsockname(), timeout=2):
            connection, _ = listener.accept()
            connection.close()


def _discover(directories, prefix):
    """The suite for every named directory, or ``None`` and why it cannot run."""
    if not TESTS_ROOT.is_dir():
        print(
            f"{prefix} cannot attempt the experiment: no test tree at {TESTS_ROOT}",
            file=sys.stderr,
        )
        return None
    loader = unittest.defaultTestLoader
    suite = unittest.TestSuite()
    for name in directories:
        start = TESTS_ROOT / name
        if not start.is_dir():
            print(
                f"{prefix} cannot attempt the experiment: no tests at {start}",
                file=sys.stderr,
            )
            return None
        suite.addTests(
            loader.discover(str(start), pattern="test*.py", top_level_dir=str(TESTS_ROOT))
        )
    return suite
