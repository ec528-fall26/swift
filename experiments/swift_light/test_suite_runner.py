#!/usr/bin/env python3
"""Shared runner for the experiments that execute the swift_light test suites.

``exp3`` runs the per-component unit tests and ``exp4`` runs the end-to-end
service integration tests.  The two experiments differ only in which
directories they discover, so the discovery rules, the prerequisite checks and
the 0/1/77 result convention live here once.

Both also take an optional write mode -- ``atomic`` (the default) or
``non_atomic``, the same two ``deploy.py up`` accepts -- which decides how the
data servers the tests start write their objects.  The mode is authoritative:
every store the suites build writes the way the run says, including the ones
that ask for ``atomic=True``.  So ``non_atomic`` is a *negative control* -- the
tests that pin the atomic guarantees fail, because those are exactly the
guarantees the mode gives up.  A green ``non_atomic`` run would mean the mode
never reached the stores.

This module is a helper, not an experiment: it has no ``main`` of its own.
"""
from __future__ import annotations

import argparse
import importlib
import socket
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

EXIT_OK = 0
"""Every test ran and passed."""

EXIT_FAIL = 1
"""A test failed, errored, or was skipped."""

EXIT_SKIP = 77
"""The suite could not be attempted (missing tests, temp dir, or loopback)."""

WRITE_MODES = ("atomic", "non_atomic")
"""How the tests' data servers write: temp file and rename, or in place."""

DEFAULT_WRITE_MODE = "atomic"
"""Mode the suites run in unless told otherwise."""

TESTS_ROOT = Path(__file__).resolve().parents[2] / "src" / "test"
"""The suite root.  Discovery uses it as the top-level directory, so the tests
import ``functional_test_support`` and ``swift_light.*`` exactly as they do
under pytest or ``python -m unittest``."""


def run(
    argv=None,
    *,
    number,
    description,
    directories,
    needs_loopback=True,
    write_modes=False,
):
    """Discover and run ``directories`` under the suite root.

    ``write_modes`` adds the optional ``MODE`` argument -- ``atomic`` or
    ``non_atomic`` -- that picks how the data servers the tests start write.

    Returns the experiment exit code: ``0`` when every test ran and passed,
    ``1`` when one failed, errored or was skipped, and ``77`` when the suite
    could not be attempted at all.
    """
    args = _parser(number, description, write_modes).parse_args(argv)
    mode = getattr(args, "mode", DEFAULT_WRITE_MODE)
    prefix = f"[exp{number}]"
    print(f"{prefix} {description}", flush=True)
    print(f"{prefix} tests     {TESTS_ROOT}", flush=True)
    print(f"{prefix} writes    {mode}", flush=True)

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

    # The mode must be in place before discovery, because importing a test
    # module binds the fixture it will call.
    with _write_mode(mode):
        suite = _discover(directories, prefix)
        if suite is None:
            return EXIT_SKIP
        count = suite.countTestCases()
        if count == 0:
            print(
                f"{prefix} cannot attempt the experiment: no tests found",
                file=sys.stderr,
            )
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
    if mode != DEFAULT_WRITE_MODE:
        print(
            f"{prefix} note: {mode} writes give up the crash-safety guarantees "
            "the atomic-save tests assert, so failures there are the expected "
            "outcome of this run, not a regression",
            flush=True,
        )
    print("RESULT: PASS" if ok else "RESULT: FAIL", flush=True)
    return EXIT_OK if ok else EXIT_FAIL


# ---------------------------------------------------------------------------
# Command line, prerequisites, discovery
# ---------------------------------------------------------------------------


def _parser(number, description, write_modes):
    """The command-line grammar the experiments share."""
    parser = argparse.ArgumentParser(prog=f"exp{number}", description=description)
    if write_modes:
        parser.add_argument(
            "mode",
            nargs="?",
            choices=WRITE_MODES,
            default=DEFAULT_WRITE_MODE,
            metavar="MODE",
            help=(
                "how the tests' data servers write to disk: 'atomic' writes a "
                "temp file and renames it into place, 'non_atomic' writes "
                f"straight to the destination (default: {DEFAULT_WRITE_MODE})"
            ),
        )
    parser.add_argument(
        "--temp-dir",
        metavar="PATH",
        help="existing writable directory for the tests' temporary state "
        "(default: the OS temporary directory)",
    )
    return parser


# ---------------------------------------------------------------------------
# Write mode
# ---------------------------------------------------------------------------


@contextmanager
def _write_mode(mode):
    """Make every store the suites build write the way the run says.

    The mode wins over a test's own request.  That is deliberate: ``non_atomic``
    is meant to be a *negative control*, so the atomic-save suite -- which asks
    for ``DataStore(..., atomic=True)`` because it exists to pin that promise --
    must actually be built non-atomically and fail.  If it passed, the mode
    never reached the stores and the run would have proved nothing.

    Two seams carry the mode, and nothing else in the suites picks one:
    ``DataStore``'s constructor and the ``data_node`` fixture that starts a
    data server.  The ring keeps its own ``atomic`` arguments -- the tests that
    care already pass both values explicitly.
    """
    if mode == DEFAULT_WRITE_MODE:
        yield
        return

    # The suites import their fixtures from here; patching before discovery
    # means the test modules bind the patched object.
    if str(TESTS_ROOT) not in sys.path:
        sys.path.insert(0, str(TESTS_ROOT))
    support = importlib.import_module("functional_test_support")
    from swift_light.data_server import storage

    # Every mode that is not the default writes in place.
    forced = mode == "atomic"
    original_store_init = storage.DataStore.__init__
    original_data_node = support.data_node

    def store_init(self, root=None, *, atomic=True):
        """Build a store in the run's mode, whatever the caller asked for."""
        original_store_init(self, root, atomic=forced)

    @contextmanager
    def data_node(root, **kwargs):
        """Start a data server in the run's mode, whatever the caller asked."""
        kwargs["atomic"] = forced
        with original_data_node(root, **kwargs) as started:
            yield started

    storage.DataStore.__init__ = store_init
    support.data_node = data_node
    try:
        yield
    finally:
        storage.DataStore.__init__ = original_store_init
        support.data_node = original_data_node


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
