#!/usr/bin/env python3
"""exp3 -- run every swift_light unit test.

What this checks
----------------
The per-component suites under ``src/test/``:

    src/test/client/   the remote path -> partition key mapping
    src/test/data/     the config record, file store, atomic save and crash
                       injection, HTTP routes
    src/test/proxy/    ring layout and placement, the heartbeat monitor

Each test pins one component against its specification; flows that wire the
client, proxy and data servers together belong to exp4.  This is a correctness
run, not a benchmark.

Write mode
----------
The optional ``MODE`` argument -- ``atomic`` (the default) or ``non_atomic``,
the same two ``deploy.py up`` accepts -- decides how the data servers these
tests start write their objects.  The mode is authoritative: every store the
suite builds writes that way, including the ones that ask for ``atomic=True``.

That makes ``non_atomic`` a negative control.  The suite is expected to be green
under ``atomic`` and red under ``non_atomic``, because nine checks in
``data/test_atomic_save.py`` and ``data/test_crash_atomicity.py`` assert exactly
what a non-atomic write gives up: an interrupted save leaves the old file
intact, a reader never sees a mixture, and a save writes, flushes, syncs and
closes before it replaces anything.  Three of the nine kill a child writer
outright -- ``SIGKILL`` on POSIX, ``TerminateProcess`` on Windows -- instead of
injecting an exception.  A green ``non_atomic`` run would mean the mode never
reached the stores.

Prerequisites
-------------
Python 3.12, the repository source, and writable temporary storage. Several
component tests start isolated data servers on OS-assigned loopback ports, so
local connections must be allowed.  No deployed cluster, input file, or earlier
experiment is required.

Usage
-----
Run from the repository root::

    python3 experiments/swift_light/exp3_unit_tests.py

    # run the same suite with non-atomic writes
    python3 experiments/swift_light/exp3_unit_tests.py non_atomic

    # or keep the tests' temporary state somewhere specific
    python3 experiments/swift_light/exp3_unit_tests.py --temp-dir /tmp

Exit codes
----------
    0   every test ran and passed
    1   a test failed, errored, or was skipped
    77  the suite could not be attempted: missing tests, an unwritable
        temporary directory, or no localhost connections

Tests live in ``src/test/``.
"""
from __future__ import annotations

from test_suite_runner import run

UNIT_TEST_DIRECTORIES = ("client", "data", "proxy")
"""The component suites: one directory per swift_light process."""


def main(argv: list[str] | None = None) -> int:
    """Run the unit suites and return the experiment exit code."""
    return run(
        argv,
        number=3,
        description="unit tests for the swift_light components",
        directories=UNIT_TEST_DIRECTORIES,
        write_modes=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
