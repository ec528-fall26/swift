#!/usr/bin/env python3
"""exp3 -- run every swift_light unit test.

What this checks
----------------
The per-component suites under ``src/test/``:

    src/test/client/   the remote path -> partition key mapping
    src/test/data/     the config record, file store, atomic save, HTTP routes
    src/test/proxy/    ring layout and placement, the heartbeat monitor

Each test pins one component against its specification; flows that wire the
client, proxy and data servers together belong to exp4.  This is a correctness
run, not a benchmark.

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
    )


if __name__ == "__main__":
    raise SystemExit(main())
