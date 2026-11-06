#!/usr/bin/env python3
"""exp4 -- run every swift_light service integration test.

What this checks
----------------
The end-to-end suite under ``src/test/service_integration_test/``, where the
services are wired together instead of tested one at a time:

    client -> proxy -> data servers   the object lifecycle (push, pull, delete)
    client -> data servers            write quorum and read fallback over HTTP
    proxy  <-> data servers           placement answers and heartbeat health
    ring   -> data servers            migration over HTTP: copy, handoff, lock

The servers, proxies and rings are started in-process on OS-assigned loopback
ports with temporary state, so no deployed cluster is touched.  This is a
correctness run, not a benchmark.

Write mode
----------
The optional ``MODE`` argument -- ``atomic`` (the default) or ``non_atomic``,
the same two ``deploy.py up`` accepts -- decides how the data servers these
tests start write their objects.  The mode is authoritative, so every store the
suite builds writes that way.

This suite is expected to stay green under either mode, and that is not a gap
in the switch: every check here asserts an *outcome* -- the bytes come back, the
quorum was reached, the fallback happened -- and an uninterrupted write leaves
the same outcome in both modes.  The checks that can tell the modes apart are
in exp3, where a failed save is simulated.

Prerequisites
-------------
Python 3.12, the repository source, and writable temporary storage.  The tests
bind localhost ports, so local connections must be allowed.  No deployed
cluster, input file, or earlier experiment is required.

Usage
-----
Run from the repository root::

    python3 experiments/swift_light/exp4_integration_tests.py

    # run the same suite with non-atomic writes
    python3 experiments/swift_light/exp4_integration_tests.py non_atomic

    # or keep the tests' temporary state somewhere specific
    python3 experiments/swift_light/exp4_integration_tests.py --temp-dir /tmp

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

INTEGRATION_TEST_DIRECTORIES = ("service_integration_test",)
"""The end-to-end suite: the only package that spans several services."""


def main(argv: list[str] | None = None) -> int:
    """Run the service integration suite and return the experiment exit code."""
    return run(
        argv,
        number=4,
        description="service integration tests for swift_light",
        directories=INTEGRATION_TEST_DIRECTORIES,
        write_modes=True,
    )


if __name__ == "__main__":
    raise SystemExit(main())
