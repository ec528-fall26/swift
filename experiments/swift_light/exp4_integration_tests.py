#!/usr/bin/env python3
"""exp4 -- run every swift_light service integration test.

What this checks
----------------
The end-to-end suite under ``src/test/service_integration_test/``, where the
services are wired together instead of tested one at a time:

    client -> proxy -> data servers   the object lifecycle (push, pull, delete)
    client -> data servers            write quorum and read fallback over HTTP
    proxy  <-> data servers           placement answers and heartbeat health

The servers, proxies and rings are started in-process on OS-assigned loopback
ports with temporary state, so no deployed cluster is touched.  This is a
correctness run, not a benchmark.

Prerequisites
-------------
Python 3.12, the repository source, and writable temporary storage.  The tests
bind localhost ports, so local connections must be allowed.  No deployed
cluster, input file, or earlier experiment is required.

Usage
-----
Run from the repository root::

    python3 experiments/swift_light/exp4_integration_tests.py

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
    )


if __name__ == "__main__":
    raise SystemExit(main())
