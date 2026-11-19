#!/usr/bin/env python3
"""exp5 -- p95 write and read latency, against the design proposal's baseline.

What this checks
----------------
Two of the three numbers the design proposal sets as its performance baseline::

    p95 write latency   <= 50 ms     (metric 1)
    p95 read latency    <= 20 ms     (metric 3)

The measurement itself is a test, not this script: it lives in
``src/test/performance_eval/`` with the rest of the suite, and this script only
runs it.  That keeps one copy of the numbers and lets the same check run under
``python -m pytest src/test`` and in CI, the way experiments 3 and 4 run the
unit and integration suites.

What the test measures
----------------------
The backend, with no sockets at all: a real consistent-hashing ring answers
placement and a real ``DataStore`` per data server takes the bytes through the
atomic temp file, flush and rename write.  A write is the ring lookup plus one
copy per replica, in parallel; a read is the lookup plus the primary's store.
No TCP handshake, no connection close, no HTTP -- which is why these numbers are
a floor for what a client sees rather than that number itself.  The test module,
``src/test/performance_eval/test_backend_latency.py``, says exactly what is in
and out of the measurement.

Prerequisites
-------------
Python 3.12 and writable temporary storage.  No deployed cluster, no input
file, and no earlier experiment: every ring and store the test uses is built in
a temporary directory it owns, and no port is bound.

Usage
-----
Run from the repository root::

    python3 experiments/swift_light/exp5_latency.py

    # or keep the test's temporary state somewhere specific
    python3 experiments/swift_light/exp5_latency.py --temp-dir /tmp

The sample count is the test's own setting: the ``SWIFT_LIGHT_PERF_SAMPLES``
environment variable overrides the default of 100 writes and 100 reads.

Exit codes
----------
    0   both baselines were met: no case failed and both p95s are inside budget
    1   a baseline is missed, a case failed, or a test errored
    77  the suite could not be attempted: no tests found, or no writable
        temporary directory

Tests live in ``src/test/``.
"""
from __future__ import annotations

from test_suite_runner import run

PERFORMANCE_DIRECTORIES = ("performance_eval",)
"""The suite this experiment runs: the backend latency test."""


def main(argv: list[str] | None = None) -> int:
    """Run the backend latency test and return the experiment exit code."""
    return run(
        argv,
        number=5,
        description="backend write/read latency against the p95 baselines",
        directories=PERFORMANCE_DIRECTORIES,
        needs_loopback=False,
    )


if __name__ == "__main__":
    raise SystemExit(main())
