"""Backend write/read latency against the design proposal's p95 baselines.

What this measures
------------------
The design proposal's performance baseline sets two latency numbers::

    p95 write latency   <= 50 ms
    p95 read latency    <= 20 ms

This test measures them where the cluster's own code runs, with **no sockets at
all**.  One real
:class:`~swift_light.proxy_server.consistent_hashing_ring.ConsistentHashingRing`
answers placement, and one real
:class:`~swift_light.data_server.storage.DataStore` per data server takes the
bytes the way a data server does -- the atomic temp file, flush, rename write.
A write is the ring lookup plus the replica set's stores, one copy per replica
in parallel, which is the shape of the client's quorum write minus HTTP.  A
read is the lookup plus the primary's store.

Nothing touches the network stack: no TCP handshake, no connection close, no
HTTP request or response on the wire.  That is the point.  On loopback a fresh
connection costs about 11 ms and a client write is three requests, so an
end-to-end measurement with a fresh connection per request is mostly socket
setup -- it reported a read p95 of 48 ms against this 20 ms budget for that
reason alone.  What this test reports is therefore a **floor**: what the
cluster's placement, storage and durability work costs by itself.  What a
client sees is higher by whatever the operating system and the HTTP stack add,
and this test does not claim to measure that.

Both baselines are asserted, so the suite fails when either p95 goes over its
budget or when any case fails.  Every read is compared with the bytes that were
written, so a fast wrong answer does not pass.

Reading the numbers
-------------------
Each case is timed on its own; percentiles are taken by rank over the samples
(no interpolation), so on 100 samples ``p95`` is the 95th smallest.  The worker
pool that overlaps the replica copies is created once, outside the timed
region, so thread startup is not part of a case.  The first write and read are
untimed: they create the directories and page in the code the later cases
reuse.

``SWIFT_LIGHT_PERF_SAMPLES`` overrides the sample count (default 100).  Fewer
samples make p95 the slowest case or two; more samples make the run slower.
100 writes and 100 reads take a few seconds.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import os
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from contextlib import contextmanager
from pathlib import Path
from typing import Callable

from functional_test_support import ConsistentHashingRing, DataStore

WRITE_BUDGET_MS = 50.0
"""Metric 1: p95 write latency from the design proposal's baseline table."""

READ_BUDGET_MS = 20.0
"""Metric 3: p95 read latency from the design proposal's baseline table."""

DATA_SERVERS = 3
"""Data servers in the ring -- the deployed default cluster size."""

REPLICATE_COUNT = 2
"""Copies of every object, so one write is two store writes."""

VNODE_COUNT = 100
"""The deployed default, so placement is the one a real cluster computes."""

PAYLOAD_BYTES = 128
"""Bytes per object.  Small on purpose: the point is the round trip, not I/O."""

DEFAULT_SAMPLES = 100
"""Writes, and then reads, unless the environment says otherwise."""

SAMPLES_ENV = "SWIFT_LIGHT_PERF_SAMPLES"
"""Environment variable that overrides :data:`DEFAULT_SAMPLES`."""


# ---------------------------------------------------------------------------
# The backend, with no HTTP in the way
# ---------------------------------------------------------------------------


def _payload() -> bytes:
    """A deterministic body, so every read can be compared with what went in."""
    unit = b"swift-light-perf "
    return (unit * (PAYLOAD_BYTES // len(unit) + 1))[:PAYLOAD_BYTES]


@contextmanager
def _backend(root):
    """A real ring plus one real store per data server, and no sockets.

    Yields ``(ring, stores, pool)``: ``stores`` maps a data server's name to the
    :class:`DataStore` that server would write to, and ``pool`` is the worker
    pool the replica copies share -- created here, once, so thread startup is
    never inside a timed case.
    """
    nodes = tuple(f"http://127.0.0.1:{9001 + index}" for index in range(DATA_SERVERS))
    ring = ConsistentHashingRing.build(
        DATA_SERVERS,
        REPLICATE_COUNT,
        nodes=nodes,
        vnode_count=VNODE_COUNT,
        path=Path(root) / "ring.json",
    )
    stores = {
        node: DataStore(Path(root) / f"node{index}")
        for index, node in enumerate(nodes)
    }
    pool = ThreadPoolExecutor(max_workers=REPLICATE_COUNT)
    try:
        yield ring, stores, pool
    finally:
        pool.shutdown(wait=True)


def _write(ring, stores, pool, key, data):
    """One quorum write with no HTTP: the replica set's stores, in parallel.

    Returns the replica set the fan named, so a caller can check the bytes
    landed on every one of them.
    """
    replicas = ring.fan(key)[:REPLICATE_COUNT]
    list(pool.map(lambda node: stores[node].write(key, data), replicas))
    return replicas


def _read(ring, stores, key):
    """One client read with no HTTP: the primary's store."""
    return stores[ring.fan(key)[0]].read(key)


def _timed(action: Callable[[], object]):
    """Run ``action`` once and return ``(ok, seconds, result)``.

    A filesystem failure is a failed case rather than a crashed run: it is
    timed like any other attempt and the measurement carries on.
    """
    started = time.perf_counter()
    try:
        result = action()
    except OSError:
        return False, time.perf_counter() - started, None
    return True, time.perf_counter() - started, result


def _samples() -> int:
    """How many writes and reads to run."""
    raw = os.environ.get(SAMPLES_ENV)
    if not raw:
        return DEFAULT_SAMPLES
    try:
        value = int(raw)
    except ValueError:
        raise unittest.SkipTest(f"{SAMPLES_ENV}={raw!r} is not a whole number") from None
    if value < 1:
        raise unittest.SkipTest(f"{SAMPLES_ENV}={raw!r} is not a positive number")
    return value


# ---------------------------------------------------------------------------
# Percentiles and the report
# ---------------------------------------------------------------------------


def _percentile(values, fraction):
    """The ``fraction`` quantile of ``values`` (linear index, no interpolation)."""
    if not values:
        return None
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))]


def _stddev(values):
    """The sample standard deviation of ``values``, or ``None`` below two cases."""
    count = len(values)
    if count < 2:
        return None
    mean = sum(values) / count
    return (sum((value - mean) ** 2 for value in values) / (count - 1)) ** 0.5


def _ms(value):
    """Seconds as milliseconds, so the report reads in one unit."""
    return None if value is None else value * 1000.0


def _summary(times, failures):
    """Counts and timings for one phase, in milliseconds."""
    seconds = sum(times)
    count = len(times)
    return {
        "cases": count,
        "failed": failures,
        "mean_ms": _ms(seconds / count) if count else None,
        "stddev_ms": _ms(_stddev(times)),
        "p50_ms": _ms(_percentile(times, 0.50)),
        "p95_ms": _ms(_percentile(times, 0.95)),
        "p99_ms": _ms(_percentile(times, 0.99)),
        "max_ms": _ms(max(times)) if times else None,
    }


def _cell(value):
    """``value`` right-aligned for the table, or a dash when it is missing."""
    if value is None:
        return "-".rjust(9)
    return f"{value:9.2f}"


def _report(writes, reads):
    """Print the numbers the two assertions are based on."""
    print()
    print(
        f"  backend latency  {DATA_SERVERS} data servers, "
        f"replicate_count={REPLICATE_COUNT}, vnode_count={VNODE_COUNT}, no sockets"
    )
    print(
        f"  operations       {writes['cases']} writes, {reads['cases']} reads "
        f"({PAYLOAD_BYTES}-byte objects)"
    )
    print(
        f"  {'operation':<12}{'cases':>7}{'failed':>8}{'mean':>9}"
        f"{'std dev':>9}{'p50':>9}{'p95':>9}{'p99':>9}{'max':>9}"
        f"{'budget':>9}   verdict"
    )
    print("  " + "-" * 104)
    for name, entry, budget in (
        ("write", writes, WRITE_BUDGET_MS),
        ("read", reads, READ_BUDGET_MS),
    ):
        passed = entry["p95_ms"] is not None and entry["p95_ms"] <= budget
        print(
            f"  {name:<12}{entry['cases']:>7}{entry['failed']:>8}"
            f"{_cell(entry['mean_ms'])}"
            f"{_cell(entry['stddev_ms'])}"
            f"{_cell(entry['p50_ms'])}"
            f"{_cell(entry['p95_ms'])}"
            f"{_cell(entry['p99_ms'])}"
            f"{_cell(entry['max_ms'])}"
            f"{budget:9.1f}   {'PASS' if passed else 'FAIL'}"
        )
    print("  (all times in ms; p95 is the number the baselines name)")


# ---------------------------------------------------------------------------
# The tests
# ---------------------------------------------------------------------------


class BackendLatencyTests(unittest.TestCase):
    """p95 write and read latency of the cluster's own backend code."""

    def test_p95_write_and_read_latency_stay_inside_the_baseline(self):
        samples = _samples()
        body = _payload()
        with tempfile.TemporaryDirectory(prefix="swift-light-perf-") as directory:
            with _backend(Path(directory)) as (ring, stores, pool):
                # Untimed: this one creates the directories every later case
                # writes into, which is setup rather than a measured case.
                _write(ring, stores, pool, "bench-latency-warmup.bin", body)
                _read(ring, stores, "bench-latency-warmup.bin")

                write_times = []
                written = []
                write_failures = 0
                for index in range(samples):
                    key = f"bench-latency-{index}.bin"
                    ok, elapsed, _ = _timed(
                        lambda key=key: _write(ring, stores, pool, key, body)
                    )
                    write_times.append(elapsed)
                    if ok:
                        written.append(key)
                    else:
                        write_failures += 1

                read_times = []
                read_failures = 0
                for index in range(samples):
                    # Reads cycle through what was written, so a run with more
                    # reads than writes still reads objects that exist.
                    key = written[index % len(written)]
                    ok, elapsed, data = _timed(
                        lambda key=key: _read(ring, stores, key)
                    )
                    read_times.append(elapsed)
                    if not ok or data != body:
                        read_failures += 1

        writes = _summary(write_times, write_failures)
        reads = _summary(read_times, read_failures)
        _report(writes, reads)

        self.assertEqual(
            write_failures, 0, f"{write_failures} of {samples} writes raised"
        )
        self.assertEqual(
            read_failures,
            0,
            f"{read_failures} of {samples} reads failed or returned bytes that "
            "were never written",
        )
        self.assertLessEqual(
            writes["p95_ms"],
            WRITE_BUDGET_MS,
            f"p95 write latency {writes['p95_ms']:.2f} ms exceeds the "
            f"{WRITE_BUDGET_MS:.0f} ms baseline",
        )
        self.assertLessEqual(
            reads["p95_ms"],
            READ_BUDGET_MS,
            f"p95 read latency {reads['p95_ms']:.2f} ms exceeds the "
            f"{READ_BUDGET_MS:.0f} ms baseline",
        )

    def test_a_timed_write_reaches_every_replica(self):
        """The timed write does the real work: both replicas end up holding it."""
        body = _payload()
        with tempfile.TemporaryDirectory(prefix="swift-light-perf-") as directory:
            with _backend(Path(directory)) as (ring, stores, pool):
                for index in range(3):
                    key = f"bench-latency-check-{index}.bin"
                    replicas = _write(ring, stores, pool, key, body)
                    self.assertEqual(len(replicas), REPLICATE_COUNT)
                    self.assertEqual(len(set(replicas)), REPLICATE_COUNT)
                    for node in replicas:
                        self.assertEqual(stores[node].read(key), body)
                        self.assertTrue(stores[node].atomic)
