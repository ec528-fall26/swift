#!/usr/bin/env python3
"""exp0 -- what atomic writes cost while the cluster is running.

What this measures
------------------
Atomic writes are the safer ones: the bytes go to a temp file, are flushed, and
are renamed into place, so an interruption never leaves a half-written file.
Non-atomic writes go straight to the destination: fewer operations, no temp
file, but a crash mid-write leaves the file truncated.  This measures what that
safety costs in operation, which is the overhead claim the design document
leaves to an experiment.

The same mixed workload runs unchanged against a cluster left running by
``deploy.py up atomic`` and by ``deploy.py up non_atomic``.  Each run reports
one mode; the two reports are what get compared.

Prerequisites
-------------
The cluster must already be up, deployed in the mode ``--mode`` names.  This
script never starts or stops it::

    python3 src/swift_light/deploy.py up non_atomic     # in another terminal

The workload
------------
``--operations`` cases (10,000 by default) are executed one at a time against
the live cluster, in a seeded random order:

* 80% of the cases touch **data** (``--data-share``), half writing a file and
  half deleting one that was written earlier.  They go through the real client,
  so one case is one object write (or delete) reaching ``replicate_count`` data
  servers, not one file.
* 20% of the cases touch the **ring**: adding a data server, then removing one.
  Both are rebalances, and a rebalance moves one vnode point at a time -- with
  the default 100 vnodes, one membership change is 100 ring writes.  So 2,000
  of these cases write the ring 200,000 times, which is where atomic mode's temp
  file, rename and directory flush are paid over and over.

The ring is benchmarked on a copy, never on the deployed one
------------------------------------------------------------
A membership change is replayed against ``bench_ring.json`` -- a copy of the
deployed ring, in the same directory so it is the same filesystem, deleted
afterwards.  The deployed ring is what placed every stored object, so churning
its membership would leave a layout no running proxy agrees with; the ring's own
write path is identical either way, which is all this measures.

The rebalance is handed a data server that holds no files, so a ring case
measures the ring's writes and not the movement of the cluster's objects.  The
in-memory work per step -- rebuilding the fan areas, finding the changed arcs --
is the same in both modes and is included in the numbers; the ring difference
between the two modes therefore *understates* the pure write cost.

Usage
-----
Run from the repository root, with the cluster already up::

    python3 experiments/swift_light/exp0_atomic_overhead.py --mode non_atomic

and again with ``--mode atomic`` after redeploying.

The full 10,000 cases take roughly 20 minutes per mode, because the ring cases
are 100 ring writes each -- ``--operations 200`` is a smoke test, but only the
full run averages away the run-to-run noise (about 5% here).

``--mode`` must name the mode the cluster was actually deployed with: it labels
the run (nothing on the cluster records the mode) and picks the ring write path
this experiment uses for its own membership changes.  Labelling a run with the
other mode would measure that mode's write path under a misleading name.

Each run reports one mode.  ``--json out.json`` saves the report, so a run in
each mode leaves two files to read side by side -- run them one after the other
on the same machine, and prefer A/B/A if the machine is noisy.

Reading the numbers
-------------------
Every case is timed on its own.  A *file write* case is a quorum write, so it
is ``replicate_count`` disk writes; a *node join* or *node leave* case is one
rebalance, i.e. ``vnode_count`` ring writes plus the in-memory re-planning.

The cases go through the shipped client's own lookup and quorum code, but over
one reused connection per server (:class:`KeepAliveClient`): opening a new
connection per request costs about 11 ms on loopback, and a case is three
requests, so the TCP setup would be several times the write being measured --
and, being equal in both modes, it would hide the difference this exists to
find.  What the client *does* is unchanged.

Exit codes
----------
``0``   the workload ran and every case succeeded
``1``   the workload ran but some cases failed, or the report could not be
        written
``77``  could not be attempted: the proxy is unreachable, the cluster refused
        the probe write, bad arguments, or ``--mode`` was not given
"""

from __future__ import annotations

import argparse
import http.client
import json
import os
import random
import shutil
import sys
import tempfile
import time
import urllib.error
import urllib.request
from collections import deque
from pathlib import Path
from typing import Any, Callable, Iterable
from urllib.parse import quote, urlsplit

# The package is imported by its full path (there is no ``__init__.py``), so
# ``src/`` must be importable before any ``swift_light.*`` module is.  This runs
# before those imports on purpose, exactly as deploy.py does it.  From
# ``experiments/swift_light/`` the repository root is two directories up.
_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from swift_light.client.client import (  # noqa: E402
    PROXY_BASE_URL,
    Client,
    ClientError,
)
from swift_light.proxy_server import consistent_hashing_ring as _ring  # noqa: E402
from swift_light.proxy_server.consistent_hashing_ring import (  # noqa: E402
    ConsistentHashingRing,
)

__all__ = [
    "CATEGORIES",
    "DEFAULT_DATA_SHARE",
    "DEFAULT_OPERATIONS",
    "DEFAULT_PAYLOAD_BYTES",
    "EXIT_SKIP",
    "EXIT_FAIL",
    "EXIT_OK",
    "BenchError",
    "KeepAliveClient",
    "main",
    "run",
]

EXIT_OK = 0
"""Every case succeeded."""

EXIT_FAIL = 1
"""The workload ran, but at least one case failed."""

EXIT_SKIP = 77
"""Could not be attempted: no cluster, bad arguments, wrong mode."""

DEFAULT_OPERATIONS = 10_000
"""Cases to run -- the workload the design document asks for."""

DEFAULT_DATA_SHARE = 0.8
"""Fraction of the cases that touch data rather than the ring."""

DEFAULT_PAYLOAD_BYTES = 128
"""Bytes per stored object.  Small on purpose: the point is the write step."""

DEFAULT_ACCOUNT = "bench"
"""Account name every object is stored under, so a run can find its own keys."""

DEFAULT_SEED = 0
"""Seed for the interleaving, so two modes see the same order of cases."""

DEFAULT_PROGRESS_EVERY = 1000
"""Cases between progress lines (to stderr)."""

FILE_WRITE = "file write"
FILE_DELETE = "file delete"
NODE_JOIN = "node join"
NODE_LEAVE = "node leave"

CATEGORIES = (FILE_WRITE, FILE_DELETE, NODE_JOIN, NODE_LEAVE)
"""The four kinds of case, in report order."""

LIVE_RING_PATH = Path(_ring.__file__).with_name(_ring.DEFAULT_RING_FILENAME)
"""The deployed ring, at the ring module's own default (what the proxy reads)."""

BENCH_RING_NAME = "bench_" + _ring.DEFAULT_RING_FILENAME
"""The copy this experiment churns, beside the deployed ring -- same filesystem."""


class BenchError(RuntimeError):
    """A failure that already knows the exit code the process should return."""

    def __init__(self, message: str, code: int = EXIT_FAIL) -> None:
        super().__init__(message)
        self.code = code


# ---------------------------------------------------------------------------
# Talking to the cluster
# ---------------------------------------------------------------------------


class KeepAliveClient(Client):
    """The real client, holding one connection per server instead of one each.

    ``Client`` opens a fresh TCP connection for every request.  On loopback that
    costs about 11 ms and is paid three times per case (the lookup and the two
    replica writes) -- several times the write it is meant to measure, and
    identical in both modes, so it would bury the difference atomic mode makes.
    This reuses one connection per host instead: the same lookup, the same
    quorum walk and the same reports, only the transport differs.

    Only the transport is replaced.  ``_request`` and ``_send`` are the client's
    two transport seams, and they keep their contract: a status is returned
    rather than raised, and a server that cannot be reached raises
    :class:`ClientError` so the quorum walk can move on.  A connection the
    server closed while idle is reopened once and the request repeated.
    """

    __slots__ = ("_connections",)

    def __init__(self, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self._connections: dict[tuple[str, str, int], http.client.HTTPConnection] = {}

    def close(self) -> None:
        """Drop every open connection."""
        for connection in self._connections.values():
            connection.close()
        self._connections.clear()

    # -- the client's two transport seams -----------------------------------

    def _request(self, method: str, path: str) -> tuple[int, bytes]:
        return self._call(method, self._proxy, path)

    def _send(
        self, method: str, server: str, key: str, *, data: bytes | None = None
    ) -> tuple[int, bytes]:
        return self._call(method, server, f"/files?key={quote(key, safe='')}", data)

    # -- transport ----------------------------------------------------------

    def _call(
        self, method: str, base: str, path: str, data: bytes | None = None
    ) -> tuple[int, bytes]:
        """One request over this host's connection; 4xx/5xx come back as a status."""
        last: Exception | None = None
        for attempt in range(2):
            connection, prefix = self._connection(base)
            try:
                connection.request(method, prefix + path, body=data)
                reply = connection.getresponse()
                return reply.status, reply.read()
            except (OSError, http.client.HTTPException) as exc:
                last = exc
                self._drop(base)
                if attempt:
                    break
        raise ClientError(f"cannot reach {base}: {last}")

    def _connection(self, base: str) -> tuple[http.client.HTTPConnection, str]:
        """The connection for ``base``, opened on first use."""
        parts = urlsplit(base)
        host = parts.hostname or "127.0.0.1"
        port = parts.port or (443 if parts.scheme == "https" else 80)
        key = (parts.scheme, host, port)
        connection = self._connections.get(key)
        if connection is None:
            factory = (
                http.client.HTTPSConnection
                if parts.scheme == "https"
                else http.client.HTTPConnection
            )
            connection = factory(host, port, timeout=self._timeout)
            self._connections[key] = connection
        return connection, parts.path.rstrip("/")

    def _drop(self, base: str) -> None:
        """Forget the connection for ``base`` so the next request reconnects."""
        parts = urlsplit(base)
        key = (parts.scheme, parts.hostname or "127.0.0.1", parts.port or 80)
        connection = self._connections.pop(key, None)
        if connection is not None:
            connection.close()


# ---------------------------------------------------------------------------
# The cluster, as the experiment sees it
# ---------------------------------------------------------------------------


def _http_json(url: str, *, timeout: float) -> dict[str, Any]:
    """Send one GET and decode its JSON body."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _payload_bytes(size: int) -> bytes:
    """A deterministic body of ``size`` bytes."""
    unit = b"swift-light-exp0 "
    return (unit * (size // len(unit) + 1))[:size]


def _preflight(client: Client, proxy: str, *, timeout: float, size: int) -> dict[str, Any]:
    """Prove the cluster can write, read and delete before anything is timed.

    A run that starts against a cluster that is half up would report the
    failure as overhead, so the probe goes first and a broken cluster exits 77
    instead of producing numbers.
    """
    directory = Path(tempfile.mkdtemp(prefix="swift-bench-probe-"))
    local = directory / "payload.bin"
    local.write_bytes(_payload_bytes(size))
    echo = directory / "echo.bin"
    probe = f"bench/preflight-{os.getpid()}-{int(time.time())}.bin"
    try:
        push = client.push(str(local), probe)
        if not push["ok"]:
            raise BenchError(
                f"the cluster accepted the probe write on {len(push['acknowledged'])} "
                f"of {push['quorum']} replicas: {push['failed']}",
                EXIT_SKIP,
            )
        pull = client.pull(probe, str(echo))
        if not pull["ok"]:
            raise BenchError(
                f"the probe object could not be read back: {pull['tried']}",
                EXIT_SKIP,
            )
        if echo.read_bytes() != local.read_bytes():
            raise BenchError(
                "the probe object came back with different bytes", EXIT_SKIP
            )
        client.delete(probe)
    except ClientError as exc:
        raise BenchError(
            f"the proxy at {proxy} is not answering ({exc}); is the cluster up?",
            EXIT_SKIP,
        ) from exc
    finally:
        shutil.rmtree(directory, ignore_errors=True)

    try:
        nodes = _http_json(f"{proxy}/nodes", timeout=timeout)["nodes"]
    except (urllib.error.URLError, OSError, ValueError, KeyError) as exc:
        raise BenchError(
            f"the proxy at {proxy} did not list its nodes ({exc})", EXIT_SKIP
        ) from exc
    return {"nodes": [str(node) for node in nodes], "replicate_count": push["quorum"]}


# ---------------------------------------------------------------------------
# The ring, benchmarked on a private copy
# ---------------------------------------------------------------------------


class _EmptyDataServer:
    """The data server a rebalance moves files through -- holding none.

    A ring case is meant to measure the ring's own writes, so the movement it
    drives is counted and otherwise ignored.
    """

    __slots__ = ("writes", "handoffs")

    def __init__(self) -> None:
        self.writes = 0
        self.handoffs = 0

    def list_files(
        self, node: str, start_slot: int, end_slot: int
    ) -> Iterable[str]:
        return ()

    def write(self, key: str, to_node: str) -> None:
        self.writes += 1

    def write_handoff(self, key: str, from_node: str) -> None:
        self.handoffs += 1


def _scratch_ring(source: Path) -> Path:
    """Copy the deployed ring where the experiment may churn it.

    The copy sits beside the deployed ring so the write path -- and the
    filesystem and directory -- is the same one the cluster itself uses.
    """
    if not source.is_file():
        raise BenchError(
            f"there is no ring at {source}; deploy a cluster first "
            f"(python src/swift_light/deploy.py up)",
            EXIT_SKIP,
        )
    target = source.with_name(BENCH_RING_NAME)
    _remove_scratch_ring(target)
    shutil.copyfile(source, target)
    return target


def _remove_scratch_ring(target: Path) -> None:
    """Delete the copy and every file a rebalance may have left beside it."""
    for leftover in (
        target,
        target.with_name("balanced_" + target.name),
        target.with_name(target.name + ".temp"),
        *target.parent.glob(target.name + ".*.tmp"),
        *target.parent.glob(target.name + ".temp.*.tmp"),
    ):
        try:
            leftover.unlink()
        except OSError:
            pass


# ---------------------------------------------------------------------------
# The workload
# ---------------------------------------------------------------------------


def _build_schedule(operations: int, data_share: float, rng: random.Random) -> list[str]:
    """The cases, in the seeded order they will run.

    Which slots touch the ring is chosen at random, but the cases inside each
    family keep their own order: joins and leaves alternate, and so do writes
    and deletes.  A leave with nothing to remove, or a delete of a file that was
    never written, would measure something other than what it claims to.
    """
    ring_cases = round(operations * (1.0 - data_share))
    ring_cases -= ring_cases % 2  # joins and leaves come in pairs
    data_cases = operations - ring_cases
    if ring_cases < 2 or data_cases < 2:
        raise BenchError(
            f"{operations} cases at a {data_share:.0%} data share leaves no room "
            f"for both kinds; ask for more cases",
            EXIT_SKIP,
        )

    ring_slots = set(rng.sample(range(operations), ring_cases))
    schedule: list[str] = []
    data_index = 0
    ring_index = 0
    for index in range(operations):
        if index in ring_slots:
            schedule.append(NODE_JOIN if ring_index % 2 == 0 else NODE_LEAVE)
            ring_index += 1
        else:
            schedule.append(FILE_WRITE if data_index % 2 == 0 else FILE_DELETE)
            data_index += 1
    return schedule


def _timed(action: Callable[[], bool]) -> tuple[bool, float]:
    """Run one case; return whether it succeeded and how long it took."""
    start = time.perf_counter()
    try:
        return bool(action()), time.perf_counter() - start
    except (ClientError, OSError, ValueError, RuntimeError):
        return False, time.perf_counter() - start


def _settled_members(path: Path) -> set[str] | None:
    """The ring's membership if it is a complete layout, else ``None``.

    A rebalance caught half-way is a legal file but not a finished change, and
    its node list already names the arriving server -- so the membership alone
    does not answer "did the change land?".  Only a ring that validates as
    complete does.
    """
    try:
        ring = ConsistentHashingRing.load(path, complete=False)
        ring.validate()
    except (OSError, ValueError):
        return None
    return set(ring.nodes)


def _joined(path: Path, name: str) -> bool:
    """Whether ``name`` is a member of the settled ring at ``path``."""
    members = _settled_members(path)
    return members is not None and name in members


def _left(path: Path, name: str) -> bool:
    """Whether ``name`` is gone from the settled ring at ``path``."""
    members = _settled_members(path)
    return members is not None and name not in members


def _ring_change(
    change: Callable[[], None],
    *,
    landed: Callable[[], bool],
    retries: int = 2,
) -> bool:
    """Apply one rebalance, judged by the ring's state rather than by the call.

    A step can commit and *then* raise: the rename that installs the ring and
    the marker's removal are separate filesystem calls, and on Windows one of
    them is occasionally refused while another process holds the file.  The
    change has then happened, so looking at the exception instead of the ring
    would report a failure that is not there -- and worse, leave the caller's
    bookkeeping describing a ring it no longer has.  So every attempt is
    followed by a look at the membership, and only a ring that is still wrong
    is tried again (a rebalance is resumable, and finishing an interrupted one
    ends in exactly the state asked for).
    """
    for attempt in range(retries + 1):
        try:
            change()
            return True
        except (OSError, ValueError):
            if landed():
                return True
            if attempt == retries:
                return False
    return False


def run(args: argparse.Namespace) -> dict[str, Any]:
    """Execute the workload and return the report as a plain dict."""
    rng = random.Random(args.seed)
    client = KeepAliveClient(proxy=args.proxy, user=args.account, timeout=args.timeout)
    try:
        return _workload(args, rng, client)
    finally:
        client.close()


def _workload(
    args: argparse.Namespace, rng: random.Random, client: KeepAliveClient
) -> dict[str, Any]:
    """Run the cases and build the report, with the client already connected."""
    shape = _preflight(client, args.proxy, timeout=args.timeout, size=args.bytes)
    atomic = args.mode == "atomic"

    scratch = _scratch_ring(args.ring)
    try:
        deployed = ConsistentHashingRing.load(scratch, complete=False)
        schedule = _build_schedule(args.operations, args.data_share, rng)
        empty = _EmptyDataServer()

        directory = Path(tempfile.mkdtemp(prefix="swift-bench-"))
        payload = directory / "payload.bin"
        payload.write_bytes(_payload_bytes(args.bytes))
        outstanding: deque[str] = deque()
        joined: list[str] = []
        joins = 0
        run_id = f"{int(time.time())}-{os.getpid()}"
        timings: dict[str, list[float]] = {name: [] for name in CATEGORIES}
        failures: dict[str, int] = {name: 0 for name in CATEGORIES}
        expected = {name: schedule.count(name) for name in CATEGORIES}
        every = max(1, args.progress_every)

        print(
            f"[exp0] {len(schedule)} cases against {args.proxy} "
            f"({len(shape['nodes'])} data servers, "
            f"replicate_count={shape['replicate_count']}, "
            f"vnode_count={deployed.vnode_count}), {args.mode} writes",
            flush=True,
        )
        started = time.perf_counter()
        for index, case in enumerate(schedule, start=1):
            path = f"bench/{run_id}/{index}.bin"
            if case == FILE_WRITE:

                def action() -> bool:
                    report = client.push(str(payload), path)
                    if report["ok"]:
                        outstanding.append(path)
                    return bool(report["ok"])

            elif case == FILE_DELETE:

                def action() -> bool:
                    if not outstanding:  # cannot happen: the two alternate
                        return False
                    return bool(client.delete(outstanding.popleft())["ok"])

            elif case == NODE_JOIN:

                def action() -> bool:
                    nonlocal joins
                    name = f"bench-node-{joins}"
                    joins += 1
                    if not _ring_change(
                        lambda: ConsistentHashingRing.add_node(
                            name, data=empty, path=scratch, atomic=atomic
                        ),
                        landed=lambda: _joined(scratch, name),
                    ):
                        return False
                    joined.append(name)
                    return True

            else:

                def action() -> bool:
                    if not joined:
                        return False
                    name = joined.pop(0)
                    if not _ring_change(
                        lambda: ConsistentHashingRing.remove_node(
                            name, data=empty, path=scratch, atomic=atomic
                        ),
                        landed=lambda: _left(scratch, name),
                    ):
                        joined.insert(0, name)
                        return False
                    return True

            ok, elapsed = _timed(action)
            timings[case].append(elapsed)
            if not ok:
                failures[case] += 1
            if index % every == 0:
                remaining = sum(
                    (expected[name] - len(timings[name]))
                    * (sum(timings[name]) / len(timings[name]))
                    for name in CATEGORIES
                    if timings[name]
                )
                print(
                    f"[exp0] {index}/{len(schedule)} cases, "
                    f"{time.perf_counter() - started:.0f}s elapsed, "
                    f"~{remaining:.0f}s left",
                    file=sys.stderr,
                    flush=True,
                )
        measured = time.perf_counter() - started

        ring_bytes = scratch.stat().st_size if scratch.is_file() else 0
        leftover = len(outstanding)
        for path in list(outstanding):
            try:
                client.delete(path)
            except ClientError:
                break
        shutil.rmtree(directory, ignore_errors=True)

        result = {
            "mode": args.mode,
            "proxy": args.proxy,
            "nodes": shape["nodes"],
            "replicate_count": shape["replicate_count"],
            "vnode_count": deployed.vnode_count,
            "operations": len(schedule),
            "payload_bytes": args.bytes,
            "seed": args.seed,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "seconds": measured,
            "ring_file_bytes": ring_bytes,
            "categories": {
                name: _summary(name, timings[name], failures[name])
                for name in CATEGORIES
            },
            "ring_writes": (
                len(timings[NODE_JOIN]) + len(timings[NODE_LEAVE])
            )
            * deployed.vnode_count,
            "leftover_objects": leftover,
        }
        return result
    finally:
        if not args.keep_ring:
            _remove_scratch_ring(scratch)


def _percentile(values: list[float], fraction: float) -> float | None:
    """The ``fraction`` quantile of ``values`` (linear index, no interpolation)."""
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return ordered[index]


def _summary(name: str, timings: list[float], failures: int) -> dict[str, Any]:
    """Counts and timings for one kind of case, in milliseconds."""
    seconds = sum(timings)
    count = len(timings)
    return {
        "cases": count,
        "failed": failures,
        "seconds": seconds,
        "mean_ms": (seconds / count * 1000.0) if count else None,
        "p50_ms": _ms(_percentile(timings, 0.50)),
        "p95_ms": _ms(_percentile(timings, 0.95)),
        "p99_ms": _ms(_percentile(timings, 0.99)),
        "cases_per_second": (count / seconds) if seconds else None,
    }


def _ms(seconds: float | None) -> float | None:
    return None if seconds is None else seconds * 1000.0


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------


def _format(value: Any, spec: str, *, width: int = 10) -> str:
    """``value`` right-aligned in ``width``, or a dash when it is missing."""
    if value is None:
        return "-".rjust(width)
    return format(value, spec).rjust(width)


def print_report(result: dict[str, Any]) -> None:
    """Print the run's numbers as a table."""
    print()
    print(f"swift_light exp0 -- {result['mode']} writes")
    print(f"  proxy            {result['proxy']}")
    print(f"  data servers     {len(result['nodes'])}")
    print(f"  replicate_count  {result['replicate_count']}")
    print(f"  vnode_count      {result['vnode_count']}")
    print(
        f"  cases            {result['operations']} "
        f"({result['payload_bytes']}-byte objects, seed {result['seed']})"
    )
    print()
    header = (
        f"  {'case':<14}{'cases':>8}{'failed':>8}{'seconds':>10}"
        f"{'ms/case':>10}{'p50':>10}{'p95':>10}{'p99':>10}{'cases/s':>10}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))
    for name in CATEGORIES:
        entry = result["categories"][name]
        print(
            f"  {name:<14}{entry['cases']:>8}{entry['failed']:>8}"
            f"{_format(entry['seconds'], '.2f')}"
            f"{_format(entry['mean_ms'], '.3f')}"
            f"{_format(entry['p50_ms'], '.3f')}"
            f"{_format(entry['p95_ms'], '.3f')}"
            f"{_format(entry['p99_ms'], '.3f')}"
            f"{_format(entry['cases_per_second'], '.1f')}"
        )
    print("  " + "-" * (len(header) - 2))
    print(f"  {'all cases':<14}{result['operations']:>8}"
          f"{sum(c['failed'] for c in result['categories'].values()):>8}"
          f"{_format(result['seconds'], '.2f')}")

    memberships = (
        result["categories"][NODE_JOIN]["cases"]
        + result["categories"][NODE_LEAVE]["cases"]
    )
    print()
    print(
        f"  {memberships} membership changes x {result['vnode_count']} vnodes "
        f"= {result['ring_writes']} ring writes "
        f"({result['ring_file_bytes']} bytes per write)"
    )
    ring_seconds = (
        result["categories"][NODE_JOIN]["seconds"]
        + result["categories"][NODE_LEAVE]["seconds"]
    )
    if result["ring_writes"]:
        print(
            f"  ring write path   {ring_seconds:.2f}s total, "
            f"{ring_seconds / result['ring_writes'] * 1000:.3f} ms per ring write "
            f"(includes the rebalance's in-memory work)"
        )
    if result["leftover_objects"]:
        print(
            f"  cleaned up        {result['leftover_objects']} leftover objects "
            f"after the timed run (not counted)"
        )
    failures = sum(entry["failed"] for entry in result["categories"].values())
    if failures:
        print(f"  {failures} cases FAILED -- the numbers are not comparable")
    else:
        print("  every case succeeded")


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


class _ArgumentParser(argparse.ArgumentParser):
    """An argument parser whose usage errors honour this module's exit codes."""

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(EXIT_SKIP, f"{self.prog}: error: {message}\n")


def _parser() -> argparse.ArgumentParser:
    parser = _ArgumentParser(
        prog="exp0_atomic_overhead",
        description=(
            "Run a mixed 10,000-case workload against a deployed swift_light "
            "cluster and report what each kind of case costs."
        ),
    )
    parser.add_argument(
        "--mode",
        required=True,
        choices=("atomic", "non_atomic"),
        help=(
            "the write mode the cluster was deployed with (deploy.py up MODE); "
            "it labels the run and picks the ring write path used here"
        ),
    )
    parser.add_argument(
        "--operations",
        type=int,
        default=DEFAULT_OPERATIONS,
        metavar="N",
        help=f"cases to run (default: {DEFAULT_OPERATIONS})",
    )
    parser.add_argument(
        "--data-share",
        type=float,
        default=DEFAULT_DATA_SHARE,
        metavar="FRACTION",
        help=(
            "share of the cases that touch data rather than the ring, "
            f"split evenly between writes and deletes (default: {DEFAULT_DATA_SHARE})"
        ),
    )
    parser.add_argument(
        "--bytes",
        type=int,
        default=DEFAULT_PAYLOAD_BYTES,
        metavar="N",
        help=f"bytes per stored object (default: {DEFAULT_PAYLOAD_BYTES})",
    )
    parser.add_argument(
        "--proxy",
        default=PROXY_BASE_URL,
        metavar="URL",
        help=f"the proxy's client address (default: {PROXY_BASE_URL})",
    )
    parser.add_argument(
        "--ring",
        type=Path,
        default=LIVE_RING_PATH,
        metavar="PATH",
        help=(
            "the deployed ring to copy and churn "
            f"(default: {LIVE_RING_PATH})"
        ),
    )
    parser.add_argument(
        "--account",
        default=DEFAULT_ACCOUNT,
        metavar="NAME",
        help=f"account the objects are stored under (default: {DEFAULT_ACCOUNT})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        metavar="N",
        help=f"seed for the interleaving (default: {DEFAULT_SEED})",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="seconds to wait for any one request (default: 5.0)",
    )
    parser.add_argument(
        "--progress-every",
        type=int,
        default=DEFAULT_PROGRESS_EVERY,
        metavar="N",
        help=f"cases between progress lines on stderr (default: {DEFAULT_PROGRESS_EVERY})",
    )
    parser.add_argument(
        "--json",
        type=Path,
        metavar="PATH",
        help="write the report as JSON, to keep the two modes' reports side by side",
    )
    parser.add_argument(
        "--keep-ring",
        action="store_true",
        help="keep the churned copy of the ring (bench_ring.json) for inspection",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run the benchmark; return this module's exit code."""
    args = _parser().parse_args(argv)
    if args.operations < 4:
        print("benchmark: --operations must be at least 4", file=sys.stderr)
        return EXIT_SKIP
    if not 0.0 < args.data_share < 1.0:
        print("benchmark: --data-share must be between 0 and 1", file=sys.stderr)
        return EXIT_SKIP

    try:
        result = run(args)
    except BenchError as exc:
        print(f"benchmark: {exc}", file=sys.stderr)
        return exc.code

    print_report(result)

    if args.json is not None:
        try:
            args.json.write_text(
                json.dumps(result, indent=2) + "\n", encoding="utf-8"
            )
        except OSError as exc:
            print(f"benchmark: cannot write {args.json}: {exc}", file=sys.stderr)
            return EXIT_FAIL
        print(f"\n  report written to {args.json}")

    failed = sum(entry["failed"] for entry in result["categories"].values())
    return EXIT_FAIL if failed else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
