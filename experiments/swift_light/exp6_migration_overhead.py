#!/usr/bin/env python3
"""exp6 -- what a membership change costs when it moves real objects.

What this measures
------------------
Experiment 0 replays a membership change against a copy of the ring whose data
server holds no files, so its join and leave cases time the ring's own writes
and nothing else.  A production rebalance is not that: every batch copies the
objects whose placement changed before it commits the ring, and the commit
itself -- ``ring.json.temp`` written, then renamed, then the directory flushed
-- happens around those copies.  In atomic mode the scratch ring is written on
a background thread while the copies run, so after the migration only the
rename is left; hiding that write behind the copies is the whole point.

This experiment does only that: joins and leaves of a real data server, against
the live cluster, with objects on the servers for the migration to move.  It
measures the one number experiment 0 cannot -- the cost of a rebalance that
actually transfers data, per write mode.

Prerequisites
-------------
The cluster must already be up, deployed in the mode ``--mode`` names.  This
script never starts or stops it::

    python3 src/swift_light/deploy.py up non_atomic     # in another terminal

Do not push to the cluster while it runs.  A migration holds the write lock on
the nodes it touches, so a concurrent client write would be refused with 503,
and the seeded objects are the ones being moved.

The workload
------------
``--cases`` cases (30 by default) are executed one at a time, all of them
membership changes: joins and leaves alternate, so 15 of each.  One case is
one rebalance -- the target ring is built and flushed, then the ring is taken
``DEFAULT_FLUSH_EVERY`` (50) vnode points at a time, copying the objects each
batch disturbs.  At the default 100 vnodes that is two batches, so a case pays
three ring writes plus every object copy the change needs.

``--objects`` objects (100 by default) are seeded through the shipped client
before anything is timed, spread over the cluster by the deployed ring.  Their
keys are fixed (``bench/objects/obj-<n>.bin``) rather than per-run: the objects
a change moves are the ones whose keys hash into the arcs it disturbs, so
per-run keys would let the two modes move different amounts of data.  The
cleanup deletes exactly those keys before and after the timed run.

The client is not used for the timed cases themselves: the ring drives the
copies through :class:`~swift_light.data_server.remote.RemoteDataServer`, the
same HTTP adapter ``deploy.py add-node`` uses.

The server that joins
---------------------
The arriving server is a real ``DataHTTPServer`` the experiment starts on a
fixed loopback port (``--join-port``, 9100 by default) with a store of its own,
so the batch copies real bytes over real HTTP onto a real disk.  The port is
fixed because the ring hashes the node's URL: an OS-picked port would give
every run its own placement, and with it its own copy count, so the two modes
could not be compared.  If the port is taken, pass the same ``--join-port`` to
both runs.  The server is removed again by the next case, which drains it the
way a departing node is drained.  No deployed node is ever drained or removed,
so the cluster's own objects are only ever read, or copied back with the same
bytes.

The ring is benchmarked on a copy, never on the deployed one
------------------------------------------------------------
As in experiment 0, the membership changes are replayed against
``bench_ring.json`` -- a copy of the deployed ring in the same directory, so the
same filesystem, deleted afterwards.  The deployed ring keeps describing the
cluster, so its proxy keeps serving.

Reading the numbers
-------------------
Every case is timed on its own, and each category reports its mean, the sample
standard deviation (``n - 1``), p50/p95/p99 and cases per second.  A case is
the whole rebalance: ring reads, the target build and flush, the two batches
(ring commits plus object copies over HTTP), and the marker removal.  The copy
counts beside the table say how much of that was data movement.

The **ring saves** line times only the ring writes that happen on the case's own
clock -- i.e. what the write mode costs inside a case, separately from the
copies.  In atomic mode a batch writes ``ring.json.temp`` on the ring's
background thread while the copies run, so the case pays only the rename that
installs it; the target build is not overlapped with anything and is counted
whole.  In non-atomic mode every ring write happens after the copies, so all of
it is counted.

Compare the two modes from their JSON reports: a gap smaller than the reported
sample standard deviation is jitter, not a result.  The join port and the
seeded keys are fixed, so both modes move the same objects and should report
the same copy count; a difference means the cluster held different objects
when the second run started.  The ring writes differ between the modes by a
few milliseconds while the copies cost hundreds, so expect the gap here to be
much smaller than in experiment 0 -- and, for atomic mode, to sit behind the
copies rather than in front of them.

Usage
-----
Run from the repository root, with the cluster already up::

    python3 experiments/swift_light/exp6_migration_overhead.py --mode non_atomic

and again with ``--mode atomic`` after redeploying.  The two reports are what
get compared; ``--json out.json`` keeps one per mode side by side.  Use
``--cases 10 --objects 20`` for a smoke test.

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
import json
import math
import os
import shutil
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Iterator
from unittest.mock import patch
from urllib.parse import quote

# The package is imported by its full path (there is no ``__init__.py``), so
# ``src/`` must be importable before any ``swift_light.*`` module is.  From
# ``experiments/swift_light/`` the repository root is two directories up.
_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from swift_light.client.client import (  # noqa: E402
    PROXY_BASE_URL,
    Client,
    ClientError,
)
from swift_light.data_server import data_HTTP_server as _data_http_server  # noqa: E402
from swift_light.data_server.data_HTTP_server import DataHTTPServer  # noqa: E402
from swift_light.data_server.remote import RemoteDataServer  # noqa: E402
from swift_light.data_server.storage import DataServerConfig  # noqa: E402
from swift_light.proxy_server import consistent_hashing_ring as _ring  # noqa: E402
from swift_light.proxy_server.consistent_hashing_ring import (  # noqa: E402
    ConsistentHashingRing,
)

__all__ = [
    "CATEGORIES",
    "DEFAULT_CASES",
    "DEFAULT_JOIN_PORT",
    "DEFAULT_OBJECTS",
    "DEFAULT_PAYLOAD_BYTES",
    "EXIT_SKIP",
    "EXIT_FAIL",
    "EXIT_OK",
    "BenchError",
    "CountingRemoteDataServer",
    "main",
    "run",
]

EXIT_OK = 0
"""Every case succeeded."""

EXIT_FAIL = 1
"""The workload ran, but at least one case failed."""

EXIT_SKIP = 77
"""Could not be attempted: no cluster, bad arguments, wrong mode."""

DEFAULT_CASES = 30
"""Membership changes to run, split evenly between joins and leaves."""

DEFAULT_OBJECTS = 100
"""Objects seeded on the cluster for the migrations to move."""

DEFAULT_PAYLOAD_BYTES = 128
"""Bytes per seeded object.  Small on purpose: the cost measured is the copy."""

DEFAULT_ACCOUNT = "bench"
"""Account name every seeded object is stored under."""

NODE_JOIN = "node join"
NODE_LEAVE = "node leave"

CATEGORIES = (NODE_JOIN, NODE_LEAVE)
"""The two kinds of case, in report order."""

LIVE_RING_PATH = Path(_ring.__file__).with_name(_ring.DEFAULT_RING_FILENAME)
"""The deployed ring, at the ring module's own default (what the proxy reads)."""

BENCH_RING_NAME = "bench_" + _ring.DEFAULT_RING_FILENAME
"""The copy this experiment churns, beside the deployed ring -- same filesystem."""

DEFAULT_JOIN_PORT = 9100
"""The fixed port the arriving server binds, so every run hashes the same node.

The ring hashes the node's URL, so the port decides which arcs a join or a
leave disturbs, and therefore how many objects move.  An OS-picked port would
make every run -- and so every mode -- move a different amount of data.
"""

SEED_DIRECTORY = "bench/objects"
"""Where the seeded objects live; fixed, so both modes move the same keys."""


class BenchError(RuntimeError):
    """A failure that already knows the exit code the process should return."""

    def __init__(self, message: str, code: int = EXIT_FAIL) -> None:
        super().__init__(message)
        self.code = code


# ---------------------------------------------------------------------------
# The migration's data server, with the copies counted
# ---------------------------------------------------------------------------


class CountingRemoteDataServer(RemoteDataServer):
    """The shipped HTTP migration adapter, counting the copies it makes.

    ``copy`` is one object written to one gaining node; ``release`` is one
    handoff the departing node dropped.  Counting them is what proves the
    case moved data, and it is the number the report puts beside the timings.
    """

    __slots__ = ("copies", "releases")

    def __init__(self, ring: ConsistentHashingRing, *, timeout: float) -> None:
        super().__init__(ring, timeout=timeout)
        self.copies = 0
        self.releases = 0

    def write(self, key: str, to_node: str) -> None:
        super().write(key, to_node)
        self.copies += 1

    def write_handoff(self, key: str, from_node: str) -> None:
        released = key in self._copied
        super().write_handoff(key, from_node)
        if released:
            self.releases += 1


# ---------------------------------------------------------------------------
# Talking to the cluster
# ---------------------------------------------------------------------------


def _http_json(url: str, *, timeout: float) -> dict[str, Any]:
    """Send one GET and decode its JSON body."""
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _http_delete(url: str, *, timeout: float) -> int:
    """Send one DELETE; a 4xx/5xx answer comes back as a status."""
    request = urllib.request.Request(url, method="DELETE")
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status
    except urllib.error.HTTPError as exc:
        return exc.code


def _payload_bytes(size: int) -> bytes:
    """A deterministic body of ``size`` bytes."""
    unit = b"swift-light-exp6 "
    return (unit * (size // len(unit) + 1))[:size]


def _preflight(client: Client, proxy: str, *, timeout: float, size: int) -> dict[str, Any]:
    """Prove the cluster can write, read and delete before anything is timed.

    A run that starts against a cluster that is half up would report the
    failure as overhead, so the probe goes first and a broken cluster exits 77
    instead of producing numbers.
    """
    directory = Path(tempfile.mkdtemp(prefix="swift-exp6-probe-"))
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


def _seed_paths(count: int) -> list[str]:
    """The remote paths the objects live at -- the same in every run and mode.

    Run-specific names would change which objects a change moves (the keys are
    what hash into the arcs), so the two modes would not move the same data.
    """
    return [f"{SEED_DIRECTORY}/obj-{index}.bin" for index in range(count)]


def _seed(client: Client, root: Path, *, paths: list[str], size: int) -> list[str]:
    """Write the objects the migrations will move, and return their keys."""
    payload = root / "payload.bin"
    payload.write_bytes(_payload_bytes(size))
    keys: list[str] = []
    for index, path in enumerate(paths):
        report = client.push(str(payload), path)
        if not report["ok"]:
            raise BenchError(
                f"seeding object {index} reached {len(report['acknowledged'])} of "
                f"{report['quorum']} replicas: {report['failed']}",
                EXIT_SKIP,
            )
        keys.append(client.object_key(path))
    return keys


def _key_counts(nodes: list[str], *, timeout: float) -> list[int]:
    """How many objects each node holds, ``-1`` where it cannot be asked."""
    counts: list[int] = []
    for node in nodes:
        try:
            body = _http_json(f"{node.rstrip('/')}/keys", timeout=timeout)
            counts.append(len(body["keys"]))
        except (urllib.error.URLError, OSError, ValueError, KeyError):
            counts.append(-1)
    return counts


def _delete_keys(nodes: list[str], keys: list[str], *, timeout: float) -> int:
    """Delete exactly ``keys`` from every node that holds one.

    Goes to each node directly rather than through the proxy: after a failed
    case the scratch layout may name a replica the deployed ring does not, so
    the client's fan could miss a copy.  An earlier run's leftovers are removed
    the same way, so a run starts from the objects the previous one did.
    """
    deleted = 0
    for node in nodes:
        try:
            present = set(
                _http_json(f"{node.rstrip('/')}/keys", timeout=timeout)["keys"]
            )
        except (urllib.error.URLError, OSError, ValueError, KeyError):
            continue
        for key in keys:
            if str(key) not in present:
                continue
            url = f"{node.rstrip('/')}/files?key={quote(str(key), safe='')}"
            try:
                if _http_delete(url, timeout=timeout) in (200, 204, 404):
                    deleted += 1
            except (urllib.error.URLError, OSError):
                pass
    return deleted


# ---------------------------------------------------------------------------
# The ring, benchmarked on a private copy
# ---------------------------------------------------------------------------


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


@contextmanager
def _joining_server(
    store_root: Path, *, proxy: str, atomic: bool, port: int, timeout: float
) -> Iterator[tuple[DataHTTPServer, str]]:
    """A real data server the experiment adds to and removes from the ring.

    Bound to ``port`` rather than an OS-picked one, with a store this run owns,
    so the migration copies real bytes over real HTTP onto a real disk and the
    node the ring hashes -- and so the arcs a case disturbs -- is the same in
    every run and mode.

    The bind is exclusive.  The shipped listener sets ``allow_reuse_address``,
    which on Windows lets a second socket bind a port another process already
    holds, so an occupied port would otherwise be shared silently; here it
    fails, and ``--join-port`` is the way out.  Boot handoff reconciliation is
    disabled: the server is private to the run and must not ask the cluster's
    proxy for anyone else's handoffs.
    """
    config = DataServerConfig(f"http://127.0.0.1:{port}", proxy)
    try:
        with patch.object(
            _data_http_server._RequestServer, "allow_reuse_address", False
        ):
            server = DataHTTPServer(
                config,
                store_root=store_root,
                atomic=atomic,
                host="127.0.0.1",
                port=port,
            )
    except OSError as exc:
        raise BenchError(
            f"cannot bind 127.0.0.1:{port} for the joining server ({exc}); "
            f"pass --join-port with a free port, the same value to both modes",
            EXIT_SKIP,
        ) from exc
    url = f"http://127.0.0.1:{server.port}"
    try:
        with patch.object(
            DataHTTPServer, "request_handoff_reconcile", return_value=False
        ):
            server.start()
        _check_joining_server(url, timeout=timeout)
        yield server, url
    finally:
        server.close()


def _check_joining_server(url: str, *, timeout: float) -> None:
    """Prove that requests to ``url`` reach the server this run started.

    The address the ring hashes must be the address the copies arrive on; a
    port shared with another process would move data into the wrong store.
    """
    try:
        body = _http_json(f"{url}/heartbeat", timeout=timeout)
    except (urllib.error.URLError, OSError, ValueError) as exc:
        raise BenchError(
            f"the joining server at {url} does not answer ({exc}); is another "
            f"process using --join-port?",
            EXIT_SKIP,
        ) from exc
    reported = body.get("server") if isinstance(body, dict) else None
    if str(reported) != url:
        raise BenchError(
            f"{url} is answered by {reported!r}, not by this run's joining "
            f"server; another process is using --join-port",
            EXIT_SKIP,
        )


# ---------------------------------------------------------------------------
# The workload
# ---------------------------------------------------------------------------


def _timed(action: Callable[[], bool]) -> tuple[bool, float]:
    """Run one case; return whether it succeeded and how long it took."""
    start = time.perf_counter()
    try:
        return bool(action()), time.perf_counter() - start
    except (ClientError, OSError, ValueError, RuntimeError):
        return False, time.perf_counter() - start

@contextmanager
def _ring_write_timer() -> Iterator[list[float]]:
    """Collect the seconds each ring write takes on *this* thread.

    Atomic batches write ``ring.json.temp`` on the ring's own background
    thread, so that write is not on the case's clock and is not collected --
    what a case still pays for it is the rename that installs it.  Every other
    ring write (the target build, and in non-atomic mode each batch's write to
    the live ring) runs on this thread and is counted whole.

    Patches the ring module's two write seams, so it measures the production
    path rather than a copy of it; both are restored on exit.
    """
    seconds: list[float] = []
    original_save = ConsistentHashingRing.save
    original_install = _ring._install_prepared
    main = threading.current_thread()

    def timed_save(ring, path=None, *, atomic=True):
        start = time.perf_counter()
        try:
            return original_save(ring, path, atomic=atomic)
        finally:
            if threading.current_thread() is main:
                seconds.append(time.perf_counter() - start)

    def timed_install(prepared, target):
        start = time.perf_counter()
        try:
            original_install(prepared, target)
        finally:
            if threading.current_thread() is main:
                seconds.append(time.perf_counter() - start)

    with patch.object(ConsistentHashingRing, "save", new=timed_save), patch.object(
        _ring, "_install_prepared", new=timed_install
    ):
        yield seconds


def _ring_writes_per_change(vnode_count: int) -> int:
    """Ring writes one membership change costs (the target, then one per batch)."""
    batches = -(-vnode_count // _ring.DEFAULT_FLUSH_EVERY)
    return batches + 1


def _settled_members(path: Path) -> set[str] | None:
    """The ring's membership if it is a complete layout, else ``None``."""
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
    would report a failure that is not there.  Every attempt is followed by a
    look at the membership, and only a ring that is still wrong is tried again.
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
    """Run the workload and return the report."""
    atomic = args.mode == "atomic"
    client = Client(proxy=args.proxy, user=args.account, timeout=args.timeout)
    shape = _preflight(client, args.proxy, timeout=args.timeout, size=args.bytes)
    if shape["replicate_count"] > len(shape["nodes"]) + 1:
        raise BenchError(
            f"replicate_count={shape['replicate_count']} cannot fit "
            f"{len(shape['nodes'])} data servers plus the one that joins",
            EXIT_SKIP,
        )

    scratch = _scratch_ring(args.ring)
    directory = Path(tempfile.mkdtemp(prefix="swift-exp6-"))
    paths = _seed_paths(args.objects)
    expected_keys = [client.object_key(path) for path in paths]
    try:
        deployed = ConsistentHashingRing.load(scratch, complete=False)
        print(
            f"[exp6] {args.cases} membership changes against {args.proxy} "
            f"({len(shape['nodes'])} data servers, "
            f"replicate_count={deployed.replicate_count}, "
            f"vnode_count={deployed.vnode_count}), {args.mode} writes",
            flush=True,
        )
        joining_root = directory / "joining"
        with _joining_server(
            joining_root,
            proxy=args.proxy,
            atomic=atomic,
            port=args.join_port,
            timeout=args.timeout,
        ) as (_server, url):
            stale = _delete_keys(
                shape["nodes"], expected_keys, timeout=args.timeout
            )
            if stale:
                print(
                    f"[exp6] removed {stale} leftover copies of the seeded keys "
                    f"before seeding (an earlier run did not finish)",
                    file=sys.stderr,
                    flush=True,
                )
            keys = _seed(client, directory, paths=paths, size=args.bytes)
            held = _key_counts(shape["nodes"], timeout=args.timeout)
            print(
                f"[exp6] seeded {len(keys)} objects ({args.bytes} bytes each); "
                f"nodes hold {', '.join(str(count) for count in held)} keys "
                f"(a change moves every object in the arcs it disturbs)",
                flush=True,
            )

            timings: dict[str, list[float]] = {name: [] for name in CATEGORIES}
            failures: dict[str, int] = {name: 0 for name in CATEGORIES}
            ring_seconds: dict[str, float] = {name: 0.0 for name in CATEGORIES}
            copies = 0
            releases = 0
            members: list[str] = []
            started = time.perf_counter()

            for index in range(args.cases):
                joining = index % 2 == 0
                data = CountingRemoteDataServer(deployed, timeout=args.timeout)

                if joining:

                    def action() -> bool:
                        if not _ring_change(
                            lambda: ConsistentHashingRing.add_node(
                                url, data=data, path=scratch, atomic=atomic
                            ),
                            landed=lambda: _joined(scratch, url),
                        ):
                            return False
                        members.append(url)
                        return True

                else:

                    def action() -> bool:
                        if not members:
                            return False
                        if not _ring_change(
                            lambda: ConsistentHashingRing.remove_node(
                                url, data=data, path=scratch, atomic=atomic
                            ),
                            landed=lambda: _left(scratch, url),
                        ):
                            return False
                        members.pop()
                        return True

                with _ring_write_timer() as saves:
                    ok, elapsed = _timed(action)
                category = NODE_JOIN if joining else NODE_LEAVE
                timings[category].append(elapsed)
                ring_seconds[category] += sum(saves)
                if not ok:
                    failures[category] += 1
                if ok and not joining:
                    # The drained server leaves with whatever it still held;
                    # start the next join from an empty one, like a new node.
                    shutil.rmtree(joining_root, ignore_errors=True)
                copies += data.copies
                releases += data.releases
                if index % 20 == 19:
                    print(
                        f"[exp6] {index + 1}/{args.cases} cases, "
                        f"{time.perf_counter() - started:.0f}s elapsed, "
                        f"{copies} objects copied",
                        file=sys.stderr,
                        flush=True,
                    )

            measured = time.perf_counter() - started
            leftover = _delete_keys(
                shape["nodes"], keys, timeout=args.timeout
            )

        return {
            "mode": args.mode,
            "proxy": args.proxy,
            "join_port": args.join_port,
            "nodes": shape["nodes"],
            "replicate_count": deployed.replicate_count,
            "vnode_count": deployed.vnode_count,
            "operations": args.cases,
            "objects": len(keys),
            "node_keys": held,
            "payload_bytes": args.bytes,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S%z"),
            "seconds": measured,
            "copies": copies,
            "releases": releases,
            "ring_seconds": sum(ring_seconds.values()),
            "ring_writes": args.cases
            * _ring_writes_per_change(deployed.vnode_count),
            "ring_file_bytes": scratch.stat().st_size if scratch.is_file() else 0,
            "categories": {
                name: {
                    **_summary(name, timings[name], failures[name]),
                    "ring_seconds": ring_seconds[name],
                }
                for name in CATEGORIES
            },
            "seeded_objects_cleaned": leftover,
        }
    finally:
        shutil.rmtree(directory, ignore_errors=True)
        if not args.keep_ring:
            _remove_scratch_ring(scratch)


# ---------------------------------------------------------------------------
# The report
# ---------------------------------------------------------------------------


def _percentile(values: list[float], fraction: float) -> float | None:
    """The ``fraction`` quantile of ``values`` (linear index, no interpolation)."""
    if not values:
        return None
    ordered = sorted(values)
    index = min(len(ordered) - 1, int(round(fraction * (len(ordered) - 1))))
    return ordered[index]


def _stddev(values: list[float]) -> float | None:
    """The sample standard deviation of ``values``, or ``None`` below two cases."""
    count = len(values)
    if count < 2:
        return None
    mean = sum(values) / count
    variance = sum((value - mean) ** 2 for value in values) / (count - 1)
    return math.sqrt(variance)


def _summary(name: str, timings: list[float], failures: int) -> dict[str, Any]:
    """Counts and timings for one kind of case, in milliseconds."""
    seconds = sum(timings)
    count = len(timings)
    return {
        "cases": count,
        "failed": failures,
        "seconds": seconds,
        "mean_ms": (seconds / count * 1000.0) if count else None,
        "stddev_ms": _ms(_stddev(timings)),
        "p50_ms": _ms(_percentile(timings, 0.50)),
        "p95_ms": _ms(_percentile(timings, 0.95)),
        "p99_ms": _ms(_percentile(timings, 0.99)),
        "cases_per_second": (count / seconds) if seconds else None,
    }


def _ms(seconds: float | None) -> float | None:
    return None if seconds is None else seconds * 1000.0


def _format(value: Any, spec: str, *, width: int = 10) -> str:
    """``value`` right-aligned in ``width``, or a dash when it is missing."""
    if value is None:
        return "-".rjust(width)
    return format(value, spec).rjust(width)


def print_report(result: dict[str, Any]) -> None:
    """Print the run's numbers as a table."""
    print()
    print(f"swift_light exp6 -- {result['mode']} writes")
    print(f"  proxy            {result['proxy']}")
    print(f"  join port        {result['join_port']}")
    print(f"  data servers     {len(result['nodes'])}")
    print(f"  replicate_count  {result['replicate_count']}")
    print(f"  vnode_count      {result['vnode_count']}")
    print(f"  cases            {result['operations']} membership changes")
    print(
        f"  objects          {result['objects']} "
        f"({result['payload_bytes']}-byte objects seeded)"
    )
    print()
    header = (
        f"  {'case':<14}{'cases':>8}{'failed':>8}{'seconds':>10}"
        f"{'ms/case':>10}{'std dev':>10}{'p50':>10}{'p95':>10}{'p99':>10}"
        f"{'cases/s':>10}"
    )
    print(header)
    print("  " + "-" * (len(header) - 2))
    for name in CATEGORIES:
        entry = result["categories"][name]
        print(
            f"  {name:<14}{entry['cases']:>8}{entry['failed']:>8}"
            f"{_format(entry['seconds'], '.2f')}"
            f"{_format(entry['mean_ms'], '.3f')}"
            f"{_format(entry['stddev_ms'], '.3f')}"
            f"{_format(entry['p50_ms'], '.3f')}"
            f"{_format(entry['p95_ms'], '.3f')}"
            f"{_format(entry['p99_ms'], '.3f')}"
            f"{_format(entry['cases_per_second'], '.1f')}"
        )
    print("  " + "-" * (len(header) - 2))
    print(
        f"  {'all cases':<14}{result['operations']:>8}"
        f"{sum(c['failed'] for c in result['categories'].values()):>8}"
        f"{_format(result['seconds'], '.2f')}"
    )
    print()
    per_change = result["copies"] / result["operations"] if result["operations"] else 0
    print(
        f"  data movement    {result['copies']} objects copied, "
        f"{result['releases']} released ({per_change:.1f} per change), "
        f"ring {result['ring_file_bytes']} bytes per write"
    )
    ring_total = result["ring_seconds"]
    ring_change = _ms(ring_total / result["operations"]) if result["operations"] else 0.0
    ring_write = _ms(ring_total / result["ring_writes"]) if result["ring_writes"] else 0.0
    print(
        f"  ring saves       {ring_total:.2f}s total, {ring_change:.3f} ms per "
        f"change, {ring_write:.3f} ms per ring write "
        f"({result['ring_writes']} writes)"
    )
    if result["mode"] == "atomic":
        print(
            "                   target write in full, each batch only its rename "
            "(the scratch write runs during the copies)"
        )
    else:
        print(
            "                   every ring write in full (nothing overlaps it "
            "in this mode)"
        )
    if result["seeded_objects_cleaned"]:
        print(
            f"  cleaned up        {result['seeded_objects_cleaned']} seeded "
            f"copies after the timed run (not counted)"
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
        prog="exp6_migration_overhead",
        description=(
            "Run 30 membership changes against a deployed swift_light cluster, "
            "moving real objects, and report what a join and a leave cost."
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
        "--cases",
        type=int,
        default=DEFAULT_CASES,
        metavar="N",
        help=f"membership changes to run, joins and leaves alternating "
        f"(default: {DEFAULT_CASES})",
    )
    parser.add_argument(
        "--objects",
        type=int,
        default=DEFAULT_OBJECTS,
        metavar="N",
        help=f"objects seeded for the migrations to move (default: {DEFAULT_OBJECTS})",
    )
    parser.add_argument(
        "--bytes",
        type=int,
        default=DEFAULT_PAYLOAD_BYTES,
        metavar="N",
        help=f"bytes per seeded object (default: {DEFAULT_PAYLOAD_BYTES})",
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
        help=f"the deployed ring to copy and churn (default: {LIVE_RING_PATH})",
    )
    parser.add_argument(
        "--account",
        default=DEFAULT_ACCOUNT,
        metavar="NAME",
        help=f"account the objects are stored under (default: {DEFAULT_ACCOUNT})",
    )
    parser.add_argument(
        "--join-port",
        type=int,
        default=DEFAULT_JOIN_PORT,
        metavar="PORT",
        help=(
            f"port the arriving server binds (default: {DEFAULT_JOIN_PORT}); "
            f"fixing it keeps the ring placement, and so the copy count, the "
            f"same across runs -- use the same value for both modes"
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=5.0,
        metavar="SECONDS",
        help="seconds to wait for any one request (default: 5.0)",
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
    if args.cases < 2 or args.cases % 2:
        print("benchmark: --cases must be even and at least 2", file=sys.stderr)
        return EXIT_SKIP
    if args.objects < 1:
        print("benchmark: --objects must be at least 1", file=sys.stderr)
        return EXIT_SKIP
    if args.bytes < 1:
        print("benchmark: --bytes must be at least 1", file=sys.stderr)
        return EXIT_SKIP
    if not 1 <= args.join_port <= 65535:
        print("benchmark: --join-port must be between 1 and 65535", file=sys.stderr)
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
