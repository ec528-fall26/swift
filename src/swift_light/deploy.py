#!/usr/bin/env python3
"""Bring a swift_light cluster up on one machine.

The swift_light modules are libraries: a data server is a ``DataHTTPServer``
and the proxy is a ``ProxyHTTPServer``, but neither has a ``__main__``, and the
ring has to exist on disk before the proxy will even start.  This is the one
place that puts those pieces together for a local run.

    build the ring (once)          ConsistentHashingRing.build(nodes=<URLs>)
    write each node's parameters   DataServerConfig.save(...)
    start the proxy                ProxyHTTPServer.serve_forever()
    start the data servers         DataHTTPServer.serve_forever()
    talk to it                     client.py push / pull / delete

Commands
--------
::

    up       build the ring if there is none, write every node's parameters,
             start the proxy and the data servers, and supervise them until
             Ctrl-C (which stops everything)
    status   ask the running cluster what it is and how healthy it is
    down     stop the cluster a previous ``up`` left running

Usage
-----
Run from the repository root::

    python src/swift_light/deploy.py up
    python src/swift_light/deploy.py status
    python src/swift_light/deploy.py down

Then use the client, which talks to the proxy on ``http://127.0.0.1:8000``::

    python src/swift_light/client/client.py push notes.txt a/b/notes.txt
    python src/swift_light/client/client.py pull a/b/notes.txt notes.txt

``up`` serves three data servers on 127.0.0.1:9001-9003 by default, keeps two
copies of every object, and supervises four processes.  ``--nodes`` and
``--replicas`` choose otherwise; ``--rebuild`` throws away an existing ring
(and with it the layout every stored object was placed by).

Order of events
---------------
The proxy is started first, then the data servers.  A data server reconciles
handoffs at boot by asking the proxy's ``/nodes`` route which peers exist, so
the proxy has to be answering before they start -- otherwise that boot
reconciliation is simply lost (it is best-effort).  The proxy's own boot probe
of the data servers is allowed to fail, so starting it first costs nothing:
after the data servers are up, ``up`` re-probes them through the proxy's
``POST /heartbeat`` and prints the refreshed health.  A later ``POST
/heartbeat`` that sees a data server answer after a failed probe also asks that
server to reconcile its handoffs again, which is how a server that stayed up
through a failure collects what was routed elsewhere while it was unreachable.

State
-----
Everything a cluster writes lives under ``src/swift_light/``, beside the code
that reads it:

* the ring, at its module default -- ``proxy_server/ring.json``;
* one directory per data server under ``data_server/cluster/<node>/``, holding
  that server's ``data_server.json`` and its ``store/``;
* ``data_server/cluster/pids.json``, the PIDs of the processes ``up`` started,
  which is what ``down`` reads.

A data server's parameters default to *beside the storage module*, which only
one server on a machine can use, so each node is given its own directory here
instead.  The ring, though, sits at the true default, so the proxy loads it
without being told where it is.

Exit codes
----------
``0``   the command succeeded
``1``   the command ran but the cluster misbehaved: a process died, the ring
        does not describe the cluster asked for, or nothing answered in time
``77``  the command could not be attempted: bad arguments, a port already in
        use, or a ring that cannot be built

Tests live in ``src/test/``.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# The package is imported by its full path (there is no ``__init__.py``), so
# the repository's ``src/`` directory must be importable before any
# ``swift_light.*`` module is.  This runs before those imports on purpose.
_SRC_ROOT = Path(__file__).resolve().parents[1]
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from swift_light.data_server import storage as _storage  # noqa: E402
from swift_light.data_server.data_HTTP_server import DataHTTPServer  # noqa: E402
from swift_light.data_server.storage import DataServerConfig  # noqa: E402
from swift_light.proxy_server import (  # noqa: E402
    consistent_hashing_ring as _ring,
)
from swift_light.proxy_server.consistent_hashing_ring import (  # noqa: E402
    ConsistentHashingRing,
)
from swift_light.proxy_server.proxy_HTTP_server import (  # noqa: E402
    DEFAULT_DATA_PORT,
    DEFAULT_USER_PORT,
    ProxyHTTPServer,
)

__all__ = [
    "DEFAULT_BASE_DATA_PORT",
    "DEFAULT_HOST",
    "DEFAULT_NODE_COUNT",
    "DEFAULT_REPLICATE_COUNT",
    "DeployError",
    "down",
    "main",
    "status",
    "up",
]

EXIT_OK = 0
EXIT_FAILURE = 1
EXIT_ENVIRONMENT = 77

DEFAULT_HOST = "127.0.0.1"
"""Where a local cluster binds.  Not the modules' empty string, which is every
interface: a local cluster has no business listening off the machine."""

DEFAULT_NODE_COUNT = 3
"""Data servers ``up`` starts unless ``--nodes`` says otherwise."""

DEFAULT_REPLICATE_COUNT = 2
"""Copies of every object the cluster keeps unless ``--replicas`` says otherwise."""

DEFAULT_BASE_DATA_PORT = 9001
"""Port of the first data server; nodes count up from here."""

DEFAULT_START_TIMEOUT = 20.0
"""How long ``up`` waits for one process to start answering."""

DEFAULT_REQUEST_TIMEOUT = 3.0
"""How long a single status request may take."""

RING_PATH = Path(_ring.__file__).with_name(_ring.DEFAULT_RING_FILENAME)
"""The ring, at the ring module's own default -- where the proxy looks for it."""

CLUSTER_ROOT = Path(_storage.__file__).parent / "cluster"
"""Per-node parameters and stores, beside the storage module that reads them."""

PID_FILE = CLUSTER_ROOT / "pids.json"
"""What ``up`` started, so ``down`` can stop it."""


class DeployError(RuntimeError):
    """A failure that already knows the exit code the process should return."""

    def __init__(self, message: str, code: int = EXIT_FAILURE) -> None:
        super().__init__(message)
        self.code = code


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------


def _http_json(
    url: str, *, timeout: float = DEFAULT_REQUEST_TIMEOUT, method: str = "GET"
) -> dict[str, Any]:
    """Send one request and decode its JSON body."""
    request = urllib.request.Request(url, method=method)
    with urllib.request.urlopen(request, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _await_ready(
    label: str,
    url: str,
    *,
    timeout: float,
    proc: subprocess.Popen[bytes],
) -> None:
    """Poll ``url`` until it answers, or give up.  Raises :class:`DeployError`.

    The process is watched while waiting: a child that has already exited is
    never going to answer, so say so at once rather than sitting out the whole
    timeout -- a port already in use is the usual reason.
    """
    deadline = time.monotonic() + timeout
    last = "no answer yet"
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise DeployError(
                f"{label} exited before it answered {url} (code {proc.returncode}); "
                f"its output above says why -- a port already in use is the usual "
                f"cause",
                EXIT_ENVIRONMENT,
            )
        try:
            _http_json(url)
            return
        except (urllib.error.URLError, OSError, ValueError) as exc:
            last = str(exc)
        time.sleep(0.15)
    raise DeployError(f"{label} did not answer {url} within {timeout:g}s ({last})")


# ---------------------------------------------------------------------------
# Process helpers
# ---------------------------------------------------------------------------


def _spawn(*argv: str) -> subprocess.Popen[bytes]:
    """Start one child process running this same file, in a named mode.

    Output is inherited, so every server's own lines land on the terminal that
    ran ``up``.
    """
    return subprocess.Popen([sys.executable, str(Path(__file__).resolve()), *argv])


def _terminate(procs: list[tuple[str, subprocess.Popen[bytes]]]) -> None:
    """Ask every child to stop, then make sure it did."""
    for _, proc in procs:
        if proc.poll() is None:
            proc.terminate()
    for name, proc in procs:
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            print(f"[deploy] {name} ignored terminate; killing it", file=sys.stderr)
            proc.kill()


def _pid_alive(pid: int) -> bool:
    """Whether a process started by an earlier ``up`` still exists."""
    if os.name == "nt":
        listing = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}", "/NH"],
            capture_output=True,
            text=True,
        )
        return str(pid) in listing.stdout
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _kill_pid(pid: int) -> None:
    """Stop one process by PID (used by ``down``, which has no Popen object)."""
    if os.name == "nt":
        subprocess.run(
            ["taskkill", "/PID", str(pid), "/T", "/F"],
            capture_output=True,
            text=True,
        )
        return
    os.kill(pid, 15)


# ---------------------------------------------------------------------------
# Cluster layout
# ---------------------------------------------------------------------------


def _node_names(count: int) -> list[str]:
    return [f"node{index + 1}" for index in range(count)]


def _addresses(host: str, base_port: int, count: int) -> list[str]:
    """The data servers' addresses -- which are exactly what the ring stores."""
    return [f"http://{host}:{base_port + index}" for index in range(count)]


def _node_dir(name: str) -> Path:
    return CLUSTER_ROOT / name


def _ensure_ring(
    *,
    addresses: list[str],
    replicate_count: int,
    vnode_count: int,
    rebuild: bool,
) -> ConsistentHashingRing:
    """Build the ring once, or check that the existing one fits the request."""
    if RING_PATH.is_file():
        if not rebuild:
            ring = ConsistentHashingRing.load(RING_PATH, complete=False)
            if tuple(ring.nodes) != tuple(addresses) or (
                ring.replicate_count != replicate_count
            ):
                raise DeployError(
                    f"{RING_PATH} already describes a different cluster "
                    f"({ring.tot_node_count} nodes, "
                    f"replicate_count={ring.replicate_count}); pass --rebuild to "
                    f"discard it -- the ring is what placed every stored object, "
                    f"so rebuilding moves them",
                    EXIT_ENVIRONMENT,
                )
            print(f"[deploy] reusing the ring at {RING_PATH}")
            return ring
        RING_PATH.unlink()
        print(f"[deploy] removed {RING_PATH} (--rebuild)")

    ring = ConsistentHashingRing.build(
        len(addresses),
        replicate_count,
        nodes=addresses,
        path=RING_PATH,
        vnode_count=vnode_count,
    )
    print(
        f"[deploy] built {RING_PATH}: {ring.tot_node_count} data servers, "
        f"replicate_count={ring.replicate_count}, vnode_count={ring.vnode_count}"
    )
    return ring


def _write_node_parameters(addresses: list[str], proxy_address: str) -> list[Path]:
    """Give every data server its own parameters file, and its own directory."""
    configs: list[Path] = []
    for name, own in zip(_node_names(len(addresses)), addresses):
        directory = _node_dir(name)
        directory.mkdir(parents=True, exist_ok=True)
        config = directory / _storage.DEFAULT_CONFIG_FILENAME
        DataServerConfig(own, proxy_address).save(config)
        configs.append(config)
    return configs


def _write_pids(procs: list[tuple[str, subprocess.Popen[bytes]]]) -> None:
    CLUSTER_ROOT.mkdir(parents=True, exist_ok=True)
    PID_FILE.write_text(
        json.dumps({name: proc.pid for name, proc in procs}, indent=2) + "\n",
        encoding="utf-8",
    )


# ---------------------------------------------------------------------------
# up
# ---------------------------------------------------------------------------


def _print_ready(user_port: int, addresses: list[str], replicate_count: int) -> None:
    print("[deploy] cluster is up:")
    print(f"  clients       -> http://{DEFAULT_HOST}:{user_port}   (the proxy)")
    for address in addresses:
        print(f"  data server   -> {address}")
    print(f"  replicas      -> {replicate_count} copies of every object")
    print("[deploy] try it:")
    print("  python src/swift_light/client/client.py push notes.txt a/b/notes.txt")
    print("  python src/swift_light/client/client.py pull a/b/notes.txt notes.txt")
    print("  python src/swift_light/deploy.py status")
    print("[deploy] press Ctrl-C to stop the cluster")


def _supervise(procs: list[tuple[str, subprocess.Popen[bytes]]]) -> int:
    """Watch the children until Ctrl-C, or until one dies.

    A child that dies while the PID file is still there is a crash.  If the PID
    file is gone, a ``down`` in another terminal is stopping the cluster on
    purpose -- that is an orderly stop, not a failure.
    """
    try:
        while True:
            for name, proc in procs:
                code = proc.poll()
                if code is not None:
                    if not PID_FILE.is_file():
                        print("[deploy] stopping the cluster ('down' was called)")
                        return EXIT_OK
                    print(
                        f"[deploy] {name} exited unexpectedly (code {code})",
                        file=sys.stderr,
                    )
                    return EXIT_FAILURE
            time.sleep(0.5)
    except KeyboardInterrupt:
        print("\n[deploy] stopping the cluster")
        return EXIT_OK


def up(args: argparse.Namespace) -> int:
    """Start a whole cluster and supervise it until Ctrl-C."""
    if args.nodes < 1:
        raise DeployError("--nodes must be at least 1", EXIT_ENVIRONMENT)
    if args.nodes < args.replicas:
        raise DeployError(
            f"replicate_count={args.replicas} needs at least that many data "
            f"servers, but --nodes is {args.nodes}",
            EXIT_ENVIRONMENT,
        )

    addresses = _addresses(args.host, args.base_port, args.nodes)
    proxy_address = f"http://{args.host}:{args.data_port}"
    user_base = f"http://{args.host}:{args.user_port}"

    _ensure_ring(
        addresses=addresses,
        replicate_count=args.replicas,
        vnode_count=args.vnode_count,
        rebuild=args.rebuild,
    )
    configs = _write_node_parameters(addresses, proxy_address)

    procs: list[tuple[str, subprocess.Popen[bytes]]] = []
    try:
        # The proxy first: the data servers ask it who their peers are as they
        # boot, and that question is asked only once.
        proxy = _spawn(
            "_serve-proxy",
            "--ring",
            str(RING_PATH),
            "--host",
            args.host,
            "--user-port",
            str(args.user_port),
            "--data-port",
            str(args.data_port),
        )
        procs.append(("proxy", proxy))
        _await_ready(
            "the proxy",
            f"{proxy_address}/nodes",
            timeout=args.start_timeout,
            proc=proxy,
        )

        # All the data servers at once, then wait for each: a slow node must not
        # delay the next one's boot.
        starting: list[tuple[str, str, subprocess.Popen[bytes]]] = []
        for name, config, address in zip(
            _node_names(args.nodes), configs, addresses
        ):
            proc = _spawn(
                "_serve-data",
                "--config",
                str(config),
                "--store",
                str(_node_dir(name) / _storage.DEFAULT_STORE_DIRNAME),
            )
            procs.append((name, proc))
            starting.append((name, address, proc))
        for name, address, proc in starting:
            _await_ready(
                f"the data server {name} at {address}",
                f"{address}/heartbeat",
                timeout=args.start_timeout,
                proc=proc,
            )

        # The proxy probed the ring before any data server existed, so it is
        # holding three failures.  Ask it to look again now.
        try:
            health = _http_json(f"{proxy_address}/heartbeat", method="POST")
            for server, status in health.get("servers", {}).items():
                state = "healthy" if status.get("healthy") else "FAILED"
                print(f"[deploy]   {server}: {state}")
        except (urllib.error.URLError, OSError, ValueError) as exc:
            print(f"[deploy] could not re-probe through the proxy: {exc}")

        _write_pids(procs)
        _print_ready(args.user_port, addresses, args.replicas)
        return _supervise(procs)
    except DeployError as exc:
        print(f"[deploy] {exc}", file=sys.stderr)
        return exc.code
    finally:
        _terminate(procs)
        PID_FILE.unlink(missing_ok=True)


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def status(args: argparse.Namespace) -> int:
    """Describe the cluster on disk and ask the running proxy about its health."""
    if not RING_PATH.is_file():
        raise DeployError(
            f"there is no ring at {RING_PATH}; run 'up' first", EXIT_ENVIRONMENT
        )
    ring = ConsistentHashingRing.load(RING_PATH, complete=False)
    print(f"ring: {RING_PATH}")
    print(
        f"  data servers    {ring.tot_node_count}\n"
        f"  replicate_count {ring.replicate_count}\n"
        f"  vnode_count     {ring.vnode_count}"
    )
    for node in ring.nodes:
        print(f"  - {node}")

    if PID_FILE.is_file():
        pids = json.loads(PID_FILE.read_text(encoding="utf-8"))
        running = [name for name, pid in pids.items() if _pid_alive(pid)]
        print(f"processes: {', '.join(running) if running else 'none running'}")
    else:
        print("processes: none recorded (no 'up' has run, or it was stopped)")

    proxy_address = f"http://{args.host}:{args.data_port}"
    try:
        # POST, not GET: the proxy only re-probes its data servers when asked,
        # so a GET would report whatever the last probe saw -- which is stale by
        # the time anyone runs this, and would read as FAILED for a live node.
        health = _http_json(f"{proxy_address}/heartbeat", method="POST")
    except (urllib.error.URLError, OSError, ValueError) as exc:
        print(f"health: the proxy at {proxy_address} did not answer ({exc})")
        return EXIT_FAILURE
    print(f"health: re-probed via {proxy_address}")
    for server, entry in health.get("servers", {}).items():
        state = "healthy" if entry.get("healthy") else "FAILED"
        message = entry.get("message") or "-"
        print(f"  {server}: {state} ({message})")
    return EXIT_OK


# ---------------------------------------------------------------------------
# down
# ---------------------------------------------------------------------------


def down(args: argparse.Namespace) -> int:
    """Stop the processes a previous ``up`` recorded."""
    if not PID_FILE.is_file():
        print("[deploy] no cluster is recorded as running")
        return EXIT_OK
    pids = json.loads(PID_FILE.read_text(encoding="utf-8"))
    # Removed before anything is killed, not after: the supervising 'up' reads
    # this file to tell a deliberate stop from a crash.
    PID_FILE.unlink(missing_ok=True)
    for name, pid in pids.items():
        if _pid_alive(pid):
            print(f"[deploy] stopping {name} (pid {pid})")
            _kill_pid(pid)
        else:
            print(f"[deploy] {name} (pid {pid}) was already gone")
    return EXIT_OK


# ---------------------------------------------------------------------------
# The modes a child process runs in
# ---------------------------------------------------------------------------


def _serve_data(args: argparse.Namespace) -> int:
    """Run one data server, blocking.  Started by ``up``."""
    server = DataHTTPServer(
        config=DataServerConfig.load(args.config),
        store_root=args.store,
    )
    print(
        f"[data] {server.config.own_address} serving on "
        f"{server.host}:{server.port}, store {args.store}",
        flush=True,
    )
    server.serve_forever()
    return EXIT_OK


def _serve_proxy(args: argparse.Namespace) -> int:
    """Run the proxy, blocking.  Started by ``up``."""
    proxy = ProxyHTTPServer(
        ring_path=args.ring,
        host=args.host,
        user_port=args.user_port,
        data_port=args.data_port,
        fail_after_seconds=args.fail_after_seconds,
    )
    print(
        f"[proxy] clients on {args.host}:{proxy.user_port}, "
        f"data servers on {args.host}:{proxy.data_port}",
        flush=True,
    )
    proxy.serve_forever()
    return EXIT_OK


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


class _ArgumentParser(argparse.ArgumentParser):
    """An argument parser whose usage errors honour this module's exit codes."""

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(EXIT_ENVIRONMENT, f"{self.prog}: error: {message}\n")


def _parser() -> argparse.ArgumentParser:
    """The command-line grammar for ``up`` / ``status`` / ``down``."""
    parser = _ArgumentParser(
        prog="swift-light-deploy",
        description="Run a swift_light cluster on this machine.",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser("up", help="start a cluster and supervise it")
    start.add_argument(
        "--nodes",
        type=int,
        default=DEFAULT_NODE_COUNT,
        metavar="N",
        help=f"how many data servers to run (default: {DEFAULT_NODE_COUNT})",
    )
    start.add_argument(
        "--replicas",
        type=int,
        default=DEFAULT_REPLICATE_COUNT,
        metavar="R",
        help=(
            "copies of every object, including the primary "
            f"(default: {DEFAULT_REPLICATE_COUNT})"
        ),
    )
    start.add_argument(
        "--base-port",
        type=int,
        default=DEFAULT_BASE_DATA_PORT,
        metavar="PORT",
        help=f"port of the first data server (default: {DEFAULT_BASE_DATA_PORT})",
    )
    start.add_argument(
        "--user-port",
        type=int,
        default=DEFAULT_USER_PORT,
        metavar="PORT",
        help=f"the proxy's client port (default: {DEFAULT_USER_PORT})",
    )
    start.add_argument(
        "--data-port",
        type=int,
        default=DEFAULT_DATA_PORT,
        metavar="PORT",
        help=f"the proxy's data-server port (default: {DEFAULT_DATA_PORT})",
    )
    start.add_argument(
        "--host",
        default=DEFAULT_HOST,
        metavar="ADDR",
        help=f"address every process binds (default: {DEFAULT_HOST})",
    )
    start.add_argument(
        "--vnode-count",
        type=int,
        default=_ring.DEFAULT_VNODE_COUNT,
        metavar="V",
        help=(
            "virtual nodes per data server (default: "
            f"{_ring.DEFAULT_VNODE_COUNT}; only used when building)"
        ),
    )
    start.add_argument(
        "--rebuild",
        action="store_true",
        help="discard an existing ring and lay the cluster out again",
    )
    start.add_argument(
        "--start-timeout",
        type=float,
        default=DEFAULT_START_TIMEOUT,
        metavar="SECONDS",
        help=(
            "how long to wait for the proxy and each data server to answer "
            f"(default: {DEFAULT_START_TIMEOUT:g})"
        ),
    )

    report = commands.add_parser("status", help="report the cluster's state and health")
    report.add_argument("--host", default=DEFAULT_HOST, metavar="ADDR")
    report.add_argument("--data-port", type=int, default=DEFAULT_DATA_PORT, metavar="PORT")

    commands.add_parser("down", help="stop the cluster a previous 'up' started")
    return parser


def _serve(argv: list[str]) -> int:
    """Run one child mode -- an internal command 'up' launches processes with.

    Kept out of :func:`_parser` so ``--help`` shows only the three commands a
    person uses; the child argv is generated by :func:`_spawn`, never typed.
    """
    mode, rest = argv[0], argv[1:]
    parser = argparse.ArgumentParser(prog=f"swift-light-deploy {mode}", add_help=False)
    if mode == "_serve-data":
        parser.add_argument("--config", required=True)
        parser.add_argument("--store", required=True)
        return _serve_data(parser.parse_args(rest))
    parser.add_argument("--ring", default=None)
    parser.add_argument("--host", default=DEFAULT_HOST)
    parser.add_argument("--user-port", type=int, default=DEFAULT_USER_PORT)
    parser.add_argument("--data-port", type=int, default=DEFAULT_DATA_PORT)
    parser.add_argument("--fail-after-seconds", type=float, default=10.0)
    return _serve_proxy(parser.parse_args(rest))


def main(argv: list[str] | None = None) -> int:
    """Run one command; return the process exit code.

    ``argparse`` signals ``--help`` and a usage error by raising ``SystemExit``;
    its code is returned instead so a caller (and a test) always gets a number.
    """
    argv = list(sys.argv[1:] if argv is None else argv)
    if argv and argv[0] in ("_serve-data", "_serve-proxy"):
        # A child that cannot bind its port should say which port and stop,
        # not spill a traceback into the terminal that ran 'up'.
        try:
            return _serve(argv)
        except OSError as exc:
            print(f"{argv[0]}: {exc}", file=sys.stderr)
            return EXIT_ENVIRONMENT

    try:
        args = _parser().parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else EXIT_ENVIRONMENT

    handlers = {"up": up, "status": status, "down": down}
    try:
        return handlers[args.command](args)
    except DeployError as exc:
        print(f"deploy: {exc}", file=sys.stderr)
        return exc.code
    except OSError as exc:
        # A port already in use, or a file that cannot be written: the machine,
        # not this command line.
        print(f"deploy: {exc}", file=sys.stderr)
        return EXIT_ENVIRONMENT


if __name__ == "__main__":
    sys.exit(main())
