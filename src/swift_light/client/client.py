#!/usr/bin/env python3
"""The swift_light command-line client.

A client stores and fetches objects without knowing where they live.  It asks
the proxy which data servers may hold a key, then talks to those servers
directly:

    GET    /servers?key=<partition key>   which data servers may hold the key
    GET    /files?key=<partition key>     read an object from one data server
    PUT    /files?key=<partition key>     write an object to one data server
    DELETE /files?key=<partition key>     remove an object from one data server

The lookup goes through the proxy, so every client in the cluster agrees on
placement without holding a copy of the ring itself.  The proxy's answer is the
key's *fan*: the data servers to try, ``fan[0]`` (the primary) first, then its
secondaries, then the handoff servers.  The reply also carries
``replicate_count``, how many copies of every object the cluster keeps.

Commands
--------
::

    push <local-file> <remote-path>   store a local file under a cluster path
    pull <remote-path> <local-file>   fetch a cluster object into a local file
    delete <remote-path>              remove a cluster object

The remote path is a slash-separated object path such as ``a/b/file.txt``.  The
cluster's partition key turns its separators into ``-`` and puts the account
name in front::

    a/b/file.txt   by user jdoe   ->   key "jdoe-a-b-file.txt"

The account name is the operating system's login name unless ``--user`` names
another one.  A path this cannot turn into a usable key is refused before
anything is sent.

Durability
----------
A write is not sent to one server.  ``push`` and ``delete`` send the change to
the whole *replica set* -- the first ``replicate_count`` servers of the fan --
**at once**, one thread per copy, and count the ones that durably accepted it.
That count is the *quorum*: a write costs one round trip, however many copies
it keeps, instead of one round trip per copy.

A write still succeeds while a replica is down, because a short quorum then
goes on to the handoff servers later in the fan, in fan order, until the quorum
is met -- the sloppy quorum.  That second phase is deliberately serial and
stops the moment the quorum is reached: it is the degraded path, and writing
more substitutes than the quorum needs would only leave the later handoff
reconciliation more to undo.

The report says how many replicas acknowledged, and the command fails (exit 1)
when the quorum is not reached.  A delete of a key a server does not hold counts
as accepted: the object is already gone there, which is the outcome asked for.

A read is the other way round.  ``pull`` goes to the primary (``fan[0]``) and
falls back to the next server only when the primary is unreachable or does not
hold the key.  When the answer comes from anywhere but the primary the client
says so on stderr -- the explicitly-marked fallback read the design calls for,
so the caller can tell a normal read from one that is still catching up.

Usage
-----
Run from the repository root::

    # store ./notes.txt as the object a/b/notes.txt
    python src/swift_light/client/client.py push notes.txt a/b/notes.txt

    # fetch it back
    python src/swift_light/client/client.py pull a/b/notes.txt notes.txt

    # remove it
    python src/swift_light/client/client.py delete a/b/notes.txt

    # act as another account
    python src/swift_light/client/client.py --user alice push notes.txt a/b/notes.txt

The client talks to the proxy at ``http://127.0.0.1:8000`` and has no option to
point it elsewhere.

Exit codes
----------
``0``   the command succeeded
``1``   the command ran but the operation failed -- a write or delete reached
        fewer than ``replicate_count`` replicas, or no data server held the key
        being pulled
``77``  the command could not be attempted: bad arguments, an invalid path, an
        unreadable local file, or the proxy could not be reached

Tests live in ``src/test/``.
"""

from __future__ import annotations

import argparse
import getpass
import json
import sys
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from urllib.parse import quote

__all__ = [
    "Client",
    "ClientError",
    "object_key",
    "main",
    "PROXY_BASE_URL",
    "DEFAULT_TIMEOUT",
    "EXIT_OK",
    "EXIT_FAILURE",
    "EXIT_ENVIRONMENT",
]

PROXY_BASE_URL = "http://127.0.0.1:8000"
"""The proxy every client talks to.  There is no option to change it."""

DEFAULT_TIMEOUT = 5.0
"""Seconds to wait for any one request before giving up on it."""

EXIT_OK = 0
"""The command succeeded."""

EXIT_FAILURE = 1
"""The command ran but the operation failed (quorum short, or key not found)."""

EXIT_ENVIRONMENT = 77
"""The command could not be attempted (bad input, or the proxy unreachable)."""

_PATH_SEPARATORS = ("/", "\\")
"""A remote path may be written with either separator."""

_FORBIDDEN_IN_SEGMENT = ("/", "\\", ":", "\x00")
"""Characters the store refuses inside one path segment."""


class ClientError(RuntimeError):
    """A request could not be attempted, or a path could not be made a key."""


# ---------------------------------------------------------------------------
# Path -> partition key
# ---------------------------------------------------------------------------


def object_key(path: str, user: str) -> str:
    """Turn a remote object path and account into the cluster's partition key.

    ``a/b/file.txt`` for user ``jdoe`` becomes ``jdoe-a-b-file.txt``: the
    separators turn into ``-`` and the account name goes in front.  The result
    is checked against the same rules the data server's store applies -- every
    ``-``-separated segment must be a usable directory name -- so an impossible
    key is refused here, before anything is sent.

    Raises :class:`ClientError` for a path or account that cannot become a key.
    """
    if not isinstance(path, str):
        raise ClientError(f"remote path must be text, got {type(path).__name__}")
    if not isinstance(user, str):
        raise ClientError(f"account name must be text, got {type(user).__name__}")
    account = _account(user)
    return "-".join([account, *_segments(path)])


def _segments(path: str) -> list[str]:
    """The usable segments of ``path``, its separators normalised and trimmed."""
    normalised = path.strip()
    for separator in _PATH_SEPARATORS:
        normalised = normalised.replace(separator, "/")
    normalised = normalised.strip("/")
    if not normalised:
        raise ClientError("remote path must name an object")
    segments = normalised.split("/")
    for segment in segments:
        _check_segment(segment, path)
    return segments


def _account(user: str) -> str:
    """The account name as a key prefix, refusing one that cannot be a path."""
    account = user.strip()
    if not account:
        raise ClientError("account name must not be blank")
    for part in account.split("-"):
        _check_segment(part, user)
    return account


def _check_segment(segment: str, source: str) -> None:
    """Refuse a segment the store could not turn into a directory name."""
    if (
        segment in ("", ".", "..")
        or ":" in segment
        or "\x00" in segment
        or any(separator in segment for separator in _PATH_SEPARATORS)
    ):
        raise ClientError(f"{source!r} is not a usable object path")


# ---------------------------------------------------------------------------
# The client
# ---------------------------------------------------------------------------


class Client:
    """Talks to the cluster on one user's behalf, one command at a time.

    Parameters
    ----------
    proxy:
        Base URL of the proxy to ask for placement (default:
        :data:`PROXY_BASE_URL`).
    user:
        The account name the partition key is prefixed with.  Left out, the
        operating system's login name is used.
    timeout:
        Seconds to wait for any one request.
    """

    __slots__ = ("_proxy", "_user", "_timeout")

    def __init__(
        self,
        *,
        proxy: str = PROXY_BASE_URL,
        user: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self._proxy = proxy.rstrip("/")
        self._user = _default_user() if user is None else user
        self._timeout = float(timeout)

    @property
    def user(self) -> str:
        """The account name this client acts as."""
        return self._user

    def object_key(self, path: str) -> str:
        """The partition key ``path`` maps to for this client's account."""
        return object_key(path, self._user)

    # -- placement ----------------------------------------------------------

    def lookup(self, key: str) -> dict[str, Any]:
        """Ask the proxy which data servers may hold ``key``.

        Returns the proxy's answer as ``{"key", "slot", "replicate_count",
        "servers"}``, with ``servers`` the fan in the order to try: primary,
        secondaries, then handoff servers.

        Raises :class:`ClientError` when the proxy cannot be reached or does
        not answer a lookup with ``200``.
        """
        status, body = self._request("GET", f"/servers?key={quote(key, safe='')}")
        if status != 200:
            raise ClientError(f"the proxy answered {status} for the key lookup")
        payload = json.loads(body)
        return {
            "key": key,
            "slot": payload.get("slot"),
            "replicate_count": int(payload["replicate_count"]),
            "servers": tuple(str(server).rstrip("/") for server in payload["servers"]),
        }

    # -- commands -----------------------------------------------------------

    def push(self, local: str, path: str) -> dict[str, Any]:
        """Store the local file ``local`` under ``path``, to a quorum.

        Returns a report: ``key``, ``quorum``, ``acknowledged`` (the servers
        that took it), ``failed`` (the servers that did not, and why), and
        ``ok`` -- whether the quorum was reached.
        """
        key = self.object_key(path)
        data = _read_local(local)
        return self._quorum(key, self.lookup(key), method="PUT", data=data, ok_statuses=(200, 201))

    def delete(self, path: str) -> dict[str, Any]:
        """Remove the object ``path``, to a quorum.

        A server that does not hold the key answers ``404``, which counts as
        accepted -- the object is already gone there.  Returns the same report
        shape as :meth:`push`.
        """
        key = self.object_key(path)
        return self._quorum(
            key, self.lookup(key), method="DELETE", ok_statuses=(200, 204, 404)
        )

    def pull(self, path: str, local: str) -> dict[str, Any]:
        """Fetch the object ``path`` into the local file ``local``.

        Reads from the primary and falls back through the fan until a server
        answers, writing the bytes to ``local``.  Returns ``key``, ``ok``,
        ``source``, ``role`` (``"primary"``, ``"secondary"`` or ``"handoff"``),
        ``bytes`` and ``tried`` -- or ``ok: False`` and ``tried`` when no
        server held the key.
        """
        key = self.object_key(path)
        lookup = self.lookup(key)
        replicate_count = lookup["replicate_count"]
        tried: list[dict[str, str]] = []
        for index, server in enumerate(lookup["servers"]):
            try:
                status, body = self._send("GET", server, key)
            except ClientError as exc:
                tried.append({"server": server, "error": str(exc)})
                continue
            if status == 200:
                _write_local(local, body)
                return {
                    "key": key,
                    "ok": True,
                    "source": server,
                    "role": _role(index, replicate_count),
                    "bytes": len(body),
                    "tried": tried,
                }
            tried.append({"server": server, "error": _reason(status)})
        return {"key": key, "ok": False, "tried": tried}

    # -- quorum walk --------------------------------------------------------

    def _quorum(
        self,
        key: str,
        lookup: dict[str, Any],
        *,
        method: str,
        ok_statuses: tuple[int, ...],
        data: bytes | None = None,
    ) -> dict[str, Any]:
        """Reach ``replicate_count`` acknowledged servers, replica set first.

        Two phases.  The replica set -- the first ``replicate_count`` servers of
        the fan -- is sent ``method`` *at once*, so a write's latency is the
        slowest copy rather than the sum of the copies.  Only if that leaves the
        quorum short are the handoff servers walked, in fan order, stopping as
        soon as the quorum is met.

        A server that cannot be reached is recorded and the walk goes on -- that
        is what the later servers are for.  ``acknowledged`` and ``failed`` come
        back in fan order, so a report reads the same however the race finished.
        """
        quorum = lookup["replicate_count"]
        servers = list(lookup["servers"])
        outcomes = list(
            zip(
                servers[:quorum],
                self._send_all(servers[:quorum], method, key, data, ok_statuses),
            )
        )
        accepted = sum(1 for _, error in outcomes if error is None)
        for server in servers[quorum:]:
            if accepted >= quorum:
                break
            error = self._attempt(server, method, key, data, ok_statuses)
            outcomes.append((server, error))
            if error is None:
                accepted += 1
        acknowledged = [server for server, error in outcomes if error is None]
        return {
            "key": key,
            "quorum": quorum,
            "acknowledged": acknowledged,
            "failed": [
                {"server": server, "error": error}
                for server, error in outcomes
                if error is not None
            ],
            "ok": len(acknowledged) >= quorum,
        }

    def _send_all(
        self,
        servers: list[str],
        method: str,
        key: str,
        data: bytes | None,
        ok_statuses: tuple[int, ...],
    ) -> list[str | None]:
        """Send ``method`` to every server at once; the results keep fan order.

        One thread per server: the replica set is the size of ``replicate_count``
        and the requests share nothing, so the copies overlap instead of queueing
        behind each other.  A server that never answers still costs its timeout,
        but every timeout runs alongside the others rather than after them.

        ``None`` means that server accepted the change; anything else is why it
        did not.  ``pool.map`` yields in submission order, so the caller can zip
        the results straight back onto ``servers``.
        """
        if not servers:
            return []
        if len(servers) == 1:
            return [self._attempt(servers[0], method, key, data, ok_statuses)]
        with ThreadPoolExecutor(max_workers=len(servers)) as pool:
            return list(
                pool.map(
                    lambda server: self._attempt(
                        server, method, key, data, ok_statuses
                    ),
                    servers,
                )
            )

    def _attempt(
        self,
        server: str,
        method: str,
        key: str,
        data: bytes | None,
        ok_statuses: tuple[int, ...],
    ) -> str | None:
        """One write to one server: ``None`` if it accepted, else why it did not."""
        try:
            status, _ = self._send(method, server, key, data=data)
        except ClientError as exc:
            return str(exc)
        return None if status in ok_statuses else _reason(status)

    # -- transport ----------------------------------------------------------

    def _request(self, method: str, path: str) -> tuple[int, bytes]:
        """One request to the proxy; the status is returned, not raised."""
        url = self._proxy + path
        request = urllib.request.Request(url, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as reply:
                return reply.status, reply.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()
        except (urllib.error.URLError, OSError) as exc:
            raise ClientError(f"cannot reach the proxy at {self._proxy}: {_cause(exc)}") from exc

    def _send(
        self, method: str, server: str, key: str, *, data: bytes | None = None
    ) -> tuple[int, bytes]:
        """One request to a data server; 4xx/5xx come back as a status.

        A connection that cannot be made raises :class:`ClientError`, so the
        caller can record it and try the next server.
        """
        url = f"{server}/files?key={quote(key, safe='')}"
        request = urllib.request.Request(url, data=data, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self._timeout) as reply:
                return reply.status, reply.read()
        except urllib.error.HTTPError as exc:
            return exc.code, exc.read()
        except (urllib.error.URLError, OSError) as exc:
            raise ClientError(f"cannot reach {server}: {_cause(exc)}") from exc


# ---------------------------------------------------------------------------
# Local files
# ---------------------------------------------------------------------------


def _read_local(local: str) -> bytes:
    """The bytes of the local file, or a :class:`ClientError` saying why not."""
    try:
        return Path(local).read_bytes()
    except OSError as exc:
        raise ClientError(f"cannot read the local file {local!r}: {exc}") from exc


def _write_local(local: str, data: bytes) -> None:
    """Write ``data`` to the local file, making any missing parent first."""
    destination = Path(local)
    try:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
    except OSError as exc:
        raise ClientError(f"cannot write the local file {local!r}: {exc}") from exc


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _default_user() -> str:
    """The operating system's login name, or a last-resort placeholder."""
    try:
        return getpass.getuser()
    except Exception:  # noqa: BLE001 - a missing login name must not stop the client
        return "user"


def _role(index: int, replicate_count: int) -> str:
    """What the server at ``index`` of the fan is for a key."""
    if index == 0:
        return "primary"
    if index < replicate_count:
        return "secondary"
    return "handoff"


def _reason(status: int) -> str:
    """A short explanation for a data server's non-200 status."""
    if status == 404:
        return "does not hold the key"
    return f"answered {status}"


def _cause(exc: BaseException) -> str:
    """The root reason of a connection error, without its wrapper type."""
    reason = getattr(exc, "reason", None)
    return str(reason) if reason is not None else str(exc)


# ---------------------------------------------------------------------------
# Command line
# ---------------------------------------------------------------------------


class _ArgumentParser(argparse.ArgumentParser):
    """An argument parser whose usage errors honour this module's exit codes."""

    def error(self, message: str) -> None:
        self.print_usage(sys.stderr)
        self.exit(EXIT_ENVIRONMENT, f"{self.prog}: error: {message}\n")


def _parser() -> argparse.ArgumentParser:
    """The command-line grammar for ``push`` / ``pull`` / ``delete``."""
    parser = _ArgumentParser(
        prog="swift-light",
        description="Store and fetch objects through the swift_light cluster.",
    )
    parser.add_argument(
        "--user",
        metavar="NAME",
        help="account name to file the object under (default: the OS login name)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    push = commands.add_parser("push", help="store a local file under a cluster path")
    push.add_argument("local_file", metavar="local-file")
    push.add_argument("remote_path", metavar="remote-path")

    pull = commands.add_parser("pull", help="fetch a cluster object into a local file")
    pull.add_argument("remote_path", metavar="remote-path")
    pull.add_argument("local_file", metavar="local-file")

    delete = commands.add_parser("delete", help="remove a cluster object")
    delete.add_argument("remote_path", metavar="remote-path")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Run one command; return the process exit code.

    ``argparse`` signals ``--help`` and a usage error by raising ``SystemExit``;
    its code is returned instead so a caller (and a test) always gets a number.
    """
    try:
        args = _parser().parse_args(argv)
    except SystemExit as exc:
        return exc.code if isinstance(exc.code, int) else EXIT_ENVIRONMENT
    client = Client(user=args.user)
    try:
        if args.command == "push":
            return _report_write("pushed", client.push(args.local_file, args.remote_path))
        if args.command == "pull":
            return _report_pull(client.pull(args.remote_path, args.local_file))
        return _report_write("deleted", client.delete(args.remote_path))
    except ClientError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return EXIT_ENVIRONMENT


def _report_write(verb: str, report: dict[str, Any]) -> int:
    """Print the outcome of a push or delete, and return its exit code."""
    accepted = len(report["acknowledged"])
    if report["ok"]:
        print(f"{verb} {report['key']!r} to {accepted}/{report['quorum']} replicas")
        return EXIT_OK
    print(
        f"error: {verb} {report['key']!r} reached only "
        f"{accepted}/{report['quorum']} replicas",
        file=sys.stderr,
    )
    for failure in report["failed"]:
        print(f"  {failure['server']}: {failure['error']}", file=sys.stderr)
    return EXIT_FAILURE


def _report_pull(report: dict[str, Any]) -> int:
    """Print the outcome of a pull, and return its exit code."""
    if not report["ok"]:
        print(f"error: no data server holds {report['key']!r}", file=sys.stderr)
        for failure in report["tried"]:
            print(f"  {failure['server']}: {failure['error']}", file=sys.stderr)
        return EXIT_FAILURE
    if report["role"] != "primary":
        print(
            f"note: {report['key']!r} came from {report['source']} "
            f"(a {report['role']}, not the primary) -- a fallback read",
            file=sys.stderr,
        )
    print(f"pulled {report['key']!r} ({report['bytes']} bytes) from {report['source']}")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
