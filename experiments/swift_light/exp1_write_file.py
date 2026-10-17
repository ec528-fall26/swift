#!/usr/bin/env python3
"""exp1 -- write a local file into the swift_light cluster.

What this measures
------------------
That the write path the design document describes works end to end: a client
asks the proxy which data servers may hold a key, then walks that fan in order
until ``replicate_count`` servers have durably accepted the bytes -- the
quorum.  The script uploads one file, by default ``SwiftLightFiles/Hello.txt``
under the cluster path ``MyTxtFiles/Hello.txt``, and records the key the
cluster filed it under and the SHA-256 of what went in, so that experiment 2
can show the same bytes coming back out.

Prerequisites
-------------
The cluster must already be up.  This script never starts or stops it::

    python3 src/swift_light/deploy.py up      # in another terminal

Usage
-----
Run from the repository root::

    python3 experiments/swift_light/exp1_write_file.py

    # or name the file and the cluster path yourself
    python3 experiments/swift_light/exp1_write_file.py \\
        --file SwiftLightFiles/Hello.txt --remote-path MyTxtFiles/Hello.txt

The object is filed under the account named by ``--user``, which defaults to
the operating system's login name -- the same default the client uses, so the
read-back experiment finds the object again.

Exit codes
----------
    0   the file was stored on a quorum of data servers
    1   the cluster misbehaved: fewer than ``replicate_count`` servers accepted
    77  the experiment could not be attempted: the proxy did not answer, the
        local file could not be read, or the path is not a usable object path

Tests live in ``src/test/``.
"""

from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

# There is no ``__init__.py`` anywhere under ``src/``; the tree is imported by
# its full path, so ``src/`` has to be importable before any ``swift_light.*``
# module is.  This runs before those imports on purpose, exactly as deploy.py
# does it.
_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from swift_light.client import client as _client  # noqa: E402

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_SKIP = 77

DEFAULT_FILE = "SwiftLightFiles/Hello.txt"
"""The local file this experiment uploads unless ``--file`` says otherwise."""

DEFAULT_REMOTE_PATH = "MyTxtFiles/Hello.txt"
"""The cluster path it files the object under unless ``--remote-path`` says so."""


def _sha256(data: bytes) -> str:
    """The hex digest of ``data``, so the read-back can be compared by value."""
    return hashlib.sha256(data).hexdigest()


def _role(index: int, replicate_count: int) -> str:
    """What the server at ``index`` of the fan is for a key."""
    if index == 0:
        return "primary"
    if index < replicate_count:
        return "secondary"
    return "handoff"


def _parser() -> argparse.ArgumentParser:
    """The command-line grammar for this experiment."""
    parser = argparse.ArgumentParser(
        prog="exp1_write_file",
        description="Store a local file in the swift_light cluster (the write path).",
    )
    parser.add_argument(
        "--file",
        default=DEFAULT_FILE,
        metavar="PATH",
        help=f"local file to upload (default: {DEFAULT_FILE})",
    )
    parser.add_argument(
        "--remote-path",
        default=DEFAULT_REMOTE_PATH,
        metavar="PATH",
        help=f"cluster path to file it under (default: {DEFAULT_REMOTE_PATH})",
    )
    parser.add_argument(
        "--user",
        default=None,
        metavar="NAME",
        help="account to file the object under (default: the OS login name)",
    )
    return parser


def _print_fan(lookup: dict) -> None:
    """Print the fan the proxy returned: the order the client will walk."""
    replicate_count = lookup["replicate_count"]
    print(f"[exp1] fan       slot {lookup['slot']}, replicate_count {replicate_count}")
    for index, server in enumerate(lookup["servers"]):
        print(f"[exp1]           {index}: {server} ({_role(index, replicate_count)})")


def main(argv: list[str] | None = None) -> int:
    """Upload one file to a quorum; return the process exit code."""
    args = _parser().parse_args(argv)
    print("[exp1] writing a file into the swift_light cluster")

    try:
        data = Path(args.file).read_bytes()
    except OSError as exc:
        print(f"[exp1] cannot read the local file {args.file!r}: {exc}", file=sys.stderr)
        print("[exp1] nothing was sent; create the file and try again", file=sys.stderr)
        return EXIT_SKIP

    client = _client.Client(user=args.user)
    key = client.object_key(args.remote_path)
    print(f"[exp1] file      {args.file} ({len(data)} bytes, sha256 {_sha256(data)})")
    print(f"[exp1] object    {args.remote_path}  ->  key {key!r}")
    print(f"[exp1] account   {client.user!r}")
    print(f"[exp1] proxy     {_client.PROXY_BASE_URL}")

    try:
        lookup = client.lookup(key)
    except _client.ClientError as exc:
        print(f"[exp1] the proxy did not answer: {exc}", file=sys.stderr)
        print(
            "[exp1] start the cluster first: python3 src/swift_light/deploy.py up",
            file=sys.stderr,
        )
        return EXIT_SKIP
    _print_fan(lookup)

    try:
        report = client.push(args.file, args.remote_path)
    except _client.ClientError as exc:
        print(f"[exp1] the write could not be attempted: {exc}", file=sys.stderr)
        return EXIT_SKIP

    accepted = len(report["acknowledged"])
    print(f"[exp1] wrote {report['key']!r} to {accepted}/{report['quorum']} replicas")
    for server in report["acknowledged"]:
        print(f"[exp1]   accepted by {server}")
    for failure in report["failed"]:
        print(
            f"[exp1]   refused by {failure['server']}: {failure['error']}",
            file=sys.stderr,
        )

    if not report["ok"]:
        print(
            f"[exp1] FAIL: the write reached only {accepted}/{report['quorum']} "
            "replicas",
            file=sys.stderr,
        )
        return EXIT_FAIL

    print(
        f"[exp1] OK: {len(data)} bytes stored on {accepted} data servers "
        f"(sha256 {_sha256(data)})"
    )
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
