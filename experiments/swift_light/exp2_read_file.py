#!/usr/bin/env python3
"""exp2 -- read a file back out of the swift_light cluster.

What this measures
------------------
That the read path the design document describes works end to end, and that
what comes back is the same object experiment 1 put in.  A client asks the
proxy for the key's fan, then reads from the primary (``fan[0]``), falling
back through the secondaries and handoff servers only when one is unreachable
or does not hold the key.  The script downloads ``MyTxtFiles/Hello.txt`` into
``SwiftLightFiles/Hello_downloaded.txt`` and compares it byte-for-byte with the
original, so a short read or a stale replica fails loudly instead of quietly.

Prerequisites
-------------
The cluster must already be up::

    python3 src/swift_light/deploy.py up      # in another terminal

Experiment 1 must have run first, so the object exists::

    python3 experiments/swift_light/exp1_write_file.py

Usage
-----
Run from the repository root::

    python3 experiments/swift_light/exp2_read_file.py

    # or name the object, the destination, and the original yourself
    python3 experiments/swift_light/exp2_read_file.py \\
        --remote-path MyTxtFiles/Hello.txt \\
        --output SwiftLightFiles/Hello_downloaded.txt \\
        --expect SwiftLightFiles/Hello.txt

The object is looked up under the account named by ``--user``, which defaults
to the operating system's login name -- the same default experiment 1 used.

Exit codes
----------
    0   the object was fetched and is byte-identical to the original
    1   the cluster misbehaved: no data server held the key (run experiment 1
        first), or the bytes that came back differ from the original
    77  the experiment could not be attempted: the proxy did not answer, or a
        local file could not be read or written

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

DEFAULT_REMOTE_PATH = "MyTxtFiles/Hello.txt"
"""The cluster path this experiment reads unless ``--remote-path`` says otherwise."""

DEFAULT_OUTPUT = "SwiftLightFiles/Hello_downloaded.txt"
"""Where the fetched object is written unless ``--output`` says otherwise."""

DEFAULT_EXPECT = "SwiftLightFiles/Hello.txt"
"""The original the download is compared with unless ``--expect`` says otherwise."""


def _sha256(data: bytes) -> str:
    """The hex digest of ``data``, so the two files can be compared by value."""
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
        prog="exp2_read_file",
        description="Fetch a swift_light object back into a local file (the read path).",
    )
    parser.add_argument(
        "--remote-path",
        default=DEFAULT_REMOTE_PATH,
        metavar="PATH",
        help=f"cluster object to fetch (default: {DEFAULT_REMOTE_PATH})",
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT,
        metavar="PATH",
        help=f"local file to write it to (default: {DEFAULT_OUTPUT})",
    )
    parser.add_argument(
        "--expect",
        default=DEFAULT_EXPECT,
        metavar="PATH",
        help=(
            "the original the download is compared with "
            f"(default: {DEFAULT_EXPECT})"
        ),
    )
    parser.add_argument(
        "--user",
        default=None,
        metavar="NAME",
        help="account the object is filed under (default: the OS login name)",
    )
    return parser


def _print_fan(lookup: dict) -> None:
    """Print the fan the proxy returned: the order the client will walk."""
    replicate_count = lookup["replicate_count"]
    print(f"[exp2] fan       slot {lookup['slot']}, replicate_count {replicate_count}")
    for index, server in enumerate(lookup["servers"]):
        print(f"[exp2]           {index}: {server} ({_role(index, replicate_count)})")


def main(argv: list[str] | None = None) -> int:
    """Fetch one object and check it against the original; return the exit code."""
    args = _parser().parse_args(argv)
    print("[exp2] reading a file back out of the swift_light cluster")

    try:
        expected = Path(args.expect).read_bytes()
    except OSError as exc:
        print(f"[exp2] cannot read the original {args.expect!r}: {exc}", file=sys.stderr)
        print(
            "[exp2] the download cannot be checked without it; run experiment 1 "
            "first, or pass --expect",
            file=sys.stderr,
        )
        return EXIT_SKIP

    client = _client.Client(user=args.user)
    key = client.object_key(args.remote_path)
    print(f"[exp2] object    {args.remote_path}  ->  key {key!r}")
    print(f"[exp2] account   {client.user!r}")
    print(f"[exp2] proxy     {_client.PROXY_BASE_URL}")
    print(f"[exp2] expect    {args.expect} ({len(expected)} bytes, sha256 {_sha256(expected)})")

    try:
        lookup = client.lookup(key)
    except _client.ClientError as exc:
        print(f"[exp2] the proxy did not answer: {exc}", file=sys.stderr)
        print(
            "[exp2] start the cluster first: python3 src/swift_light/deploy.py up",
            file=sys.stderr,
        )
        return EXIT_SKIP
    _print_fan(lookup)

    try:
        report = client.pull(args.remote_path, args.output)
    except _client.ClientError as exc:
        print(f"[exp2] the read could not be attempted: {exc}", file=sys.stderr)
        return EXIT_SKIP

    if not report["ok"]:
        print(f"[exp2] FAIL: no data server holds {report['key']!r}", file=sys.stderr)
        for failure in report["tried"]:
            print(
                f"[exp2]   {failure['server']}: {failure['error']}",
                file=sys.stderr,
            )
        print(
            "[exp2] run experiment 1 first: "
            "python3 experiments/swift_light/exp1_write_file.py",
            file=sys.stderr,
        )
        return EXIT_FAIL

    if report["role"] != "primary":
        print(
            f"[exp2] note: {report['key']!r} came from {report['source']} "
            f"(a {report['role']}, not the primary) -- a fallback read",
            file=sys.stderr,
        )
    print(
        f"[exp2] fetched {report['key']!r} ({report['bytes']} bytes) "
        f"from {report['source']} ({report['role']})"
    )

    try:
        downloaded = Path(args.output).read_bytes()
    except OSError as exc:
        print(f"[exp2] cannot read back {args.output!r}: {exc}", file=sys.stderr)
        return EXIT_SKIP
    print(
        f"[exp2] wrote     {args.output} "
        f"({len(downloaded)} bytes, sha256 {_sha256(downloaded)})"
    )

    if downloaded != expected:
        print(
            f"[exp2] FAIL: {args.output} differs from {args.expect} "
            f"({len(downloaded)} bytes vs {len(expected)} bytes)",
            file=sys.stderr,
        )
        return EXIT_FAIL

    print(f"[exp2] OK: {args.output} is identical to {args.expect}")
    return EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
