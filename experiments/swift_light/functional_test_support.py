"""Shared fixtures for Swift Light functional experiments (standard library only).

These tests own their temporary files and optional loopback servers. They never
start deploy.py, use its fixed ports, or change a running cluster's data.
"""
from __future__ import annotations

import argparse
import socket
import sys
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from swift_light.client.client import Client, ClientError
from swift_light.data_server.storage import DataServerConfig, DataStore
from swift_light.data_server.data_HTTP_server import DataHTTPServer
from swift_light.proxy_server.consistent_hashing_ring import ConsistentHashingRing


def lookup(servers):
    return {"servers": tuple(servers), "replicate_count": 2, "slot": 0}


@contextmanager
def data_servers(count=3):
    """Real HTTP handlers and stores, with ephemeral ports and no boot handoffs."""
    with tempfile.TemporaryDirectory(prefix="swift-light-http-") as directory:
        servers = []
        try:
            for index in range(count):
                config = DataServerConfig("http://127.0.0.1:1", "http://127.0.0.1:1")
                server = DataHTTPServer(
                    config, store_root=Path(directory) / str(index), port=0,
                )
                servers.append(server)
                # Handoff discovery is unrelated to these tests. Do not contact
                # any external proxy when starting an isolated storage server.
                with patch.object(DataHTTPServer, "request_handoff_reconcile", return_value=False):
                    server.start()
            yield servers, [f"http://127.0.0.1:{s.port}" for s in servers]
        finally:
            for server in reversed(servers):
                server.close()


def run(test_case, *, number, description, argv=None, needs_http=False):
    """Shared CLI and 0/1/77 result convention, matching experiments 1 and 2."""
    parser = argparse.ArgumentParser(prog=f"exp{number}", description=description)
    parser.add_argument(
        "--temp-dir", metavar="PATH",
        help="existing writable directory for temporary test state (default: OS temporary directory)",
    )
    args = parser.parse_args(argv)
    prefix = f"[exp{number}]"
    print(f"{prefix} {description}", flush=True)
    print(f"{prefix} source    {SRC_ROOT}", flush=True)
    old_tempdir = tempfile.tempdir
    try:
        try:
            selected = Path(args.temp_dir).resolve() if args.temp_dir else Path(tempfile.gettempdir())
            if not selected.is_dir():
                raise OSError(f"temporary directory does not exist: {selected}")
            # Check before running tests so an unavailable environment is not
            # confused with a failed correctness assertion. mkdtemp alone does
            # not establish that files inside the directory are writable.
            with tempfile.TemporaryDirectory(prefix=f"swift-light-exp{number}-check-", dir=selected) as probe:
                check = Path(probe) / "check"
                check.write_bytes(b"check")
                if check.read_bytes() != b"check":
                    raise OSError("temporary file read-back failed")
            if needs_http:
                with socket.socket() as listener:
                    listener.bind(("127.0.0.1", 0))
                    listener.listen(1)
                    with socket.create_connection(listener.getsockname(), timeout=2):
                        connection, _ = listener.accept()
                        connection.close()
            tempfile.tempdir = str(selected)
        except OSError as exc:
            print(f"{prefix} cannot attempt the experiment: {exc}", file=sys.stderr)
            print(f"{prefix} use --temp-dir with a writable directory"
                  + (" and allow localhost connections" if needs_http else ""), file=sys.stderr)
            return 77

        print(f"{prefix} temporary {selected}", flush=True)
        print(f"{prefix} no deployed cluster or input file required", flush=True)
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_case)
        result = unittest.TextTestRunner(verbosity=2).run(suite)
        ok = result.wasSuccessful() and result.testsRun > 0 and not result.skipped
        print(f"{prefix} {'OK' if ok else 'FAIL'}: {result.testsRun} named checks; "
              f"{len(result.failures)} failures, {len(result.errors)} errors, "
              f"{len(result.skipped)} skipped", flush=True)
        print("RESULT: PASS" if ok else "RESULT: FAIL", flush=True)
        return 0 if ok else 1
    finally:
        tempfile.tempdir = old_tempdir
