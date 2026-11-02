"""Shared fixtures for Swift Light functional experiments (standard library only).

These tests own their temporary files and optional loopback servers. They never
start deploy.py, use its fixed ports, or change a running cluster's data.
"""
from __future__ import annotations

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


def run(test_case):
    """Named checks, unittest summary, and a process failure on any failed check."""
    suite = unittest.defaultTestLoader.loadTestsFromTestCase(test_case)
    result = unittest.TextTestRunner(verbosity=2).run(suite)
    print("RESULT: PASS" if result.wasSuccessful() else "RESULT: FAIL", flush=True)
    return 0 if result.wasSuccessful() else 1
