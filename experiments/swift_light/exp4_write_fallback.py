#!/usr/bin/env python3
"""exp4 -- write acknowledgements and substitute fallback.

What this checks
----------------
Correctness checks for Swift Light, not benchmark measurements. The named tests
below describe each scenario. We use temporary state and the current checkout's
implementation; no deployed cluster or Hello.txt file is required.
This experiment starts isolated HTTP storage servers on OS-assigned localhost
ports. It does not stop or modify the deployed cluster.

Prerequisites
-------------
Python 3.12, the repository source, and writable temporary storage. Keep
functional_test_support.py beside this script. No third-party packages needed.
Use --temp-dir PATH to select an existing writable temporary directory.


Usage
-----
Run from the repository root::

    python3 experiments/swift_light/exp4_write_fallback.py

Exit codes
----------
    0   every named check ran and passed
    1   a check failed, errored, or was skipped
    77  prerequisite checks failed before the tests could run
"""
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from functional_test_support import Client, ClientError, data_servers, lookup, run


class WriteFallbackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-write-")
        self.addCleanup(self.temp.cleanup)
        self.file = Path(self.temp.name) / "input.bin"
        self.payload = b"replicated\x00bytes\xff"
        self.file.write_bytes(self.payload)
        self.client = Client(user="test", timeout=1)

    def test_acknowledgement_threshold_and_substitutes(self):
        cases = [
            ("healthy", {}, ["B", "C"], True),
            ("connection_failure", {"B": "offline"}, ["C", "A"], True),
            ("http_failure", {"B": 503}, ["C", "A"], True),
            ("insufficient", {"B": "offline", "A": 500}, ["C"], False),
            ("all_failed", {"B": 500, "C": 503, "A": "offline"}, [], False),
        ]
        for name, failures, expected, success in cases:
            with self.subTest(case=name):
                received, attempted = {}, []
                lock = threading.Lock()

                def send(client, method, server, key, data=None):
                    with lock:
                        attempted.append(server)
                    self.assertEqual(method, "PUT")
                    self.assertEqual(data, self.payload)
                    outcome = failures.get(server, 201)
                    if outcome == "offline":
                        raise ClientError("injected connection failure")
                    if outcome == 201:
                        received[server] = data
                    return outcome, b""

                with patch.object(Client, "lookup", return_value=lookup(["B", "C", "A"])), \
                     patch.object(Client, "_send", send):
                    report = self.client.push(str(self.file), "docs/object")
                self.assertEqual(report["ok"], success)
                self.assertEqual(report["acknowledged"], expected)
                self.assertEqual(report["quorum"], 2)
                self.assertEqual(set(received), set(expected))
                self.assertTrue(all(value == self.payload for value in received.values()))
                self.assertEqual({f["server"] for f in report["failed"]}, set(failures))
                self.assertEqual(sorted(attempted), sorted(["B", "C"] if not failures else ["B", "C", "A"]))

    def test_intended_replica_requests_overlap(self):
        # A rendezvous, not a speed threshold: sequential sends cannot both enter.
        barrier = threading.Barrier(2)
        def send(client, method, server, key, data=None):
            barrier.wait(timeout=5)
            return 201, b""
        with patch.object(Client, "lookup", return_value=lookup(["B", "C", "A"])), \
             patch.object(Client, "_send", send):
            self.assertTrue(self.client.push(str(self.file), "docs/object")["ok"])

    def test_real_http_writes_store_bytes_on_replica_and_substitute(self):
        with data_servers() as (servers, urls):
            original_send = Client._send
            def send(client, method, server, key, data=None):
                if server == urls[0]:
                    raise ClientError("injected unreachable primary")
                return original_send(client, method, server, key, data=data)
            with patch.object(Client, "lookup", return_value=lookup(urls)), patch.object(Client, "_send", send):
                report = self.client.push(str(self.file), "docs/object")
            self.assertTrue(report["ok"], report)
            self.assertEqual(report["acknowledged"], urls[1:])
            for server in servers[1:]:
                self.assertEqual(server.store.read(report["key"]), self.payload)
            with self.assertRaises(FileNotFoundError):
                servers[0].store.read(report["key"])


def main(argv: list[str] | None = None) -> int:
    """Run the functional checks and return the experiment exit code."""
    return run(WriteFallbackTests, number=4,
               description="write acknowledgements and substitute fallback", argv=argv, needs_http=True)


if __name__ == "__main__":
    raise SystemExit(main())
