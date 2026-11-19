"""Write acknowledgement and substitute-fallback checks for Swift Light.

A write must succeed only after the required acknowledgements, using
substitutes when intended replicas fail. The tests start isolated HTTP storage
servers on OS-assigned localhost ports; they do not stop or modify the
deployed cluster.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from functional_test_support import Client, ClientError, data_servers, lookup


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


if __name__ == "__main__":
    unittest.main()
