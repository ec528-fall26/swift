"""Read order and fallback checks for Swift Light.

Reads must try servers in placement order and return the first successful copy,
or report failure when no copy is available. The tests start isolated HTTP
storage servers on OS-assigned localhost ports; they do not stop or modify the
deployed cluster.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from functional_test_support import Client, ClientError, data_servers, lookup


class ReadFallbackTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-read-")
        self.addCleanup(self.temp.cleanup)
        self.output = Path(self.temp.name) / "download.bin"
        self.client = Client(user="test", timeout=1)

    def test_read_order_roles_and_failure_preserves_destination(self):
        cases = [
            ("primary", {}, "B", "primary", ["B"]),
            ("unreachable", {"B": "offline"}, "C", "secondary", ["B", "C"]),
            ("missing", {"B": 404}, "C", "secondary", ["B", "C"]),
            ("substitute", {"B": 404, "C": 503}, "A", "handoff", ["B", "C", "A"]),
            ("absent", {"B": 404, "C": 404, "A": 404}, None, None, ["B", "C", "A"]),
            ("offline", {"B": "offline", "C": "offline", "A": "offline"}, None, None, ["B", "C", "A"]),
        ]
        for name, failures, source, role, expected_attempts in cases:
            with self.subTest(case=name):
                self.output.write_bytes(b"existing download")
                attempted = []
                def send(client, method, server, key, data=None):
                    self.assertEqual(method, "GET")
                    attempted.append(server)
                    outcome = failures.get(server, 200)
                    if outcome == "offline":
                        raise ClientError("injected connection failure")
                    return outcome, b"correct\x00bytes" if outcome == 200 else b"error"
                with patch.object(Client, "lookup", return_value=lookup(["B", "C", "A"])), patch.object(Client, "_send", send):
                    report = self.client.pull("docs/object", str(self.output))
                self.assertEqual(attempted, expected_attempts)
                self.assertEqual(report["ok"], source is not None)
                self.assertEqual([f["server"] for f in report["tried"]], expected_attempts if source is None else expected_attempts[:-1])
                if source is None:
                    self.assertEqual(self.output.read_bytes(), b"existing download")
                else:
                    self.assertEqual((report["source"], report["role"]), (source, role))
                    self.assertEqual(self.output.read_bytes(), b"correct\x00bytes")

    def test_first_success_is_returned_without_version_comparison(self):
        payloads = {"B": b"older copy", "C": b"newer copy", "A": b"newer copy"}
        with patch.object(Client, "lookup", return_value=lookup(payloads)), \
             patch.object(Client, "_send", autospec=True, side_effect=lambda client, method, server, key: (200, payloads[server])) as send:
            report = self.client.pull("docs/object", str(self.output))
        self.assertEqual(report["source"], "B")
        self.assertEqual(self.output.read_bytes(), b"older copy")
        self.assertEqual(send.call_count, 1)

    def test_real_http_missing_primary_and_secondary_fall_back(self):
        with data_servers() as (servers, urls):
            key = self.client.object_key("docs/object")
            for index, role in [(1, "secondary"), (2, "handoff")]:
                with self.subTest(role=role):
                    servers[index].store.write(key, b"stored\xffbytes")
                    with patch.object(Client, "lookup", return_value=lookup(urls)):
                        report = self.client.pull("docs/object", str(self.output))
                    self.assertTrue(report["ok"], report)
                    self.assertEqual((report["source"], report["role"]), (urls[index], role))
                    self.assertEqual(self.output.read_bytes(), b"stored\xffbytes")
                    servers[index].store.delete(key)


if __name__ == "__main__":
    unittest.main()
