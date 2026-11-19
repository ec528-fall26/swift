"""End-to-end client checks against a real proxy and real data servers.

These tests use the same pieces production uses -- the client, the proxy's
user port, the data servers' HTTP routes and file stores -- on ephemeral ports
and with temporary state.  They assert the basic object lifecycle: a push
reaches a quorum of replicas, a pull returns the stored bytes, and a delete
removes the copies.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import tempfile
import unittest
from pathlib import Path

from functional_test_support import Client, ClientError, cluster


class ClientClusterTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-client-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def client_for(self, proxy):
        return Client(proxy=f"http://127.0.0.1:{proxy.user_port}", user="test", timeout=3.0)

    def test_push_pull_delete_round_trip(self):
        payload = b"swift-light\x00end-to-end\xff" * 32
        source = self.root / "input.bin"
        source.write_bytes(payload)
        download = self.root / "download.bin"
        with cluster() as (servers, urls, proxy, _ring_path):
            client = self.client_for(proxy)
            by_url = dict(zip(urls, servers))

            pushed = client.push(str(source), "docs/report.bin")
            self.assertTrue(pushed["ok"], pushed)
            self.assertEqual(pushed["key"], "test-docs-report.bin")
            self.assertEqual(pushed["quorum"], 2)
            self.assertEqual(pushed["failed"], [])
            self.assertEqual(len(pushed["acknowledged"]), 2)
            for url in pushed["acknowledged"]:
                self.assertEqual(by_url[url].store.read(pushed["key"]), payload)

            pulled = client.pull("docs/report.bin", str(download))
            self.assertTrue(pulled["ok"], pulled)
            self.assertEqual(pulled["role"], "primary")
            self.assertEqual(pulled["source"], pushed["acknowledged"][0])
            self.assertEqual(pulled["bytes"], len(payload))
            self.assertEqual(pulled["tried"], [])
            self.assertEqual(download.read_bytes(), payload)

            deleted = client.delete("docs/report.bin")
            self.assertTrue(deleted["ok"], deleted)
            self.assertEqual(deleted["failed"], [])
            for url in pushed["acknowledged"]:
                self.assertFalse(by_url[url].store.exists(pushed["key"]))
            self.assertFalse(client.pull("docs/report.bin", str(download))["ok"])

    def test_push_overwrites_the_previous_contents(self):
        first = self.root / "first.bin"
        first.write_bytes(b"first version")
        second = self.root / "second.bin"
        second.write_bytes(b"second version")
        download = self.root / "download.bin"
        with cluster() as (servers, urls, proxy, _ring_path):
            client = self.client_for(proxy)
            by_url = dict(zip(urls, servers))
            self.assertTrue(client.push(str(first), "docs/notes.txt")["ok"])
            pushed = client.push(str(second), "docs/notes.txt")
            self.assertTrue(pushed["ok"], pushed)
            for url in pushed["acknowledged"]:
                self.assertEqual(by_url[url].store.read(pushed["key"]), b"second version")
            self.assertTrue(client.pull("docs/notes.txt", str(download))["ok"])
            self.assertEqual(download.read_bytes(), b"second version")

    def test_delete_of_an_absent_object_still_reaches_the_quorum(self):
        with cluster() as (_servers, _urls, proxy, _ring_path):
            client = self.client_for(proxy)
            report = client.delete("docs/never-written.bin")
            self.assertTrue(report["ok"], report)
            self.assertEqual(report["failed"], [])
            self.assertEqual(len(report["acknowledged"]), 2)

    def test_missing_local_file_fails_before_any_request(self):
        with cluster() as (_servers, _urls, proxy, _ring_path):
            client = self.client_for(proxy)
            with self.assertRaises(ClientError):
                client.push(str(self.root / "does-not-exist.bin"), "docs/report.bin")


if __name__ == "__main__":
    unittest.main()
