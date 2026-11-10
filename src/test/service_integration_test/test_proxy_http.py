"""Checks for the proxy's two HTTP ports.

The user port answers clients (``/servers``, ``/nodes``) and the data port
answers data-server health probes.  Both are exercised against a real proxy on
an OS-assigned port, with a ring whose members are the running data servers, so
the answers can be compared with the ring the proxy loaded.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import unittest
from contextlib import ExitStack
from urllib.parse import urlencode

from functional_test_support import ConsistentHashingRing as Ring, cluster, http_request, json_request


class ProxyHTTPTests(unittest.TestCase):
    # The tests only read from the cluster, so one cluster serves the whole
    # class: starting three data servers and a proxy per test would dominate
    # the runtime.
    @classmethod
    def setUpClass(cls):
        stack = ExitStack()
        cls.addClassCleanup(stack.close)
        cls.servers, cls.urls, cls.proxy, cls.ring_path = stack.enter_context(cluster())
        cls.ring = Ring.load(cls.ring_path)
        cls.user_base = f"http://127.0.0.1:{cls.proxy.user_port}"
        cls.data_base = f"http://127.0.0.1:{cls.proxy.data_port}"

    def servers_url(self, key):
        return f"{self.user_base}/servers?{urlencode({'key': key})}"

    # -- user port ----------------------------------------------------------

    def test_servers_answers_with_the_rings_fan(self):
        for key in ["alice-docs-one", "empty", "\u6587\u4ef6"]:
            with self.subTest(key=key):
                status, payload = json_request(self.servers_url(key))
                self.assertEqual(status, 200)
                self.assertEqual(payload["key"], key)
                self.assertEqual(payload["slot"], self.ring.slot_for_key(key))
                self.assertEqual(payload["replicate_count"], self.ring.replicate_count)
                self.assertEqual(payload["servers"], list(self.ring.fan(key)))

    def test_servers_requires_a_key(self):
        status, payload = json_request(f"{self.user_base}/servers")
        self.assertEqual(status, 400)
        self.assertIn("key", payload["error"])

    def test_nodes_lists_the_ring_membership(self):
        status, payload = json_request(f"{self.user_base}/nodes")
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"nodes": self.urls})

    def test_an_unknown_user_route_is_404(self):
        self.assertEqual(http_request(f"{self.user_base}/objects")[0], 404)

    # -- data port ----------------------------------------------------------

    def test_heartbeat_reports_every_ring_member(self):
        status, payload = json_request(f"{self.data_base}/heartbeat")
        self.assertEqual(status, 200)
        self.assertEqual(set(payload["servers"]), set(self.urls))
        for url, record in payload["servers"].items():
            with self.subTest(server=url):
                self.assertTrue(record["healthy"], record)
                self.assertEqual(record["message"], "alive")

    def test_reprobing_reports_the_same_members(self):
        status, payload = json_request(f"{self.data_base}/heartbeat", method="POST", data=b"")
        self.assertEqual(status, 200)
        self.assertEqual(set(payload["servers"]), set(self.urls))

    def test_data_port_nodes_lists_the_ring_membership(self):
        status, payload = json_request(f"{self.data_base}/nodes")
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"nodes": self.urls})

    def test_an_unknown_data_route_is_404(self):
        self.assertEqual(http_request(f"{self.data_base}/objects")[0], 404)


if __name__ == "__main__":
    unittest.main()
