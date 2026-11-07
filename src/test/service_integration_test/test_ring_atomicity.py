"""Ring replacement seen through a running proxy.

The ring's atomic save exists so a reader never observes a half-written ring.
These tests check that promise at the service level: a torn ``ring.json`` --
what a crash or a non-atomic writer caught mid-write leaves -- never reaches
routing, and a complete replacement ring is picked up whole.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import json
import unittest
from contextlib import ExitStack
from urllib.parse import urlencode

from functional_test_support import ConsistentHashingRing as Ring, cluster, json_request


class RingReplacementTests(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.servers, self.urls, self.proxy, self.ring_path = stack.enter_context(cluster())
        self.ring = Ring.load(self.ring_path)
        self.user_base = f"http://127.0.0.1:{self.proxy.user_port}"

    def lookup(self, key):
        """The proxy's placement answer for ``key``, asserted to be a 200."""
        status, payload = json_request(
            f"{self.user_base}/servers?{urlencode({'key': key})}"
        )
        self.assertEqual(status, 200)
        return payload

    def test_a_torn_ring_file_never_reaches_routing(self):
        key = "alice-docs-one"
        before = self.lookup(key)
        self.assertEqual(before["servers"], list(self.ring.fan(key)))

        # Half a ring file: what a crash, or a non-atomic write caught in the
        # middle, leaves where a good ring was.
        original = self.ring_path.read_bytes()
        self.ring_path.write_bytes(original[: len(original) // 2])
        with self.assertRaises(ValueError):
            json.loads(self.ring_path.read_text(encoding="utf-8"))

        # The file changed, so the proxy notices it and tries to reload; it
        # must keep serving the ring it already holds rather than a torn one.
        with self.assertLogs(
            "swift_light.proxy_server.proxy_HTTP_server", level="WARNING"
        ) as logs:
            self.assertEqual(self.lookup(key), before)
        self.assertTrue(
            any("unusable ring" in line for line in logs.output), logs.output
        )

    def test_a_complete_replacement_ring_is_picked_up_whole(self):
        key = "alice-docs-one"
        before = self.lookup(key)

        replacement = Ring.build(
            len(self.urls), 1, nodes=self.urls, vnode_count=4,
            path=self.ring_path.with_name("replacement.json"),
        )
        replacement.save(self.ring_path)

        after = self.lookup(key)
        self.assertEqual(after["replicate_count"], 1)
        self.assertNotEqual(after["replicate_count"], before["replicate_count"])
        self.assertEqual(after["servers"], list(replacement.fan(key)))
        self.assertEqual(after["slot"], replacement.slot_for_key(key))


if __name__ == "__main__":
    unittest.main()
