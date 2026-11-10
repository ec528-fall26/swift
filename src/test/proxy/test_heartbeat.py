"""Checks for the proxy's heartbeat monitor.

The monitor is what lets a proxy tell a live data server from a dead one without
ever blocking a request on it.  These tests cover the record it keeps: a live
server is healthy, an unreachable one is recorded rather than raised, a stale
check expires, and a server removed from the ring can be forgotten.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from functional_test_support import ConsistentHashingRing as Ring, DataServerConfig, data_node
from swift_light.data_server.heartbeat import ALIVE_MESSAGE, heartbeat_response
from swift_light.proxy_server import heartbeat as heartbeat_module
from swift_light.proxy_server.heartbeat import Heartbeat

UNREACHABLE = "no-such-node"


class _Clock:
    """A stand-in for the ``time`` module inside the monitor, so age is exact."""

    def __init__(self, now=1000.0):
        self.now = float(now)

    def monotonic(self):
        return self.now


class HeartbeatMonitorTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-heartbeat-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_an_unchecked_server_is_not_alive(self):
        monitor = Heartbeat()
        self.assertFalse(monitor.is_alive(UNREACHABLE))
        self.assertEqual(monitor.get_status(), {})

    def test_a_non_positive_fail_window_is_refused(self):
        for value in [0, -1]:
            with self.subTest(value=value), self.assertRaises(ValueError):
                Heartbeat(fail_after_seconds=value)

    def test_a_live_server_is_healthy(self):
        with data_node(self.root / "store") as (_server, url):
            monitor = Heartbeat(fail_after_seconds=30)
            self.assertEqual(monitor.log_heartbeat(url), ALIVE_MESSAGE)
            self.assertTrue(monitor.is_alive(url))
            status = monitor.get_status()
            self.assertTrue(status[url]["healthy"])
            self.assertEqual(status[url]["message"], ALIVE_MESSAGE)

    def test_an_unreachable_server_is_recorded_not_raised(self):
        monitor = Heartbeat(fail_after_seconds=30)
        message = monitor.log_heartbeat(UNREACHABLE)
        self.assertTrue(message)
        self.assertFalse(monitor.is_alive(UNREACHABLE))
        self.assertFalse(monitor.get_status()[UNREACHABLE]["healthy"])

    def test_status_expires_when_the_last_check_is_old(self):
        clock = _Clock()
        with data_node(self.root / "store") as (_server, url):
            monitor = Heartbeat(fail_after_seconds=10)
            with patch.object(heartbeat_module, "time", clock):
                self.assertEqual(monitor.log_heartbeat(url), ALIVE_MESSAGE)
                self.assertTrue(monitor.is_alive(url))

                clock.now += 10
                self.assertFalse(monitor.is_alive(url))
                self.assertFalse(monitor.get_status()[url]["healthy"])

                monitor.log_heartbeat(url)
                self.assertTrue(monitor.is_alive(url))

    def test_forget_drops_a_server(self):
        monitor = Heartbeat()
        monitor.log_heartbeat(UNREACHABLE)
        monitor.forget(UNREACHABLE)
        self.assertEqual(monitor.get_status(), {})
        self.assertFalse(monitor.is_alive(UNREACHABLE))

    def test_status_is_a_copy(self):
        monitor = Heartbeat()
        monitor.log_heartbeat(UNREACHABLE)
        status = monitor.get_status()
        status[UNREACHABLE]["healthy"] = True
        self.assertFalse(monitor.get_status()[UNREACHABLE]["healthy"])

    def test_from_ring_probes_every_member(self):
        with data_node(self.root / "store") as (_server, url):
            path = self.root / "ring.json"
            Ring.build(2, 2, nodes=[url, UNREACHABLE], vnode_count=4, path=path)

            monitor = Heartbeat.from_ring(path, fail_after_seconds=30)

            status = monitor.get_status()
            self.assertEqual(set(status), {url, UNREACHABLE})
            self.assertTrue(status[url]["healthy"])
            self.assertFalse(status[UNREACHABLE]["healthy"])

    def test_heartbeat_payload_names_the_configured_server(self):
        config = DataServerConfig("http://10.0.0.5:8080", "http://10.0.0.1:8000")
        self.assertEqual(
            heartbeat_response(config),
            {"server": "http://10.0.0.5:8080", "message": ALIVE_MESSAGE},
        )


if __name__ == "__main__":
    unittest.main()
