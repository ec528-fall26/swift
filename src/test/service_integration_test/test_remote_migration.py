"""Migration over HTTP against real data servers.

``RemoteDataServer`` is the ring's ``DataServer`` protocol implemented over the
data server's routes.  These tests use real HTTP servers on OS-assigned
localhost ports, a real ring, and the real write-lock lease, so they cover the
copy, the handoff, the locking and the end-to-end rebalance rather than a mock.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import socket
import tempfile
import unittest
from contextlib import ExitStack
from pathlib import Path
from urllib.parse import quote

from functional_test_support import (
    ConsistentHashingRing as Ring,
    data_servers,
    http_request,
)
from swift_light.data_server.data_HTTP_server import LOCK_TOKEN_HEADER
from swift_light.data_server.remote import MigrationError, RemoteDataServer


def files_url(node, key):
    """``node``'s object route for ``key``."""
    return f"{node}/files?key={quote(key, safe='')}"


class RemoteDataServerTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-remote-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.servers, self.urls = stack.enter_context(data_servers())
        self.ring = Ring.build(
            3, 1, nodes=self.urls, vnode_count=8, path=self.root / "ring.json"
        )

    def server(self, url):
        return self.servers[self.urls.index(url)]

    def test_list_files_filters_by_slot_and_skips_handoff_entries(self):
        keys = ["alice-docs-one", "bob-docs-two", "carol-docs-three", "dave-docs-four"]
        for key in keys:
            self.server(self.urls[0]).store.write(key, b"stored")
        self.server(self.urls[0]).store.write_handoff(
            "erin-docs-five", self.urls[1], b"held"
        )

        data = RemoteDataServer(self.ring)
        everything = data.list_files(self.urls[0], 0, self.ring.ring_size)
        self.assertEqual(sorted(everything), sorted(keys))

        slot = self.ring.slot_for_key(keys[0])
        expected = [key for key in keys if slot <= self.ring.slot_for_key(key) < slot + 1]
        self.assertEqual(
            sorted(data.list_files(self.urls[0], slot, slot + 1)), sorted(expected)
        )

    def test_write_requires_a_source_and_copies_the_stored_bytes(self):
        key = "alice-docs-one"
        payload = b"payload\x00bytes\xff"
        data = RemoteDataServer(self.ring)

        with self.assertRaises(MigrationError):
            data.write(key, self.urls[1])

        self.server(self.urls[0]).store.write(key, payload)
        data.list_files(self.urls[0], 0, self.ring.ring_size)
        data.write(key, self.urls[1])
        self.assertEqual(self.server(self.urls[1]).store.read(key), payload)

        with self.assertRaises(MigrationError):
            data.write("ghost-docs-missing", self.urls[1])

    def test_write_handoff_drops_only_keys_this_arc_copied(self):
        key = "alice-docs-one"
        payload = b"payload"
        source, target = self.urls[0], self.urls[1]
        self.server(source).store.write(key, payload)
        data = RemoteDataServer(self.ring)
        data.list_files(source, 0, self.ring.ring_size)

        data.write_handoff(key, source)
        self.assertEqual(self.server(source).store.read(key), payload)

        data.write(key, target)
        data.write_handoff(key, source)
        with self.assertRaises(FileNotFoundError):
            self.server(source).store.read(key)

        # A 404 is tolerated once the copy was made: the key may already be gone.
        self.server(source).store.write(key, payload)
        data.list_files(source, 0, self.ring.ring_size)
        data.write(key, target)
        self.server(source).store.delete(key)
        data.write_handoff(key, source)

    def test_lock_blocks_foreign_writes_but_lets_the_holder_through(self):
        key = "alice-docs-one"
        payload = b"payload"
        source, target = self.urls[0], self.urls[1]
        self.server(source).store.write(key, payload)
        data = RemoteDataServer(self.ring)
        data.list_files(source, 0, self.ring.ring_size)

        with data.lock(source, target):
            status, _, _ = http_request(
                files_url(target, key), method="PUT", data=b"foreign"
            )
            self.assertEqual(status, 503)
            data.write(key, target)
            self.assertEqual(self.server(target).store.read(key), payload)
            self.assertEqual(http_request(files_url(target, key))[0], 200)
            self.assertEqual(http_request(target + "/keys")[0], 200)
            self.assertIsNotNone(self.server(source).lock_holder())
            self.assertIsNotNone(self.server(target).lock_holder())

        self.assertIsNone(self.server(source).lock_holder())
        self.assertIsNone(self.server(target).lock_holder())
        status, _, _ = http_request(
            files_url(target, key), method="PUT", data=b"after"
        )
        self.assertEqual(status, 201)
        self.assertEqual(self.server(target).store.read(key), b"after")

    def test_a_refused_lock_releases_the_nodes_already_acquired(self):
        first, second = sorted(self.urls[:2])
        token = "another-migration"
        status, _, _ = http_request(
            second + "/lock", method="POST", headers={LOCK_TOKEN_HEADER: token}
        )
        self.assertEqual(status, 200)
        try:
            data = RemoteDataServer(self.ring)
            with self.assertRaises(MigrationError):
                with data.lock(first, second):
                    self.fail("the lock should have been refused")
            self.assertIsNone(self.server(first).lock_holder())
        finally:
            http_request(
                second + "/lock", method="DELETE", headers={LOCK_TOKEN_HEADER: token}
            )

    def test_an_unreachable_server_is_a_migration_error(self):
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        data = RemoteDataServer(self.ring, timeout=1.0)
        with self.assertRaises(MigrationError):
            data.list_files(f"http://127.0.0.1:{port}", 0, self.ring.ring_size)

    def test_rebalance_moves_real_objects_onto_a_joined_server(self):
        ring_path = self.root / "migration" / "ring.json"
        ring_path.parent.mkdir()
        old = Ring.build(2, 1, nodes=self.urls[:2], vnode_count=8, path=ring_path)
        payloads = {
            f"user{index}-docs-file{index}.txt": f"payload {index}".encode()
            for index in range(16)
        }
        for key, payload in payloads.items():
            self.server(old.fan(key)[0]).store.write(key, payload)

        data = RemoteDataServer(old)
        new = Ring.add_node(self.urls[2], data=data, path=ring_path)

        self.assertEqual(set(new.nodes), set(self.urls))
        changed = [key for key in payloads if old.fan(key)[0] != new.fan(key)[0]]
        self.assertTrue(changed, "the joined server took no new primary slots")
        for key in changed:
            self.assertEqual(self.server(new.fan(key)[0]).store.read(key), payloads[key])
            with self.assertRaises(FileNotFoundError):
                self.server(old.fan(key)[0]).store.read(key)


if __name__ == "__main__":
    unittest.main()
