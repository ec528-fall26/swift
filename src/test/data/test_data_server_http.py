"""HTTP route checks for one real data server.

Everything here talks HTTP to a server started on an OS-assigned loopback port,
so the routes, status codes and payloads are exactly what a client or proxy
sees: heartbeat, the file routes, key enumeration, the write lock a migration
takes, and the handoff routes a booting server uses.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import json
import tempfile
import time
import unittest
from contextlib import ExitStack
from pathlib import Path
from urllib.parse import urlencode

from functional_test_support import data_node, http_request, json_request
from swift_light.data_server.storage import HANDOFF_DELETE_MARKER

OWNER = "http://10.0.0.9:8080"
LOCK_TOKEN_HEADER = "X-Lock-Token"


def header(headers, name):
    """One response header, compared case-insensitively."""
    for key, value in headers.items():
        if key.lower() == name.lower():
            return value
    return None


class DataServerHTTPTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-http-node-")
        self.addCleanup(self.temp.cleanup)
        stack = ExitStack()
        self.addCleanup(stack.close)
        self.server, self.base = stack.enter_context(
            data_node(Path(self.temp.name) / "store")
        )

    # -- url helpers --------------------------------------------------------

    def files_url(self, key=None):
        """``/files`` with the key query, when one is given."""
        url = f"{self.base}/files"
        return url if key is None else f"{url}?{urlencode({'key': key})}"

    def handoff_url(self, *, owner=None, key=None):
        """``/handoff`` with whichever query parameters are given."""
        params = {
            name: value
            for name, value in (("owner", owner), ("key", key))
            if value is not None
        }
        return f"{self.base}/handoff?{urlencode(params)}" if params else f"{self.base}/handoff"

    # -- basics -------------------------------------------------------------

    def test_heartbeat_names_the_server(self):
        status, payload = json_request(f"{self.base}/heartbeat")
        self.assertEqual(status, 200)
        self.assertEqual(
            payload,
            {"server": self.server.config.own_address, "message": "alive"},
        )

    def test_files_route_round_trip(self):
        key = "test-docs-report.bin"
        payload = bytes(range(256))

        status, _, body = http_request(self.files_url(key), method="PUT", data=payload)
        self.assertEqual(status, 201)
        self.assertEqual(json.loads(body), {"key": key, "bytes": len(payload)})
        self.assertEqual(self.server.store.read(key), payload)

        status, _, body = http_request(self.files_url(key))
        self.assertEqual((status, body), (200, payload))

        status, _, _ = http_request(self.files_url(key), method="DELETE")
        self.assertEqual(status, 204)
        self.assertFalse(self.server.store.exists(key))
        self.assertEqual(http_request(self.files_url(key))[0], 404)

    def test_bad_file_requests_are_rejected(self):
        # No key at all, and a key the store cannot turn into a path.
        self.assertEqual(json_request(self.files_url())[0], 400)
        status, payload = json_request(self.files_url(".."))
        self.assertEqual(status, 400)
        self.assertIn("error", payload)
        # A well-formed key this server simply does not hold.
        self.assertEqual(json_request(self.files_url("test-missing.txt"))[0], 404)

    def test_unknown_routes_are_404(self):
        self.assertEqual(http_request(f"{self.base}/no-such-route")[0], 404)
        self.assertEqual(
            http_request(f"{self.base}/heartbeat", method="PUT", data=b"")[0], 404
        )

    def test_keys_route_lists_local_objects_only(self):
        self.server.store.write("test-b.txt", b"b")
        self.server.store.write("test-a-c.txt", b"c")
        self.server.store.write_handoff("test-hidden.txt", OWNER, b"held")

        status, payload = json_request(f"{self.base}/keys")
        self.assertEqual(status, 200)
        self.assertEqual(payload["keys"], ["test-a-c.txt", "test-b.txt"])

    # -- handoff routes -----------------------------------------------------

    def test_handoff_routes_expose_and_clear_borrowed_objects(self):
        self.server.store.write_handoff("test-a.txt", OWNER, b"borrowed")
        self.server.store.delete_handoff("test-b.txt", OWNER)

        status, payload = json_request(self.handoff_url(owner=OWNER))
        self.assertEqual(status, 200)
        self.assertEqual(payload, {"owner": OWNER, "keys": ["test-a.txt", "test-b.txt"]})

        status, headers, body = http_request(self.handoff_url(owner=OWNER, key="test-a.txt"))
        self.assertEqual((status, body), (200, b"borrowed"))
        self.assertEqual(header(headers, "X-Handoff-Delete"), "false")

        status, headers, body = http_request(self.handoff_url(owner=OWNER, key="test-b.txt"))
        self.assertEqual((status, body), (200, HANDOFF_DELETE_MARKER))
        self.assertEqual(header(headers, "X-Handoff-Delete"), "true")

        self.assertEqual(json_request(f"{self.base}/handoff")[0], 400)
        self.assertEqual(
            json_request(self.handoff_url(key="test-a.txt"))[0], 400
        )
        self.assertEqual(
            http_request(self.handoff_url(owner=OWNER, key="test-missing.txt"))[0], 404
        )

        self.assertEqual(
            http_request(self.handoff_url(owner=OWNER, key="test-a.txt"), method="DELETE")[0],
            204,
        )
        self.assertEqual(
            http_request(self.handoff_url(owner=OWNER, key="test-a.txt"))[0], 404
        )
        self.assertEqual(http_request(f"{self.base}/handoff", method="DELETE")[0], 400)

    # -- write lock ---------------------------------------------------------

    def test_write_lock_blocks_other_writers(self):
        token = {LOCK_TOKEN_HEADER: "migration-1"}
        self.assertEqual(json_request(f"{self.base}/lock", method="POST")[0], 400)

        status, payload = json_request(f"{self.base}/lock", method="POST", headers=token)
        self.assertEqual(status, 200)
        self.assertTrue(payload["locked"])
        self.assertEqual(self.server.lock_holder(), "migration-1")

        # Another writer is refused while the migration holds the lease.
        self.assertEqual(
            http_request(self.files_url("test-locked.txt"), method="PUT", data=b"x")[0],
            503,
        )
        self.assertEqual(
            http_request(self.files_url("test-locked.txt"), method="DELETE")[0], 503
        )
        # The migration's own traffic, carrying the token, is allowed.
        self.assertEqual(
            http_request(
                self.files_url("test-locked.txt"), method="PUT", data=b"x", headers=token
            )[0],
            201,
        )

        self.assertEqual(
            http_request(
                f"{self.base}/lock", method="DELETE", headers={LOCK_TOKEN_HEADER: "other"}
            )[0],
            409,
        )
        self.assertEqual(http_request(f"{self.base}/lock", method="DELETE", headers=token)[0], 204)
        self.assertIsNone(self.server.lock_holder())
        self.assertEqual(
            http_request(self.files_url("test-locked.txt"), method="PUT", data=b"y")[0],
            201,
        )

    def test_lease_expires_without_a_release(self):
        # A migration that dies mid-copy never sends DELETE /lock, so the node
        # has to free itself once the lease runs out.
        token = {LOCK_TOKEN_HEADER: "migration-lease"}
        status, payload = json_request(
            f"{self.base}/lock?seconds=1.0", method="POST", headers=token
        )
        self.assertEqual(status, 200)
        self.assertEqual(payload["seconds"], 1.0)
        self.assertEqual(self.server.lock_holder(), "migration-lease")
        self.assertEqual(
            http_request(self.files_url("test-lease.txt"), method="PUT", data=b"x")[0],
            503,
        )

        deadline = time.monotonic() + 10.0
        while self.server.lock_holder() is not None:
            if time.monotonic() > deadline:
                self.fail("the write lock outlived its 1.0 s lease")
            time.sleep(0.01)

        # Expiry, not release: an untokened write lands again, and a different
        # migration can take the lock without waiting for the old holder.
        self.assertEqual(
            http_request(self.files_url("test-lease.txt"), method="PUT", data=b"y")[0],
            201,
        )
        status, _ = json_request(
            f"{self.base}/lock", method="POST", headers={LOCK_TOKEN_HEADER: "migration-next"}
        )
        self.assertEqual(status, 200)
        self.assertEqual(self.server.lock_holder(), "migration-next")


if __name__ == "__main__":
    unittest.main()
