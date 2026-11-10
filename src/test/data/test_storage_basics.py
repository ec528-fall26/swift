"""Basic checks for the data server's configuration record and file store.

Two pieces are covered here, both used by every data server:

* :class:`DataServerConfig` -- the server's own address and its proxy, kept as
  an immutable record and persisted atomically.
* :class:`DataStore` -- the partition-key-to-path mapping, reads and writes in
  both write modes, deletes that prune the branch they empty, key enumeration,
  and the handoff tree a substitute keeps for an unavailable owner.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import tempfile
import unittest
from pathlib import Path

from functional_test_support import DataServerConfig, DataStore
from swift_light.data_server.storage import HANDOFF_DELETE_MARKER, InvalidKeyError

OWNER = "http://10.0.0.9:8080"


class DataServerConfigTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-config-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def test_config_round_trips_through_disk(self):
        config = DataServerConfig("http://10.0.0.5:8080", "http://10.0.0.1:8000")
        path = config.save(self.root / "nested" / "data_server.json")
        self.assertTrue(path.is_file())
        self.assertEqual(DataServerConfig.load(path), config)
        self.assertEqual(DataServerConfig.from_dict(config.to_dict()), config)
        self.assertEqual(config.to_dict()["schema_version"], 1)
        self.assertEqual(config.own_address, "http://10.0.0.5:8080")
        self.assertEqual(config.proxy_address, "http://10.0.0.1:8000")

    def test_saving_a_reconfiguration_replaces_the_record(self):
        path = self.root / "data_server.json"
        original = DataServerConfig("http://10.0.0.5:8080", "http://10.0.0.1:8000")
        original.save(path)

        moved = DataServerConfig("http://10.0.0.7:8080", original.proxy_address)
        moved.save(path)

        self.assertEqual(DataServerConfig.load(path), moved)
        self.assertNotEqual(DataServerConfig.load(path), original)
        # The atomic write leaves no temporary file behind.
        self.assertEqual([entry.name for entry in self.root.iterdir()], ["data_server.json"])

    def test_invalid_addresses_are_refused(self):
        cases = [
            ("scheme-less own address", "10.0.0.5:8080", "http://10.0.0.1:8000", ValueError),
            ("blank proxy address", "http://10.0.0.5:8080", "   ", ValueError),
            ("non-text own address", 8080, "http://10.0.0.1:8000", TypeError),
        ]
        for label, own, proxy, error in cases:
            with self.subTest(case=label), self.assertRaises(error):
                DataServerConfig(own, proxy)

    def test_from_dict_rejects_unusable_records(self):
        cases = [
            (
                "missing own_address",
                {"schema_version": 1, "proxy_address": "http://10.0.0.1:8000"},
                ValueError,
            ),
            (
                "future schema",
                {
                    "schema_version": 99,
                    "own_address": "http://10.0.0.5:8080",
                    "proxy_address": "http://10.0.0.1:8000",
                },
                ValueError,
            ),
            ("not a mapping", ["http://10.0.0.5:8080"], TypeError),
        ]
        for label, data, error in cases:
            with self.subTest(case=label), self.assertRaises(error):
                DataServerConfig.from_dict(data)

    def test_load_of_a_missing_record_raises(self):
        with self.assertRaises(FileNotFoundError):
            DataServerConfig.load(self.root / "nothing.json")


class DataStoreTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-store-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name) / "store"
        self.store = DataStore(self.root)

    def test_write_read_delete_round_trip(self):
        key = "jdoe-docs-notes.txt"
        path = self.store.write(key, b"hello\x00world")
        self.assertEqual(path, self.root / "jdoe" / "docs" / "notes.txt")
        self.assertTrue(self.store.exists(key))
        self.assertEqual(self.store.read(key), b"hello\x00world")

        self.store.write(key, b"replacement")
        self.assertEqual(self.store.read(key), b"replacement")

        self.store.delete(key)
        self.assertFalse(self.store.exists(key))
        with self.assertRaises(FileNotFoundError):
            self.store.read(key)
        # The directory branch that only this file kept alive is pruned.
        self.assertEqual(list(self.root.iterdir()), [])

    def test_deleting_a_missing_key_raises(self):
        with self.assertRaises(FileNotFoundError):
            self.store.delete("jdoe-missing.txt")

    def test_an_escaped_dash_stays_inside_one_path_segment(self):
        # "notes\-v2.txt" is one name (a file called notes-v2.txt) ...
        dashed = "jdoe-home-notes\\-v2.txt"
        self.assertEqual(
            self.store.path_for(dashed).relative_to(self.root).parts,
            ("jdoe", "home", "notes-v2.txt"),
        )
        # ... while "notes-v2.txt" is still separator, dash, separator.
        nested = "jdoe-home-notes-v2.txt"
        self.assertEqual(
            self.store.path_for(nested).relative_to(self.root).parts,
            ("jdoe", "home", "notes", "v2.txt"),
        )

        # Two objects, two files: neither overwrites the other.
        self.store.write(dashed, b"dashed")
        self.store.write(nested, b"nested")
        self.assertEqual(self.store.read(dashed), b"dashed")
        self.assertEqual(self.store.read(nested), b"nested")
        self.assertEqual(self.store.list_keys(), [nested, dashed])

    def test_unusable_keys_are_refused(self):
        for key in ["", ".", "..", "a/b", "a\\b", "a\\", "a:b", "a-", "-a", "a-\x00b"]:
            with self.subTest(key=key), self.assertRaises(InvalidKeyError):
                self.store.path_for(key)
        with self.assertRaises(TypeError):
            self.store.path_for(123)

    def test_keys_are_listed_sorted_and_skip_handoffs_and_temp_files(self):
        self.store.write("jdoe-b.txt", b"b")
        self.store.write("jdoe-a-c.txt", b"c")
        self.store.write_handoff("jdoe-hidden.txt", OWNER, b"held")
        leftover = self.store.path_for("jdoe-a-c.txt").with_name("leftover.txt.tmp")
        leftover.write_bytes(b"partial write")

        self.assertEqual(self.store.list_keys(), ["jdoe-a-c.txt", "jdoe-b.txt"])

    def test_handoff_lifecycle(self):
        self.store.write_handoff("jdoe-borrowed.txt", OWNER, b"held for owner")
        self.assertEqual(self.store.read_handoff("jdoe-borrowed.txt", OWNER), b"held for owner")
        self.assertEqual(self.store.list_handoff_owners(), [OWNER])
        self.assertEqual(self.store.list_handoffs(OWNER), ["jdoe-borrowed.txt"])
        self.assertFalse(self.store.is_delete_handoff("jdoe-borrowed.txt", OWNER))

        self.store.delete_handoff("jdoe-gone.txt", OWNER)
        self.assertTrue(self.store.is_delete_handoff("jdoe-gone.txt", OWNER))
        self.assertEqual(self.store.read_handoff("jdoe-gone.txt", OWNER), HANDOFF_DELETE_MARKER)
        self.assertEqual(self.store.list_handoffs(OWNER), ["jdoe-borrowed.txt", "jdoe-gone.txt"])

        self.store.clear_handoff("jdoe-borrowed.txt", OWNER)
        self.store.clear_handoff("jdoe-gone.txt", OWNER)
        self.assertEqual(self.store.list_handoff_owners(), [])
        with self.assertRaises(FileNotFoundError):
            self.store.read_handoff("jdoe-borrowed.txt", OWNER)

    def test_handoff_keys_round_trip_with_an_escaped_dash(self):
        key = "jdoe-home-notes\\-v2.txt"
        self.store.write_handoff(key, OWNER, b"held")
        self.assertEqual(self.store.list_handoffs(OWNER), [key])
        self.assertEqual(self.store.read_handoff(key, OWNER), b"held")

    def test_handoffs_do_not_escape_the_store(self):
        with self.assertRaises(ValueError):
            self.store.write_handoff("jdoe-a.txt", "not-a-url", b"x")
        with self.assertRaises(InvalidKeyError):
            self.store.write_handoff("../escape", OWNER, b"x")

    def test_non_atomic_store_round_trip(self):
        store = DataStore(self.root / "direct", atomic=False)
        self.assertFalse(store.atomic)
        store.write("jdoe-plain.txt", b"plain")
        self.assertEqual(store.read("jdoe-plain.txt"), b"plain")
        store.delete("jdoe-plain.txt")
        self.assertFalse(store.exists("jdoe-plain.txt"))
        self.assertEqual(list((self.root / "direct").iterdir()), [])

    def test_writes_accept_bytes_like_values_only(self):
        self.store.write("jdoe-x.txt", bytearray(b"bytearray"))
        self.assertEqual(self.store.read("jdoe-x.txt"), b"bytearray")
        self.store.write("jdoe-x.txt", memoryview(b"memoryview"))
        self.assertEqual(self.store.read("jdoe-x.txt"), b"memoryview")
        with self.assertRaises(TypeError):
            self.store.write("jdoe-x.txt", "text")


if __name__ == "__main__":
    unittest.main()
