"""Partition-key mapping checks for the swift_light client.

``object_key`` is what turns a remote object path into the key the ring hashes,
so the tests pin its two rules: segments joined with ``-`` behind the account,
and every value refused that the store could not turn into a safe path.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import tempfile
import unittest

from functional_test_support import Client, ClientError
from swift_light.client.client import object_key
from swift_light.data_server.storage import DataStore


class ObjectKeyTests(unittest.TestCase):
    def test_segments_become_a_dash_joined_key_behind_the_account(self):
        self.assertEqual(object_key("a/b/file.txt", "jdoe"), "jdoe-a-b-file.txt")
        self.assertEqual(object_key("notes.txt", "jdoe"), "jdoe-notes.txt")
        self.assertEqual(object_key("a/b/c", "jdoe"), "jdoe-a-b-c")

    def test_separators_and_stray_slashes_are_normalised(self):
        self.assertEqual(object_key("a\\b\\c.txt", "jdoe"), "jdoe-a-b-c.txt")
        self.assertEqual(object_key("/a/b/", "jdoe"), "jdoe-a-b")
        self.assertEqual(object_key("  a/b  ", "jdoe"), "jdoe-a-b")
        self.assertEqual(object_key("a/b/", "jdoe"), "jdoe-a-b")

    def test_client_prefixes_its_own_account(self):
        client = Client(user="alice")
        self.assertEqual(client.user, "alice")
        self.assertEqual(client.object_key("photos/cat.png"), "alice-photos-cat.png")

    def test_unusable_paths_are_refused(self):
        for path in ["", "   ", "/", "//", "a//b", "a/./b", "a/../b", "a:b/c", "a/b:c", "a/b\x00c"]:
            with self.subTest(path=path):
                with self.assertRaises(ClientError):
                    object_key(path, "jdoe")

    def test_unusable_accounts_are_refused(self):
        for user in ["", "   ", "a:b", "a/b", "a\\b", "a\x00b"]:
            with self.subTest(user=user):
                with self.assertRaises(ClientError):
                    object_key("a/b", user)

    def test_non_text_inputs_are_refused(self):
        for path, user in [(123, "jdoe"), ("a/b", 123), (None, "jdoe")]:
            with self.subTest(path=path, user=user):
                with self.assertRaises(ClientError):
                    object_key(path, user)

    def test_a_key_produces_a_path_the_store_accepts(self):
        # The client refuses here exactly what the store refuses later, so a
        # path that maps to a key can always be written.
        with tempfile.TemporaryDirectory(prefix="swift-light-key-") as directory:
            store = DataStore(directory)
            path = store.path_for(object_key("a/b/file.txt", "jdoe"))
            self.assertEqual(path.relative_to(store.root).parts, ("jdoe", "a", "b", "file.txt"))


if __name__ == "__main__":
    unittest.main()
