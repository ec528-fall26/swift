"""Rebalance batch ordering: the scratch ring is flushed while files move.

``test_ring_basics`` covers building, saving and loading; these tests pin down
the order inside ``ConsistentHashingRing.rebalance`` -- a batch writes its
``ring.json.temp`` on a background thread while the files are copied, so the
only ring work left after the migration is the rename that commits it.  A
failure in that write must leave the live ring alone.

Run from the repository root::

    python -m unittest discover -s src/test -t src/test -v
"""

import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from functional_test_support import ConsistentHashingRing as Ring

NODES = ["A", "B", "C"]
KEYS = [f"user{index}-docs-file{index}.txt" for index in range(60)]


class _Data:
    """The ring's ``DataServer`` protocol over an in-memory key list."""

    def __init__(self, ring):
        self.ring = ring
        self.moved = []

    def list_files(self, node, start_slot, end_slot):
        return [
            key
            for key in KEYS
            if start_slot <= self.ring.slot_for_key(key) < end_slot
        ]

    def write(self, key, to_node):
        self.moved.append((key, to_node))

    def write_handoff(self, key, from_node):
        pass


class RebalanceScratchWriteTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-rebalance-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.path = self.root / "ring.json"
        self.ring = Ring.build(
            len(NODES), 2, nodes=NODES, vnode_count=8, path=self.path
        )

    def recording_saves(self):
        """Patch ``save`` to note the file, the mode and the calling thread."""
        calls = []
        original = Ring.save

        def record(ring, path=None, *, atomic=True):
            calls.append(
                (
                    Path(path).name if path is not None else "ring.json",
                    atomic,
                    threading.current_thread(),
                )
            )
            return original(ring, path, atomic=atomic)

        return calls, patch.object(Ring, "save", new=record)

    def test_a_batch_flushes_its_scratch_ring_off_the_moving_thread(self):
        calls, patcher = self.recording_saves()
        data = _Data(self.ring)
        with patcher:
            joined = Ring.add_node("D", data=data, path=self.path)

        scratch = [call for call in calls if call[0] == "ring.json.temp"]
        self.assertTrue(scratch, "a batch should write ring.json.temp")
        self.assertTrue(data.moved, "the join should move at least one file")
        self.assertFalse(
            [call for call in scratch if call[2] is threading.main_thread()],
            "the scratch ring must be written while the files move, not before",
        )
        self.assertTrue(
            all(call[1] is False for call in scratch),
            "the scratch ring is written directly -- the rename is the commit",
        )

        self.assertEqual(joined.nodes, ("A", "B", "C", "D"))
        self.assertEqual(Ring.load(self.path).nodes, ("A", "B", "C", "D"))
        self.assertFalse((self.root / "ring.json.temp").exists())

    def test_a_failed_scratch_write_never_commits_the_batch(self):
        original = Ring.save

        def fail_scratch(ring, path=None, *, atomic=True):
            if path is not None and Path(path).name == "ring.json.temp":
                raise OSError("no space left on device")
            return original(ring, path, atomic=atomic)

        data = _Data(self.ring)
        with patch.object(Ring, "save", new=fail_scratch):
            with self.assertRaises(OSError):
                Ring.add_node("D", data=data, path=self.path)

        self.assertEqual(Ring.load(self.path).nodes, tuple(NODES))


if __name__ == "__main__":
    unittest.main()
