#!/usr/bin/env python3
"""exp3 -- ring placement and persistence.

What this checks
----------------
Correctness checks for Swift Light, not benchmark measurements. The named tests
below describe each scenario. We use temporary state and the current checkout's
implementation; no deployed cluster or Hello.txt file is required.

Prerequisites
-------------
Python 3.12, the repository source, and writable temporary storage. Keep
functional_test_support.py beside this script. No third-party packages needed.
Use --temp-dir PATH to select an existing writable temporary directory.


Usage
-----
Run from the repository root::

    python3 experiments/swift_light/exp3_ring_placement.py

Exit codes
----------
    0   every named check ran and passed
    1   a check failed, errored, or was skipped
    77  prerequisite checks failed before the tests could run
"""
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from functional_test_support import ConsistentHashingRing as Ring, run


class RingPlacementTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-ring-")
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / "ring.json"
        self.ring = Ring.build(3, 2, nodes=["B", "C", "A"], vnode_count=8,
                               ring_size=65536, path=self.path)

    def test_keys_have_repeatable_distinct_server_lists(self):
        for key in ["alice-docs-one", "bob-images-two", "empty", "\u6587\u4ef6"]:
            with self.subTest(key=key):
                expected_slot = int.from_bytes(hashlib.sha1(key.encode()).digest()[:8], "big") % 65536
                self.assertEqual(self.ring.slot_for_key(key), expected_slot)
                fan = self.ring.fan(key)
                # Independently walk the serialized vnode points; do not use
                # the stored fan table as its own correctness oracle.
                points = self.ring.to_dict()["points"]
                eligible = [i for i, (slot, _) in enumerate(points) if slot <= expected_slot]
                start = eligible[-1] if eligible else len(points) - 1
                expected = []
                for offset in range(len(points)):
                    node = points[(start + offset) % len(points)][1]
                    if node not in expected:
                        expected.append(node)
                self.assertEqual(fan, tuple(expected))
                self.assertEqual(len(fan), 3)
                self.assertEqual(set(fan), {"B", "C", "A"})
                self.assertEqual(len(set(fan[:2])), 2)
                for _ in range(3):
                    self.assertEqual(self.ring.fan(key), fan)

    def test_boundary_and_wraparound_use_expected_clockwise_order(self):
        # Explicit points and expected orders are independent of the builder.
        # B owns [10,40), C owns [40,80), A owns [80,100) and [0,10).
        ring = Ring.from_dict({
            "schema_version": 1, "ring_size": 100, "hash_algorithm": "sha1",
            "tot_node_count": 3, "replicate_count": 2, "vnode_count": 1,
            "nodes": ["B", "C", "A"],
            "points": [[10, "B"], [40, "C"], [80, "A"]],
            "fans": [["B", "C", "A"], ["C", "A", "B"], ["A", "B", "C"]],
        })
        cases = [(0, ("A", "B", "C")), (9, ("A", "B", "C")),
                 (10, ("B", "C", "A")), (39, ("B", "C", "A")),
                 (40, ("C", "A", "B")), (79, ("C", "A", "B")),
                 (80, ("A", "B", "C")), (99, ("A", "B", "C"))]
        for slot, expected in cases:
            with self.subTest(slot=slot), patch.object(Ring, "slot_for_key", return_value=slot):
                self.assertEqual(ring.fan("boundary"), expected)

    def test_loaded_and_independently_built_rings_preserve_placement(self):
        loaded = Ring.load(self.path)
        rebuilt = Ring.build(3, 2, nodes=["B", "C", "A"], vnode_count=8,
                             path=Path(self.temp.name) / "other.json")
        self.assertEqual(loaded.to_dict(), self.ring.to_dict())
        for index in range(100):
            key = f"alice-docs-object{index}"
            self.assertEqual(loaded.fan(key), self.ring.fan(key))
            self.assertEqual(rebuilt.fan(key), self.ring.fan(key))


def main(argv: list[str] | None = None) -> int:
    """Run the functional checks and return the experiment exit code."""
    return run(RingPlacementTests, number=3,
               description="ring placement and persistence", argv=argv, needs_http=False)


if __name__ == "__main__":
    raise SystemExit(main())
