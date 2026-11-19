"""Basic construction, persistence and validation checks for the ring.

``test_ring_placement.py`` covers where keys land.  These tests cover the ring
as an object: the layout ``build()`` produces, the write-once rule, save/load in
both write modes, and the integrity checks that stop a corrupt ``ring.json``
from being served.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import copy
import tempfile
import unittest
from collections import Counter
from pathlib import Path

from functional_test_support import ConsistentHashingRing as Ring
from swift_light.proxy_server.consistent_hashing_ring import RingAlreadyBuiltError

KEYS = ["alice-docs-one", "bob-images-two", "empty", "\u6587\u4ef6"]


class RingConstructionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-ringbasic-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def build(self, name="ring.json", **kwargs):
        options = {"nodes": ["A", "B", "C"], "vnode_count": 8}
        options.update(kwargs)
        path = options.pop("path", self.root / name)
        return Ring.build(3, 2, path=path, **options)

    def test_build_lays_out_a_complete_ring(self):
        ring = self.build(ring_size=4096)
        self.assertEqual(ring.nodes, ("A", "B", "C"))
        self.assertEqual(
            (ring.tot_node_count, ring.replicate_count, ring.vnode_count, ring.ring_size),
            (3, 2, 8, 4096),
        )
        ring.validate()

        data = ring.to_dict()
        self.assertEqual(len(data["points"]), 24)
        self.assertEqual(len(data["fans"]), 24)
        self.assertEqual(Counter(node for _, node in data["points"]), Counter({"A": 8, "B": 8, "C": 8}))
        for slot, _ in data["points"]:
            self.assertTrue(0 <= slot < 4096)
        for index, (_, node) in enumerate(data["points"]):
            fan = data["fans"][index]
            self.assertEqual(len(fan), 3)
            self.assertEqual(fan[0], node)
            self.assertEqual(set(fan), {"A", "B", "C"})

    def test_build_refuses_to_overwrite_an_existing_ring(self):
        path = self.root / "ring.json"
        self.build()
        before = path.read_bytes()

        with self.assertRaises(RingAlreadyBuiltError) as caught:
            self.build()

        self.assertIsInstance(caught.exception, FileExistsError)
        self.assertEqual(path.read_bytes(), before)

    def test_save_and_load_round_trip_in_both_write_modes(self):
        ring = self.build(vnode_count=4, path=self.root / "built.json")
        for atomic, name in [(True, "atomic.json"), (False, "direct.json")]:
            with self.subTest(atomic=atomic):
                path = ring.save(self.root / name, atomic=atomic)
                self.assertEqual(Ring.load(path).to_dict(), ring.to_dict())

    def test_from_dict_rejects_an_inconsistent_layout(self):
        ring = self.build()

        def broken(mutate):
            data = copy.deepcopy(ring.to_dict())
            mutate(data)
            return data

        cases = {
            "duplicate node": lambda data: data["nodes"].__setitem__(1, "A"),
            "unsorted points": lambda data: data["points"].reverse(),
            "missing vnode points": lambda data: data["points"].pop(),
            "short fan": lambda data: data["fans"][0].pop(),
            "fan not starting with its primary": lambda data: data["fans"][0].append(
                data["fans"][0].pop(0)
            ),
        }
        for label, mutate in cases.items():
            with self.subTest(case=label), self.assertRaises(ValueError):
                Ring.from_dict(broken(mutate))

    def test_constructor_refuses_impossible_parameters(self):
        cases = {
            "replicas exceed servers": lambda: Ring(2, 3),
            "zero replicas": lambda: Ring(3, 0),
            "zero vnodes": lambda: Ring(3, 2, vnode_count=0),
            "unknown hash algorithm": lambda: Ring(3, 2, hash_algorithm="not-a-hash"),
        }
        for label, make in cases.items():
            with self.subTest(case=label), self.assertRaises(ValueError):
                make()

    def test_placement_is_deterministic_across_independent_builds(self):
        first = self.build("one.json")
        second = self.build("two.json")
        self.assertEqual(first.to_dict(), second.to_dict())
        for key in KEYS:
            with self.subTest(key=key):
                self.assertEqual(first.slot_for_key(key), second.slot_for_key(key))
                self.assertTrue(0 <= first.slot_for_key(key) < first.ring_size)
                self.assertEqual(first.fan(key), second.fan(key))
                self.assertEqual(set(first.fan(key)), {"A", "B", "C"})

    def test_load_of_a_missing_ring_raises(self):
        with self.assertRaises(FileNotFoundError):
            Ring.load(self.root / "nothing.json")


if __name__ == "__main__":
    unittest.main()
