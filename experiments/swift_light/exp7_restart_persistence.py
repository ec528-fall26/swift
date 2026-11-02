#!/usr/bin/env python3
"""exp7 -- persistence across a fresh process.

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

    python3 experiments/swift_light/exp7_restart_persistence.py

Exit codes
----------
    0   every named check ran and passed
    1   a check failed, errored, or was skipped
    77  prerequisite checks failed before the tests could run
"""
import hashlib
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

from functional_test_support import SRC_ROOT, ConsistentHashingRing as Ring, DataServerConfig, DataStore, run


class RestartPersistenceTests(unittest.TestCase):
    def test_completed_state_reopens_in_fresh_process(self):
        with tempfile.TemporaryDirectory(prefix="swift-light-restart-") as directory:
            root = Path(directory)
            ring = Ring.build(3, 2, nodes=["B", "C", "A"], vnode_count=8, path=root / "ring.json")
            config = DataServerConfig("http://127.0.0.1:9001", "http://127.0.0.1:8010")
            config.save(root / "config.json")
            store = DataStore(root / "objects")
            payloads = {"test-docs-empty": b"", "test-docs-text": b"hello\n",
                        "test-docs-binary": bytes(range(256)) * 8}
            for key, payload in payloads.items():
                store.write(key, payload)
            store.write("test-docs-text", b"updated complete contents")
            payloads["test-docs-text"] = b"updated complete contents"
            expected = {"config": config.to_dict(), "ring": ring.to_dict(),
                        "fans": {key: list(ring.fan(key)) for key in payloads},
                        "objects": {key: {"size": len(value), "sha256": hashlib.sha256(value).hexdigest()}
                                    for key, value in payloads.items()}}
            # Only paths and keys cross the process boundary, never live objects
            # or expected contents. The child must read the on-disk state itself.
            code = '''
import hashlib, json, sys
from pathlib import Path
sys.path.insert(0, sys.argv[1])
from swift_light.proxy_server.consistent_hashing_ring import ConsistentHashingRing
from swift_light.data_server.storage import DataServerConfig, DataStore
root = Path(sys.argv[2])
ring = ConsistentHashingRing.load(root / "ring.json")
config = DataServerConfig.load(root / "config.json")
store = DataStore(root / "objects")
keys = json.loads(sys.argv[3])
objects = {}
for key in keys:
    data = store.read(key)
    objects[key] = {"size": len(data), "sha256": hashlib.sha256(data).hexdigest()}
print(json.dumps({"config": config.to_dict(), "ring": ring.to_dict(),
                  "fans": {key: list(ring.fan(key)) for key in keys}, "objects": objects}))
'''
            completed = subprocess.run(
                [sys.executable, "-c", code, str(SRC_ROOT), str(root), json.dumps(list(payloads))],
                capture_output=True, text=True, timeout=20,
            )
            self.assertEqual(completed.returncode, 0, completed.stderr)
            self.assertEqual(json.loads(completed.stdout), expected)


def main(argv: list[str] | None = None) -> int:
    """Run the functional checks and return the experiment exit code."""
    return run(RestartPersistenceTests, number=7,
               description="persistence across a fresh process", argv=argv, needs_http=False)


if __name__ == "__main__":
    raise SystemExit(main())
