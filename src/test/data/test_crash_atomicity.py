"""Hard-kill crash-injection checks for Swift Light's atomic saves.

A real child process performs one save, pauses at a chosen point, and is killed
with the platform's hard kill (``SIGKILL`` on POSIX, ``TerminateProcess`` on
Windows).  Cleanup code never runs, so the parent reopens the target and proves
it holds either the complete old contents or the complete new contents -- never
a mixture -- and that a later save still succeeds.

``test_atomic_save.py`` covers handled exceptions; this module covers the
unhandled kill.  Neither simulates power loss: the operating system stays up.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
from __future__ import annotations

import multiprocessing
import os
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import patch

from functional_test_support import ConsistentHashingRing as Ring, DataStore
from swift_light.data_server import storage
from swift_light.proxy_server import consistent_hashing_ring as ring_module

KEY = "crash-injection-object"
OLD = b"previous complete object"
NEW = b"replacement\x00object\xff" * 256


class _PausingHandle:
    """Delegate file I/O, pausing half-way through the first write."""

    def __init__(self, handle, ready, release):
        self.handle, self.ready, self.release = handle, ready, release
        self.paused = False

    def __enter__(self):
        self.handle.__enter__()
        return self

    def __exit__(self, *args):
        return self.handle.__exit__(*args)

    def __getattr__(self, name):
        return getattr(self.handle, name)

    def write(self, value):
        if self.paused:
            return self.handle.write(value)
        self.paused = True
        half = max(1, len(value) // 2)
        written = self.handle.write(value[:half])
        self.handle.flush()
        self.ready.set()
        self.release.wait(60)
        return written + self.handle.write(value[half:])


def _crash_writer(kind, location, candidate, payload, atomic, stage, ready, release):
    """Perform one save in a child process, pausing where the parent kills it."""
    location = Path(location)
    module = storage if kind == "object" else ring_module

    if kind == "object":
        store = DataStore(location, atomic=atomic)
        save = lambda: store.write(KEY, payload)
    else:
        ring = Ring.load(candidate)
        save = lambda: ring.save(location, atomic=atomic)

    real_replace = os.replace

    def paused_replace(source, destination):
        if stage == "before_replace":
            ready.set()
            release.wait(60)
        real_replace(source, destination)
        if stage == "after_replace":
            ready.set()
            release.wait(60)

    with patch.object(module.os, "replace", side_effect=paused_replace):
        if stage != "mid_write":
            save()
            return
        real_fdopen = os.fdopen

        def paused_fdopen(*args, **kwargs):
            return _PausingHandle(real_fdopen(*args, **kwargs), ready, release)

        with patch.object(module.os, "fdopen", side_effect=paused_fdopen):
            save()


class CrashAtomicityTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-crash-")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def fixture(self, kind, label):
        """Return ``(location, target, candidate, old, loader, payload, atomic)``.

        ``location`` is what the child opens: the store root for an object, the
        ring file for a ring.  The store asks for ``atomic=True``; the suite's
        write-mode seam may rewrite that request, so the child is told what the
        store actually got.
        """
        directory = self.root / kind / label
        directory.mkdir(parents=True)
        if kind == "object":
            store = DataStore(directory, atomic=True)
            target = store.write(KEY, OLD)
            loader = lambda: DataStore(directory).read(KEY)
            return (
                directory,
                target,
                None,
                target.read_bytes(),
                loader,
                NEW,
                store.atomic,
            )
        target = directory / "ring.json"
        candidate = directory / "candidate.json"
        Ring.build(3, 2, nodes=["B", "C", "A"], vnode_count=4, path=target)
        Ring.build(3, 2, nodes=["C", "A", "B"], vnode_count=5, path=candidate)
        loader = lambda: Ring.load(target).to_dict()
        return target, target, candidate, target.read_bytes(), loader, None, True

    def kill_at(self, kind, stage):
        """Run one save in a child, hard-kill it at ``stage``, return the fixture."""
        location, target, candidate, old, loader, payload, atomic = self.fixture(
            kind, stage
        )
        old_state = loader()
        context = multiprocessing.get_context("spawn")
        ready, release = context.Event(), context.Event()
        process = context.Process(
            target=_crash_writer,
            args=(
                kind,
                str(location),
                str(candidate) if candidate else None,
                payload,
                atomic,
                stage,
                ready,
                release,
            ),
        )
        process.start()
        try:
            deadline = time.monotonic() + 30
            while not ready.wait(0.1):
                self.assertTrue(
                    process.is_alive(),
                    f"writer exited without reaching {stage}",
                )
                self.assertLess(
                    time.monotonic(), deadline, f"writer did not reach {stage}"
                )
            process.kill()
            process.join(10)
            self.assertFalse(process.is_alive(), "writer survived the hard kill")
        finally:
            if process.is_alive():
                release.set()
                process.kill()
                process.join(5)
            process.close()
        return location, target, candidate, old, old_state, loader, payload, atomic

    def retry(self, kind, location, candidate, payload, atomic):
        """Prove a killed save does not block the next one."""
        if kind == "object":
            DataStore(location, atomic=atomic).write(KEY, payload)
        else:
            Ring.load(candidate).save(location, atomic=atomic)

    def test_a_kill_while_writing_the_temporary_file_preserves_the_old_contents(self):
        for kind in ("object", "ring"):
            with self.subTest(kind=kind):
                location, target, candidate, old, old_state, loader, payload, atomic = (
                    self.kill_at(kind, "mid_write")
                )
                self.assertEqual(target.read_bytes(), old)
                self.assertEqual(loader(), old_state)
                self.retry(kind, location, candidate, payload, atomic)
                expected = payload if kind == "object" else candidate.read_bytes()
                self.assertEqual(target.read_bytes(), expected)
                loader()

    def test_a_kill_before_replacement_preserves_the_old_contents(self):
        for kind in ("object", "ring"):
            with self.subTest(kind=kind):
                location, target, candidate, old, old_state, loader, payload, atomic = (
                    self.kill_at(kind, "before_replace")
                )
                self.assertEqual(target.read_bytes(), old)
                self.assertEqual(loader(), old_state)
                self.retry(kind, location, candidate, payload, atomic)
                expected = payload if kind == "object" else candidate.read_bytes()
                self.assertEqual(target.read_bytes(), expected)
                loader()

    def test_a_kill_after_replacement_publishes_the_new_contents(self):
        for kind in ("object", "ring"):
            with self.subTest(kind=kind):
                _location, target, candidate, _old, _old_state, loader, payload, _atomic = (
                    self.kill_at(kind, "after_replace")
                )
                expected = payload if kind == "object" else candidate.read_bytes()
                self.assertEqual(target.read_bytes(), expected)
                loader()


if __name__ == "__main__":
    unittest.main()
