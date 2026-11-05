"""Atomic-save failure-safety and visibility checks for Swift Light.

In atomic mode, handled failures before replacement must preserve the previous
file, and replacement must publish complete new contents. Both object files and
ring metadata are covered.

This tests Swift Light local files, not OpenStack Swift's RingBuilder. It does
not simulate abrupt process termination, power loss, or cross-replica atomicity.

Run from the repository root::

    python -m pytest src/test
    python -m unittest discover -s src/test -t src/test -v
"""
import os
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

from functional_test_support import ConsistentHashingRing as Ring, DataStore
from swift_light.data_server import storage
from swift_light.proxy_server import consistent_hashing_ring as ring_module


class ObservedFile:
    """Delegate real file I/O while observing or failing a specific save stage."""
    def __init__(self, handle, events, error=None, stage=None):
        self.handle, self.events, self.error, self.stage = handle, events, error, stage

    def __enter__(self):
        self.handle.__enter__()
        return self

    def __exit__(self, *args):
        try:
            return self.handle.__exit__(*args)
        finally:
            self.events.append("close")

    def write(self, value):
        self.events.append("write")
        if self.stage == "write":
            self.handle.write(value[:max(1, len(value) // 2)])
            raise self.error
        return self.handle.write(value)

    def flush(self):
        self.events.append("flush")
        if self.stage == "flush":
            raise self.error
        return self.handle.flush()

    def fileno(self):
        return self.handle.fileno()


class AtomicSaveTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="swift-light-atomic-")
        self.addCleanup(self.temp.cleanup)

    def fixture(self, kind, label):
        directory = Path(self.temp.name) / kind / label
        directory.mkdir(parents=True)
        if kind == "object":
            store = DataStore(directory, atomic=True)
            key = "test-docs-object"
            target = store.write(key, b"previous complete object")
            new = b"replacement\x00object\xff" * 64
            return storage, target, new, lambda: store.write(key, new), lambda: store.read(key)
        target = directory / "ring.json"
        Ring.build(3, 2, nodes=["B", "C", "A"], vnode_count=4, path=target)
        replacement = Ring.build(3, 2, nodes=["C", "A", "B"], vnode_count=5,
                                 path=directory / "candidate.json")
        new = (directory / "candidate.json").read_bytes()
        return ring_module, target, new, lambda: replacement.save(target), lambda: Ring.load(target).to_dict()

    def test_handled_failures_preserve_old_contents_and_remove_temporary_files(self):
        for kind in ["object", "ring"]:
            for stage in ["write", "flush", "fsync", "replace"]:
                with self.subTest(kind=kind, stage=stage):
                    module, target, new, save, load = self.fixture(kind, stage)
                    old, loaded = target.read_bytes(), load()
                    before = set(target.parent.iterdir())
                    error = OSError("injected " + stage)
                    events = []
                    fdopen = os.fdopen
                    def opened(*args, **kwargs):
                        return ObservedFile(fdopen(*args, **kwargs), events, error, stage)
                    with patch.object(module.os, "fdopen", side_effect=opened):
                        if stage in ("write", "flush"):
                            with self.assertRaises(OSError) as caught:
                                save()
                        else:
                            name = "fsync" if stage == "fsync" else "replace"
                            with patch.object(module.os, name, side_effect=error):
                                with self.assertRaises(OSError) as caught:
                                    save()
                    self.assertIs(caught.exception, error)
                    self.assertEqual(target.read_bytes(), old)
                    self.assertEqual(load(), loaded)
                    self.assertEqual(set(target.parent.iterdir()), before)
                    # Cleanup must leave the destination usable by a later save.
                    save()
                    self.assertEqual(target.read_bytes(), new)
                    load()

    def test_successful_save_orders_real_io_before_atomic_replacement(self):
        for kind in ["object", "ring"]:
            with self.subTest(kind=kind):
                module, target, new, save, load = self.fixture(kind, "order")
                old = target.read_bytes()
                events, handles = [], []
                fdopen, fsync, replace = os.fdopen, os.fsync, os.replace
                directory_sync = module._fsync_directory
                def opened(*args, **kwargs):
                    observed = ObservedFile(fdopen(*args, **kwargs), events)
                    handles.append(observed.handle)
                    return observed
                def synced(fd):
                    events.append("file_fsync" if stat.S_ISREG(os.fstat(fd).st_mode) else "directory_fsync")
                    return fsync(fd)
                def replaced(source, destination):
                    self.assertEqual(Path(source).parent.resolve(), target.parent.resolve())
                    self.assertTrue(handles[0].closed)
                    self.assertEqual(Path(source).read_bytes(), new)
                    self.assertEqual(target.read_bytes(), old)
                    events.append("replace")
                    return replace(source, destination)
                def directory_synced(directory):
                    self.assertEqual(Path(directory).resolve(), target.parent.resolve())
                    events.append("directory_sync_attempt")
                    return directory_sync(directory)
                with patch.object(module.os, "fdopen", side_effect=opened), \
                     patch.object(module.os, "fsync", side_effect=synced), \
                     patch.object(module.os, "replace", side_effect=replaced), \
                     patch.object(module, "_fsync_directory", side_effect=directory_synced):
                    save()
                self.assertEqual(events[:6], ["write", "flush", "file_fsync", "close", "replace", "directory_sync_attempt"])
                if os.name == "posix":
                    self.assertEqual(events[6:], ["directory_fsync"])
                self.assertEqual(target.read_bytes(), new)
                load()

    def test_readers_see_old_before_replacement_and_new_after(self):
        for kind in ["object", "ring"]:
            with self.subTest(kind=kind):
                module, target, new, save, load = self.fixture(kind, "visibility")
                old, loaded = target.read_bytes(), load()
                ready, release = threading.Event(), threading.Event()
                errors = []
                replace = os.replace
                def paused_replace(source, destination):
                    ready.set()
                    if not release.wait(5):
                        raise TimeoutError("reader did not release writer")
                    return replace(source, destination)
                def writer():
                    try:
                        save()
                    except BaseException as exc:
                        errors.append(exc)
                with patch.object(module.os, "replace", side_effect=paused_replace):
                    thread = threading.Thread(target=writer, daemon=True)
                    thread.start()
                    try:
                        self.assertTrue(ready.wait(5), "writer never reached replacement")
                        self.assertEqual(target.read_bytes(), old)
                        self.assertEqual(load(), loaded)
                    finally:
                        release.set()
                        thread.join(6)
                self.assertFalse(thread.is_alive(), "writer did not finish")
                self.assertEqual(errors, [])
                self.assertEqual(target.read_bytes(), new)
                load()


if __name__ == "__main__":
    unittest.main()
