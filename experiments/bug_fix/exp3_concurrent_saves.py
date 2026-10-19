#!/usr/bin/env python3
"""Concurrent threads save complete builders using independent temporary files.
This checks two builder instances in one process, not distributed locking.

Run with the upstream Swift checkout on PYTHONPATH and its dependencies installed.
Exit codes: 0 = passed, 1 = test failure, 77 = Linux required.
"""

import pickle
import threading
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest import mock

# Unsupported environments must not report a passing experiment.
if __name__ == '__main__' and not sys.platform.startswith('linux'):
    print('RESULT: SKIP (Linux required)')
    sys.exit(77)

from swift.common.ring.builder import RingBuilder
from swift.common.ring import builder as builder_module


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux is authoritative')
class TestConcurrentSaves(unittest.TestCase):

    def setUp(self):
            self.temp = tempfile.TemporaryDirectory()
            self.addCleanup(self.temp.cleanup)
            self.path = str(Path(self.temp.name) / 'ring.builder')
            self.rb = RingBuilder(6, 3, 0)
            self.rb.save(self.path)
            self.before = Path(self.path).read_bytes()

    def test_concurrent_threads_use_independent_temporary_files(self):
            barrier = threading.Barrier(2)
            names, errors = [], []
            lock = threading.Lock()
            real_dump = pickle.dump
            builders = [RingBuilder.load(self.path) for _ in range(2)]
            for index, rb in enumerate(builders):
                rb.version += index + 1

            def simultaneous_dump(obj, handle, *args, **kwargs):
                with lock:
                    names.append(handle.name)
                barrier.wait(timeout=10)
                return real_dump(obj, handle, *args, **kwargs)

            def save(rb):
                try:
                    rb.save(self.path)
                except Exception as exc:
                    with lock:
                        errors.append(repr(exc))

            with mock.patch.object(builder_module.pickle, 'dump', simultaneous_dump):
                threads = [threading.Thread(target=save, args=(rb,), daemon=True)
                           for rb in builders]
                for thread in threads:
                    thread.start()
                for thread in threads:
                    thread.join(15)
                self.assertTrue(all(not t.is_alive() for t in threads))
            self.assertEqual([], errors, 'concurrent saves raised errors')
            self.assertEqual(2, len(set(names)), 'writers shared a temporary file')
            result = RingBuilder.load(self.path).to_dict()
            self.assertIn(result, [rb.to_dict() for rb in builders])


if __name__ == '__main__':
    unittest.main(verbosity=2)
