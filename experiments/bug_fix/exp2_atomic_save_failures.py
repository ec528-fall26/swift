#!/usr/bin/env python3
"""An interrupted or failed individual-builder save preserves the previous valid file.
Also checks the durability operation order, not actual power-loss survival.

Run with the upstream Swift checkout on PYTHONPATH and its dependencies installed.
Exit codes: 0 = passed, 1 = test failure, 77 = Linux required.
"""

import multiprocessing
import pickle
import stat
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


def interrupted_save(path, ready, release):
    """Pause a real child process after writing half a serialized builder."""
    rb = RingBuilder.load(path)

    def partial_dump(obj, handle, protocol=2):
        payload = pickle.dumps(obj, protocol=protocol)
        handle.write(payload[:len(payload) // 2])
        handle.flush()
        ready.set()
        if not release.wait(30):
            raise RuntimeError('parent did not terminate paused writer')

    with mock.patch.object(builder_module.pickle, 'dump', partial_dump):
        rb.save(path)


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux is authoritative')
class TestAtomicSaveFailures(unittest.TestCase):

    def setUp(self):
            self.temp = tempfile.TemporaryDirectory()
            self.addCleanup(self.temp.cleanup)
            self.path = str(Path(self.temp.name) / 'ring.builder')
            self.rb = RingBuilder(6, 3, 0)
            self.rb.save(self.path)
            self.before = Path(self.path).read_bytes()

    def assert_original_survives(self):
            self.assertEqual(self.before, Path(self.path).read_bytes())
            self.assertEqual(self.rb.to_dict(),
                             RingBuilder.load(self.path).to_dict())

    def test_killed_writer_preserves_original_and_next_save_works(self):
            ctx = multiprocessing.get_context('spawn')
            ready, release = ctx.Event(), ctx.Event()
            process = ctx.Process(target=interrupted_save,
                                  args=(self.path, ready, release))
            process.start()
            try:
                self.assertTrue(ready.wait(15), 'child never reached partial write')
                process.kill()  # No Python exception cleanup runs.
                process.join(10)
                self.assertFalse(process.is_alive())
                self.assert_original_survives()
                self.rb.version += 1
                self.rb.save(self.path)
                self.assertEqual(self.rb.to_dict(),
                                 RingBuilder.load(self.path).to_dict())
            finally:
                if process.is_alive():
                    process.kill()
                process.join(10)
                process.close()

    def test_file_fsync_failure_preserves_original(self):
            with mock.patch.object(builder_module.os, 'fsync',
                                   side_effect=OSError('injected fsync failure')):
                with self.assertRaises(OSError):
                    self.rb.save(self.path)
            self.assert_original_survives()
            self.assertEqual([Path(self.path)], list(Path(self.temp.name).iterdir()))

    def test_rename_failure_preserves_original(self):
            with mock.patch.object(builder_module, 'renamer',
                                   side_effect=OSError('injected rename failure')):
                with self.assertRaises(OSError):
                    self.rb.save(self.path)
            self.assert_original_survives()
            self.assertEqual([Path(self.path)], list(Path(self.temp.name).iterdir()))

    def test_file_sync_then_rename_then_directory_sync(self):
            # Execute real operations while recording their order.
            events = []
            real_sync, real_rename = os.fsync, os.rename

            def tracked_sync(fd):
                kind = ('directory' if stat.S_ISDIR(os.fstat(fd).st_mode)
                        else 'file')
                result = real_sync(fd)
                events.append(kind)
                return result

            def tracked_rename(*args, **kwargs):
                result = real_rename(*args, **kwargs)
                events.append('rename')
                return result

            with mock.patch.object(os, 'fsync', tracked_sync), \
                    mock.patch.object(os, 'rename', tracked_rename):
                self.rb.save(self.path)
            self.assertIn('file', events)
            self.assertIn('rename', events)
            self.assertIn('directory', events)
            self.assertLess(events.index('file'), events.index('rename'))
            self.assertLess(events.index('rename'), events.index('directory'))
            RingBuilder.load(self.path)


if __name__ == '__main__':
    unittest.main(verbosity=2)
