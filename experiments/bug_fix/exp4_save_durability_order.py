#!/usr/bin/env python3
"""exp4 -- successful individual-builder saves use the durability protocol.

Claim
-----
A changed builder is serialized to a same-directory temporary file, explicitly
flushed, file-synced, closed, renamed into place, then its directory is synced.
The installed state is complete and loadable, and the old state is visible until
replacement. This observes real filesystem operations; it does not simulate
power loss or claim that storage hardware honors flushes in every environment.

Usage (from EC528_swift)
-----------------------
    python3 experiments/exp4_save_durability_order.py
    python3 experiments/exp4_save_durability_order.py --swift-src /path/to/Swift
    python3 experiments/exp4_save_durability_order.py --results-json exp4.json

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp4_save_durability_order.py
    echo "exit=$?"

If the checkout is at /opt/ec528-swift, as in exp0's example, use that
location in the cd command instead. Run from the checkout containing the fix;
the script does not switch branches or modify the implementation.

To keep a result file and set an explicit runtime bound on Lightsail:

    python3 experiments/exp4_save_durability_order.py \
        --results-json exp4-results.json \
        --timeout 90
    echo "exit=$?"

Setup is provided by experiments/setup.sh on a clean Linux machine. The
prepared instance should already have .venv; keep atomic_save_support.py
beside this script. The selected builder source path and revision are printed
before the checks, so verify they identify the intended checkout.

Exit codes: 0 = checks passed; 1 = check failed; 77 = could not run or timed out.
Linux, upstream dependencies, and a writable temporary filesystem are required.
The worker has a 90-second default deadline. See experiments/README.md.
"""

import builtins
import os
from pathlib import Path
import pickle
import stat
from unittest import mock

from atomic_save_support import BuilderCase, ObservedFile, run


class TestSaveDurabilityOrder(BuilderCase):
    def test_complete_update_follows_durability_order(self):
        self.builder.version += 1
        expected = self.builder.to_dict()
        events, handles, candidates = [], [], []
        real_dump, real_sync, real_rename = pickle.dump, os.fsync, os.rename

        def observed_open(path, *args, **kwargs):
            candidate = Path(path).resolve()
            self.assertNotEqual(self.path.resolve(), candidate,
                                'save opened the live builder for writing')
            self.assertEqual(self.path.parent.resolve(), candidate.parent)
            handle = builtins.open(path, *args, **kwargs)
            handles.append(handle)
            candidates.append(candidate)
            return ObservedFile(
                handle, after_flush=lambda: events.append('flush'),
                after_close=lambda: events.append('close'))

        def observed_dump(obj, handle, *args, **kwargs):
            self.assert_previous()
            result = real_dump(obj, handle, *args, **kwargs)
            events.append('serialize')
            return result

        def observed_sync(fd):
            descriptor = os.fstat(fd)
            directory = stat.S_ISDIR(descriptor.st_mode)
            if directory:
                parent = self.path.parent.stat()
                self.assertEqual((parent.st_dev, parent.st_ino),
                                 (descriptor.st_dev, descriptor.st_ino))
                self.assertEqual(expected, pickle.loads(self.path.read_bytes()))
            else:
                self.assert_previous()
                self.assertTrue(handles)
                self.assertEqual(handles[-1].fileno(), fd)
            result = real_sync(fd)
            events.append('directory_fsync' if directory else 'file_fsync')
            return result

        def observed_rename(old, new):
            self.assert_previous()
            self.assertEqual(candidates[-1], Path(old).resolve())
            self.assertEqual(self.path.resolve(), Path(new).resolve())
            self.assertTrue(handles[-1].closed, 'temporary file was not closed')
            self.assertEqual(expected, pickle.loads(Path(old).read_bytes()))
            result = real_rename(old, new)
            events.append('rename')
            return result

        with mock.patch.object(self.module, 'open', observed_open, create=True), \
                mock.patch.object(self.module.pickle, 'dump', observed_dump), \
                mock.patch.object(os, 'fsync', observed_sync), \
                mock.patch.object(os, 'rename', observed_rename):
            self.builder.save(str(self.path))

        self.assertEqual(['serialize', 'flush', 'file_fsync', 'close',
                          'rename', 'directory_fsync'], events)
        self.assertEqual(1, len(candidates))
        self.assertFalse(candidates[0].exists())
        self.assertNotEqual(self.before, self.path.read_bytes())
        loaded = self.module.RingBuilder.load(str(self.path))
        self.assertEqual(expected, loaded.to_dict())
        for partition in range(2 ** loaded.part_power):
            self.assertEqual(self.builder.get_part_devices(partition),
                             loaded.get_part_devices(partition))
        print('  verified operations: %s' % ' -> '.join(events), flush=True)


if __name__ == '__main__':
    raise SystemExit(run('Successful individual-builder saves install a complete '
                         'update with ordered file and directory syncing.',
                         TestSaveDurabilityOrder, __file__))
