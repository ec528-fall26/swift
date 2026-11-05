#!/usr/bin/env python3
"""exp3 -- handled individual-builder save failures preserve existing state.

Claim
-----
Serialization, explicit flush, file-fsync, and replacement errors leave the
previous builder unchanged and loadable, remove this attempt's temporary file,
and propagate the error. A failed first save restores an unpersisted builder ID.
This tests caught exceptions, not SIGKILL cleanup, concurrency, or composites.

Usage (from EC528_swift)
-----------------------
    python3 experiments/exp3_save_failure_cleanup.py
    python3 experiments/exp3_save_failure_cleanup.py --swift-src /path/to/Swift
    python3 experiments/exp3_save_failure_cleanup.py --results-json exp3.json

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp3_save_failure_cleanup.py
    echo "exit=$?"

If the checkout is at /opt/ec528-swift, as in exp0's example, use that
location in the cd command instead. Run from the checkout containing the fix;
the script does not switch branches or modify the implementation.

To keep a result file and set an explicit runtime bound on Lightsail:

    python3 experiments/exp3_save_failure_cleanup.py \
        --results-json exp3-results.json \
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
import pickle
from unittest import mock

from atomic_save_support import BuilderCase, ObservedFile, make_builder, run


class TestSaveFailureCleanup(BuilderCase):
    def check_failure(self, stage):
        before_files = set(self.path.parent.iterdir())
        self.builder.version += 1
        injected = []
        message = 'injected %s failure' % stage

        def fail(*args, **kwargs):
            injected.append(stage)
            raise OSError(message)

        def partial_dump(obj, handle, protocol=2):
            payload = pickle.dumps(obj, protocol=protocol)
            handle.write(payload[:len(payload) // 2])
            handle.flush()
            fail()

        def observed_open(*args, **kwargs):
            return ObservedFile(builtins.open(*args, **kwargs), before_flush=fail)

        if stage == 'serialization':
            patch = mock.patch.object(self.module.pickle, 'dump', partial_dump)
        elif stage == 'flush':
            patch = mock.patch.object(self.module, 'open', observed_open, create=True)
        elif stage == 'file_fsync':
            patch = mock.patch.object(os, 'fsync', fail)
        else:
            # Swift's renamer may retry os.rename. Both attempts must fail.
            patch = mock.patch.object(os, 'rename', fail)
        with patch:
            with self.assertRaisesRegex(OSError, message):
                self.builder.save(str(self.path))
        self.assertTrue(injected, 'the intended failure point was not reached')
        self.assert_previous()
        self.assertEqual(before_files, set(self.path.parent.iterdir()),
                         'failed save left an extra file')
        print('  verified handled failure: %s' % stage, flush=True)

    def test_serialization_failure(self):
        self.check_failure('serialization')

    def test_explicit_flush_failure(self):
        self.check_failure('flush')

    def test_file_fsync_failure(self):
        self.check_failure('file_fsync')

    def test_replacement_failure(self):
        self.check_failure('replacement')

    def test_failed_first_save_restores_unpersisted_id(self):
        fresh = make_builder(self.module)
        self.assertIsNone(fresh._id)
        path = self.path.parent / 'first-save.builder'
        before_files = set(self.path.parent.iterdir())
        with mock.patch.object(self.module.pickle, 'dump',
                               side_effect=OSError('injected first-save failure')) as dump:
            with self.assertRaisesRegex(OSError, 'injected first-save failure'):
                fresh.save(str(path))
            dump.assert_called_once()
        self.assertIsNone(fresh._id)
        self.assertFalse(path.exists())
        self.assertEqual(before_files, set(self.path.parent.iterdir()))
        self.assert_previous()


if __name__ == '__main__':
    raise SystemExit(run('Handled individual-builder save failures preserve the '
                         'old file and clean up temporary files.',
                         TestSaveFailureCleanup, __file__))
