#!/usr/bin/env python3
"""exp8 -- handled composite-builder save failures preserve existing state.

Claim
-----
Serialization, explicit flush, file-fsync, and replacement errors leave the
previous composite file unchanged and loadable, remove this attempt's temporary
file, and propagate the error. A failed first save creates no composite file.
A failing cleanup does not mask the original error, a save of an uncomposed
builder is rejected without touching the file, and a retry after a failure
succeeds. This tests caught exceptions, not SIGKILL cleanup, concurrency, or
the component builders themselves.

Usage (from EC528_swift)
-----------------------
    python3 experiments/exp8_composite_save_failure_cleanup.py
    python3 experiments/exp8_composite_save_failure_cleanup.py --swift-src /path/to/Swift
    python3 experiments/exp8_composite_save_failure_cleanup.py --results-json exp8.json

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp8_composite_save_failure_cleanup.py
    echo "exit=$?"

If the checkout is at /opt/ec528-swift, use that location in the cd command
instead. Run from the checkout containing the fix; the script does not switch
branches or modify the implementation.

To keep a result file and set an explicit runtime bound on Lightsail:

    python3 experiments/exp8_composite_save_failure_cleanup.py \
        --results-json exp8-results.json \
        --timeout 90
    echo "exit=$?"

Setup is provided by experiments/setup.sh on a clean Linux machine. The
prepared instance should already have .venv; keep composite_save_support.py
beside this script. The selected composite builder source path and revision are
printed before the checks, so verify they identify the intended checkout.

Exit codes: 0 = checks passed; 1 = check failed; 77 = could not run or timed out.
Linux, upstream dependencies, and a writable temporary filesystem are required.
The worker has a 90-second default deadline.
"""

import json
import os
import tempfile
from unittest import mock

from composite_save_support import (CompositeCase, ObservedFile, leftovers,
                                    make_composite, run)


class TestCompositeSaveFailureCleanup(CompositeCase):
    def check_failure(self, stage):
        before_files = set(self.path.parent.iterdir())
        self.composite.version += 1
        injected = []
        message = 'injected %s failure' % stage

        def fail(*args, **kwargs):
            injected.append(stage)
            raise OSError(message)

        def partial_dump(obj, handle, **kwargs):
            payload = json.dumps(obj, **kwargs)
            handle.write(payload[:len(payload) // 2])
            handle.flush()
            fail()

        def observed_temporary(*args, **kwargs):
            return ObservedFile(tempfile.NamedTemporaryFile(*args, **kwargs),
                                before_flush=fail)

        if stage == 'serialization':
            patch = mock.patch.object(self.module.json, 'dump', partial_dump)
        elif stage == 'flush':
            patch = mock.patch.object(self.module, 'NamedTemporaryFile',
                                      observed_temporary, create=True)
        elif stage == 'file_fsync':
            patch = mock.patch.object(os, 'fsync', fail)
        else:
            # Swift's renamer may retry os.rename. Both attempts must fail.
            patch = mock.patch.object(os, 'rename', fail)
        with patch:
            with self.assertRaisesRegex(OSError, message):
                self.composite.save(str(self.path))
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

    def test_failed_first_save_creates_no_file(self):
        directory = self.path.parent / 'first-save'
        directory.mkdir()
        fresh = make_composite(self.module, directory)
        path = directory / 'first-save.json'
        before_files = set(directory.iterdir())
        with mock.patch.object(self.module.json, 'dump',
                               side_effect=OSError('injected first-save failure')) as dump:
            with self.assertRaisesRegex(OSError, 'injected first-save failure'):
                fresh.save(str(path))
            dump.assert_called_once()
        self.assertFalse(path.exists())
        self.assertEqual(before_files, set(directory.iterdir()))
        self.assert_previous()

    def test_cleanup_failure_does_not_mask_original_error(self):
        self.composite.version += 1
        unlinked = []

        def refuse_unlink(path, *args, **kwargs):
            unlinked.append(path)
            raise OSError('injected unlink failure')

        with mock.patch.object(os, 'fsync',
                               side_effect=OSError('injected file_fsync failure')), \
                mock.patch.object(os, 'unlink', refuse_unlink):
            with self.assertRaisesRegex(OSError, 'injected file_fsync failure'):
                self.composite.save(str(self.path))
        self.assertTrue(unlinked, 'cleanup of the temporary file was not attempted')
        self.assert_previous()
        # Cleanup could not run, so exactly that attempt's file may remain.
        self.assertEqual([os.path.basename(str(unlinked[0]))],
                         leftovers(self.path))
        os.unlink(unlinked[0])
        print('  verified cleanup failure keeps the original error', flush=True)

    def test_uncomposed_save_is_rejected_without_touching_file(self):
        before_files = set(self.path.parent.iterdir())
        for empty in (self.module.CompositeRingBuilder(),
                      self.module.CompositeRingBuilder(['file1', 'file2'])):
            with self.assertRaisesRegex(ValueError, 'No composed ring to save'):
                empty.save(str(self.path))
        self.assert_previous()
        self.assertEqual(before_files, set(self.path.parent.iterdir()))

    def test_retry_after_failure_installs_update(self):
        self.composite.version += 1
        expected = self.composite.to_dict()
        with mock.patch.object(os, 'rename',
                               side_effect=OSError('injected replacement failure')):
            with self.assertRaisesRegex(OSError, 'injected replacement failure'):
                self.composite.save(str(self.path))
        self.assert_previous()
        self.composite.save(str(self.path))
        self.assert_installed(expected)
        self.assertEqual([], leftovers(self.path))


if __name__ == '__main__':
    raise SystemExit(run('Handled composite-builder save failures preserve the '
                         'old file and clean up temporary files.',
                         TestCompositeSaveFailureCleanup, __file__))
