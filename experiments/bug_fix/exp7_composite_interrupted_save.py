#!/usr/bin/env python3
"""exp7 -- process interruption around the composite-builder replacement.

Claim
-----
Before replacement, a killed composite save preserves the old composite file.
After replacement, the new composite file is complete and loadable, and both
compose the same ring. Four windows are covered: mid-serialization, after
serialization but before the file fsync, before the rename, and after it. This
is process-crash behavior, not power-loss durability, component-builder
saving, or concurrent-writer isolation.

Usage (from EC528_swift)
-----------------------
    python3 experiments/exp7_composite_interrupted_save.py
    python3 experiments/exp7_composite_interrupted_save.py --swift-src /path/to/Swift
    python3 experiments/exp7_composite_interrupted_save.py --results-json exp7.json

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp7_composite_interrupted_save.py
    echo "exit=$?"

If the checkout is at /opt/ec528-swift, use that location in the cd command
instead. Run from the checkout containing the fix; the script does not switch
branches or modify the implementation.

To keep a result file and set an explicit runtime bound on Lightsail:

    python3 experiments/exp7_composite_interrupted_save.py \
        --results-json exp7-results.json \
        --timeout 90
    echo "exit=$?"

Setup is provided by experiments/setup.sh on a clean Linux machine. The
prepared instance should already have .venv; keep composite_save_support.py
beside this script. The selected composite builder source path and revision are
printed before the checks, so verify they identify the intended checkout.

Exit codes: 0 = checks passed; 1 = check failed; 77 = could not run or timed out.
Linux, upstream dependencies, and a writable temporary filesystem are required.
The complete worker has a 90-second default deadline.
"""

import copy
import json
import multiprocessing
import os
import signal
import time
from unittest import mock

from composite_save_support import CompositeCase, import_composite, run


def interrupted_writer(source, path, stage, ready, release):
    module = import_composite(source)
    composite = module.CompositeRingBuilder.load(path)
    composite.version += 1

    def pause():
        ready.set()
        if not release.wait(45):
            raise RuntimeError('parent did not kill paused writer')

    def partial_dump(obj, handle, **kwargs):
        payload = json.dumps(obj, **kwargs)
        handle.write(payload[:len(payload) // 2])
        handle.flush()
        pause()

    actual_fsync = os.fsync

    def gated_fsync(fd):
        # The first fsync of a save is the temporary file's; pause before it.
        if stage == 'before_file_fsync':
            pause()
        actual_fsync(fd)

    actual_rename = os.rename

    def gated_rename(old, new):
        if stage == 'before_replace':
            pause()
        actual_rename(old, new)
        if stage == 'after_replace':
            pause()

    if stage == 'mid_serialization':
        with mock.patch.object(module.json, 'dump', partial_dump):
            composite.save(path)
    elif stage == 'before_file_fsync':
        with mock.patch.object(os, 'fsync', gated_fsync):
            composite.save(path)
    else:
        with mock.patch.object(os, 'rename', gated_rename):
            composite.save(path)


class TestCompositeInterruptedSave(CompositeCase):
    def check_interruption(self, stage):
        context = multiprocessing.get_context('spawn')
        ready, release = context.Event(), context.Event()
        process = context.Process(target=interrupted_writer,
                                  args=(self.source, str(self.path), stage,
                                        ready, release))
        expected = copy.deepcopy(self.previous)
        expected['version'] += 1
        process.start()
        try:
            deadline = time.monotonic() + 15
            while not ready.wait(0.1):
                self.assertTrue(process.is_alive(),
                                'writer exited without reaching %s' % stage)
                self.assertLess(time.monotonic(), deadline,
                                'writer did not reach %s' % stage)
            # Verify the exact state at the selected window before the kill.
            if stage != 'after_replace':
                self.assert_previous()
            process.kill()
            process.join(5)
            self.assertFalse(process.is_alive())
            self.assertEqual(-signal.SIGKILL, process.exitcode)
            if stage == 'after_replace':
                self.assert_installed(expected)
            else:
                self.assert_previous()
                # A stale temporary file from a killed writer must not prevent
                # a subsequent successful update. Hard kills need not clean it.
                self.composite.version += 2
                self.composite.save(str(self.path))
                self.assertEqual(
                    self.composite.to_dict(),
                    self.module.CompositeRingBuilder.load(str(self.path)).to_dict())
            print('  verified interruption stage: %s' % stage, flush=True)
        finally:
            if process.is_alive():
                process.kill()
            process.join(5)
            if not process.is_alive():
                process.close()

    def test_kill_mid_serialization(self):
        self.check_interruption('mid_serialization')

    def test_kill_before_file_fsync(self):
        self.check_interruption('before_file_fsync')

    def test_kill_before_replacement(self):
        self.check_interruption('before_replace')

    def test_kill_after_replacement(self):
        self.check_interruption('after_replace')


if __name__ == '__main__':
    raise SystemExit(run('Interrupted composite-builder saves leave a complete '
                         'old or new composite file.',
                         TestCompositeInterruptedSave, __file__))
