#!/usr/bin/env python3
"""exp2 -- process interruption around the individual-builder replacement.

Claim
-----
Before replacement, a killed save preserves the old builder. After replacement,
the new builder is complete and loadable. This is process-crash behavior, not
power-loss durability, composite saving, or concurrent-writer isolation.

Usage (from EC528_swift)
-----------------------
    python3 experiments/exp2_interrupted_save.py
    python3 experiments/exp2_interrupted_save.py --swift-src /path/to/Swift
    python3 experiments/exp2_interrupted_save.py --results-json exp2.json

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp2_interrupted_save.py
    echo "exit=$?"

If the checkout is at /opt/ec528-swift, as in exp0's example, use that
location in the cd command instead. Run from the checkout containing the fix;
the script does not switch branches or modify the implementation.

To keep a result file and set an explicit runtime bound on Lightsail:

    python3 experiments/exp2_interrupted_save.py \
        --results-json exp2-results.json \
        --timeout 90
    echo "exit=$?"

Setup is provided by experiments/setup.sh on a clean Linux machine. The
prepared instance should already have .venv; keep atomic_save_support.py
beside this script. The selected builder source path and revision are printed
before the checks, so verify they identify the intended checkout.

Exit codes: 0 = checks passed; 1 = check failed; 77 = could not run or timed out.
Linux, upstream dependencies, and a writable temporary filesystem are required.
The complete worker has a 90-second default deadline. See experiments/README.md.
"""

import copy
import multiprocessing
import os
import pickle
import signal
import time
from unittest import mock

from atomic_save_support import BuilderCase, import_builder, run


def interrupted_writer(source, path, stage, ready, release):
    module = import_builder(source)
    builder = module.RingBuilder.load(path)
    builder.version += 1

    def pause():
        ready.set()
        if not release.wait(45):
            raise RuntimeError('parent did not kill paused writer')

    def partial_dump(obj, handle, protocol=2):
        payload = pickle.dumps(obj, protocol=protocol)
        handle.write(payload[:len(payload) // 2])
        handle.flush()
        pause()

    actual_rename = os.rename

    def gated_rename(old, new):
        if stage == 'before_replace':
            pause()
        actual_rename(old, new)
        if stage == 'after_replace':
            pause()

    if stage == 'mid_serialization':
        with mock.patch.object(module.pickle, 'dump', partial_dump):
            builder.save(path)
    else:
        with mock.patch.object(os, 'rename', gated_rename):
            builder.save(path)


class TestInterruptedSave(BuilderCase):
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
                loaded = self.module.RingBuilder.load(str(self.path))
                self.assertEqual(expected, loaded.to_dict())
                for partition in range(2 ** loaded.part_power):
                    self.assertEqual(self.builder.get_part_devices(partition),
                                     loaded.get_part_devices(partition))
            else:
                self.assert_previous()
                # A stale temporary file from a killed writer must not prevent
                # a subsequent successful update. Hard kills need not clean it.
                self.builder.version += 2
                self.builder.save(str(self.path))
                self.assertEqual(self.builder.to_dict(),
                                 self.module.RingBuilder.load(str(self.path)).to_dict())
            print('  verified interruption stage: %s' % stage, flush=True)
        finally:
            if process.is_alive():
                process.kill()
            process.join(5)
            if not process.is_alive():
                process.close()

    def test_kill_mid_serialization(self):
        self.check_interruption('mid_serialization')

    def test_kill_before_replacement(self):
        self.check_interruption('before_replace')

    def test_kill_after_replacement(self):
        self.check_interruption('after_replace')


if __name__ == '__main__':
    raise SystemExit(run('Interrupted individual-builder saves leave a complete '
                         'old or new builder.', TestInterruptedSave, __file__))
