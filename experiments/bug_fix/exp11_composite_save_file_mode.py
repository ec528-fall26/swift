#!/usr/bin/env python3
"""exp11 -- composite-builder saves keep the composite file's permission bits.

Claim
-----
Replacing the composite file must not make its permission bits more
permissive. An update may retain or remove existing permissions, but must not
add permissions that the original file did not grant. A first save must not
grant permissions beyond those of an ordinary file created in the same
directory under the current umask. For example, changing 0644 to 0600 is
acceptable, while changing 0644 to 0664 is not.

This checks a side effect of the atomic-save fix: both upstream and the patched
tree are expected to pass, even if the patched tree produces more restrictive
permissions. The checks cover the final composite file, not temporary-file
access during saving. Ownership changes and ACLs are out of scope.

Usage (from EC528_swift)
-----------------------
    python3 experiments/exp11_composite_save_file_mode.py
    python3 experiments/exp11_composite_save_file_mode.py --swift-src /path/to/Swift
    python3 experiments/exp11_composite_save_file_mode.py --results-json exp11.json

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp11_composite_save_file_mode.py
    echo "exit=$?"

If the checkout is at /opt/ec528-swift, use that location in the cd command
instead. Run from the checkout containing the fix; the script does not switch
branches or modify the implementation.

To keep a result file and set an explicit runtime bound on Lightsail:

    python3 experiments/exp11_composite_save_file_mode.py \
        --results-json exp11-results.json \
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

import os
import stat

from composite_save_support import CompositeCase, make_composite, run


def mode_of(path):
    return stat.S_IMODE(os.stat(str(path)).st_mode)


class TestCompositeSaveFileMode(CompositeCase):
    def assert_update_does_not_add_permissions(self, before, after):
        added = after & ~before
        self.assertEqual(
            0, added,
            'saving made file permissions more generous: '
            '%04o -> %04o (added %04o)' % (before, after, added))

    def test_update_keeps_existing_file_mode(self):
        for before in (0o600, 0o640, 0o644, 0o660, 0o664):
            with self.subTest(initial_mode='%04o' % before):
                os.chmod(str(self.path), before)
                self.composite.version += 1
                self.composite.save(str(self.path))
                after = mode_of(self.path)
                print(
                    '  mode after update %04o (was %04o)' % (
                        after, before),
                    flush=True)
                self.assert_update_does_not_add_permissions(before, after)

    def test_first_save_does_not_exceed_umask_mode(self):
        directory = self.path.parent / 'first-save'
        directory.mkdir()
        probe = directory / 'probe.json'
        with open(str(probe), 'wt') as handle:
            handle.write('{}')
        expected = mode_of(probe)
        path = directory / 'first-save.json'
        make_composite(self.module, directory).save(str(path))
        actual = mode_of(path)
        print(
            '  mode after first save %04o (plain create gives %04o)' % (
                actual, expected),
            flush=True)
        self.assert_update_does_not_add_permissions(expected, actual)

if __name__ == '__main__':
    raise SystemExit(run('Composite-builder saves keep the composite file '
                         'permission bits.', TestCompositeSaveFileMode, __file__))
