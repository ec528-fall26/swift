#!/usr/bin/env python3
"""exp6 -- a partial composite-metadata write preserves the previous file.

Claim
-----
If serialization writes only part of the composite ring metadata and then
raises an error, the previous composite builder file stays byte-identical,
loads, and still composes the same ring. Upstream CompositeRingBuilder.save()
opens the live file with 'wt', so the expected result before the fix is a
failure with "File does not contain valid composite ring data". This is an
injected error, not a killed process, power loss, or concurrent writers.

Usage (from EC528_swift)
-----------------------
    python3 experiments/exp6_composite_partial_dump.py
    python3 experiments/exp6_composite_partial_dump.py --swift-src /path/to/Swift
    python3 experiments/exp6_composite_partial_dump.py --results-json exp6.json

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp6_composite_partial_dump.py
    echo "exit=$?"

If the checkout is at /opt/ec528-swift, use that location in the cd command
instead. Run from the checkout containing the fix; the script does not switch
branches or modify the implementation.

To keep a result file and set an explicit runtime bound on Lightsail:

    python3 experiments/exp6_composite_partial_dump.py \
        --results-json exp6-results.json \
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
from unittest import mock

from composite_save_support import CompositeCase, leftovers, run


class TestCompositePartialDump(CompositeCase):
    def loadable(self):
        try:
            self.module.CompositeRingBuilder.load(str(self.path))
        except Exception as exc:
            return 'NO (%s: %s)' % (type(exc).__name__, exc)
        return 'yes'

    def test_partial_dump_preserves_previous_file(self):
        self.composite.version += 1
        injected = []

        def partial_dump(obj, handle, **kwargs):
            payload = json.dumps(obj, **kwargs)
            handle.write(payload[:len(payload) // 2])
            handle.flush()
            injected.append('serialization')
            raise OSError('simulated failure during dump')

        print('  before       %6d bytes, loads: %s' % (
            len(self.before), self.loadable()), flush=True)
        with mock.patch.object(self.module.json, 'dump', partial_dump):
            with self.assertRaisesRegex(OSError, 'simulated failure during dump'):
                self.composite.save(str(self.path))
        self.assertTrue(injected, 'the intended failure point was not reached')
        after = self.path.read_bytes()
        print('  after        %6d bytes, loads: %s' % (
            len(after), self.loadable()), flush=True)
        print('  identical    %s' % (after == self.before), flush=True)
        self.assert_previous()
        self.assertEqual([], leftovers(self.path),
                         'the half-written temporary file was not removed')
        print('  verified partial serialization preserved the previous file',
              flush=True)


if __name__ == '__main__':
    raise SystemExit(run('A partial composite-metadata write preserves the '
                         'previous composite builder file.',
                         TestCompositePartialDump, __file__))
