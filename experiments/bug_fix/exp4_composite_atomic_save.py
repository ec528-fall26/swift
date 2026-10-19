#!/usr/bin/env python3
"""A partial composite-metadata serialization preserves the previous valid metadata.
This does not test process termination or ring-position lookups.

Run with the upstream Swift checkout on PYTHONPATH and its dependencies installed.
Exit codes: 0 = passed, 1 = test failure, 77 = Linux required.
"""

import json
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
from swift.common.ring.composite_builder import CompositeRingBuilder


@unittest.skipUnless(sys.platform.startswith('linux'), 'Linux is authoritative')
class TestCompositeAtomicSave(unittest.TestCase):

    def setUp(self):
            self.temp = tempfile.TemporaryDirectory()
            self.addCleanup(self.temp.cleanup)
            self.path = str(Path(self.temp.name) / 'ring.builder')
            self.rb = RingBuilder(6, 3, 0)
            self.rb.save(self.path)
            self.before = Path(self.path).read_bytes()

    def test_partial_composite_save_preserves_original(self):
            # Valid persisted metadata; no data movement is needed to test save().
            composite = CompositeRingBuilder([self.path])
            composite.components = [{'id': self.rb.id,
                                     'version': self.rb.version,
                                     'replicas': self.rb.replicas}]
            path = str(Path(self.temp.name) / 'ring.composite')
            composite.save(path)
            before = Path(path).read_bytes()
            expected = CompositeRingBuilder.load(path).to_dict()

            def partial_json(obj, handle, *args, **kwargs):
                payload = json.dumps(obj)
                handle.write(payload[:len(payload) // 2])
                handle.flush()
                raise OSError('injected partial JSON write')

            with mock.patch('swift.common.ring.composite_builder.json.dump',
                            partial_json):
                with self.assertRaises(OSError):
                    composite.save(path)
            self.assertEqual(before, Path(path).read_bytes())
            self.assertEqual(expected, CompositeRingBuilder.load(path).to_dict())


if __name__ == '__main__':
    unittest.main(verbosity=2)
