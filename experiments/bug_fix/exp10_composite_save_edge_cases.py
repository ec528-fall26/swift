#!/usr/bin/env python3
"""exp10 -- composite-builder saves handle paths, repetition and live readers.

Claim
-----
A save given a bare relative file name writes its temporary file in the current
directory and installs the file there with an absolute saved_path. A save to a
new path leaves the original composite untouched. Many consecutive saves leave
no temporary files behind. A reader process loading the composite while saves
run only ever sees a complete old or new file. The persisted metadata keeps
upstream's format. This does not test power loss, multiple concurrent writers,
or network filesystems.

Usage (from EC528_swift)
-----------------------
    python3 experiments/exp10_composite_save_edge_cases.py
    python3 experiments/exp10_composite_save_edge_cases.py --swift-src /path/to/Swift
    python3 experiments/exp10_composite_save_edge_cases.py --results-json exp10.json

On the EC528 Lightsail box (SwiftBugFix):

    # Connect using SSH in the AWS Lightsail dashboard, then:
    cd /home/ubuntu/EC528_swift
    source .venv/bin/activate
    python3 experiments/exp10_composite_save_edge_cases.py
    echo "exit=$?"

If the checkout is at /opt/ec528-swift, use that location in the cd command
instead. Run from the checkout containing the fix; the script does not switch
branches or modify the implementation.

To keep a result file and set an explicit runtime bound on Lightsail:

    python3 experiments/exp10_composite_save_edge_cases.py \
        --results-json exp10-results.json \
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
import multiprocessing
import os
from pathlib import Path
import queue
import tempfile
import time
from unittest import mock

from composite_save_support import (CompositeCase, ObservedFile,
                                    import_composite, leftovers, run,
                                    saved_form)

SAVES_UNDER_READER = 200
REPEATED_SAVES = 25


def concurrent_reader(source, path, ready, stop, results):
    module = import_composite(source)
    loads, errors, versions = 0, [], set()
    while not stop.is_set():
        try:
            versions.add(module.CompositeRingBuilder.load(path).version)
            loads += 1
        except Exception as exc:
            errors.append('%s: %s' % (type(exc).__name__, exc))
            if len(errors) >= 20:
                break
        ready.set()
    results.put((loads, errors, sorted(versions)))


class TestCompositeSaveEdgeCases(CompositeCase):
    def observe_temporary(self, candidates):
        real_temporary = tempfile.NamedTemporaryFile

        def observed_temporary(*args, **kwargs):
            handle = real_temporary(*args, **kwargs)
            candidates.append(Path(handle.name).resolve())
            return ObservedFile(handle)

        return mock.patch.object(self.module, 'NamedTemporaryFile',
                                 observed_temporary, create=True)

    def test_relative_path_save_uses_current_directory(self):
        directory = self.path.parent / 'relative'
        directory.mkdir()
        self.addCleanup(os.chdir, os.getcwd())
        os.chdir(str(directory))
        self.composite.version += 1
        expected = self.composite.to_dict()
        candidates = []
        with self.observe_temporary(candidates):
            self.composite.save('relative-composite.json')
        target = directory / 'relative-composite.json'
        self.assertTrue(target.is_file())
        self.assertEqual(1, len(candidates))
        self.assertEqual(directory.resolve(), candidates[0].parent)
        saved = json.loads(target.read_text())
        self.assertTrue(os.path.isabs(saved['saved_path']))
        self.assertEqual(target.resolve(), Path(saved['saved_path']).resolve())
        self.assertEqual(
            expected, self.module.CompositeRingBuilder.load(str(target)).to_dict())
        self.assertEqual([], leftovers(target))
        self.assert_previous()
        print('  verified relative save in %s' % directory, flush=True)

    def test_save_to_new_path_leaves_original_untouched(self):
        directory = self.path.parent / 'elsewhere'
        directory.mkdir()
        target = directory / 'copy-composite.json'
        self.composite.version += 1
        expected = self.composite.to_dict()
        candidates = []
        with self.observe_temporary(candidates):
            self.composite.save(str(target))
        self.assertEqual(1, len(candidates))
        self.assertEqual(directory.resolve(), candidates[0].parent)
        self.assertEqual(saved_form(self.composite, target),
                         json.loads(target.read_text()))
        self.assertEqual(
            expected, self.module.CompositeRingBuilder.load(str(target)).to_dict())
        self.assertEqual([], leftovers(target))
        self.assert_previous()

    def test_repeated_saves_leave_no_temporary_files(self):
        before_files = set(self.path.parent.iterdir())
        for _ in range(REPEATED_SAVES):
            self.composite.version += 1
            self.composite.save(str(self.path))
            self.assertEqual(
                self.composite.version,
                self.module.CompositeRingBuilder.load(str(self.path)).version)
        self.assertEqual([], leftovers(self.path))
        self.assertEqual(before_files, set(self.path.parent.iterdir()))
        print('  verified %s consecutive saves' % REPEATED_SAVES, flush=True)

    def test_saved_metadata_keeps_upstream_format(self):
        self.composite.version += 1
        self.composite.save(str(self.path))
        saved = json.loads(self.path.read_text())
        self.assertEqual({'components', 'component_builder_files', 'version',
                          'saved_path', 'serialization_version'}, set(saved))
        self.assertEqual(saved_form(self.composite, self.path), saved)
        self.assertEqual(1, saved['serialization_version'])
        self.assertEqual(os.path.abspath(str(self.path)), saved['saved_path'])

    def test_concurrent_reader_only_sees_complete_files(self):
        context = multiprocessing.get_context('spawn')
        ready, stop = context.Event(), context.Event()
        results = context.Queue()
        process = context.Process(target=concurrent_reader,
                                  args=(self.source, str(self.path), ready,
                                        stop, results))
        first = self.composite.version
        process.start()
        try:
            self.assertTrue(ready.wait(15), 'reader did not start loading')
            for _ in range(SAVES_UNDER_READER):
                self.composite.version += 1
                self.composite.save(str(self.path))
            time.sleep(0.2)
            stop.set()
            try:
                loads, errors, versions = results.get(timeout=15)
            except queue.Empty:
                self.fail('reader did not report its results')
            process.join(5)
            self.assertEqual([], errors, 'reader saw an incomplete composite file')
            self.assertGreater(loads, 0)
            self.assertTrue(set(versions) <= set(range(first,
                                                       self.composite.version + 1)))
            self.assertEqual([], leftovers(self.path))
            print('  verified %s reader loads across %s saves, versions %s..%s' % (
                loads, SAVES_UNDER_READER, versions[0], versions[-1]), flush=True)
        finally:
            stop.set()
            if process.is_alive():
                process.kill()
            process.join(5)
            if not process.is_alive():
                process.close()


if __name__ == '__main__':
    raise SystemExit(run('Composite-builder saves handle relative paths, new '
                         'paths, repetition and concurrent readers.',
                         TestCompositeSaveEdgeCases, __file__))
