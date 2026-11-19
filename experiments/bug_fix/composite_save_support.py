"""Shared runner and real-Swift fixtures for experiments 6--10.

No Swift implementation is substituted: import_composite verifies the selected
checkout. Only the explicitly documented failure points are patched by tests.
This module is self-contained; it does not import the RingBuilder experiments'
atomic_save_support.py.
"""

import argparse
import importlib
import json
import os
from pathlib import Path
import platform
import signal
import subprocess
import sys
import tempfile
import time
import unittest


def banner(title):
    print('\n=== %s ===' % title, flush=True)


def finish(code, message):
    print('\nRESULT: %s\nexit=%s' % (message, code), flush=True)
    return code


def import_composite(source):
    source = Path(source).resolve()
    sys.path.insert(0, str(source))
    module = importlib.import_module('swift.common.ring.composite_builder')
    if (Path(module.__file__).resolve() !=
            source / 'swift/common/ring/composite_builder.py'):
        raise ImportError('an installed Swift shadows the selected checkout')
    return module


def make_composite(module, directory):
    """Compose two saved component builders that occupy distinct regions."""
    directory = Path(directory)
    builder_files = []
    for region in range(2):
        builder = module.RingBuilder(6, 3, 0)
        for index in range(3):
            port = 6000 + 10 * region + index
            builder.add_dev(dict(
                id=3 * region + index, region=region, zone=index, weight=1,
                device='sd%s' % index, ip='127.0.%s.1' % region, port=port,
                replication_ip='127.0.%s.1' % region, replication_port=port,
                meta='composite-save-experiment'))
        builder.rebalance(seed=12345)
        builder_file = directory / ('component-%s.builder' % region)
        builder.save(str(builder_file))
        builder_files.append(str(builder_file))
    composite = module.CompositeRingBuilder(builder_files)
    composite.compose()
    return composite


def placement(ring_data):
    """The composed partition-to-device table and device list, comparably."""
    return ([list(row) for row in ring_data._replica2part2dev_id],
            [dict(dev) if dev else None for dev in ring_data.devs])


def saved_form(composite, path):
    """The metadata a successful save of this composite must install."""
    metadata = composite.to_dict()
    metadata['saved_path'] = os.path.abspath(str(path))
    metadata['serialization_version'] = 1
    return json.loads(json.dumps(metadata))


def leftovers(path):
    """Files beside path whose names start with its name (temporary files)."""
    path = Path(path)
    return sorted(candidate.name for candidate in path.parent.iterdir()
                  if candidate.name.startswith(path.name) and candidate != path)


class CompositeCase(unittest.TestCase):
    source = None

    def setUp(self):
        self.module = import_composite(self.source)
        self.temp = tempfile.TemporaryDirectory(prefix='swift-composite-save-')
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'test-composite.json'
        make_composite(self.module, self.temp.name).save(str(self.path))
        self.before = self.path.read_bytes()
        # Compare loaded states on both sides: load() normalizes builder paths,
        # so a freshly composed instance is not the right reference.
        self.composite = self.module.CompositeRingBuilder.load(str(self.path))
        self.previous = self.composite.to_dict()
        # compose() bumps the version of the instance it runs on, so take the
        # reference placement from a separate load.
        self.placement = placement(self.module.CompositeRingBuilder.load(
            str(self.path)).compose(force=True))

    def assert_installed(self, expected):
        loaded = self.module.CompositeRingBuilder.load(str(self.path))
        self.assertEqual(expected, loaded.to_dict())
        # Ring-position lookups: the reloaded composite must still compose the
        # same partition-to-device assignment from its component builders.
        self.assertEqual(self.placement, placement(loaded.compose(force=True)))

    def assert_previous(self):
        self.assertEqual(self.before, self.path.read_bytes())
        self.assert_installed(self.previous)


class ObservedFile:
    """Delegate actual file I/O, observing only explicit flush and close."""

    def __init__(self, handle, before_flush=None, after_flush=None, after_close=None):
        self.handle = handle
        self.before_flush = before_flush
        self.after_flush = after_flush
        self.after_close = after_close

    def __getattr__(self, name):
        return getattr(self.handle, name)

    def __enter__(self):
        self.handle.__enter__()
        return self

    def __exit__(self, *args):
        result = self.handle.__exit__(*args)
        if self.after_close:
            self.after_close()
        return result

    def flush(self):
        if self.before_flush:
            self.before_flush()
        self.handle.flush()
        if self.after_flush:
            self.after_flush()


def revision(source):
    info = {}
    for key, arguments in (
            ('branch', ['branch', '--show-current']),
            ('commit', ['rev-parse', 'HEAD']),
            ('modified_source', ['status', '--porcelain', '--', 'swift', 'test'])):
        try:
            result = subprocess.run(['git', '-C', str(source), *arguments],
                                    capture_output=True, text=True, timeout=5)
            info[key] = result.stdout.strip() if result.returncode == 0 else None
        except (OSError, subprocess.SubprocessError):
            info[key] = None
    return info


def run(description, case, script):
    parser = argparse.ArgumentParser(description=description)
    parser.add_argument('--swift-src', default=None,
                        help='Swift source checkout (default: SWIFT_SRC or this repo)')
    parser.add_argument('--timeout', type=int, default=90,
                        help='worker wall-clock limit in seconds (default: 90)')
    parser.add_argument('--results-json', help='optional JSON result file')
    parser.add_argument('--_worker', action='store_true', help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.timeout <= 0:
        return finish(77, 'SKIP (timeout must be positive)')
    source = Path(args.swift_src or os.environ.get('SWIFT_SRC') or
                  Path(script).resolve().parents[1]).expanduser().resolve()

    if not args._worker:
        print('Measuring: %s' % description, flush=True)
        banner('environment')
        print('  python       %s (%s)' % (platform.python_version(), sys.executable))
        print('  platform     %s\n  cwd          %s' % (sys.platform, Path.cwd()))
    if not sys.platform.startswith('linux'):
        return finish(77, 'SKIP (Linux required; no Windows shims)')
    if not ((source / 'swift/common/ring/composite_builder.py').is_file() and
            (source / 'test/unit').is_dir()):
        return finish(77, 'SKIP (selected source is not an upstream Swift checkout)')

    if args._worker:
        banner('dependencies')
        try:
            module = import_composite(source)
        except Exception as exc:
            print('  import failed: %s: %s' % (type(exc).__name__, exc))
            return finish(77, 'SKIP (Swift cannot be imported from selected source)')
        print('  composite    %s' % Path(module.__file__).resolve(), flush=True)
        case.source = str(source)
        banner('run')
        suite = unittest.defaultTestLoader.loadTestsFromTestCase(case)
        started = time.monotonic()
        result = unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(suite)
        banner('result')
        print('  elapsed      %.3fs\n  tests run    %s\n  failures     %s\n'
              '  errors       %s\n  skipped      %s' % (
                  time.monotonic() - started, result.testsRun,
                  len(result.failures), len(result.errors), len(result.skipped)))
        if not result.testsRun or result.testsRun == len(result.skipped):
            return finish(77, 'SKIP (no checks executed)')
        return finish(0 if result.wasSuccessful() else 1,
                      'PASS (claim checks passed)' if result.wasSuccessful()
                      else 'FAIL (claim checks failed; inspect named checks)')

    banner('swift checkout')
    info = revision(source)
    print('  source       %s' % source)
    for key, value in info.items():
        print('  %-14s %s' % (key, value if value is not None else 'unavailable'))
    print('  deadline     %ss' % args.timeout, flush=True)
    command = [sys.executable, str(Path(script).resolve()), '--swift-src',
               str(source), '--_worker']
    started = time.monotonic()
    try:
        process = subprocess.Popen(command, cwd=str(source),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.STDOUT, text=True,
                                   errors='replace', start_new_session=True)
    except OSError as exc:
        return finish(77, 'SKIP (worker could not start: %s)' % exc)
    try:
        output, _ = process.communicate(timeout=args.timeout)
        code = process.returncode
        # Signals and unexpected interpreter exit codes are not passing tests.
        if code not in (0, 1, 77):
            code = 77
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        output, _ = process.communicate()
        code = 77
        output += '\nRESULT: SKIP (worker deadline exceeded)\n'
    print(output, end='', flush=True)
    report = dict(claim=description, source=str(source), revision=info,
                  elapsed_s=round(time.monotonic() - started, 3), exit_code=code,
                  result={0: 'PASS', 1: 'FAIL', 77: 'SKIP'}[code], output=output)
    if args.results_json:
        destination = Path(args.results_json).expanduser().resolve()
        destination.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
        print('  results JSON %s' % destination)
    return code
