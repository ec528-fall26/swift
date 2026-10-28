"""Shared runner and real-Swift fixtures for experiments 2--4.

No Swift implementation is substituted: import_builder verifies the selected
checkout. Only the explicitly documented failure points are patched by tests.
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


def import_builder(source):
    source = Path(source).resolve()
    sys.path.insert(0, str(source))
    module = importlib.import_module('swift.common.ring.builder')
    if Path(module.__file__).resolve() != source / 'swift/common/ring/builder.py':
        raise ImportError('an installed Swift shadows the selected checkout')
    return module


def make_builder(module):
    builder = module.RingBuilder(6, 3, 0)
    for index in range(3):
        builder.add_dev(dict(
            id=index, region=1, zone=index, weight=1, device='sd%s' % index,
            ip='127.0.0.1', port=6000 + index,
            replication_ip='127.0.0.1', replication_port=6000 + index,
            meta='atomic-save-experiment'))
    builder.rebalance(seed=12345)
    return builder


class BuilderCase(unittest.TestCase):
    source = None

    def setUp(self):
        self.module = import_builder(self.source)
        self.temp = tempfile.TemporaryDirectory(prefix='swift-atomic-save-')
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'test.builder'
        make_builder(self.module).save(str(self.path))
        self.before = self.path.read_bytes()
        # Loading may normalize device metadata. Compare loaded states on both
        # sides, rather than comparing a raw add_dev fixture with a loaded one.
        self.builder = self.module.RingBuilder.load(str(self.path))
        self.previous = self.builder.to_dict()

    def assert_previous(self):
        self.assertEqual(self.before, self.path.read_bytes())
        loaded = self.module.RingBuilder.load(str(self.path))
        self.assertEqual(self.previous, loaded.to_dict())
        for partition in range(2 ** loaded.part_power):
            self.assertEqual(self.builder.get_part_devices(partition),
                             loaded.get_part_devices(partition))
        return loaded


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
    if not ((source / 'swift/common/ring/builder.py').is_file() and
            (source / 'test/unit').is_dir()):
        return finish(77, 'SKIP (selected source is not an upstream Swift checkout)')

    if args._worker:
        banner('dependencies')
        try:
            module = import_builder(source)
        except Exception as exc:
            print('  import failed: %s: %s' % (type(exc).__name__, exc))
            return finish(77, 'SKIP (Swift cannot be imported from selected source)')
        print('  builder.py   %s' % Path(module.__file__).resolve(), flush=True)
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
