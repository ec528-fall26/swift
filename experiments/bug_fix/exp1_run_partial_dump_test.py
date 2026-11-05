#!/usr/bin/env python3
"""Run exactly one Swift unit test -- and only that one.

Target
------
    test/unit/common/ring/test_builder.py
        TestRingBuilder.test_save_partial_dump_does_nothing

What the test asserts
---------------------
``RingBuilder.save()`` must be atomic: if the write dies half-way (disk full,
process killed, ENOSPC part-way through ``pickle.dump``), the builder file on
disk must still be the *previous* good file -- not a half-written one.  The
test simulates the crash by monkeypatching ``pickle.dump`` so it writes half the
payload and then raises ``OSError``, and then checks that the file on disk is
byte-identical to the pre-crash copy and still loadable.

Current master is not atomic: ``save()`` opens the builder file with
``open(path, 'wb')``, which truncates it before a single byte is written, and
streams the pickle straight into the live file.  So **the expected result before
the fix is a failure**; the same script should go green once ``save()`` writes to
a temp file and renames it into place.

Usage
-----
    python exp1_run_partial_dump_test.py                       # autodetect the tree
    python exp1_run_partial_dump_test.py --swift-src E:/GitHub/EC528_swift
    SWIFT_SRC=/vagrant/swift python exp1_run_partial_dump_test.py

Exit codes (the same 0 / 1 / 77 contract the experiments/ scripts use)
    0   the test PASSED    -> save() is atomic (fix is in, or never needed)
    1   the test FAILED    -> the bug is still present (expected today)
    77  COULD NOT RUN      -> no tree, wrong tree, missing deps, test absent

A 77 is never a pass and never a failure: it says something about this
environment, not about the code.

Where it runs
-------------
Linux (the VSAIO VM) is the authoritative environment.  The test itself is pure
file I/O plus pickle, so it also runs on the Windows host -- see
``install_windows_shim()`` below.  A Windows run is a convenience for the inner
dev loop and is never evidence about production.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import time
import types
import unittest
from pathlib import Path

EXIT_OK = 0
EXIT_FAIL = 1
EXIT_SKIP = 77

TEST_CLASS = "TestRingBuilder"
TEST_NAME = "test_save_partial_dump_does_nothing"
TEST_REL = Path("test") / "unit" / "common" / "ring" / "test_builder.py"

# The tree holding test/unit is discovered independently of where this script
# lives, because there are two supported layouts:
#
#   fork-as-both (EC528) -- this repo IS a Swift fork, so it holds test/unit and
#                           experiments/ in one tree. Repo-relative, tried first.
#   VSAIO (two clones)   -- the course repo and the Swift source are separate
#                           checkouts, e.g. /vagrant/swift.
#
# Repo-relative candidates resolve against the SCRIPT ROOT, not the cwd, so the
# result does not depend on where the caller happened to stand. $SWIFT_SRC is
# read at call time (not at import time) so exporting it always takes effect.
REPO_RELATIVE_SWIFT_DIRS = (
    ".",                                     # fork-as-both: repo holds test/unit
    "swift",
)

CWD_RELATIVE_SWIFT_DIRS = (
    "/vagrant/swift",                        # the VSAIO default
    "/opt/swift",
    "~/swift",
    "../swift",                              # VSAIO host layout
    "swift",
)

# swift/__init__.py does `import pbr.version`, so pbr is a hard import-time dep
# for the package even though this test never touches versioning.
RUNTIME_REQUIREMENTS = ("pbr", "eventlet")


# --------------------------------------------------------------------------
# output helpers
# --------------------------------------------------------------------------

def banner(text: str) -> None:
    print(f"\n=== {text} ===")


def say(text: str = "") -> None:
    print(text, flush=True)


def finish(code: int, headline: str) -> int:
    say(f"\nRESULT: {headline}")
    return code


# --------------------------------------------------------------------------
# Windows host shim
# --------------------------------------------------------------------------
# Swift declares "Operating System :: POSIX :: Linux".  `swift.common.utils`
# imports fcntl / grp / pwd / resource at module scope and binds
# libc.getifaddrs, so on Windows the whole swift.common.ring package refuses to
# import -- even though the ring *builder* is pure computation and touches none
# of it.  The shims below are no-ops; they exist so the one ring test can be run
# locally.  Nothing here provides real locking, uid lookup or rlimit handling.
#
# Derived from ring-visualization/_swiftsrc/_win_stubs/_unixstub.py in the
# assistant workspace; kept inline so this script stays self-contained when it
# is copied into the repo or into the VM.

_IPADDRS_SHIM = r'''
import re
import socket

IPV6_RE = re.compile(r"^\[(?P<address>.*)\](:(?P<port>[0-9]+))?$")


def is_valid_ipv4(ip):
    try:
        socket.inet_pton(socket.AF_INET, ip)
    except AttributeError:
        try:
            socket.inet_aton(ip)
        except socket.error:
            return False
        return ip.count('.') == 3
    except socket.error:
        return False
    return True


def is_valid_ipv6(ip):
    try:
        socket.inet_pton(socket.AF_INET6, ip)
    except socket.error:
        return False
    return True


def is_valid_ip(ip):
    return is_valid_ipv4(ip) or is_valid_ipv6(ip)


def expand_ipv6(address):
    return socket.inet_ntop(socket.AF_INET6,
                            socket.inet_pton(socket.AF_INET6, address))


def whataremyips(ring_ip=None):
    """Best-effort replacement for the getifaddrs-based enumeration."""
    if ring_ip:
        try:
            _, _, _, _, sockaddr = socket.getaddrinfo(
                ring_ip, None, 0, socket.SOCK_STREAM, 0, socket.AI_NUMERICHOST)[0]
            if sockaddr[0] not in ('0.0.0.0', '::'):
                return [ring_ip]
        except socket.gaierror:
            pass
    addresses = []
    try:
        infos = socket.getaddrinfo(socket.gethostname(), None, socket.AF_UNSPEC,
                                   socket.SOCK_STREAM, 0, socket.AI_PASSIVE)
    except socket.gaierror:
        infos = []
    for info in infos:
        addr = info[4][0].split('%', 1)[0]
        if addr not in addresses:
            addresses.append(addr)
    return addresses


def parse_socket_string(socket_string, default_port):
    if not socket_string:
        return None, default_port
    if is_valid_ipv6(socket_string):
        return socket_string, default_port
    if socket_string.startswith('['):
        match = IPV6_RE.match(socket_string)
        if not match:
            return None, None
        addr, port = match.groups()
        try:
            port = int(port) if port else default_port
        except ValueError:
            return None, None
        return addr, port
    parts = socket_string.rsplit(':', 1)
    if len(parts) == 2 and parts[1].isdigit():
        return parts[0], int(parts[1])
    elif len(parts) == 2 and ':' in parts[0]:
        return socket_string, default_port
    return (parts[0], default_port) if parts[0] else (None, default_port)
'''


class _ShimFinder:
    """Serve a Windows-safe swift.common.utils.ipaddrs.

    The real module raises part-way through executing (msvcrt has no
    getifaddrs), and Python drops a module from sys.modules when its body
    raises, so it cannot be repaired after the fact -- it has to be intercepted
    before it loads.
    """

    TARGET = "swift.common.utils.ipaddrs"

    def find_spec(self, fullname, path=None, target=None):
        if fullname != self.TARGET or sys.platform != "win32":
            return None
        return importlib.util.spec_from_loader(fullname, _ShimLoader())


class _ShimLoader:
    def create_module(self, spec):
        return None

    def exec_module(self, module):
        exec(compile(_IPADDRS_SHIM, "<ipaddrs-windows-shim>", "exec"),
             module.__dict__)


def install_windows_shim() -> None:
    """Register no-op stand-ins for Swift's Unix-only imports.  Idempotent."""
    if sys.platform != "win32":
        return

    import ctypes
    import ctypes.util

    # swift.common.utils does ctypes.CDLL(find_library('c')), which is
    # CDLL(None) on Windows and raises TypeError.  Point it at a DLL that always
    # loads so Swift's own "symbol missing -> no-op" fallback takes over.
    real_find = ctypes.util.find_library

    def find_library(name):
        found = real_find(name)
        if found is None and name in ("c", "libc", "m"):
            return "msvcrt"
        return found

    ctypes.util.find_library = find_library

    def noop(*args, **kwargs):
        return None

    def stub(name, **attrs):
        if name in sys.modules:
            return
        module = types.ModuleType(name)
        module.__doc__ = "Windows shim installed by exp1_run_partial_dump_test.py"
        for key, value in attrs.items():
            setattr(module, key, value)
        sys.modules[name] = module

    stub("fcntl", F_DUPFD=0, F_GETFD=1, F_SETFD=2, F_GETFL=3, F_SETFL=4,
         F_GETLK=5, F_SETLK=6, F_SETLKW=7, FD_CLOEXEC=1, O_NONBLOCK=2048,
         O_NDELAY=2048, O_APPEND=1024, F_RDLCK=0, F_WRLCK=1, F_UNLCK=2,
         LOCK_SH=1, LOCK_EX=2, LOCK_NB=4, LOCK_UN=8, F_FULLFSYNC=51,
         flock=noop, lockf=noop, fcntl=noop, ioctl=noop)

    class struct_group(tuple):
        n_fields = 4
        n_sequence_fields = 4

        def __new__(cls, name="", passwd="", gid=0, mem=()):
            return super().__new__(cls, (name, passwd, gid, list(mem)))

        name = property(lambda self: self[0])
        gr_name = name
        passwd = property(lambda self: self[1])
        gr_passwd = passwd
        gid = property(lambda self: self[2])
        gr_gid = gid
        mem = property(lambda self: self[3])

    stub("grp", struct_group=struct_group,
         getgrgid=lambda gid: struct_group("", "", gid, ()),
         getgrnam=lambda name: struct_group(name, "", 0, ()),
         getgrall=lambda: [])

    class struct_passwd(tuple):
        n_fields = 7
        n_sequence_fields = 7

        def __new__(cls, name="", passwd="", uid=0, gid=0, gecos="", home="",
                    shell=""):
            return super().__new__(cls,
                                   (name, passwd, uid, gid, gecos, home, shell))

        pw_name = property(lambda self: self[0])
        name = pw_name
        pw_passwd = property(lambda self: self[1])
        passwd = pw_passwd
        pw_uid = property(lambda self: self[2])
        uid = pw_uid
        pw_gid = property(lambda self: self[3])
        gid = pw_gid
        pw_gecos = property(lambda self: self[4])
        gecos = pw_gecos
        pw_dir = property(lambda self: self[5])
        home = pw_dir
        pw_shell = property(lambda self: self[6])
        shell = pw_shell

    stub("pwd", struct_passwd=struct_passwd,
         getpwuid=lambda uid: struct_passwd("", "", uid, 0, "", "", ""),
         getpwnam=lambda name: struct_passwd(name, "", 0, 0, "", "", ""),
         getpwall=lambda: [])

    stub("resource", getpagesize=lambda: 4096, RLIMIT_CPU=0, RLIMIT_FSIZE=1,
         RLIMIT_DATA=2, RLIMIT_STACK=3, RLIMIT_CORE=4, RLIMIT_RSS=5,
         RLIMIT_NPROC=6, RLIMIT_NOFILE=7, RLIMIT_MEMLOCK=8, RLIMIT_AS=9,
         RLIM_INFINITY=-1, getrlimit=lambda res: (-1, -1), setrlimit=noop)

    # swift.common.utils computes
    #   O_TMPFILE = getattr(os, 'O_TMPFILE', 0o20000000 | os.O_DIRECTORY)
    # at import time; neither constant exists on Windows and the fallback
    # expression itself raises.  Supply the Linux values.
    for name, value in (("O_DIRECTORY", 0o200000),
                        ("O_TMPFILE", 0o20000000 | 0o200000)):
        if not hasattr(os, name):
            setattr(os, name, value)

    finder = _ShimFinder()
    if not any(isinstance(f, _ShimFinder) for f in sys.meta_path):
        sys.meta_path.insert(0, finder)


# --------------------------------------------------------------------------
# locating the tree
# --------------------------------------------------------------------------

def script_root() -> Path:
    """This repo's root -- the experiments/ -> .. hop, where this script lives."""
    return Path(__file__).resolve().parents[1]


def find_swift_src(explicit: str | None) -> tuple[Path | None, list[Path]]:
    """Locate the Swift checkout (the tree holding test/unit)."""
    tried: list[Path] = []

    def consider(raw: str, base: Path | None = None) -> Path | None:
        if not raw:
            return None
        try:
            path = Path(raw).expanduser()
            if not path.is_absolute() and base is not None:
                path = base / path
            path = path.resolve()
        except (OSError, RuntimeError):
            return None
        if path in tried:
            return None
        tried.append(path)
        return path if (path / "test" / "unit").is_dir() else None

    if explicit:
        # An explicit path is authoritative: if it is wrong we report that,
        # rather than quietly substituting another tree and testing the wrong
        # code. But say so, because a typo otherwise looks like "no tree".
        found = consider(explicit)
        if found is None:
            say("")
            say(f"  --swift-src {explicit} does not contain test/unit.")
            repo_self = script_root()
            if (repo_self / "test" / "unit").is_dir():
                say(f"  This repo does contain test/unit:  {repo_self}")
                say(f"  Retry without --swift-src, or:  --swift-src {repo_self}")
        return found, tried

    found = consider(os.environ.get("SWIFT_SRC", ""))
    if found:
        return found, tried

    # 1. This repo. The EC528 layout: a Swift fork holding test/unit.
    root = script_root()
    for rel in REPO_RELATIVE_SWIFT_DIRS:
        found = consider(rel, base=root)
        if found:
            return found, tried

    # 2. The VSAIO / two-checkout layout.
    for rel in CWD_RELATIVE_SWIFT_DIRS:
        found = consider(rel)
        if found:
            return found, tried

    return None, tried


def detect_revision(src: Path) -> dict:
    """Commit / branch / dirty flag, so a run is attributable to a revision."""
    info = {"commit": None, "branch": None, "dirty": None}
    if not shutil.which("git"):
        return info

    def git(*args: str) -> str | None:
        try:
            out = subprocess.run(["git", "-C", str(src), *args],
                                 capture_output=True, text=True, timeout=30,
                                 check=False)
        except (OSError, subprocess.SubprocessError):
            return None
        return out.stdout.strip() if out.returncode == 0 else None

    info["commit"] = git("rev-parse", "--short", "HEAD")
    info["branch"] = git("rev-parse", "--abbrev-ref", "HEAD")
    porcelain = git("status", "--porcelain", "--", "swift", "test")
    if porcelain is not None:
        info["dirty"] = bool(porcelain)
    return info


def check_runtime_deps() -> list[tuple[str, str]]:
    import importlib
    missing = []
    for name in RUNTIME_REQUIREMENTS:
        try:
            importlib.import_module(name)
        except Exception as exc:
            missing.append((name, f"{type(exc).__name__}: {exc}"))
    return missing


def load_test_module(test_file: Path):
    """Import test_builder.py by path.

    Deliberately *not* as part of the `test.unit` package: test/unit/__init__.py
    drags in xattr and a pile of suite-wide plumbing that this single test does
    not need, and importing it by path also means no pytest/tox configuration
    has to be in play.
    """
    spec = importlib.util.spec_from_file_location("swift_test_builder",
                                                  str(test_file))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot build an import spec for {test_file}")
    module = importlib.util.module_from_spec(spec)
    sys.modules["swift_test_builder"] = module
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# the post-mortem, run only when the test fails
# --------------------------------------------------------------------------

def diagnose() -> dict:
    """Reproduce the scenario outside unittest and report what actually broke.

    The assertion failure alone says "something differs".  This separates the
    two things the test checks -- did the crash damage the file, and does the
    in-memory builder still compare equal to the loaded one -- because they can
    fail independently.

    Returns a dict of findings for the verdict to use.
    """
    import pickle
    from unittest import mock

    from swift.common import ring
    from swift.common.ring import builder as builder_mod

    banner("diagnosis -- what actually happened to the file")
    tmpdir = tempfile.mkdtemp(prefix="partial-dump-")
    builder_file = os.path.join(tmpdir, "test_save.builder")
    good_copy = os.path.join(tmpdir, "good_copy.builder")

    rb = ring.RingBuilder(8, 3, 1)
    for dev in ({'id': 0, 'region': 0, 'zone': 0, 'weight': 1,
                 'ip': '127.0.0.0', 'port': 10000, 'device': 'sda1',
                 'meta': 'meta0'},
                {'id': 1, 'region': 0, 'zone': 1, 'weight': 1,
                 'ip': '127.0.0.1', 'port': 10001, 'device': 'sdb1',
                 'meta': 'meta1'},
                {'id': 2, 'region': 0, 'zone': 2, 'weight': 2,
                 'ip': '127.0.0.2', 'port': 10002, 'device': 'sdc1',
                 'meta': 'meta2'},
                {'id': 3, 'region': 0, 'zone': 3, 'weight': 2,
                 'ip': '127.0.0.3', 'port': 10003, 'device': 'sdd1'}):
        rb.add_dev(dev)
    rb.rebalance()
    rb.save(builder_file)

    with open(builder_file, "rb") as fh:
        good_bytes = fh.read()
    # Keep an untouched copy: the "damage" question is about what the failed
    # save did to builder_file, and the "compare equal" question needs a file
    # that is known good.
    with open(good_copy, "wb") as fh:
        fh.write(good_bytes)
    say(f"  before the crash   {len(good_bytes):>6} bytes, "
        f"loads: {_loadable(builder_file)}")

    def half_dump(obj, file, protocol=None, **kwargs):
        payload = pickle.dumps(obj, protocol=protocol or 2)
        file.write(payload[:len(payload) // 2])
        file.flush()
        raise OSError("simulated failure during dump")

    try:
        with mock.patch.object(builder_mod.pickle, "dump", half_dump):
            rb.save(builder_file)
        say("  save() returned normally -- the injected failure never fired")
    except OSError as exc:
        say(f"  save() raised      {type(exc).__name__}: {exc}")

    with open(builder_file, "rb") as fh:
        after_bytes = fh.read()
    intact = after_bytes == good_bytes
    say(f"  after the crash    {len(after_bytes):>6} bytes, "
        f"loads: {_loadable(builder_file)}")
    say(f"  bytes identical    {intact}   <- what the test checks first")

    # Second question, checked separately so it cannot be confused with the
    # first: does the in-memory builder equal a freshly loaded good file?
    findings = {"bytes_identical": intact, "dicts_equal": None,
                "diff_keys": []}
    try:
        built = rb.to_dict()
        reloaded = ring.RingBuilder.load(good_copy).to_dict()
    except Exception as exc:
        say(f"  to_dict comparison  unavailable ({type(exc).__name__}: {exc})")
        shutil.rmtree(tmpdir, ignore_errors=True)
        return findings

    findings["dicts_equal"] = built == reloaded
    if built != reloaded:
        findings["diff_keys"] = sorted(
            k for k in set(built) | set(reloaded) if built.get(k) != reloaded.get(k)
        )
    say(f"  to_dict equal      {findings['dicts_equal']}"
        f"   <- what the test checks last")

    if findings["diff_keys"]:
        say("")
        say("  Differing top-level keys: " + ", ".join(findings["diff_keys"]))
        key = findings["diff_keys"][0]
        if key == "devs":
            say("  RingBuilder.load() runs setdefault() on every dev to fill in")
            say("  replication_ip / replication_port for builders that predate")
            say("  those fields.  The in-memory builder never had them -- add_dev()")
            say("  does not set them -- so the two dicts differ by exactly those")
            say("  two keys per device, no matter how save() is implemented.")
            say("  e.g. built:    " + _dev_keys(built))
            say("       reloaded: " + _dev_keys(reloaded))

    shutil.rmtree(tmpdir, ignore_errors=True)
    return findings


def _dev_keys(builder_dict: dict) -> str:
    for dev in builder_dict.get("devs") or []:
        if dev:
            return str(sorted(dev))
    return "(no devices)"


def _loadable(path: str) -> str:
    from swift.common import ring
    try:
        ring.RingBuilder.load(path)
    except Exception as exc:
        return f"NO ({type(exc).__name__})"
    return "yes"


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------

def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Run exactly TestRingBuilder." + TEST_NAME,
    )
    parser.add_argument("--swift-src", default=None,
                        help="Swift checkout to test (default: autodetect)")
    parser.add_argument("--no-diagnose", action="store_true",
                        help="skip the post-mortem when the test fails")
    args = parser.parse_args(argv)

    # Importing swift pulls in eventlet, which prints a multi-line deprecation
    # notice on every import.  It has nothing to do with this test and the wall
    # of text buries the result.  Filter it before anything imports eventlet --
    # note the leading newline in the message, hence the (?s) prefix.
    import warnings
    warnings.filterwarnings("ignore", message=r"(?s).*Eventlet is deprecated")

    say("Measuring: whether RingBuilder.save() is atomic, via one unit test")
    say(f"           test/unit/common/ring/test_builder.py::{TEST_CLASS}."
        f"{TEST_NAME}")

    banner("environment")
    say(f"  python       {sys.version.split()[0]} ({sys.executable})")
    say(f"  platform     {sys.platform}")
    say(f"  cwd          {Path.cwd()}")
    if sys.platform == "win32":
        say("  NOTE         Windows host run -- the Unix imports are shimmed as")
        say("               no-ops.  Fine for this file-I/O test, but it is not")
        say("               evidence about production; the VM is authoritative.")

    # -- locate the tree ----------------------------------------------------
    src, tried = find_swift_src(args.swift_src)
    banner("swift checkout")
    if src is None:
        say("  NOT FOUND -- no tree containing test/unit.")
        say("")
        say("  This repo IS a fork of Swift, so the tree is normally this repo")
        say(f"  itself ({script_root()}).")
        say("  Pass --swift-src PATH, or set $SWIFT_SRC, if it is elsewhere.")
        if tried:
            say("  Tried:")
            for path in tried:
                say(f"    {path}")
        return finish(EXIT_SKIP, "SKIP (no Swift checkout found)")

    test_file = src / TEST_REL
    rev = detect_revision(src)
    say(f"  source       {src}")
    say(f"  branch       {rev['branch'] or 'n/a'}")
    say(f"  commit       {rev['commit'] or 'n/a'}")
    if rev["dirty"]:
        say("  swift/ + test/ MODIFIED -- this run describes your working tree,")
        say("                       not the commit above")
    say(f"  test file    {test_file}"
        + ("" if test_file.is_file() else "   <-- MISSING"))

    if not test_file.is_file():
        say("")
        say(f"  {TEST_REL} does not exist in this checkout.")
        return finish(EXIT_SKIP, "SKIP (test file not present)")

    # -- dependencies -------------------------------------------------------
    missing_deps = check_runtime_deps()
    banner("dependencies")
    for name in RUNTIME_REQUIREMENTS:
        hit = next((err for dep, err in missing_deps if dep == name), None)
        say(f"  {name:<10} {'MISSING -- ' + hit if hit else 'OK'}")
    if missing_deps:
        say("")
        say("  swift/__init__.py imports pbr at import time, so without it")
        say("  nothing under swift.* can be imported at all.")
        say("  Fix (Windows host):   <python> -m pip install pbr")
        say("  Fix (VM):             reinstallswift")
        return finish(EXIT_SKIP, "SKIP (missing import-time dependencies)")

    # -- import the tree ----------------------------------------------------
    install_windows_shim()
    sys.path.insert(0, str(src))

    # swift/__init__.py resolves its version through pbr, which shells out to
    # git and only works when the cwd is inside the checkout (this is why
    # upstream runs `cd test/unit && pytest`).  Do the same, so the import does
    # not depend on where the caller happened to be standing.
    os.chdir(src)

    banner("import")
    say(f"  cwd        {Path.cwd()}  (moved into the checkout for pbr)")
    try:
        from swift.common import ring
        from swift.common.ring import builder as builder_mod
    except Exception as exc:
        say(f"  importing swift.common.ring failed: {type(exc).__name__}: {exc}")
        say("")
        say("  On Linux this normally means the VM is not provisioned.  On a")
        say("  Windows host it means the shim above did not cover an import --")
        say("  see ring-visualization/_swiftsrc/_win_stubs/_unixstub.py.")
        return finish(EXIT_SKIP, "SKIP (swift.common.ring will not import)")

    resolved = Path(ring.__file__).resolve()
    say(f"  swift.common.ring -> {resolved}")
    if src.resolve() not in resolved.parents:
        say("")
        say("  That is NOT the tree we asked for -- an installed copy of swift is")
        say("  shadowing it, and the result would describe the wrong code.")
        return finish(EXIT_SKIP, "SKIP (an installed swift shadows the checkout)")

    save_src = Path(builder_mod.__file__).resolve()
    say(f"  builder.py        -> {save_src}")

    # -- load just this test ------------------------------------------------
    banner("collect")
    try:
        module = load_test_module(test_file)
    except Exception as exc:
        say(f"  importing the test module failed: {type(exc).__name__}: {exc}")
        return finish(EXIT_SKIP, "SKIP (test module will not import)")

    case_cls = getattr(module, TEST_CLASS, None)
    if case_cls is None or not hasattr(case_cls, TEST_NAME):
        say(f"  {TEST_CLASS}.{TEST_NAME} not found in {test_file.name}")
        say("")
        say("  Nothing was executed.  Check the name, or that your branch has")
        say("  the test.")
        return finish(EXIT_SKIP, "SKIP (test not found)")

    say(f"  selected  {TEST_CLASS}.{TEST_NAME}  (1 test, nothing else)")

    # -- run ----------------------------------------------------------------
    banner("run")
    suite = unittest.TestSuite([case_cls(TEST_NAME)])
    started = time.monotonic()
    result = unittest.TextTestRunner(stream=sys.stdout, verbosity=2).run(suite)
    elapsed = time.monotonic() - started

    # -- verdict ------------------------------------------------------------
    banner("result")
    say(f"  tests run    {result.testsRun}")
    say(f"  failures     {len(result.failures)}")
    say(f"  errors       {len(result.errors)}")
    say(f"  elapsed      {elapsed:.1f}s")

    if result.testsRun != 1:
        return finish(EXIT_SKIP, "SKIP (the test did not actually run)")

    if result.wasSuccessful():
        say("")
        say("  save() survived a crash mid-dump: the previous builder file was")
        say("  left intact and still loads.  RingBuilder.save() is atomic.")
        return finish(EXIT_OK, "PASS (the test passed -- save() is atomic)")

    findings = None
    if not args.no_diagnose:
        try:
            findings = diagnose()
        except Exception as exc:                      # diagnosis is optional
            say(f"\n  (diagnosis unavailable: {type(exc).__name__}: {exc})")

    # Two independent things can make this test fail.  Say which one it is --
    # "the test failed" on its own sends you to the wrong file.
    if findings and findings["bytes_identical"]:
        say("")
        say("  The atomicity check PASSED: the failed save left the previous")
        say("  builder file byte-for-byte intact and still loadable, so save()")
        say("  already replaces the file atomically.")
        say("")
        say("  The test still fails on its LAST assertion --")
        say("      self.assertEqual(rb.to_dict(), loaded_rb.to_dict())")
        say("  -- and that comparison cannot succeed as written.  RingBuilder")
        say("  .load() runs setdefault('replication_ip'/'replication_port') over")
        say("  every device to fill in fields that older builder files predate,")
        say("  but add_dev() never puts those keys on the in-memory builder, so")
        say("  the loaded dict always carries two extra keys per device.")
        say("")
        say("  This is a bug in the test, not in save().  Either add")
        say("  'replication_ip'/'replication_port' to the dev dicts in the test")
        say("  (test_save_dev_id_bytes above does exactly that), or compare")
        say("  against a freshly loaded copy of the good file instead of against")
        say("  the live in-memory builder.")
        return finish(EXIT_FAIL, "FAIL (test failed -- but NOT on atomicity; "
                                 "see above)")

    say("")
    say("  This is the EXPECTED result before the fix.  RingBuilder.save() is")
    say("  not atomic: it opens the live builder file with mode 'wb', which")
    say("  truncates it, then writes the pickle straight into it.  A crash")
    say("  part-way through leaves a corrupt builder file behind, and every")
    say("  swift-ring-builder command afterwards fails to load it.")
    say("")
    say("  The fix is the usual atomic-replace dance: write to a temp file in")
    say("  the same directory, flush + fsync, then os.rename() over the target")
    say("  (same filesystem, so rename is atomic) -- cleaning the temp file up")
    say("  on the error path.  Re-run this script to confirm it goes green.")
    return finish(EXIT_FAIL, "FAIL (the bug is still present -- test detected it)")


if __name__ == "__main__":
    sys.exit(main())
