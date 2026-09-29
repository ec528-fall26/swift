#!/bin/bash
#
# setup.sh -- provision a clean Ubuntu machine to run this artifact's experiments.
#
# WHAT THIS IS FOR
# ----------------
# The artifact rubric is graded by *running the code*: "Every experimental claim
# reproduces on our machines", and the documentation criterion asks for setup that
# works "starting from a clean machine". This script is that path. Run it on a
# fresh Ubuntu 24.04 box and the two experiments in this directory will run.
#
# It is idempotent: re-running it is safe and fast. Nothing here touches the
# git checkout except to install into a virtualenv it creates at ./.venv.
#
# USAGE
# -----
#     git clone https://github.com/HankWang05/EC528_swift.git
#     cd EC528_swift
#     bash experiments/setup.sh
#
#     # then, in each new shell:
#     source .venv/bin/activate
#
# SCOPE
# -----
# This script prepares the machine and STOPS. It does not run the experiments,
# and it does not run any part of Swift's test suite -- the commands for those
# are in the design document's "Running the experiments" section, together with
# the expected runtime and the output that counts as a match. Keeping the two
# separate means a slow, system-modifying provisioning step is never confused
# with a bounded, repeatable measurement.
#
# Options:
#     --skip-xfs      do not create the XFS scratch filesystem (see WARNING below)
#     --venv PATH     virtualenv location (default: ./.venv in the repo root)
#     --check         verify an existing environment and exit; install nothing
#     -h, --help      this text
#
# EXIT CODES (the same 0 / 1 / 77 contract the experiments/ scripts use)
#     0   environment is ready (or, with --check, verified good)
#     1   something failed during provisioning -- see the message
#     77  refused to run: wrong OS, not root/sudo, or a hard prerequisite missing
#
# A 77 is never a pass and never a failure: it describes the environment, not the code.
#
# WARNING: THE XFS STEP MATTERS, AND ITS FAILURE IS SILENT
# ------------------------------------------------------
# Swift's suite needs a filesystem with large-extent xattr support. On ext4 the
# tests do not fail -- they SKIP, and upstream's README warns it is "a very large
# number". A run on ext4 therefore looks green while measuring almost nothing.
# This script mounts an XFS loopback image at /tmp and *verifies* that an 8 KB
# xattr can actually be written, because mounting XFS with agcount=1 does not by
# itself guarantee enough inline attribute space.
#
# Pass --skip-xfs only if you know /tmp is already XFS with large xattrs. The
# script still probes, and warns loudly if the probe fails.
#
set -o pipefail

EXIT_OK=0
EXIT_FAIL=1
EXIT_SKIP=77

# --------------------------------------------------------------------------
# configuration -- the pinned facts this environment is defined by
# --------------------------------------------------------------------------

readonly WANT_UBUNTU="24.04"
readonly WANT_PYTHON_MAJOR=3
readonly WANT_PYTHON_MINOR=12

readonly XFS_IMAGE="/opt/xfs-tmp.img"
readonly XFS_SIZE_MB=4096
readonly XATTR_PROBE_BYTES=8192

# From bindep.txt, for [platform:dpkg]. python3-venv and attr are additions:
# Ubuntu ships no ensurepip in the base python3, and `attr` is needed for the
# xattr probe below.
readonly APT_PACKAGES=(
    build-essential
    gcc
    liberasurecode-dev
    libffi-dev
    libxml2-dev
    libxslt1-dev
    libssl-dev
    memcached
    python3-dev
    python3-venv
    rsync
    xfsprogs
    attr
    curl
    git
    man-db
)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

SKIP_XFS=false
CHECK_ONLY=false
VENV_DIR="${REPO_ROOT}/.venv"

# --------------------------------------------------------------------------
# output helpers -- every line is written to be read by a stranger
# --------------------------------------------------------------------------

banner() { printf '\n=== %s ===\n' "$1"; }
say()    { printf '%s\n' "$1"; }
ok()     { printf '  [ ok ]  %s\n' "$1"; }
warn()   { printf '  [warn]  %s\n' "$1"; }
bad()    { printf '  [FAIL]  %s\n' "$1"; }

finish() {
    # finish <code> <message>
    local code="$1"; shift
    printf '\n=== result ===\n'
    printf '  %s\n' "$*"
    printf '  exit=%s\n' "$code"
    exit "$code"
}

# --------------------------------------------------------------------------
# argument parsing
# --------------------------------------------------------------------------

usage() {
    # Print the header comment block, stopping at the first non-comment line.
    sed -n '2,/^set -o pipefail/{/^set -o pipefail/d;p}' "${BASH_SOURCE[0]}" \
        | sed 's/^# \{0,1\}//'
}

while [ $# -gt 0 ]; do
    case "$1" in
        --skip-xfs) SKIP_XFS=true ;;
        --check)    CHECK_ONLY=true ;;
        --venv)     shift; VENV_DIR="$1" ;;
        -h|--help)  usage; exit "$EXIT_OK" ;;
        *)          bad "unknown option: $1"; usage; exit "$EXIT_SKIP" ;;
    esac
    shift
done

# --------------------------------------------------------------------------
# 1. host checks
# --------------------------------------------------------------------------

banner "host"

if [ "$(uname -s)" != "Linux" ]; then
    bad "this provisions a Linux machine; found $(uname -s)"
    say "  Swift declares 'Operating System :: POSIX :: Linux'. It will not run"
    say "  on Windows or macOS. Use an Ubuntu box or VM."
    finish "$EXIT_SKIP" "SKIP (not Linux)"
fi
ok "Linux"

if [ -r /etc/os-release ]; then
    # shellcheck disable=SC1091
    . /etc/os-release
    say "  distro       ${PRETTY_NAME:-unknown}"
    if [ "${ID:-}" != "ubuntu" ]; then
        warn "this script installs Ubuntu/Debian packages; '${ID:-unknown}' may differ"
        warn "the package list is from bindep.txt [platform:dpkg] -- adapt if apt is absent"
    elif [ "${VERSION_ID:-}" != "$WANT_UBUNTU" ]; then
        warn "tested on Ubuntu ${WANT_UBUNTU}; found ${VERSION_ID:-unknown}"
        warn "Python version is the thing that matters most -- see below"
    else
        ok "Ubuntu ${VERSION_ID}"
    fi
fi

# sudo / root
SUDO=""
if [ "$(id -u)" -ne 0 ]; then
    if command -v sudo >/dev/null 2>&1; then
        SUDO="sudo"
        # non-interactive probe, so we fail here rather than halfway through apt
        if ! sudo -n true 2>/dev/null; then
            warn "sudo will prompt for your password during the install steps"
        fi
    else
        bad "not root and sudo is not installed"
        finish "$EXIT_SKIP" "SKIP (no root, no sudo)"
    fi
fi
ok "privilege    ${SUDO:-root}"

# python version -- the single most common way this setup goes wrong
if ! command -v python3 >/dev/null 2>&1; then
    bad "python3 not found"
    say "  Install it first:  ${SUDO} apt-get install -y python3"
    finish "$EXIT_SKIP" "SKIP (no python3)"
fi

PY_VER="$(python3 -c 'import sys; print("%d.%d" % sys.version_info[:2])')"
PY_MAJOR="${PY_VER%%.*}"
PY_MINOR="${PY_VER##*.}"
say "  python3      ${PY_VER}  ($(command -v python3))"

if [ "$PY_MAJOR" -ne "$WANT_PYTHON_MAJOR" ] || [ "$PY_MINOR" -lt "$WANT_PYTHON_MINOR" ]; then
    warn "expected Python ${WANT_PYTHON_MAJOR}.${WANT_PYTHON_MINOR} (Ubuntu ${WANT_UBUNTU} default)"
    warn "Swift's setup.cfg allows >=3.7, but eventlet lags new CPython releases,"
    warn "so ${WANT_PYTHON_MAJOR}.${WANT_PYTHON_MINOR} is the version this artifact was verified on."
    warn "If the install below fails to build, that is the likely reason."
else
    ok "python3 ${PY_VER}"
fi

# disk
say "  disk on /    $(df -h / | awk 'NR==2 {print $4" free of "$2}')"

# --------------------------------------------------------------------------
# 2. system packages
# --------------------------------------------------------------------------

if [ "$CHECK_ONLY" = false ]; then
    banner "system packages"
    say "  installing ${#APT_PACKAGES[@]} packages from bindep.txt (this takes a few minutes)"

    if command -v apt-get >/dev/null 2>&1; then
        export DEBIAN_FRONTEND=noninteractive
        if ! $SUDO apt-get update -qq; then
            bad "apt-get update failed"
            finish "$EXIT_FAIL" "FAIL (apt-get update)"
        fi
        # --no-install-recommends keeps this close to the documented list
        if ! $SUDO apt-get install -y -qq --no-install-recommends "${APT_PACKAGES[@]}"; then
            bad "apt-get install failed"
            say "  Re-run without -qq to see which package is the problem:"
            say "    ${SUDO} apt-get install -y ${APT_PACKAGES[*]}"
            finish "$EXIT_FAIL" "FAIL (apt-get install)"
        fi
        ok "installed"
    else
        bad "apt-get not found; install these yourself, then re-run with --check:"
        printf '    %s\n' "${APT_PACKAGES[@]}"
        finish "$EXIT_SKIP" "SKIP (not a dpkg system)"
    fi
else
    banner "system packages"
    missing_pkgs=()
    for pkg in "${APT_PACKAGES[@]}"; do
        dpkg -s "$pkg" >/dev/null 2>&1 || missing_pkgs+=("$pkg")
    done
    if [ ${#missing_pkgs[@]} -eq 0 ]; then
        ok "all ${#APT_PACKAGES[@]} present"
    else
        bad "missing: ${missing_pkgs[*]}"
        say "  install with: ${SUDO} apt-get install -y ${missing_pkgs[*]}"
    fi
fi

# --------------------------------------------------------------------------
# 3. the XFS scratch filesystem -- the step whose failure is silent
# --------------------------------------------------------------------------

banner "xfs scratch filesystem"

probe_xattr() {
    # Returns 0 if an 8 KB xattr can be written to $1.
    local target="$1"
    local probe="${target}/.xattr-probe.$$"
    local payload
    payload="$(head -c "$XATTR_PROBE_BYTES" /dev/zero | tr '\0' 'A')"

    if ! touch "$probe" 2>/dev/null; then
        return 1
    fi
    if setfattr -n user.probe -v "$payload" "$probe" 2>/dev/null; then
        local got
        got="$(getfattr --only-values -n user.probe "$probe" 2>/dev/null | wc -c)"
        rm -f "$probe"
        [ "$got" -ge "$XATTR_PROBE_BYTES" ]
    else
        rm -f "$probe"
        return 1
    fi
}

FSTYPE="$(df -T /tmp 2>/dev/null | awk 'NR==2 {print $2}')"
say "  /tmp fstype  ${FSTYPE:-unknown}"

if [ "$SKIP_XFS" = true ]; then
    say "  --skip-xfs given; not creating a scratch filesystem"
elif [ "$FSTYPE" = "xfs" ]; then
    ok "/tmp is already XFS"
else
    say "  /tmp is not XFS -- creating a ${XFS_SIZE_MB} MB loopback image"
    say ""
    say "  WHY: Swift's suite needs large-extent xattrs. On ext4 it does not fail,"
    say "  it SKIPS -- so a green run on ext4 has measured almost nothing."

    if [ "$CHECK_ONLY" = true ]; then
        warn "--check: not modifying the system. Run without --check to create it."
    else
        # create the image if it is missing
        if [ ! -f "$XFS_IMAGE" ]; then
            if ! $SUDO dd if=/dev/zero of="$XFS_IMAGE" bs=1M count="$XFS_SIZE_MB" status=none; then
                bad "could not create ${XFS_IMAGE} (out of disk?)"
                finish "$EXIT_FAIL" "FAIL (dd)"
            fi
            ok "created ${XFS_IMAGE} (${XFS_SIZE_MB} MB)"
        else
            ok "${XFS_IMAGE} already exists"
        fi

        # format if it does not already carry an XFS signature
        if ! $SUDO xfs_admin -l "$XFS_IMAGE" >/dev/null 2>&1; then
            # agcount=1 keeps the first AG large; -m crc=1 is the modern default
            if ! $SUDO mkfs.xfs -f -m crc=1 -d agcount=1 "$XFS_IMAGE" >/dev/null; then
                bad "mkfs.xfs failed"
                finish "$EXIT_FAIL" "FAIL (mkfs.xfs)"
            fi
            ok "formatted XFS"
        else
            ok "already an XFS filesystem"
        fi

        # preserve whatever is in /tmp, then mount over it
        if ! mountpoint -q /tmp; then
            $SUDO mkdir -p /tmp.orig
            $SUDO cp -a /tmp/. /tmp.orig/ 2>/dev/null || true
        fi

        if ! $SUDO mount -o loop,noatime "$XFS_IMAGE" /tmp; then
            # already mounted on a re-run is not an error
            if mountpoint -q /tmp; then
                ok "/tmp already mounted"
            else
                bad "mount failed"
                finish "$EXIT_FAIL" "FAIL (mount)"
            fi
        else
            ok "mounted at /tmp"
        fi

        $SUDO chmod 1777 /tmp

        # persist it
        if ! grep -q "^${XFS_IMAGE} " /etc/fstab 2>/dev/null; then
            if echo "${XFS_IMAGE} /tmp xfs loop,noatime 0 0" | $SUDO tee -a /etc/fstab >/dev/null; then
                ok "added to /etc/fstab (survives reboot)"
            else
                warn "could not add to /etc/fstab -- /tmp will revert to ${FSTYPE:-its old fs} on reboot"
            fi
        else
            ok "already in /etc/fstab"
        fi
    fi
fi

# the probe -- run it either way, because mounting XFS is not the same as
# having the attribute space
if [ -d /tmp ] && [ -w /tmp ]; then
    if probe_xattr /tmp; then
        ok "8 KB xattr on /tmp works  (the suite will not mass-skip)"
    else
        bad "8 KB xattr on /tmp FAILED"
        say ""
        say "  The suite will silently skip a large number of tests without this."
        say "  Checks, in order:"
        say "    1. is 'attr' installed?      dpkg -s attr"
        say "    2. is /tmp really XFS?       df -T /tmp"
        say "    3. rebuild the image larger: ${SUDO} rm -f ${XFS_IMAGE} && re-run this script"
        XATTR_OK=false
    fi
else
    bad "/tmp is missing or not writable"
    XATTR_OK=false
fi

# --------------------------------------------------------------------------
# 4. virtualenv
# --------------------------------------------------------------------------

banner "virtualenv"

if [ "$CHECK_ONLY" = true ]; then
    if [ -x "${VENV_DIR}/bin/python" ]; then
        ok "exists at ${VENV_DIR}"
    else
        bad "no virtualenv at ${VENV_DIR}"
        finish "$EXIT_FAIL" "FAIL (venv absent)"
    fi
else
    if [ -x "${VENV_DIR}/bin/python" ]; then
        ok "reusing ${VENV_DIR}"
    else
        if ! python3 -m venv "$VENV_DIR"; then
            bad "python3 -m venv failed"
            say "  On Ubuntu this is nearly always a missing python3-venv:"
            say "    ${SUDO} apt-get install -y python3-venv"
            finish "$EXIT_FAIL" "FAIL (venv creation)"
        fi
        ok "created ${VENV_DIR}"
    fi
fi

VPY="${VENV_DIR}/bin/python"
if [ ! -x "$VPY" ]; then
    bad "no python at ${VPY}"
    finish "$EXIT_FAIL" "FAIL (venv broken)"
fi
say "  python       $("$VPY" -c 'import sys; print(sys.version.split()[0])')"

# --------------------------------------------------------------------------
# 5. python dependencies
# --------------------------------------------------------------------------

if [ "$CHECK_ONLY" = false ]; then
    banner "python dependencies"

    say "  upgrading pip / setuptools / wheel"
    "$VPY" -m pip install --quiet --upgrade pip setuptools wheel || {
        bad "pip upgrade failed"
        finish "$EXIT_FAIL" "FAIL (pip upgrade)"
    }
    ok "packaging tools current"

    # pbr FIRST, and on its own.
    # swift/__init__.py does `import pbr.version` at module scope, so pbr is a
    # hard import-time dependency of the package. pbr shells out to git to derive
    # the version, which is why upstream runs the suite as `cd test/unit && pytest`.
    # Without it, nothing under swift.* imports at all.
    say "  installing pbr (import-time dependency of swift/__init__.py)"
    "$VPY" -m pip install --quiet pbr || {
        bad "pbr install failed"
        finish "$EXIT_FAIL" "FAIL (pbr)"
    }
    ok "pbr"

    say "  installing requirements.txt + test-requirements.txt"
    if ! "$VPY" -m pip install --quiet -r "${REPO_ROOT}/requirements.txt"; then
        bad "runtime requirements failed"
        say "  pyeclib builds against liberasurecode-dev; lxml against libxml2-dev/libxslt1-dev."
        say "  Confirm those installed, then re-run."
        finish "$EXIT_FAIL" "FAIL (requirements.txt)"
    fi
    ok "requirements.txt"

    if ! "$VPY" -m pip install --quiet -r "${REPO_ROOT}/test-requirements.txt"; then
        bad "test requirements failed"
        finish "$EXIT_FAIL" "FAIL (test-requirements.txt)"
    fi
    ok "test-requirements.txt"

    # editable install so `import swift` resolves to THIS checkout and not to
    # anything else on the machine
    say "  installing the checkout in editable mode"
    if ! ( cd "$REPO_ROOT" && "$VPY" -m pip install --quiet -e . --no-deps ); then
        bad "editable install failed"
        finish "$EXIT_FAIL" "FAIL (pip install -e .)"
    fi
    ok "swift installed from ${REPO_ROOT}"
else
    banner "python dependencies"
fi

# --------------------------------------------------------------------------
# 6. verification -- the part that actually decides whether this box is ready
# --------------------------------------------------------------------------

banner "verify"

FAILED=false

# 6a. the tree is what the scripts expect
if [ -d "${REPO_ROOT}/test/unit" ]; then
    ok "test/unit present"
else
    bad "${REPO_ROOT}/test/unit missing -- is this the Swift fork?"
    FAILED=true
fi

if [ -f "${REPO_ROOT}/test/unit/common/ring/test_builder.py" ]; then
    ok "test/unit/common/ring/test_builder.py present"
else
    bad "test_builder.py missing -- exp1 will exit 77"
    FAILED=true
fi

for script in exp0_unittests.py exp1_run_partial_dump_test.py; do
    if [ -f "${REPO_ROOT}/experiments/${script}" ]; then
        ok "experiments/${script}"
    else
        bad "experiments/${script} missing"
        FAILED=true
    fi
done

# 6b. the import surface -- this is where a missing dep shows up, and it shows
#     up as "149 errors" later if you skip it here
say ""
say "  import checks (from the checkout, because pbr needs a git cwd):"

# Emit delimited records rather than parsing human-readable output: `eventlet`
# prints an EventletDeprecationWarning on import, which would otherwise be
# mistaken for a dependency line and reported as a spurious failure.
IMPORT_REPORT="$( cd "$REPO_ROOT" && "$VPY" - <<'PY' 2>/dev/null
import warnings
warnings.simplefilter("ignore")

# swift is reported as its resolved *path*, because the shadowing check below
# needs it. Every other dependency reports a plain OK / MISSING -- reporting a
# path for those would read as a failure.
try:
    import swift
    print("ROW\tswift\t%s" % swift.__file__)
except Exception as exc:
    print("ROW\tswift\tMISSING -- %s: %s" % (type(exc).__name__, exc))

for _name in ("pbr", "eventlet", "xattr", "pyeclib", "pytest", "swiftclient"):
    try:
        __import__(_name)
        print("ROW\t%s\tOK" % _name)
    except Exception as exc:
        print("ROW\t%s\tMISSING -- %s: %s" % (_name, type(exc).__name__, exc))
PY
)" || true

SWIFT_PATH=""
while IFS=$'\t' read -r tag name value; do
    [ "$tag" = "ROW" ] || continue
    case "$name" in
        swift)
            # reported as a path, not "OK", so extract it for the check below
            SWIFT_PATH="$value"
            if [ -f "$value" ]; then
                ok "swift -> $value"
            else
                bad "swift  $value"
                FAILED=true
            fi
            ;;
        *)
            if [ "$value" = "OK" ]; then
                ok "$name"
            else
                bad "$name  $value"
                FAILED=true
            fi
            ;;
    esac
done <<< "$IMPORT_REPORT"

# the shadowing check: an installed swift would make every result describe the
# wrong code
if [ -n "$SWIFT_PATH" ] && [ -f "$SWIFT_PATH" ]; then
    case "$SWIFT_PATH" in
        "${REPO_ROOT}"/*) ok "swift resolves inside this checkout" ;;
        *)
            bad "swift resolves to ${SWIFT_PATH}, OUTSIDE ${REPO_ROOT}"
            say "  An installed copy is shadowing the checkout; results would describe the wrong code."
            say "  Fix: ${VPY} -m pip install -e ${REPO_ROOT} --no-deps"
            FAILED=true
            ;;
    esac
fi

# 6c. environment summary. This script NEVER runs the experiments -- it only
#     prepares the machine. Running them is the reader's next step, documented in
#     the design document, and deliberately kept separate so that provisioning
#     (slow, system-modifying) and measuring (bounded, repeatable) cannot be
#     confused with one another.
REV="$( cd "$REPO_ROOT" && git describe --tags --always --dirty 2>/dev/null || echo "unknown" )"
say ""
say "  revision      ${REV:-unknown}"

if [ "${XATTR_OK:-true}" != true ]; then
    say ""
    warn "the xattr probe failed, so the unit suite will report many SKIPPED tests"
    warn "and a 'green' run would not mean much -- fix the xattr issue above first"
fi

banner "environment ready"
say "  Nothing has been run. This script only prepares the machine."
say ""
say "  Activate the virtualenv in each new shell:"
say "    source ${VENV_DIR#"${REPO_ROOT}/"}/bin/activate"
say ""
say "  The experiments are documented in the design document's"
say "  'Running the experiments' section, which states the command, the"
say "  expected runtime, and what output counts as a match. In brief:"
say ""
say "    python experiments/exp1_run_partial_dump_test.py      # one test, seconds"
say "    python experiments/exp0_unittests.py --results-json baseline.json --timeout 3600"
say ""
say "  Both take --swift-src if the tree holding test/unit is not this repo;"
say "  it is discovered automatically in the usual layout."
say "  Run exp0 inside tmux: it takes 20-40 minutes."
say "  Their exit codes are 0 pass / 1 ran-and-failed / 77 could-not-run."

if [ "$FAILED" = true ]; then
    finish "$EXIT_FAIL" "FAIL (one or more checks above are not satisfied)"
fi

finish "$EXIT_OK" "environment ready"
