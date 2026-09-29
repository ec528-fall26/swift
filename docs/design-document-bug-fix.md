# Design Document

*Due in the `demo-2` branch, updated for `demo-3` and `final-demo`.
**The TA will run this document.** If they cannot follow it, you lose the points.*

## 1. Architecture

What you actually built — not what you originally planned. Include a diagram.

## 2. Design decisions

The choices that mattered, the alternatives, and why you chose as you did.
Include the things that did not work.

## 3. Setup

### Provisioning: `experiments/setup.sh`

The whole environment is built by one script. On a clean Ubuntu 24.04 machine:

```bash
git clone https://github.com/HankWang05/EC528_swift/opt/ec528-swift
cd /opt/ec528-swift
bash experiments/setup.sh
```

It is idempotent — re-running is safe and fast, and the only thing it writes
into the checkout is the virtualenv at `./.venv`. It provisions the tree it sits
in (its parent directory), which is why it is run from this repository: that is
also the checkout you pass to the experiments as `--swift-src`.

What it does, in order:

| Step | What happens |
| --- | --- |
| Host checks | Requires Linux (exit `77` otherwise), reports the distro, finds `sudo`, checks `python3` and free disk. |
| System packages | Installs the 17 `[platform:dpkg]` packages from Swift's `bindep.txt`: `build-essential`, `gcc`, `liberasurecode-dev`, `libffi-dev`, `libxml2-dev`, `libxslt1-dev`, `libssl-dev`, `memcached`, `python3-dev`, `python3-venv`, `rsync`, `xfsprogs`, `attr`, `curl`, `git`, `man-db`. |
| XFS scratch filesystem | Creates a 4 GB XFS loopback image at `/opt/xfs-tmp.img`, mounts it on `/tmp`, and **verifies** an 8 KB xattr can actually be written there. |
| Virtualenv | Creates `./.venv`, or reuses it if it already exists. |
| Python dependencies | Upgrades pip/setuptools/wheel; installs `pbr` on its own first (it is an import-time dependency of `swift/__init__.py`); then `requirements.txt` and `test-requirements.txt`; then `pip install -e . --no-deps` so `import swift` resolves to this checkout. |
| Verify | Checks that `test/unit`, `test/unit/common/ring/test_builder.py` and both experiment scripts exist; imports `swift`, `pbr`, `eventlet`, `xattr`, `pyeclib`, `pytest`, `swiftclient`; and fails if an installed `swift` is shadowing the checkout. |
| Next steps | Prints the revision, the exit code each experiment is expected to produce, and how to activate the venv. |

The versions this is pinned to: **Ubuntu 24.04**, **Python 3.12**, XFS for
`/tmp`. Swift's `setup.cfg` allows Python ≥ 3.7, but eventlet lags new CPython
releases, so 3.12 is the version the artifact was verified on.

Exit codes:

| Code | Meaning |
| --- | --- |
| `0` | Environment is ready (or, with `--check`, verified good). |
| `1` | Something failed during provisioning — the message says what. |
| `77` | Refused to run: wrong OS, no root/sudo, or a hard prerequisite missing. |

A `77` is never a pass and never a failure: it describes the environment, not
the code.

## 4. Running the experiments

### Experiment 0: Swift's unit suite is green on the tree we patch (pre-fix baseline)

| | |
| --- | --- |
| Supports | The "pre-fix reference point" in the demo-2 slides: the tree we are about to change is green, so a failure that appears after our patch is attributable to our fix rather than to code that was already broken. |
| Command | `python3 experiments/exp0_unittests.py` |
| Expected runtime | Up to 30 minutes (the script's own hard timeout). |
| Expected output | A banner per phase — `environment`, `swift checkout`, `dependencies`, then pytest's own summary — ending in a `result` block with `elapsed`, `collected`, `failed`, `errors`, `skipped`, and the first failing tests if any. |

```bash
cd /opt/ec528-swift
source .venv/bin/activate

# Record the baseline as JSON -- you will diff this against the post-fix run
python experiments/exp0_unittests.py \
    --swift-src /opt/ec528-swift \
    --results-json baseline.json \
    --timeout 1800
echo "exit=$?"
```
 

### Experiment 1: `RingBuilder.save()` is not atomic (the bug we fix)

| | |
| --- | --- |
| Supports | The demo-2 bug reproduction: killing the ring builder mid-update leaves a half-written builder file on disk, which is why a later ring position lookup fails with `does not contain valid composite ring data`. |
| Command | `python3 experiments/exp1_run_partial_dump_test.py` |
| Expected runtime | Seconds. It runs exactly one unit test, `TestRingBuilder.test_save_partial_dump_does_nothing`. |
| Expected output | Before the fix: the test **fails** |

```bash
cd /opt/ec528-swift
source .venv/bin/activate
python experiments/exp1_run_partial_dump_test.py --swift-src /opt/ec528-swift
echo "exit=$?"
```

## 5. Claims and limitations

What your artifact supports, and an honest account of what does not work or was
not tested. *An honest narrower result scores better than an impressive result we
cannot reproduce.*
