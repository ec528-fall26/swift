# Design Document

*Due in the `demo-2` branch, updated for `demo-3` and `final-demo`.
**The TA will run this document.** If they cannot follow it, you lose the points.*

## 1. Architecture

What you actually built — not what you originally planned. Include a diagram.

## 2. Design decisions

The choices that mattered, the alternatives, and why you chose as you did.
Include the things that did not work.

## 3. Setup

### Setup on a new machine

The whole environment is built by one script. On a clean Ubuntu 24.04 machine:

```bash
git clone https://github.com/HankWang05/EC528_swift
cd <cloned swift repo directory>
bash experiments/bug_fix/setup.sh
```

What the script does

| Step | What happens |
| --- | --- |
| Host checks | Requires Linux (exit `77` otherwise), reports the distro, finds `sudo`, checks `python3` and free disk. |
| System packages | Installs the 16 `[platform:dpkg]` packages from Swift's `bindep.txt`: `build-essential`, `gcc`, `liberasurecode-dev`, `libffi-dev`, `libxml2-dev`, `libxslt1-dev`, `libssl-dev`, `memcached`, `python3-dev`, `python3-venv`, `rsync`, `xfsprogs`, `attr`, `curl`, `git`, `man-db`. |
| XFS scratch filesystem | Creates a 4 GB XFS loopback image at `/opt/xfs-tmp.img`, mounts it on `/tmp`, and **verifies** an 8 KB xattr can actually be written there. |
| Virtualenv | Creates `./.venv`, or reuses it if it already exists. |
| Python dependencies | Upgrades pip/setuptools/wheel; installs `pbr` on its own first (it is an import-time dependency of `swift/__init__.py`); then `requirements.txt` and `test-requirements.txt`; then `pip install -e . --no-deps` so `import swift` resolves to this checkout. |
| Verify | Checks that `test/unit`, `test/unit/common/ring/test_builder.py` and both experiment scripts exist; imports `swift`, `pbr`, `eventlet`, `xattr`, `pyeclib`, `pytest`, `swiftclient`; and fails if an installed `swift` is shadowing the checkout. |
| Next steps | Prints the revision, the exit code each experiment is expected to produce, and how to activate the venv. |

The versions this is pinned to: **Ubuntu 24.04**, **Python 3.12**, XFS for
`/tmp`.

Exit codes:

| Code | Meaning |
| --- | --- |
| `0` | Environment is ready (or, with `--check`, verified good). |
| `1` | Something failed during provisioning |
| `77` | Refused to run: wrong OS, no root/sudo, or a hard prerequisite missing. |

### Using the team's LightSail instance

The team has a LightSail instance with Ubuntu 24.04 with setup completed.
It is possible to access the instance `SwiftBugFix` after accepting the email invitation to the team's
AWS project. This can be done by clicking `Connect using SSH` in the LightSail dashboard.

![LightSail dashboard for the SwiftBugFix instance, showing the "Connect using SSH" button](Pictures/Screenshot%202026-10-15%20090839.png)

## 4. Running the experiments

### Experiment 0: Running all unit tests

| | |
| --- | --- |
| Supports | The "pre-fix reference point": Swift is fully functional before and after our fix. |
| Command | `python3 experiments/exp0_unittests.py` |
| Expected runtime | Up to 30 minutes (the script's own hard timeout). |
| Expected output | A banner per phase — `environment`, `swift checkout`, `dependencies`, then pytest's own summary — ending in a `result` block with `elapsed`, `collected`, `failed`, `errors`, `skipped`, and the first failing tests if any. |

```bash
cd <cloned swift repo directory>
source .venv/bin/activate

python experiments/exp0_unittests.py \
    --swift-src /opt/ec528-swift \
    --results-json baseline.json \
    --timeout 1800
echo "exit=$?"
```

In the team's lightsail instance

```bash
cd /home/ubuntu/EC528_swift
source .venv/bin/activate

python3 experiments/exp0_unittests.py \
    --swift-src /opt/ec528-swift \
    --results-json baseline.json \
    --timeout 1800
echo "exit=$?"
```

The first run of these experiments reports exactly **one**
failing test — `TestRingBuilder.test_save_partial_dump_does_nothing`, the
reproduction we added in this branch, which pins the bug in
`RingBuilder.save()`. That single failure is the bug, so it is expected.
After the atomic-save fix is applied, re-run the same commands: that
test passes and the suite reports zero failures.

### Experiment 1: `RingBuilder.save()` is not atomic (the bug we fix)

| | |
| --- | --- |
| Supports | The demo-2 bug reproduction: killing the ring builder mid-update leaves a half-written builder file on disk, which is why a later ring position lookup fails with `does not contain valid composite ring data`. |
| Command | `python3 experiments/exp1_run_partial_dump_test.py` |
| Expected runtime | Seconds. It runs exactly one unit test, `TestRingBuilder.test_save_partial_dump_does_nothing`. |
| Expected output | Before the fix: the test **fails** |

```bash
cd <cloned swift repo directory>
source .venv/bin/activate
python experiments/exp1_run_partial_dump_test.py
echo "exit=$?"
```

In the team's LightSail instance

```bash
cd /home/ubuntu/EC528_swift
source .venv/bin/activate
python3 experiments/exp1_run_partial_dump_test.py
echo "exit=$?"
```

## 5. Claims and limitations

What your artifact supports, and an honest account of what does not work or was
not tested. *An honest narrower result scores better than an impressive result we
cannot reproduce.*
