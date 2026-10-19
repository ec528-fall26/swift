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

### Experiments 2–4: additional atomic-save claims

These scripts live in the **course repository** under `experiments/bug_fix/`.
They import the implementation from a separate upstream Swift checkout. On
Linux, activate that checkout's prepared virtual environment and, from the
course repository root, set:

```bash
source /path/to/EC528_swift/.venv/bin/activate
export PYTHONPATH=/path/to/EC528_swift
```

### Experiment 2: individual-builder save failure safety

| | |
| --- | --- |
| Supports | An interrupted or failed individual-builder save preserves the previous valid builder. Successful saves follow the file-sync, rename, directory-sync order. |
| Command | `python3 experiments/bug_fix/exp2_atomic_save_failures.py` |
| Expected runtime | Normally seconds; allow up to 60 seconds for bounded child-process waits. |
| Expected output | `Ran 4 tests` followed by `OK` when all checks pass. Expected to pass on the inspected `bugfix` implementation; not yet verified on Linux. The process-kill check should fail on `master`. |

The four checks:

1. Kill a child process after it writes half the serialized data. Verify that
   the original builder is byte-identical and loadable, then verify that another
   save succeeds despite any temporary file left by the killed process.
2. Inject a file `fsync` error. Verify that the original survives and the
   temporary file is removed.
3. Inject a replacement error. Verify that the original survives and the
   temporary file is removed.
4. Record real file operations during a successful save. Verify that file
   `fsync` completes before rename and directory `fsync` completes afterward.

From the course repository root, after the environment setup above:

```bash
python3 experiments/bug_fix/exp2_atomic_save_failures.py
echo "exit=$?"
```

### Experiment 3: independent temporary files for concurrent saves

| | |
| --- | --- |
| Supports | Concurrent saves from two threads use independent temporary files and leave one complete, loadable builder. |
| Command | `python3 experiments/bug_fix/exp3_concurrent_saves.py` |
| Expected runtime | Normally seconds; allow up to 30 seconds for thread joins. |
| Expected output | `Ran 1 test` followed by `OK` once temporary filenames are unique per save. Expected to fail on the inspected PID-only temporary-name implementation. |

Two separate builder instances are given different versions and saved to the
same destination from threads in one process. A barrier forces both writers
to reach serialization together. The experiment checks that neither save raises
an error, their temporary filenames differ, and the final file loads as one of
the complete intended builder states. It does not require a particular writer
to win.

```bash
python3 experiments/bug_fix/exp3_concurrent_saves.py
echo "exit=$?"
```

### Experiment 4: composite-builder save failure safety

| | |
| --- | --- |
| Supports | A partial JSON serialization leaves the previous composite-builder metadata byte-identical and loadable. |
| Command | `python3 experiments/bug_fix/exp4_composite_atomic_save.py` |
| Expected runtime | Seconds. |
| Expected output | `Ran 1 test` followed by `OK` once composite saves are atomic. Expected to fail on the inspected direct-overwrite implementation. |

The experiment saves valid composite metadata, records its original bytes, then
replaces JSON serialization with a function that writes half the data and raises
`OSError`. It checks that the save reports the error, the original bytes remain
unchanged, and `CompositeRingBuilder.load()` restores the original metadata.
This checks metadata persistence, not a composed ring's object-placement lookup.

```bash
python3 experiments/bug_fix/exp4_composite_atomic_save.py
echo "exit=$?"
```

Exit `0` means the checks passed; exit `1` means a test or import failed;
exit `77` means the script refused to run outside Linux. Missing dependencies
produce an import error, not a passing result. Failing experiments are evidence
of an unmet claim, not successful demonstrations of the guarantee.

## 5. Claims and limitations

Experiments 2–4 specify individual-builder failure safety, concurrent-save
isolation, and composite-metadata failure safety respectively. They do not
measure performance, simulate actual power loss, or demonstrate an end-to-end
composite ring-position lookup. Experiment 2 kills a real child process during
serialization and observes real file/directory sync ordering; that is narrower
than proving power-loss durability. Experiment 3 covers threads in one process,
not independent processes or simultaneous membership changes.

Validation status: the new scripts passed syntax checks and refused to run on
Windows with exit `77`. Their Linux results have not been collected. Expected
outcomes above are based on source inspection, not measured passing results.
