# Experiments

Each numbered script supports a specific claim, prints what it checks, and
reports whether the checks passed. Bug-fix commands run in the prepared Swift
implementation directory on local Linux or LightSail; Swift Light commands run
from this course project's root. The two groups test different systems: `bug_fix/` checks OpenStack Swift's
individual ring-builder save behavior; `swift_light/` checks object uploads and
downloads in Swift Light, and what its atomic write mode costs.

Each experiment must have a matching entry with its claim, command, expected
runtime, and expected result in the relevant design document:

- [Bug-fix design document](../docs/design-document-bug-fix.md).
- [Swift Light design document](../docs/design-document-swift-light.md).

## Ring-builder bug-fix experiments

These experiments check `RingBuilder.save()`: a new builder is prepared in a
temporary file, flushed and synced, then renamed over the destination before
its containing directory is synced.

The bug-fix files in this project are included for submission. To reproduce
results, run with the OpenStack Swift implementation on local Linux or directly
on the team's prepared LightSail instance, as in the
[design document](../docs/design-document-bug-fix.md).

### Local Linux

On a clean Ubuntu 24.04 machine, obtain the implementation and experiments:

```bash
git clone https://github.com/HankWang05/EC528_swift.git
cd EC528_swift
git switch bugfix
bash experiments/setup.sh
source .venv/bin/activate
```

For an existing prepared installation:

```bash
cd /path/to/EC528_swift
source .venv/bin/activate
```

The commands for experiments 0–4 below run from that directory, which contains
`swift/`, `test/unit/`, and `experiments/`. Ensure the five numbered scripts and
`experiments/atomic_save_support.py` are present. If using the submitted copies,
place them directly in the implementation's `experiments/` directory.

### LightSail

Connect to `SwiftBugFix` using **Connect using SSH** in the team's AWS LightSail
dashboard, then run:

```bash
cd /home/ubuntu/EC528_swift
source .venv/bin/activate
git branch --show-current
git rev-parse HEAD
git status --short
ls experiments/
```

Use `/opt/ec528-swift` instead if that is where the prepared checkout is stored.
No course-project checkout or Swift Light cluster is required on the instance.
Run each experiment below separately and follow it with `echo "exit=$?"` to
record the result. `--swift-src "$PWD"` explicitly selects the implementation
in the current directory. These commands do not switch branches or apply a fix.

### Experiment 0: unit-suite baseline and regression checks

**Claim supported:** comparing unit-suite results before and after the change
provides evidence that the fix has not introduced new failures.

```bash
python3 experiments/exp0_unittests.py \
    --swift-src "$PWD" \
    --results-json baseline.json --timeout 1800
```

The script checks the environment, records the tested revision, runs Swift's
unit suite, and reports failures, errors, skips, and elapsed time. Use `--quick`
for only the ring tests; that is a smoke test rather than the full baseline.

**Runtime:** up to 30 minutes by default. **Expected result:** a pytest summary
and a result block; a clean completed suite returns `0`. Record actual named
failures on both revisions rather than assuming a fixed failure count.

**Interpretation:** the existing mocked `test_save` expects the old save
implementation and needs updating. The runner can also misclassify a suite
where every test fails as aborted collection, so inspect the pytest summary,
not only the final label. A passing unit suite is not proof of complete system
correctness.

### Experiment 1: preserve the builder after partial serialization

**Claim tested:** if serialization writes only part of its payload and raises
an error, the previous builder should remain unchanged and loadable.

```bash
python3 experiments/exp1_run_partial_dump_test.py \
    --swift-src "$PWD"
```

Runs `TestRingBuilder.test_save_partial_dump_does_nothing`. That test saves a
valid builder, patches `pickle.dump` to write half the serialized bytes and
raise `OSError`, then compares the original bytes and reloads the builder.

**Runtime:** normally seconds. **Expected result:** direct overwriting fails
the byte-preservation check; the atomic-save implementation should preserve
the bytes and allow loading.

**Interpretation:** the test's final dictionary comparison can fail because
loading adds device replication fields absent from its original fixture. The
runner's diagnosis distinguishes that mismatch from changed file bytes. A
failure must be investigated rather than automatically attributed to the save
implementation. This experiment injects an exception; it does not kill a process.

### Experiment 2: process interruption before and after replacement

**Claim tested:** killing a saving process before replacement preserves the
complete previous builder; killing it immediately after replacement leaves the
complete new builder available.

```bash
python3 experiments/exp2_interrupted_save.py \
    --swift-src "$PWD" --results-json exp2-results.json
```

Three tests pause and kill a child writer with `SIGKILL`:

1. During partial serialization: verify the original bytes and loaded state.
2. Immediately before rename: verify the original bytes and loaded state.
3. Immediately after rename: verify the complete updated state loads.

The checks compare partition-to-device lookups. Before-replacement cases also
verify that another save succeeds despite any temporary file left by the kill.

**Runtime:** normally seconds, with a 90-second worker deadline.
**Expected result:** `Ran 3 tests`, `OK`, `RESULT: PASS`, and exit `0` on the
patched implementation. The direct-overwrite implementation should fail checks.

**Limits:** the OS remains running. This does not simulate machine power loss,
and a killed process is not expected to perform exception cleanup.

### Experiment 3: handled-failure preservation and cleanup

**Claim tested:** handled errors before replacement preserve the previous
builder, propagate the error, and remove the attempt's temporary file. A failed
first save restores its unpersisted builder ID.

```bash
python3 experiments/exp3_save_failure_cleanup.py \
    --swift-src "$PWD" --results-json exp3-results.json
```

Five tests cover partial serialization, explicit file flush, file `fsync`,
replacement, and first-save ID recovery. Each injected failure must be reached
and produce the expected error. Existing-builder checks require byte-identical
old contents, successful loading and partition-device lookups, and no extra
files. The first-save check requires a reset ID and no destination or temporary
file left behind.

**Runtime:** normally seconds, with a 90-second worker deadline.
**Expected result:** `Ran 5 tests`, `OK`, `RESULT: PASS`, and exit `0` when all
failure-handling checks pass.

**Limits:** these checks use a writable temporary filesystem. They do not
guarantee that cleanup cannot fail due to permissions, require cleanup after
`SIGKILL`, or require rollback after replacement has already occurred.

### Experiment 4: successful-save durability ordering

**Claim tested:** a successful save prepares a complete temporary file and
follows the required operation sequence before acknowledging completion.

```bash
python3 experiments/exp4_save_durability_order.py \
    --swift-src "$PWD" --results-json exp4-results.json
```

The single test observes real operations in this order:

```text
serialize -> flush -> file_fsync -> close -> rename -> directory_fsync
```

It verifies that the temporary file is separate and beside its destination,
the original remains unchanged until replacement, the candidate is complete
and closed before rename, and the synced directory descriptor identifies the
actual parent directory. The installed builder must match the intended update,
load correctly, retain its partition mappings, and leave no temporary filename.

**Runtime:** normally seconds, with a 90-second worker deadline.
**Expected result:** `Ran 1 test`, the observed operation sequence, `OK`,
`RESULT: PASS`, and exit `0` when the protocol checks pass.

**Limits:** successful sync calls do not prove survival of actual power loss.
Swift's helper can log some directory-sync errors rather than propagating them;
this check requires a successful directory sync to be observed.

### Shared support and result interpretation

`experiments/atomic_save_support.py` supplies common populated-builder fixtures,
implementation-path verification, revision reporting, and bounded workers for
experiments 2–4. Keep it beside those scripts. They accept `--timeout SECONDS`
(default `90`) and `--results-json PATH`, and print the actual imported
`builder.py` path. Their elapsed times describe test runtime, not performance
overhead of atomic saving.

| Exit code | Meaning |
| --- | --- |
| `0` | The checks ran and passed. |
| `1` | A check failed or errored; inspect the named test and traceback. |
| `77` | No valid result was obtained, such as an unsupported host, missing dependencies, or a timeout. |

For before/after evidence, run the same scripts against the unpatched and
patched implementations using `--swift-src`, recording each revision and named
failures. Do not change the implementation during a run. These experiments do
not establish composite-builder atomicity, concurrent-writer isolation, or
running-cluster client behavior.

`experiments/setup.sh` prepares the environment; it is not an experiment and does
not run these checks. Its system-package and filesystem changes should be
reviewed before provisioning a machine.

## Swift Light experiments

Start the cluster in another terminal:

```bash
python3 src/swift_light/deploy.py up
```

The default experiments use `SwiftLightFiles/Hello.txt` and the object path
`MyTxtFiles/Hello.txt`. Run the upload before the download, using the same account.

### Experiment 0: what atomic writes cost in operation

**Claim tested:** atomic mode's write path costs more per case than non-atomic
mode's, and the size of that difference is the overhead the design document
reports.

```bash
python3 src/swift_light/deploy.py up non_atomic
python3 experiments/swift_light/exp0_atomic_overhead.py --mode non_atomic --json non_atomic.json
python3 src/swift_light/deploy.py down

python3 src/swift_light/deploy.py up
python3 experiments/swift_light/exp0_atomic_overhead.py --mode atomic --json atomic.json
python3 src/swift_light/deploy.py down
```

Deploy the cluster in the mode `--mode` names, run the experiment, then redeploy
in the other mode and run it again; each run reports one mode. A run executes
10,000 cases against the live cluster, in a seeded random order: 80% touch data
(half writing a file, half deleting one written earlier) and 20% touch the ring
(half adding a data server, half removing one). A membership change is a
rebalance, so it writes the ring once per vnode point -- 100 ring writes per add
or remove at the default vnode count. The ring is churned on a private copy, so
the deployed ring and every object it placed are left alone.

**Runtime:** roughly 20 minutes per mode at the defaults, because the ring cases
dominate; `--operations 200` is a smoke test rather than a result.

**Short version:** `experiments/swift_light/exp0_atomic_overhead_simplified.py`
runs the same two write paths with 200 saves instead of 10,000 cases — 50 file
saves, one node join (100 ring saves), then 50 more file saves — and finishes in
about two seconds per mode. It takes the same `--mode`, `--json` and `--ring`
flags and prints the same table, but with a single membership change it reports
one sample of the ring write path rather than an average. Use it to check that
the overhead exists; use the full script for numbers to quote.

**Expected result:** a table of cases, failures, seconds, ms/case,
p50/p95/p99 and cases/second; the ring writes and the cost of one; and
`every case succeeded`, with exit `0`. Atomic mode should read slower in every
row. For scale, one 200-case pair on the development machine put atomic about
10% above non-atomic per file write, 35% per delete, and 15-23% per membership
change -- a smoke-test comparison, not a result to quote.

Experiment 0 needs no input files and starts and stops its own cluster;
experiments 1 and 2 below use `SwiftLightFiles/Hello.txt` and the object path
`MyTxtFiles/Hello.txt`, uploading before downloading, under the same account.

### Experiment 1: upload and verify replicas

**Claim tested:** the configured number of distinct servers acknowledge an
upload and each returns the uploaded bytes on an immediate direct read.

```bash
python3 experiments/swift_light/exp1_write_file.py
```

The script prints the key, placement order, acknowledgements, and input hash,
then directly reads each acknowledging server and compares its response with
the input. It fails for insufficient acknowledgements or incorrect stored bytes.

**Runtime:** normally seconds, depending on request timeouts and server count.
**Expected result:** `wrote '<key>' to 2/2 replicas`, per-server verification,
and an `OK` message with exit `0` for the default configuration.

### Experiment 2: download and compare bytes

**Claim tested:** a client download returns bytes identical to the reference.

```bash
python3 experiments/swift_light/exp2_read_file.py
```

Downloads the uploaded object to `SwiftLightFiles/Hello_downloaded.txt`, reports
its serving node and role, and compares the downloaded bytes with the original.
It fails if no node supplies the object or the contents differ.

**Runtime:** normally seconds, depending on request timeouts and server count.
**Expected result:** a `fetched` message and an `OK` message confirming identical
contents, with exit `0`. A fallback is reported if it occurs, but primary
failure is not forced by this experiment.

Experiments 0-2 validate the cost of the write mode and immediate
upload/download behavior; they do not test ring-builder atomicity, power-loss
durability, or failure handling. Their exit codes are `0` for success, `1` for
an observed operation failure, and `77` when the experiment cannot be attempted.
