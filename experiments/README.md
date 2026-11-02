# Experiments

Each numbered script supports a specific claim, prints what it checks, and
reports whether the checks passed. Bug-fix commands run in the prepared Swift
implementation directory on local Linux or LightSail; Swift Light commands run
from this course project's root. The two groups test different systems: `bug_fix/` checks OpenStack Swift's
individual ring-builder save behavior; `swift_light/` checks placement,
replication, read/write fallback, atomic saving, and persistence in Swift Light,
and separately measures what its atomic write mode costs.

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

For experiments 1 and 2, start the cluster in another terminal:

```bash
python3 src/swift_light/deploy.py up
```

Experiments 1 and 2 use `SwiftLightFiles/Hello.txt` and the object path
`MyTxtFiles/Hello.txt`. Run the upload before the download, using the same account.

Experiments 3–7 are independent functional checks, not benchmarks. They create
their own temporary state and do not require a deployed cluster or experiments
1 and 2 to have run. Experiments 4 and 5 start isolated storage servers on
OS-assigned localhost ports, so local connections must be allowed.

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

### Functional experiment prerequisites (3–7)

Run these from the repository root with Python 3.12. No deployed cluster,
input file, or previous experiment is required. Each script creates temporary state;
experiments 4 and 5 also start isolated HTTP storage servers on OS-assigned
localhost ports. Local connections must be allowed. These checks do not modify
the deployed ring or object stores.

Keep `functional_test_support.py` beside the scripts. Only the Python standard
library is required. Each script checks temporary-storage access before testing;
experiments 4 and 5 also check localhost connectivity. Use `--temp-dir PATH`
with an existing writable directory if the default temporary directory is
unavailable. This directory only holds disposable test state.

### Experiment 3: ring placement and persistence

**Claim tested:** a key maps to the correct ordered list of distinct servers, and placement stays the same after saving and loading the ring.

```bash
python3 experiments/swift_light/exp3_ring_placement.py
```

We check fixed keys against independently calculated hash slots and a reference
walk of the vnode points. An explicit small ring checks positions on boundaries,
between boundaries, and across wraparound. We also compare a loaded ring and an
independently built equivalent ring. This checks deterministic placement, not
load balance or membership changes.


**Runtime:** normally seconds. **Expected result:** 3 named tests,
`Ran 3 tests`, `OK`, `[exp3] OK`, and `RESULT: PASS`, with exit `0`.
Subcases are included within the named tests.

### Experiment 4: write acknowledgements and substitute fallback

**Claim tested:** a write succeeds only after the required acknowledgements, using substitutes when intended replicas fail.

```bash
python3 experiments/swift_light/exp4_write_fallback.py
```

We use `[B, C, A]` with two required acknowledgements. Controlled transport
responses cover healthy replicas, connection and HTTP failures, insufficient
acknowledgements, and total failure. We check the reported servers and payload
bytes and confirm that A is not contacted when B and C accept. A thread
rendezvous checks concurrent replica requests without a latency threshold.

An isolated HTTP check uses the real storage handlers and verifies that a
second replica and substitute store the bytes when the first replica is made
unreachable. Placement lookup is controlled. This does not establish automatic
handoff recovery or strong consistency. Correctly rejected uploads count as
passing checks when rejection is the expected result.


**Runtime:** normally seconds. **Expected result:** 3 named tests,
`Ran 3 tests`, `OK`, `[exp4] OK`, and `RESULT: PASS`, with exit `0`.
Subcases are included within the named tests.

### Experiment 5: read order and fallback

**Claim tested:** reads try servers in placement order and return the first successful copy, or report failure when no copy is available.

```bash
python3 experiments/swift_light/exp5_read_fallback.py
```

We check primary reads, an unreachable primary, missing copies, substitute
fallback, and cases where every server is missing the object or unreachable.
We verify attempt order, returned bytes, source and role, and preservation of
an existing download destination when no server succeeds. Real HTTP checks
retrieve objects from a secondary and substitute after earlier servers return
`404`. Placement lookup is controlled.

A different-version case confirms the current first-successful-copy behavior;
it does not claim newest-version selection, read repair, or strong consistency.


**Runtime:** normally seconds. **Expected result:** 3 named tests,
`Ran 3 tests`, `OK`, `[exp5] OK`, and `RESULT: PASS`, with exit `0`.
Subcases are included within the named tests.

### Experiment 6: atomic-save failure safety and visibility

**Claim tested:** in atomic mode, handled failures before replacement preserve the previous file, and replacement publishes complete new contents.

```bash
python3 experiments/swift_light/exp6_atomic_save.py
```

We test both object files and ring metadata. We inject partial-write, flush,
file-sync, and replacement errors, then check the exact propagated error, old
bytes and loadability, temporary-file cleanup, and a successful retry.

For successful saves, we observe real I/O and check write, flush, file sync,
close, replacement, then a directory-sync attempt. The candidate must be
complete, closed, and in the destination directory. A paused writer lets a
reader verify the old contents before replacement and the new contents after.
On POSIX, the ordering check also requires an observed directory `fsync`.

This tests Swift Light local files, not OpenStack Swift RingBuilder. It does
not simulate abrupt process termination, power loss, or cross-replica atomicity.


**Runtime:** normally seconds. **Expected result:** 3 named tests,
`Ran 3 tests`, `OK`, `[exp6] OK`, and `RESULT: PASS`, with exit `0`.
Subcases are included within the named tests.

### Experiment 7: persistence across a fresh process

**Claim tested:** a fresh Python process can reopen completed ring, configuration, and object saves without changing their contents or placement.

```bash
python3 experiments/swift_light/exp7_restart_persistence.py
```

We save a ring, configuration, and empty, text, and binary objects, including
a text overwrite. A separate process receives only paths and keys, reloads
the files, and reports configuration, ring state, placement, object sizes,
and SHA-256 hashes for comparison.

This checks persisted state rather than retained memory. It reopens completed
saves; it does not restart the deployed cluster or simulate an interrupted
save or machine crash. The child has a 20-second deadline.


**Runtime:** normally seconds. **Expected result:** 1 named test,
`Ran 1 test`, `OK`, `[exp7] OK`, and `RESULT: PASS`, with exit `0`.
Subcases are included within the named tests.

For experiments 3–7, exit `0` means every check ran and passed, `1` means a
check failed, errored, or was skipped, and `77` means prerequisite checks failed
before testing. Failures during the tests remain failures; inspect the named
check and traceback. Their elapsed times are test runtimes, not benchmark
measurements. Each script also supports `--help`.
