# Experiments

Each numbered script supports a specific claim, prints what it checks, and
reports whether the checks passed. Bug-fix commands run in the prepared Swift
implementation directory on local Linux or LightSail; Swift Light commands run
from this course project's root. The two groups test different systems: `bug_fix/` checks OpenStack Swift's
individual ring-builder save behavior; `swift_light/` measures what Swift Light's
atomic write mode costs and checks that uploads and downloads move the intended
bytes. Swift Light's placement, read/write fallback, atomic-save, and
persistence checks live in a functional test suite under
[`src/test/`](../src/test/), which experiments 3 and 4 run; experiment 5 runs
that suite's latency test.

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

The placement, read/write fallback, atomic-save, and persistence checks that
used to be experiments 3–7 are now the functional test suite in
[`src/test/`](../src/test/), run by experiments 3 and 4 below. They are
independent checks, not benchmarks, and they create their own temporary state.

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

**Expected result:** a table of cases, failures, seconds, ms/case, std dev,
p50/p95/p99 and cases/second; the ring writes and the cost of one; and
`every case succeeded`, with exit `0`. The std dev column is the sample
standard deviation of the per-case times, so a gap between the two modes is
only worth quoting when it is larger than that spread. Atomic mode should read
slower in every row. For scale, one 200-case pair on the development machine
put atomic about 10% above non-atomic per file write, 35% per delete, and
15-23% per membership change -- a smoke-test comparison, not a result to quote.

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
durability, or failure handling. Those checks live in the functional test suite
described below. Their exit codes are `0` for success, `1` for an observed
operation failure, and `77` when the experiment cannot be attempted.

### Experiment 3: run the unit test suite

**Claim tested:** each swift_light component behaves as specified on its own --
the client's path-to-key mapping, the ring's layout and placement, the data
server's store and HTTP routes, and the proxy's heartbeat monitor.

```bash
python3 experiments/swift_light/exp3_unit_tests.py

# the same suite with non-atomic writes
python3 experiments/swift_light/exp3_unit_tests.py non_atomic
```

The script discovers and runs everything under `src/test/client/`,
`src/test/data/` and `src/test/proxy/`. It creates temporary state and starts
isolated storage servers on OS-assigned loopback ports, so no deployed cluster,
input file, or earlier experiment is required. The optional `MODE` -- `atomic`
(the default) or `non_atomic`, the same two `deploy.py up` accepts -- decides how
those servers write their objects, and the mode wins over what a test asks for.
That makes `non_atomic` a negative control: the nine checks in
`data/test_atomic_save.py` and `data/test_crash_atomicity.py` that assert the
atomic guarantees must fail there. The crash suite performs a real hard kill --
`SIGKILL` on POSIX, `TerminateProcess` on Windows -- while a child writer is
mid-write, before the rename, and after it, then reopens the target and retries.

**Runtime:** normally under 15 seconds. **Expected result:** under `atomic`,
`Ran 60 tests`, `OK`, `[exp3] OK` and `RESULT: PASS` with exit `0`; under
`non_atomic`, the same 60 tests with `9 failures` and `RESULT: FAIL` with exit
`1` -- those nine are the point of the run, not a regression.

### Experiment 4: run the service integration test suite

**Claim tested:** the services work together end to end -- a push that reaches a
quorum of real replicas, reads that fall back through the fan, and a proxy that
routes and reports health for its live members.

```bash
python3 experiments/swift_light/exp4_integration_tests.py

# the same suite with non-atomic writes
python3 experiments/swift_light/exp4_integration_tests.py non_atomic
```

The script discovers and runs everything under
`src/test/service_integration_test/`. It starts real data servers and proxies on
OS-assigned loopback ports with temporary state, so local connections must be
allowed; it does not start, stop, or change a deployed cluster. The optional
`MODE` works as it does for experiment 3 and is authoritative, but this suite
stays green under either mode: every check here asserts an outcome -- the bytes
come back, the quorum was reached, the fallback happened -- and an uninterrupted
write gives the same outcome in both modes. Only exp3 can tell the modes apart.
It also drives `RemoteDataServer` -- the ring's migration interface -- against
real servers: copying, handoff, the write-lock lease, and an end-to-end
`add_node` rebalance.

**Runtime:** normally under 60 seconds. **Expected result:** `Ran 27 tests`,
`OK`, `[exp4] OK`, and `RESULT: PASS`, with exit `0`. Subcases are included
within the named tests.

For experiments 3 and 4, exit `0` means every test ran and passed, `1` means a
test failed, errored, or was skipped, and `77` means the suite could not be
attempted: missing tests, an unwritable `--temp-dir`, or no localhost
connections. Both accept an optional `MODE` (`atomic` or `non_atomic`),
`--temp-dir PATH` and `--help`.

### Experiment 5: p95 write and read latency

**Claim tested:** one write and one read stay inside the design proposal's
performance baseline -- p95 write latency `<= 50 ms` and p95 read latency
`<= 20 ms`, metrics 1 and 3 of its table.

```bash
python3 experiments/swift_light/exp5_latency.py
```

The measurement is a test in [`src/test/performance_eval/`](../src/test/performance_eval/),
and this script runs it, the way experiments 3 and 4 run their suites. The test
builds a real consistent-hashing ring and one real `DataStore` per data server in
a temporary directory it owns, then times 100 writes -- ring lookup plus one copy
per replica, in parallel, through the atomic temp file, flush and rename path --
and 100 reads -- lookup plus the primary's store -- one case at a time. Every
read is compared with the bytes that were written, so a fast wrong answer fails.
It reports cases, failures, mean, standard deviation, p50/p95/p99, max and the
budget per phase. `SWIFT_LIGHT_PERF_SAMPLES` changes the sample count and
`--temp-dir PATH` changes where the temporary state lives.

Nothing in this measurement touches the network: no TCP handshake, no connection
close, no HTTP. That is deliberate -- a fresh loopback connection costs about
11 ms here and one client write is three requests, so an end-to-end run measures
socket setup as much as the cluster; with a fresh connection per request the same
workload reported a read p95 of 48 ms against this 20 ms budget for that reason
alone. The numbers are the backend's own placement, storage and durable-write
cost: a floor for what a client sees, which is higher by whatever the HTTP stack
and the TCP setup add. Metric 2 of the baseline table, 1,000 sustained
acknowledged writes, is not measured here: it needs concurrent load.

**Runtime:** about a second at the defaults. **Expected result:** the two-row
timing table with a `PASS` per row, `Ran 2 tests`, `OK`, `[exp5] OK`, and
`RESULT: PASS`, with exit `0`. Exit `1` means a p95 went over budget or a case
failed, and `77` means the suite could not be attempted.
