# QA test harnesses and scripts

This directory holds the out-of-tree QA tooling for `percona-binlog-server`:
Python test harnesses that drive the built `binlog_server` binary against a
real MySQL server (and a real S3-compatible server), the fault-injection
infrastructure they use, the nightly runner, and a one-shot script that sets
the whole environment up on a new machine.

These are **black-box** tests: they start a MySQL server, run `binlog_server`
`fetch` / `pull` against it, and check the stored binlogs. They complement the
in-tree unit and property tests under [`../../tests`](../../tests), which test
the C++ components directly.

- [Quick start](#quick-start)
- [Conventions shared by the tools](#conventions-shared-by-the-tools)
- [The shared harness: `fetch_crash_test.py`](#the-shared-harness-fetch_crash_testpy)
- [Correctness and crash tests](#correctness-and-crash-tests)
- [Fault injection](#fault-injection)
- [S3 storage tests](#s3-storage-tests)
- [The `pull` replication source](#the-pull-replication-source)
- [The nightly run](#the-nightly-run)
- [Setting up a new machine](#setting-up-a-new-machine)
- [In-tree property tests](#in-tree-property-tests)

## Quick start

```sh
# one-time: provision the whole environment (toolchain, deps, builds,
# MySQL distributions, Python libraries, S3 server, Toxiproxy, ...)
extra/qa/setup_test_environment.sh

# run a randomized crash test against MySQL 8.4, ASan build
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/fetch_crash_test.py \
  --mysql-basedir ~/ws/dist/mysql-8.4.8-linux-glibc2.28-x86_64-minimal \
  --binsrv ~/ws/percona-binlog-server-build-asan_gcc14/binlog_server \
  --mode gtid --workdir ~/ws/qa/run-crash
```

## Conventions shared by the tools

Almost every harness takes the same core options and follows the same rules.

| Option | Meaning |
| --- | --- |
| `--mysql-basedir` | an unpacked MySQL server distribution (the dir with `bin/mysqld`) |
| `--binsrv` | the `binlog_server` binary to test (from any build preset) |
| `--workdir` | a scratch directory for the MySQL data dir, the storage, configs and logs |
| `--mode` | `gtid` or `position` — the replication mode to configure |
| `--port` | base TCP port; a test uses a small range from it |

- **`PYTHONPATH` must include the Python libraries.** Every harness imports
  `fetch_crash_test`, which imports Hypothesis, so point `PYTHONPATH` at the
  directory the setup script populates (`~/ws/qa/pylib` by default). Tools that
  create S3 backends also want `AWS_EC2_METADATA_DISABLED=true` in the
  environment to avoid a multi-second EC2 metadata lookup at every start
  (ticket 16); the setup and nightly scripts export it for you.
- **Exit code 0 means every check passed**, non-zero means at least one
  failed. The tools also print one JSON line per case to stdout, so a run can
  be scanned or collected.
- **They start and stop their own MySQL server** in `--workdir`; nothing needs
  to be running beforehand except, for the fault tests, the S3 server or
  Toxiproxy (see below).
- **Run against the AddressSanitizer build** (`...-build-asan_gcc14`) to catch
  memory errors; the crash and concurrency tests set `ASAN_OPTIONS` so an ASan
  abort is detected as a failure.

## The shared harness: `fetch_crash_test.py`

`fetch_crash_test.py` is both a test and the library every other harness is
built on. Its building blocks:

- **`MySQLServer`** — initializes, starts and stops a throwaway `mysqld` with
  binary logging on; runs SQL; and reports its binary logs, including
  `binlog_as_sent()`, which returns a binlog file exactly as the server would
  send it to a replica (clearing the `LOG_EVENT_BINLOG_IN_USE_F` flag and
  recomputing the checksum), so stored binlogs can be compared byte for byte.
- **`BinlogServer`** — writes a `binlog_server` JSON config (file or S3
  storage, checkpoint settings, connection and replication settings) and runs
  `fetch` / `pull`.
- **`FetchCrashMachine`** — a Hypothesis `RuleBasedStateMachine`: it randomly
  interleaves running a workload on the source, flushing binary logs, changing
  checkpoint settings, fetching to the end, and **fetching then `SIGKILL`ing**
  the utility mid-write. After every step two invariants are checked:
  - **`storage_holds_prefixes()`** — every stored binlog is a prefix of the
    source's, and the size recorded in the storage metadata is at a
    transaction boundary (so a crash never leaves a half-written transaction
    committed);
  - **`check_complete_copy()`** — after a clean fetch, the stored binlogs are
    byte-identical to the source's.

Run it directly for the randomized crash/resume test:

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/fetch_crash_test.py \
  --mysql-basedir <dist> --binsrv <binary> --mode gtid \
  --workdir ~/ws/qa/run-crash --examples 20 --steps 10
```

`--examples` is the number of Hypothesis examples, `--steps` the rule steps per
example. With `--storage s3` plus the `--s3-*` options it runs the same state
machine against S3 storage; with `--fsfault-lib` or `--s3-fault-control` it
also exercises the disk- and S3-fault rules (see below). The other harnesses
reuse these pieces, so they share its config shape and its integrity checks.

## Correctness and crash tests

All of these import the harness and reuse its integrity checks; each prints
JSON and exits non-zero on a failure.

| Script | What it does | Found |
| --- | --- | --- |
| `fetch_crash_test.py` | randomized workload + crash/resume state machine (above) | — |
| `concurrent_storage_test.py` | runs several `fetch` instances against **one** storage at once (a cron `fetch` overrunning into the next run); checks the storage is not corrupted and heals. Two writers on one storage are an unsupported scenario (ticket 24, closed as not a bug in PBS-87), so its failures document misuse, not defects | ticket 24 |
| `source_change_test.py` | changes the source under a running backup: `RESET BINARY LOGS`, a different server (failover), binlogs purged before fetch, a mid-stream restart | ticket 25 |
| `event_variety_test.py` | drives less common event types — transaction compression, `binlog_row_image=MINIMAL`, partial JSON, XA, tagged GTIDs — through a full fetch and a kill/resume | — (no bug) |
| `query_race_test.py` | runs `binlog_server list` `--runs` times (default 500) next to a `pull` writing a filesystem storage with `checkpoint_size` `"1K"` while the source gets small inserts and a `FLUSH BINARY LOGS` every 20 of them; reports the failed `list` runs and each distinct error message | ticket 29 |

Examples:

```sh
# two overlapping fetch instances on one storage, 5 times, same server id
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/concurrent_storage_test.py \
  --mysql-basedir <dist> --binsrv <asan-binary> --workdir ~/ws/qa/concurrent \
  --instances 2 --repeat 5 --shared-server-id

# the four source-side change scenarios
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/source_change_test.py \
  --mysql-basedir <dist> --binsrv <asan-binary> --workdir ~/ws/qa/source-change \
  --scenarios reset,identity,purge,restart

# 'list' next to a writing 'pull' (ticket 29); keep --workdir short, the
# mysqld socket lives in it
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/query_race_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/qrace --runs 500
```

`query_race_test.py` prints one JSON line: `runs`, `failed`, and `errors`, a
list of the distinct error messages (runs of 3+ digits replaced by `N`, so differing file
names do not split one error) with a count and one verbatim example. It exits
with 1 when any `list` run failed or `pull` died.

## Fault injection

Three independent mechanisms make the environment misbehave under the utility.

### Disk faults — `fsfault/` and `disk_fault_sweep.py`

`fsfault/fsfault.c` builds into `libfsfault.so`, an `LD_PRELOAD` shim that
makes chosen file-system calls fail. It is configured through environment
variables:

- `FSFAULT_ROOT` — only paths under this directory are affected;
- `FSFAULT_RULES` — `op=<call>,match=<substr>,exclude=<substr>,nth=<n>,count=<n>,errno=<e>`;
- `FSFAULT_LOG` — where injected faults are recorded.

`disk_fault_sweep.py` uses it to fail every kind of file-system call `fetch`
makes in its storage directory, one at a time, and checks that the utility
fails cleanly and the storage stays openable:

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/disk_fault_sweep.py \
  --mysql-basedir <dist> --binsrv <binary> \
  --fsfault-lib ~/ws/qa/fsfault/libfsfault.so --workdir ~/ws/qa/disk-sweep
```

### Network faults — `network_fault_test.py`

Places [Toxiproxy](https://github.com/Shopify/toxiproxy) between the utility
and the MySQL source and injects connection cuts, resets, stalls, added
latency and bandwidth limits, for both `fetch` and `pull`. Start Toxiproxy
first (`~/ws/qa/start_toxiproxy.sh`):

```sh
~/ws/qa/start_toxiproxy.sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/network_fault_test.py \
  --mysql-basedir <dist> --binsrv <binary> --mode gtid \
  --workdir ~/ws/qa/net --parts fetch,pull
```

### Protocol fuzzing — `listener_fuzz_test.py`

Throws malformed MySQL client traffic at the `pull` mode replication source
listener (garbage greetings, lying length headers, the auth-method-switch path
with junk, truncated `COM_BINLOG_DUMP`, random commands, bad sequence numbers)
and checks that `pull` keeps running and still serves binlogs. The campaign is
seeded and reproducible; run it against the ASan build.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/listener_fuzz_test.py \
  --mysql-basedir <dist> --binsrv <asan-binary> \
  --workdir ~/ws/qa/listener-fuzz --connections 5000 --seed 7
```

## S3 storage tests

These need the local S3-compatible server running (`~/ws/s3/start_s3.sh`
starts Versity S3 Gateway on `127.0.0.1:9000` and creates the `pbs-mtr`
bucket). The `--s3-*` options default to that server and bucket.

- **`s3_fault_proxy.py`** — an HTTP proxy in front of the S3 server that
  injects faults (error status, truncated or slow responses, dropped
  connections) into chosen requests. Run it standalone; the sweep and volume
  tools drive it through its `/__faults__` control endpoint.

  ```sh
  python3 extra/qa/s3_fault_proxy.py --listen 127.0.0.1:9100 \
    --backend http://127.0.0.1:9000
  ```

- **`s3_fault_sweep.py`** — runs every fault of the proxy against every kind of
  S3 request `fetch` makes (bucket checks, `GET`, multipart and single `PUT`,
  `LIST`, `DELETE`), and checks the utility fails cleanly and the storage is
  recoverable.

- **`s3_upload_volume.py`** — measures how much data `fetch` uploads to S3 for
  one binlog of a given size across several `storage.checkpoint_size` values,
  through the proxy's byte counters (this measurement surfaced ticket 17, the
  whole-binlog re-upload at every checkpoint).

The `fetch_crash_test.py` state machine also runs against S3 with
`--storage s3`, and the crash test plus `s3_fault_sweep.py` are two of the
nightly/coverage layers.

## The `pull` replication source

`pull_replica_test.py` sets up a second, real MySQL server as a **replica of
`pull`** (`CHANGE REPLICATION SOURCE TO ... ; START REPLICA`), in both position
and GTID auto-position mode, and requires the replica's data to match the
source's. When it does not, it reports the replica's I/O-thread error and the
statements `pull` rejected. It is the end-to-end check behind tickets 20-22;
tickets 21 (resuming from a position) and 22 (`mysqlbinlog --raw`) are fixed
upstream in `a338dde` (PBS-42 part 5), ticket 20 is open for heartbeats.
The nightly runs it (see below); its `gtid` case fails because `pull` does
not support `COM_BINLOG_DUMP_GTID` yet and is listed as a known failure
(ticket 20), while the `position` case must pass.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_replica_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/pull-replica
```

`repro_broken_checkpointing.py` is a self-contained reproducer kept from an
earlier finding (checkpoint settings having no effect); it drives an existing
server rather than starting one.

## The nightly run

[`nightly/`](nightly) holds the nightly test runner and its data:

- **`run_nightly.sh`** — runs three stages:
  1. **C++** (`cpp`): builds the `debug_gcc14`, `asan_gcc14` and
     `tsan_gcc14` trees in place, runs the unit tests, then runs **every
     Hegel property-test case separately** with many more cases than the
     default (the profiles in `hegel.toml`), and compares each failure
     against `known_failures.txt`.
  2. **MTR** (`mtr`): the whole `binlog_streaming` suite with the debug build
     on MySQL 8.0.45, 8.4.8 and 9.7.2 (`MTR_DISTS`), one version after the
     other, each with `--parallel=4` (`MTR_PARALLEL`), `--build-thread=500`
     (`MTR_BUILD_THREAD`; the `rs_*` tests also use ports 1000 above the
     server's, so 500 keeps clear of the automatic build threads 300-399) and var directories `$WS/mtr-var-nightly-<series>`,
     so it does not collide with manual MTR runs. Every test (and
     combination) is a case of the build `mtr-<series>`, compared against
     `mtr_known_failures.txt`. Skipped tests are recorded as `skipped`.
  3. **QA harnesses** (`qa`), against MySQL 8.4 (`QA_DIST`), ports from 24470
     (`QA_PORT`): `pull_replica_test.py` (cases `position` and `gtid`),
     `query_race_test.py` (case `list_next_to_pull`, 500 `list` runs,
     `QA_RUNS`), both with the debug build, and a short
     `fetch_crash_test.py` run per mode (cases `gtid` and `position`,
     10 examples of 8 steps, `QA_EXAMPLES` / `QA_STEPS`) with the ASan build.
     They are cases of the build `qa-<series>` (program: the harness),
     compared against `qa_known_failures.txt`. A failing harness keeps its
     workdir under `qa/` in the run directory, minus the MySQL data
     directories.

  Results go to `$WS/nightly/runs/<date>/` (and the `latest` symlink):
  `summary.txt`, one log per failing case (`<build>-<test case>.log`; for MTR
  `mtr-<series>-<test>.log` plus the whole `mtr-<series>.log`, for QA
  `qa-<series>-<harness>-<case>.log`), `results.jsonl` (one JSON line per
  case of every stage: build, program, test case, status, ticket, reason,
  seconds) and `run.json`, plus a line per night in `history.tsv` (its
  counts include the MTR and QA cases). Statuses: `passed`, `known` (a listed
  failure), `new` (any other failure), `known-passed` (a listed case that
  passed: fixed, or not hit tonight), `skipped` (MTR only, not counted), and
  `build-failed` / `no-test-cases` (counted as broken). It exits non-zero when
  there is a new failure or something is broken, and it is guarded by a
  `flock` so overlapping runs do not collide. The MTR stage and the debug
  build harnesses are skipped when the debug build fails, `fetch_crash_test`
  when the ASan build fails. Tonight's whole run takes about 40-50 min for the
  C++ stage plus 35-50 min of MTR (9-26 min per version) and about 12 min of QA harnesses.

  To try a stage without touching the nightly's own results, select stages
  with `STAGES` (default `cpp mtr qa`; without `cpp` nothing is built) and
  write elsewhere with `OUT_DIR` (then `history.tsv`, `latest` and old runs
  are left alone):

  ```sh
  STAGES="mtr qa" OUT_DIR=~/ws/qa/nightly-try ~/ws/nightly/run_nightly.sh
  ```
- **`nightly_stages.py`** — runs one MTR suite or one QA harness for
  `run_nightly.sh`, parses its outcome into cases, matches them against the
  stage's known-failure list and appends the result lines.
- **`hegel.toml`** — the Hegel settings profiles (`nightly_debug`,
  `nightly_asan`, `nightly_tsan`) selected per build via
  `HEGEL_DEFAULT_PROFILE`; each has its own failure database so a failure found
  one night is replayed first the next. The database paths are templated
  (`@NIGHTLY_DIR@`) and filled in for the workspace by the setup script.
- **`known_failures.txt`** — maps property-test case names to the ticket that
  explains them; a listed failure is reported as *known*, any other as *new*.
  Remove a line once its bug is fixed. CI uses the same list (see below).
- **`mtr_known_failures.txt`** — the same for the MTR stage, one line per
  `<series> <test>  ticket N: ...`, where `<series>` is `8.0`, `8.4`, `9.7` or
  `*` (every version) and `<test>` is a test name (every combination) or
  `<test>/<combination>`.
- **`qa_known_failures.txt`** — the same for the QA-harness stage:
  `<harness> <case>  ticket N: ...` (`<case>` may be `*`), e.g.
  `pull_replica_test gtid` (ticket 20) and
  `query_race_test list_next_to_pull` (ticket 29).
- **`ci_property_tests.sh`** — what the CI job *GCC 14 Debug Property Tests*
  (`.github/workflows/cmake.yml`, configured with `-DWITH_PROPERTY_TESTS=ON`)
  runs: every property test through `ctest` at Hegel's default settings,
  skipping the cases `known_failures.txt` lists through a Boost.Test run
  filter (`BOOST_TEST_RUN_FILTERS=!CaseA:!CaseB:...`) and leaving out any
  program whose every case is listed, so a known bug keeps CI green while any
  other failure turns it red. Run it locally with
  `extra/qa/nightly/ci_property_tests.sh <build dir>`.
- **`build_report_data.py`** — turns the run directories into the JSON behind
  the nightly report page: the results of every stage (grouped by build
  label: `debug_gcc14`, ..., `mtr-8.4`, `qa-8.4`), a description of every
  build label, and the entries of all three known-failure lists (each with
  its `kind` and `scope`).

The setup script's `nightly` step installs these under `$WS/nightly` and adds a
crontab entry (`NIGHTLY_CRON`, default `30 1 * * *`).

## Setting up a new machine

`setup_test_environment.sh` provisions a fresh Ubuntu 24.04 x86_64 host with
everything above, all versions pinned to match the shared test host. It is
idempotent and step-selectable:

```sh
extra/qa/setup_test_environment.sh                 # everything
extra/qa/setup_test_environment.sh deps project    # just rebuild the code
extra/qa/setup_test_environment.sh mtr s3 toxiproxy # just those services
```

Steps: `packages repo deps project mtr pylib s3 toxiproxy fsfault helpers
nightly`. It installs the GCC 14 toolchain, builds Boost and the AWS SDK with
the project's CMake presets, builds `binlog_server` in the debug / ASan / TSan
/ coverage configurations, downloads the MySQL 8.0 / 8.4 / 9.7 server and test
distributions, installs the Python libraries (Hypothesis, gcovr), the S3 server
and AWS CLI, Toxiproxy and the `fsfault` library, and writes the `start_*.sh`
helpers. Overridable via `WS`, `REPO_URL`, `REPO_BRANCH`, `PROJECT_PRESETS`,
`JOBS` and `NIGHTLY_CRON`. Only the `packages` step needs `sudo`. A full run
ends with a summary and ready-to-paste crash-test and MTR commands.

## In-tree property tests

The black-box tests here complement the Hegel property tests under
[`../../tests`](../../tests), which test the C++ components directly and are
what the nightly runs many cases of. Among them:
`gtid_set_property_test`, `event_property_test`, `storage_property_test`
(with a concurrent reader, also run under ThreadSanitizer),
`indexed_event_block_property_test`, `cipher_context_property_test`,
`minimysql_property_test` (the `pull` protocol codec), `rewriter_property_test`,
`reader_context_property_test`, `config_value_property_test`,
`event_generation_property_test` (the artificial ROTATE / FORMAT_DESCRIPTION /
PREVIOUS_GTIDS generators), `search_helpers_property_test`,
`purge_selection_property_test` and `sender_context_property_test` (the
`pull` COM_BINLOG_DUMP streamer: the stream preamble, reading each stored byte
once, partial events at EOF and polling an empty storage). Build them with
`-DWITH_PROPERTY_TESTS=ON` and run them with `ctest`. Their design, settings profiles (`hegel.toml`), how to
replay a failure, and a per-test rundown are documented in
[`../../tests/PROPERTY_TESTS.md`](../../tests/PROPERTY_TESTS.md).
