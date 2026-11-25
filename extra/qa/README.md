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
- [Coverage](#coverage)

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
  (ticket 16, PBS-84); the setup and nightly scripts export it for you.
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
also exercises the disk- and S3-fault rules (see below). In GTID mode the
binlog server runs in the now mandatory `rewrite` mode and writes its own
binlog files, so both invariants compare transactions rather than files: the
stored transactions must be a prefix of the source's during the run and all
of them after a complete fetch. `--tolerate-ticket-14` makes the file
comparison accept ticket 14's (PBS-80) effects and nothing else: a closed
binlog stored without the ROTATE event that ends it on the source, and
binlogs without transactions missing before the last stored one. It prints
how often it accepted them. Mandatory `rewrite` mode (upstream `4717df9`)
fixes ticket 14, PBS-80, so only `upgrade_test` still needs it, for the storage the
old binary writes. The other harnesses
reuse these pieces, so they share its config shape and its integrity checks.

## Correctness and crash tests

All of these import the harness and reuse its integrity checks; each prints
JSON and exits non-zero on a failure.

| Script | What it does | Found |
| --- | --- | --- |
| `fetch_crash_test.py` | randomized workload + crash/resume state machine (above) | — |
| `concurrent_storage_test.py` | runs several `fetch` instances against **one** storage at once (a cron `fetch` overrunning into the next run); checks the storage is not corrupted and heals. Two writers on one storage are an unsupported scenario (ticket 24, PBS-87, closed as not a bug in PBS-87), so its failures document misuse, not defects | ticket 24 (PBS-87) |
| `source_change_test.py` | changes the source under a running backup: `RESET BINARY LOGS`, a different server (failover), binlogs purged before fetch, a mid-stream restart | ticket 25 (PBS-141) |
| `event_variety_test.py` | drives less common event types — transaction compression, `binlog_row_image=MINIMAL`, partial JSON, XA, tagged GTIDs — through a full fetch and a kill/resume | — (no bug) |
| `pull_replica_fault_test.py` | network faults (Toxiproxy) between `pull` and a MySQL replica — resets, cuts at 1 KB / 100 KB / inside an 8 MB event / during the login, a stall past `replica_net_timeout`, refused connections, 1-byte pieces, latency, low bandwidth — while the source writes; the replica must reconnect and end with the source's data | — (no bug) |
| `pull_restart_test.py` | a MySQL replica of `pull` (auto-positioning in GTID mode) across restarts of `pull` — SIGTERM or SIGKILL while idle, while the source writes, inside an 8 MB event, while the source rotates into binlogs without transactions, while a lagging replica catches up, and five quick kill/restart cycles; the replica must reconnect to the new `pull` and end with the source's data | — (no bug) |
| `pull_variety_test.py` | a MySQL replica of `pull` must end with the source's data for a broad workload — every column type, row images, partial JSON, compression, statement format, DDL with routines and triggers, XA, an 8 MB row, savepoints, charsets, tagged GTIDs, empty transactions, VECTOR columns and Percona Server 9.7 vector indexes — live and replayed from the storage | — (no bug) |
| `query_race_test.py` | runs `binlog_server list` `--runs` times (default 500) next to a `pull` writing a filesystem storage with `checkpoint_size` `"1K"` while the source gets small inserts and a `FLUSH BINARY LOGS` every 20 of them; reports the failed `list` runs and each distinct error message | ticket 29 (PBS-101) |
| `large_binlog_test.py` | binlogs and transactions past 4 GiB, where positions no longer fit the protocol's 32-bit fields. Case `binlog_over_4gib`: about 1 GB of 1 MiB transactions and one of about 3.5 GB leave a 4.5 GB binlog; `fetch` must store it byte for byte, a second `fetch` reopen the storage, `list` report its size, and `mysqlbinlog --raw` through `pull` copy it exactly (passes). Case `transaction_over_4gib`: one transaction of about 4.7 GB. Needs about 20 GB of disk; `--keep` keeps the data, `--cases` picks the cases. With `--storage s3 --s3-bucket-dir <dir>` the storage is in the S3 bucket, behind a counting `s3_fault_proxy.py` the harness starts (`--s3-proxy-port`), and a further check `s3_put_size` requires that no request uploads more than 5 GiB, the AWS limit for a single PUT, which local S3 servers do not enforce (only `binlog_over_4gib` by default; about 30 GB of disk). First run (2026-10-28, debug build): passes; the 4.5 GB binlog goes up as a multipart upload of 8 MiB parts (largest request 8 MiB), and `pull` streams it back from S3 byte for byte. Versity S3 Gateway 1.8.0 takes about a minute to join the parts, and the AWS SDK's CRT client repeats the CompleteMultipartUpload after about 5 s, which the gateway confirms at once, before the object exists; the harness therefore waits up to 5 min for the objects (`s3_object_late_seconds`, 54 s) before comparing. AWS S3 confirms a completion only once the object exists | ticket 41 (PBS-114) |

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

# 'list' next to a writing 'pull' (ticket 29, PBS-101); keep --workdir short, the
# mysqld socket lives in it
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/query_race_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/qrace --runs 500

# a 4.5 GB binlog only (about 5 min plus the copies)
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/large_binlog_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/large-binlog \
  --cases binlog_over_4gib

# the same on S3 storage (the local S3 server must be running)
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/large_binlog_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/large-binlog-s3 \
  --storage s3 --s3-bucket-dir ~/ws/s3/data/pbs-mtr
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
fails cleanly and the storage stays openable. Six recovery cases follow: a
failed `binlog.index` save during a rotation leaves the new binlog's files
behind, the next `fetch` (whose start removes them) fails to delete one of
them (`op=unlink`, which covers `unlink`, `unlinkat` and `remove`), and a
third `fetch` without a failure must still recover (ticket 37, PBS-110: it did not
when the metadata file was the one left, until PBS-110, upstream `e2fdd50`). `s3_fault_sweep.py` ends with the
same two cases on S3 (failing `binlog.index` uploads, then a `DELETE` of the
metadata object failing for the whole start: the S3 CRT client retries a
`DELETE` about 10 times):

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
and checks that `pull` keeps running and still serves binlogs. `pull` has an
`ssl` section (the MTR test certificate), so a third of the scenarios are
hostile TLS clients (PBS-31): an SSL request followed by nothing, garbage or a
truncated TLS record, the SSL flag in a full greeting, and, after a completed
handshake, a garbage greeting, wrong plain-text passwords, garbage commands
and a second SSL request inside TLS; at the end a TLS client must still get
the binlogs too. The campaign is seeded and reproducible; run it against the
ASan build. A first run on 2026-11-02 (debug build at upstream `6b0960f`,
2000 connections per mode) found nothing.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/listener_fuzz_test.py \
  --mysql-basedir <dist> --binsrv <asan-binary> \
  --workdir ~/ws/qa/listener-fuzz --connections 5000 --seed 7
```

### Listener exhaustion — `listener_exhaustion_test.py`

Clients that never log in against the `pull` listener. `idle_flood` lowers
`pull`'s RLIMIT_NOFILE (`--fd-limit`, default 256), opens 200 more idle
connections than that, then closes them all: the listener must serve again
afterwards. `oversized_headers` sends 150 frame headers announcing almost
16 MB and checks that resident memory stays bounded. `payload_drip` and
`header_drip` check that the per-frame read timeout ends a slowly fed
session. On 2026-11-16 (`pbt-init` @ `23ef45c`, debug build) the last three
pass (memory grew 2.4 MB for 150 connections; the drip was closed at exactly
`read_timeout`) and `idle_flood` fails (ticket 55, PBS-128): `accept()` fails with
EMFILE, `handle_exception("listener")` ends the accept loop, and `pull`
stays up without a listener for good.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/listener_exhaustion_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/listener-exhaustion
```

### A clock that jumps, drifts or sits in the future — `clock_test.py`

Fakes the clock of the binlog_server process only (libfaketime v0.9.14 by
LD_PRELOAD, built from source without root with `make -C src
libfaketime.so.1` into `~/ws/clock/libfaketime` on the QA host), so the
source and the replicas keep real time. The offset comes from a control
file, so the clock can jump while `pull` runs: a wall jump moves
CLOCK_REALTIME only, a mono jump CLOCK_MONOTONIC too (monotonic jumps do
not happen on a real system and run only by hand). Cases: heartbeats to an
idle replica, read_timeout and idle_time reconnects, `checkpoint_interval`,
telemetry scheduling and cleanup, each with jumps of +1 day, -2 h and
-10 min; a PBS clock a day ahead of or behind the source's for `fetch`, the
stored timestamps and `search_by_timestamp`; six time zones (including
Pacific/Chatham and Australia/Lord_Howe) and the end of DST; and a PBS clock
at 2100, 2106 and 2300.

On 2026-11-18 (the merge of `dd6b598`, debug build) no wall-clock jump
moved anything: heartbeats came once a second with no gap over 1.2 s,
reconnect delays stayed at 5.0 s and 8.1 s, checkpoints and telemetry
reports came on time, all on `steady_clock`. Time zones changed no answer;
logs and metadata are UTC. Found:
- with only `checkpoint_interval`, the transaction that ends a quiet period
  is not checkpointed until the next event arrives or the source connection
  times out: the interval is checked when an event arrives, and covers only
  what came before it (`checkpoint/idle_tail`, ticket 94, PBS-167);
- in GTID mode the generated events carry PBS's clock (ticket 84, PBS-151):
  with PBS a day behind the source, `min_timestamp` is a day early and 7 of
  11 `search_by_timestamp` probes answer wrongly;
- past 2262 `system_clock` overflows: the listener answers a negative
  `UNIX_TIMESTAMP()` and telemetry writes `-8032952053-<tag>.json`, which
  is never cleaned up; after 2106 the generated event timestamps wrap
  (ticket 79, PBS-145);
- telemetry ages its reports by the wall-clock time in their names, so a
  forward jump past `history_keep_interval` deletes a report it has just
  written (ticket 96).
Use a build without a sanitizer. The nightly runs it on Saturdays
(position) and Sundays (gtid) without the monotonic jumps.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/clock_test.py \
  --mysql-basedir <dist> --binsrv <debug binary> \
  --faketime-lib ~/ws/clock/libfaketime/src/libfaketime.so.1 \
  --workdir ~/ws/qa/clock --mode position [--cases 'skew/*,tz/*']
```

### Storage file modes, symlinks and the S3 buffer — `file_security_test.py`

Records the modes of everything `fetch` creates under umask 022 and 000, and
plants symlinks in the storage directory, while `pull` runs, under the names
it creates next: the next binlog, its metadata's `.tmp` and `binlog.index.tmp`.
On 2026-11-16 all three cases fail. Under umask 022, binlog data and metadata
are 0644 (MySQL makes its binlogs 0640); under umask 000, the binlogs, their
metadata, the index, `metadata.json` and the log are 0666 (ticket 56, PBS-129). Every
planted symlink is written through into the file outside the storage, and the
rename that follows leaves `binlog.index` and the binlog metadata as
permanent symlinks (ticket 57, PBS-130).

Two more cases were added on 2026-11-16, and both fail. `telemetry_symlinks`
reads the tag from the name of `pull`'s first telemetry report and plants
symlinks under the temporary names of the next ones: the file they point at,
mode 0600 and outside the telemetry directory, ends up holding a report with
mode 0644 (ticket 66). `s3_buffer` runs `pull` on the local S3 server (only
with `--s3-bucket-dir`): without `fs_buffer_directory` the backend's local
buffer under `$TMPDIR` is a 0755 directory with a 0644 file under umask 022,
and an existing `fs_buffer_directory` of mode 0777 is accepted (ticket 65, PBS-138).

`s3_buffer_swap` (2026-11-16, also only with `--s3-bucket-dir`) does what
that directory allows. The backend downloads the binlog object into the
buffer file and then opens the file again by path; a watcher renames a file
or a symlink of its own over the buffer file as soon as it appears (the test
runs as the same account, standing in for another one that can write the
directory; it only lists the directory and renames its own entry into it).
Each variant is tried five times and the swap won every time: a regular file
is uploaded as the binlog object with the new events appended; a symlink to
a canary file outside the directory gets the events appended, or is
truncated when the swap hits the start-up resize, and its content is
uploaded to the bucket. In the debug build the plain `symlink` variant ends
in an assertion (`s3_storage_backend.cpp:366`, the size of the path against
the downloaded length), which release builds do not have. The case fails
(ticket 65, PBS-138).

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/file_security_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/file-security \
  --s3-bucket-dir ~/ws/s3/data/pbs-mtr
```

### Edited binlog metadata of an encrypted storage — `metadata_tamper_test.py`

Between two `fetch` runs, someone who can write to the storage but has no
keyring edits the metadata of the last stored binlog; the configuration and
the keyring stay as they are. On 2026-11-16 both cases fail. With the
`encryption` record removed (`strip_encryption`), the next `fetch` exits 0
and appends plaintext to the encrypted storage (ticket 62, PBS-136). With the record
replaced by the previous binlog's (`transplant_encryption`), the appended
data is encrypted with that binlog's AES-CTR keystream: XORed with the
keystream the previous binlog's known plaintext gives, every appended byte
decrypts (ticket 63, PBS-137). A `fetch` that refuses the edited metadata passes
either case.

A sweep added later that day edits one member at a time (82 cases in
position mode, 84 in GTID mode; `--cases` selects some, `--storage s3` runs
them on the local S3 server, where the edits go through the S3 API): `size`,
the GTID sets, timestamps, `last_sequence_number`, `version`, every part of
the `encryption` record, the whole file, `binlog.index` and `metadata.json`.
After each edit it runs `list`, `fetch` and `list` again and requires that
no process dies or hangs, nothing readable is appended, no keystream is
reused, the other binlogs keep their bytes, and, if `fetch` exits 0, that
`pull` still serves an exact copy of the source's binlog. Malformed values,
wrong types, GTID edits and index edits are refused cleanly. 27 cases fail
(28 in GTID mode), the same on file storage, on S3 and with the ASan build:

- `"encryption": null` gives plaintext like a removed record (ticket 62, PBS-136).
- Lowering the block counter in the stored data IV makes the appended data
  reuse the keystream of the same binlog's start, and in GTID mode with
  rewrite a lowered `size` does the same over the truncated transactions
  (`size_rewrite`; both ticket 63, PBS-137).
- A `kek_id` of another KEK, or a changed IV or wrapped key, is accepted:
  `fetch` exits 0 and appends data nobody can read. A first index entry that
  is removed makes `fetch` delete that binlog (ticket 53, PBS-126).
- Sixteen edits that make the final flush fail (a cipher name that is not a
  CTR cipher, an IV or key of the wrong length, a GCM tag that no longer
  verifies) end with "successfully fetched everything" and exit 0 while
  nothing is stored: the storage destructor swallows the error (ticket 73, PBS-143).
  Since upstream `0e45db5` (PBS-150) `fetch` flushes explicitly when the
  source ends the stream, so these edits now end with exit 1 and the
  error ("the specified cipher is not a CTR cipher", "cannot finalize
  cipher context"); they pass since 2026-11-24.

Since upstream `4717df9` GTID mode is always rewrite mode, whose binlogs are
the Binlog Server's own files. In GTID mode the harness therefore sets an 8K
`rewrite.file_size` and makes each of the first two source binlogs one
transaction larger than that, so the stored binlogs still switch where the
source's do. It takes the plaintext of a stored binlog from what `pull`
serves, and the served copies must hold the source's first transactions
rather than its bytes. On 2026-11-17 (the merge build) 35 of 84 GTID cases
fail. Besides the cases above, which fail in GTID mode too (`strip_encryption`
and `transplant_encryption` included), these are new in rewrite mode:
- a lowered `size` makes the next transactions reuse the cut-off ones'
  keystream on the sweep's storage too (`size`, ticket 63, PBS-137);
- an emptied or foreign `previous_gtids` / `added_gtids` makes `fetch` store
  transactions again, an extended `added_gtids` makes it skip one, with exit
  0 (ticket 81, PBS-147);
- a `size` lowered into the last event lets rewrite mode append behind the
  broken event, which `pull` cannot serve past (`size_lowered`), and a
  swapped index pair makes `fetch` append to the wrong binlog before it
  refuses (`index_reordered_pair`; both ticket 53, PBS-126).
The nightly runs GTID mode on Saturdays.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/metadata_tamper_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/metadata-tamper
```

### Tampered data served to a replica — `replica_tamper_test.py`

The storage attack tests stop at the storage; this one puts a real MySQL
replica behind `pull`. In `forged_row` the source commits one row with a
known value into an encrypted storage; the test XORs the value's bytes in
the stored ciphertext with (old XOR new) and patches the event's CRC32
footer with `crc32(delta) XOR crc32(zeros)`, which needs no key. On
2026-11-16 the replica ends with the forged value, and neither `pull` nor
the replica reports anything (ticket 53, PBS-126, now shown end to end).
Without the footer patch (`forged_row_bad_checksum`) the replica's receiver
stops with error 13122 and applies nothing, so the replica's own checksum
verification is the only check; `pull` serves the event either way. In
`doubled_preamble` the binlog a hostile source leaves in ticket 69 (PBS-144) is served
to a replica: the replica ends with exactly the source's data, so that
malformed binlog does not corrupt a replica (recorded, not judged).

`plaintext_swap` (2026-11-16) is the read side of ticket 62 (PBS-136): the data file
of an encrypted binlog is replaced with plaintext events (the source's own
binlog with one row value changed and the CRC32 recomputed) and the
`encryption` member is deleted from its metadata. `pull` serves it and the
replica applies the attacker's row, with no error anywhere. `--mode gtid`
runs the same cases with a replica in auto-position mode: `forged_row` and
`plaintext_swap` fail the same way, and `doubled_preamble` is then produced
with `reannounce_midstream` of `hostile_source_test.py`.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/replica_tamper_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/replica-tamper
```

### Is a failing final flush reported? — `final_flush_test.py`

Without checkpointing, `fetch` writes what it received for the current
binlog when it exits. The test lets a first `fetch` store one binlog, adds
transactions without a rotation, and runs a second `fetch` with a persistent
fault on what that last flush does: ENOSPC or EIO on the write or fsync of
the binlog data and on the open, write, fsync or rename of its metadata
(`--fsfault-lib`), or, with `--s3-fault-proxy`, every PUT of the binlog
object or of its metadata failing. On 2026-11-16 all 12 disk cases and all 4
S3 cases fail the same way: `fetch` logs "successfully fetched everything
and disconnected" and exits 0 while the new data is not recorded (ticket
73). The next `fetch` without a fault stores it, as long as the source still
has it. Faults that hit a flush at a rotation are reported (the existing
sweeps cover those).

More cases (2026-11-16) show the same dropped error elsewhere. `pull`
stopped by SIGTERM or SIGINT with a fault on its last flush logs
"successfully shut down after receiving a termination signal" and exits 0
with nothing recorded (`pull_sigterm/*`, `pull_sigint/*`). With
`checkpoint_size` or `checkpoint_interval` a fault at a checkpoint is
reported, but the flush at exit still is not, so the tail after the last
checkpoint is lost silently (`checkpoint/*/final_flush/*`); the same when
only one write of the final flush fails once (`partial/*`). All of these
are ticket 73 (PBS-143). Since upstream `0e45db5` (PBS-150), which makes
`fetch` flush explicitly when the source ends the stream, the `fetch` cases
pass (2026-11-24: 37 of 44, the disk and partial-flush cases included);
`pull` stopped by a signal (`pull_sigterm/*`, `pull_sigint/*`,
`checkpoint/pull/*/final_flush/*`) still drops the error, and the S3 cases
were not re-run. Reported correctly: a failing flush at a rotation or after a
read timeout in a running `pull`, every fault while a first `fetch` creates
the storage, `purge_binlogs` (status "warning" when a binlog cannot be
deleted, "error" when the index cannot be rewritten), and a log file that
cannot be created. One more defect is separate: `list` and
`search_by_timestamp` leave out a binlog whose metadata cannot be parsed and
still answer "success" (`query/*`, ticket 80, PBS-146). 23 of 44 cases fail on file
storage and 15 of 33 with `--s3-fault-proxy` (cases `.../on_s3`; the
`s3_buffer/write` case needs `--fsfault-lib` as well). `--cases` selects
cases by substring.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/final_flush_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/final-flush \
  --fsfault-lib ~/ws/qa/fsfault/libfsfault.so
```

### Listener logins and long statements — `listener_login_test.py`

Talks to the `pull` listener with the raw client of `listener_fuzz_test.py`.
`user_name_probe` sends a client greeting with a random scramble, once with
the configured user name and once with another: after one successful login
the configured name gets ERR 1045 at once and any other name "perform full
authentication", so the first reply tells whether a name is the replication
user (ticket 67, PBS-142). `long_statement` logs in and sends `SET` followed by 1,000
to 1,000,000 spaces: at 100,000 `pull` ends with a segmentation fault, in the
debug and the release build (ticket 72, PBS-140). Both fail on 2026-11-16.

Three more cases (`--cases` selects some; `long_statement` stays last, as it
ends `pull`):

- `guess_rate` measures wrong-password logins for 10 s per phase, with 1 and
  8 clients, over the full (RSA) and the fast path. Nothing throttles them:
  about 490 a second on the full path and 1,500 on the fast path (debug
  build; 713 and 4,674 with the release build), every one answered ERR 1045
  and logged, while a legitimate login is still served within 50 ms. It
  passes: MySQL has no lockout by default either.
- `rsa_exchange` sends 15 malformed answers on the RSA path (all refused),
  compares the timing of padding-valid and padding-invalid ciphertexts (no
  difference beyond 50 µs over loopback) and replays a captured scramble
  and a captured ciphertext (both refused). It passes.
- `plain_password_without_tls` runs 66 checks of odd logins: a plain
  password outside TLS, passwords with and without the NUL, user name
  variants, other plugins and the auth switch, an SSLRequest without a
  ClientHello, packets after the OK. One fails: a line feed in the user
  name of a failed login puts a line of the client's choosing into the
  `pull` log, and a 1 MB user name is logged twice and echoed in a 1 MB
  error packet (ticket 76, PBS-152).

### What a logged-in client can send — `listener_command_fuzz_test.py`

Logs in with the raw client and sends what `mysqlbinlog` and a replica never
would: every handshake statement with long or overflowing values,
COM_REGISTER_SLAVE, COM_BINLOG_DUMP and COM_BINLOG_DUMP_GTID with truncated
bodies, absurd positions, names and GTID sets, COM_CHANGE_USER, every
command byte, wrong sequence numbers, a second handshake, commands during a
dump; then 300 random connections (`--connections`, `--seed`). After each it
requires `pull` to run, to serve a new login within 10 s and to stay under
1 GiB. 31 of 43 cases pass on 2026-11-16, the random phase included. Three
failures are new: one COM_QUERY of 16 MB keeps every client waiting for 25 s
(3 s with the release build; ticket 74, PBS-156), and a heartbeat period of 1 ns or
1,000 ns makes an idle dump send and log thousands of heartbeats a second
(ticket 75, PBS-155). The other nine are ticket 72 (PBS-140) again, with other statements: any
run of 40,000 or more characters that one of the handshake patterns matches
ends `pull`; they run last (`--no-long-runs` leaves them out).

With `--tls` (2026-11-16) `pull` gets the MTR certificate and every login
and command goes inside TLS; five cases are added: MySQL frames split
across TLS records down to one byte, plain data after the TLS handshake, a
close_notify in the middle of a command, TLS 1.3 key updates and a TLS 1.2
renegotiation request during a dump (the last two call libssl through
`ctypes` and report `skipped` where that does not work). 36 of 48 cases
pass on the debug, ASan and UBSan builds in both modes; the 12 failures are
the ones above. The nightly reports these cases as `<mode>/<case>/tls`.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/listener_command_fuzz_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/listener-commands
```

### TLS settings on both sides — `tls_config_test.py`

On the listener it enumerates the protocol versions and cipher suites with
hand-built ClientHello messages and real handshakes, and checks
renegotiation, compression, resumption and that a TLS client is never served
in plain. With Ubuntu 24.04's OpenSSL policy the listener takes TLS 1.2 and
1.3 only and no NULL, anonymous, export, RC4 or 3DES suite; it does take
four CBC-SHA1 and six static-RSA suites. The listener sets no minimum
version or cipher list itself, so under a legacy OpenSSL policy
(`OPENSSL_CONF` with `MinProtocol = TLSv1`) the same binary accepts TLS 1.0
and 1.1; that case only records what it finds (ticket 77, PBS-153). Towards a TLS
source it runs `fetch` with `connection.ssl` and `connection.tls` settings:
a client certificate for a `REQUIRE X509` user (missing, of another CA,
expired, with the wrong key), `ca` / `capath`, `crl` / `crlpath` with the
source's certificate revoked, `cipher`, TLS versions and cipher suites, and
`verify_identity` to an address that is not in the certificate. All 30
settings that must make the connection fail do, and the 10 controls
connect; none of the 30 refusals says why in the log (ticket 47, PBS-121).
All 51 cases pass on 2026-11-16.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/tls_config_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/tls-config
```

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/listener_login_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/listener-login
```

### Odd answers to the queries before the binlog dump — `source_reply_test.py`

Before the binlog dump, `fetch` sends the source `SET
@source_binlog_checksum = ...` and, in GTID mode on an empty storage, `SELECT
@@GLOBAL.gtid_purged`. A real server answers both, so the error paths after
them were reached by no test (the 2026-11-18 coverage measurement). A proxy
between `fetch` and a real MySQL source answers one of them itself: an ERR
packet to the SET query; to the SELECT an ERR packet, no rows, two rows, two
columns, a NULL value or a value that is not a GTID set; or it resets the
connection right after the SET query's answer, before the binlog dump
command. `fetch` must exit 1, store nothing and log the reason. On
2026-11-23 (MySQL 8.4.8, both modes) every case ends cleanly with exit 1 and
an empty storage, but only the unparsable GTID set is logged with its
reason ("cannot parse GTID set"). For every other case `collector_context`
logs only "unable to switch to replication" (or, for the reset, "fetching
binlog events loop did not reach EOF") and drops the server's error or the
problem with the result: the gap ticket 47 (PBS-121) describes for a failed
connect, in a second place.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/source_reply_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/source-reply \
  --mode gtid
```

The command line itself has an MTR test, `binlog_streaming.command_line`:
`binlog_server version`, and a missing or unknown operation, the wrong
number of arguments, the internal `delimiter` value and an upper-case
operation name, each refused with exit code 1 and the usage message.

### Simulated network and time — `shadow_test.py`

A MySQL 8.4 source, `pull` and a workload client run under the
[Shadow](https://shadow.github.io) simulator, each on its own simulated
host, with simulated time and a simulated network (`shadow/README.md`:
the Shadow patch `mysqld` needs, how to build it, what does not work). Each
case is one simulation by `shadow/shadow_pbs.sh`, which then compares PBS's
storage with the source (byte for byte in position mode, by GTIDs in GTID
mode, which is rewrite mode) and checks the timestamp range recorded for
every binlog. Cases: `basic` (30 rounds of inserts, binlog rotations),
`determinism` (`basic` again with the same seed: the same outcome),
`restart` (SQL `SHUTDOWN` at 40 s, a new `mysqld` at 55 s), `crash`
(SIGKILL right after `FLUSH BINARY LOGS`, then a restart: ticket 6,
PBS-72), `lossy` (2 % packet loss, 20 ms latency), `idle` (an hour of
simulated idle time in about 3 minutes), `quiet` (the last transaction
followed by silence: ticket 94, PBS-167), `slow` (one 20 MB transaction
over a 2 Mbit link), `cron` (`fetch` every 5 minutes for 3 simulated
hours, the source restarted every hour), `replica` (a MySQL replica of
`pull`'s listener on a fourth host), `replica_early` (the replica connects
while `pull` has stored nothing: tickets 26 and 38), `heartbeat`
(heartbeats to a caught-up replica after a rotation: ticket 40), `stop`
(SIGTERM to `pull` with the source idle: ticket 43), `replica_restart`
(the replica killed and started again), `replica_lossy` (2 % loss, 20 ms
between `pull` and the replica), `replicas3`, `pause_short` and
`pause_long` (a relay host between `pull` and the source goes silent for 10
or 120 s: no RST, nothing arrives), `pause_stop` (SIGTERM during that
pause), `replica_pause` (the same between the listener and the replica),
`pause_purge_safe` and `pause_purge_lost` (during the pause the source
purges binlogs `pull` already has, or ones it still needs: then `pull`
must exit with an error and leave a prefix of the source), and the S3
cases on an S3 storage (versitygw on its own host): `s3_basic`, `s3_lossy`
(2 % loss, 20 ms), `s3_pause` (the link to S3 silent for 60 s) and
`s3_restart` (the S3 server killed for 20 s). The kill sets kill `pull`
with SIGKILL at a microsecond around an anchor line of a reference run's
log and start it again 1 s later, one case per kill point
(`<set>/<offset>us`; the nightly runs 4 of the 61 offsets per set, picked
by the seed): `pbs_kill` (the first rotation), `pbs_kill_checkpoint` (the
checkpoint at the 5th transaction), `pbs_kill_reconnect` (the reconnect
after a source restart), `enc_pbs_kill` and `enc_pbs_kill_checkpoint` (the
same on an encrypted storage, checked through a replica, plus the
keystream check below), `replica_pbs_kill` (one kill while a replica
streams from `pull`), and `s3_pbs_kill` and `enc_s3_pbs_kill` (the first
rotation on an S3 storage, plain and encrypted). The nightly passes the day of the year as the seed;
a failing case's JSON line names its seed. Each run deletes its MySQL data
directories at the end (`KEEP_DATA=1` keeps them): they were about 200 MB a
run, and the sweeps filled the QA host's disk on 2026-11-24.

Results on 2026-11-24 (debug build, seeds 329 and 330): in position mode
`crash` aborts `pull` on the assertion of ticket 6 for every kill after the
rotation (kill times 28 to 60 s, seeds 1 to 4; a kill before the rotation
passes), and a release build records the dropped events' timestamp (that
of the killed source's last binlog) as the next binlog's `min_timestamp`,
18 s before its first event. In GTID mode `quiet` loses the last
transaction until the reconnect (ticket 94); the closing ROTATE that would
make PBS write it in position mode is skipped in rewrite mode. Packet loss
of 2 and 5 % with 20 and 50 ms latency, the restart and the idle hour
store everything.

The kill sweep (every microsecond from 20 before to 40 after the first
"closed binlog file" line, both modes, 122 runs) found no defect: the
restarted `pull` always ended with the source's binlogs. A process on the
`pull` host records the storage between the kill and the restart; the kills
left 9 different states in position mode and 10 in GTID mode, every step of
the rotation: the closing ROTATE not yet written, the binlog metadata only
as `.json.tmp` (once 0 bytes), the new binlog at 0 and at 4 bytes, its
metadata missing or only as `.tmp`, `binlog.index.tmp` written but not
renamed, and the index updated.

With a replica (position mode runs source and replica without GTIDs: a
position-mode PBS of a GTID source cannot serve a MySQL replica, ticket
30, PBS-102): `replica` ends with every row in both modes. In position
mode `replica_early`, which names `binlog.000001` while `pull` has not even
created it, gets fatal error 1236 "Could not find first log file name in
binary log index file" and its receiver thread stops for good (ticket 38,
PBS-111); in GTID mode the auto-position replica waits and catches up.
`heartbeat`: after the closing `FLUSH BINARY LOGS` the position-mode
replica gets no heartbeat in 20 s and reconnects every
`replica_net_timeout` (ticket 40, PBS-113); in GTID mode it gets one a
second. `stop`: `pull` exits 35.5 s after SIGTERM in both modes, when its
read of the idle source times out (ticket 43, PBS-116).

Results on 2026-11-25 (every offset of each kill set, seed 1):

* Ticket 32 (PBS-104) reproduced end to end. On an encrypted storage in
  GTID mode (rewrite mode), 11 of the 61 kill points around the first
  rotation, 14 to 4 us before the anchor, leave the last transaction and
  the ROTATE that PBS writes itself (783 bytes) in `binlog.000001` while
  its metadata still records the old size (the new metadata only as
  `binlog.000001.json.tmp`). After the restart PBS writes the range again
  under the same encryption record, and 5 of the 783 bytes differ: the low
  byte of the ROTATE's timestamp, which comes from PBS's clock (ticket 84),
  and its CRC32. The AES-CTR keystream at those offsets is used for two
  plaintexts; XOR-ing the two ciphertexts gives the XOR of the plaintexts.
  The replica still applies every row. `pbskill` keeps a copy of the
  storage between the kill and the restart (`kill_copy`), and
  `check_binlog_timestamps.py --rewritten` compares what the kill left past
  each recorded size with what is there afterwards.
* At the checkpoint (`enc_pbs_kill_checkpoint`, GTID mode; and
  `pbs_kill_checkpoint`, position mode) every kill recovers: the
  unrecorded tail is the transaction itself, written again byte for byte.
  The kills left 3 states: before the data write, data written with the
  metadata only as `.tmp`, and done.
* `pbs_kill_reconnect` (position mode): every kill recovers; a reconnect
  writes nothing to the storage, so all 61 kills find the same state.
* `slow`: the 20 MB transaction arrives intact in both modes (the source
  needs a longer `connect_timeout` for the workload's own logins on the
  saturated link). `cron`: 38 runs of `fetch`, none failed, the storage
  equal to the source. `replica_restart`, `replica_lossy`, `replicas3`
  and `replica_pbs_kill`: every replica ends with every row.
* Two fixes to the scenarios themselves: replicas get a fixed server UUID
  and their own `tmpdir` (a UUID generated inside the simulation is the
  same in every run, and concurrent replicas collided on
  `/tmp/SQL_LOAD-<uuid>`), and runs side by side make the template data
  directory under a lock.

Second round on 2026-11-25 (seed 329, both modes, 4 kill points per set):

* Ticket 98 (PBS-176, new): an S3 endpoint that cannot be reached for a few seconds
  stops `pull` for good. In `s3_restart` the next checkpoint upload fails
  (`AWS_IO_SOCKET_TIMEOUT`) 14.7 s after the S3 server is killed, and `pull`
  exits 1; in `s3_pause` the upload fails 37 s into the silence ("failure to
  meet throughput minimum"). A native run on the QA host does the same
  (versitygw killed for 30 s: `pull` exits after 8 s,
  `AWS_IO_SOCKET_CONNECTION_REFUSED`). Only MySQL errors
  (`easymysql::core_error`) lead to a reconnect; the README promises that
  for "any network-related error". The storage stays a valid prefix.
* `pause_stop` is ticket 43 again: `pull` exits 29.2 s after SIGTERM, when
  its read of the silent source times out.
* Everything else passes. `pause_long`: `pull` times out, its reconnects
  during the pause get no greeting, and after the pause it catches up.
  `pause_purge_lost`: `pull` exits 1 at the end of the pause with "Could not
  find first log file name in binary log index file: CR_1236" in position
  mode and "Cannot replicate because the source purged required binary
  logs" in GTID mode, leaving a prefix of the source (15 of 32
  transactions). `replica_pause`: the replica reconnects and gets every row.
  `s3_lossy` and the S3 kill sets (including encrypted) recover.
* Two Shadow limits found on the way: Shadow runs only dynamically linked
  binaries (versitygw is built with `-linkmode=external`), and versitygw
  needs `--disableotmp` (`shadow/README.md`).

Runs with the same seed are not byte-identical: `mysqld` needs Shadow's
native preemption (InnoDB spin-waits), which is driven by real CPU time, so
simulated times differ by microseconds. The outcome (binlogs and sizes,
transactions, process end states, verdict) repeated in every run so far.

```sh
python3 extra/qa/shadow_test.py --mysql-basedir <dist> --binsrv <binary> \
  --workdir ~/ws/shadow/h --mode position --seed 1 --jobs 4 \
  --shadow ~/ws/shadow/install/bin/shadow
# every kill point of one set:
python3 extra/qa/shadow_test.py ... --cases enc_pbs_kill --kill-points all
# one scenario, with its full report:
SEED=7 KILL_AT=35 extra/qa/shadow/shadow_pbs.sh try crash
```

### What `pull` logs at info level — `log_hygiene_test.py`

Upstream `dd6b598` (PBS-118, ticket 45) moved the per-event messages to
debug level, logs an idle replica's `fetched EOF` and `waiting for first
binlog in empty storage` once per idle period (`logger_repeat_adapter`), and
writes each client command on one info line. This harness checks that
against a real `pull` at the default level, and what a logged-in client can
make it log:
- `no_per_event_lines`: a MySQL replica and a raw dump client get 150 and
  300 transactions; the info log may grow only per binlog file;
- `idle_eof_once`: `fetched EOF` once per idle period, again after new
  events, across a rotation and with 1 s heartbeats, and `waiting for first
  binlog in empty storage` once while the source is down;
- `log_injection`: line feeds, CR, ANSI escapes and NUL in statements and
  binlog dump file names, 1 MB and 4 MB statements, large and malformed GTID
  sets in COM_BINLOG_DUMP_GTID;
- `log_flood`: 30 s of queries and 10 s each of pings and 64 KB statements
  from one logged-in client; log bytes per second and per command.

On 2026-11-18 (the merge of `dd6b598`, debug and ASan builds, both modes)
the first two pass: 300 transactions add 14 info lines, all per binlog
file, and every idle line comes once per period. The other two fail. The
statement, the dump's file name and the GTID set are logged raw and whole:
a line feed forges a log line, CR / ESC / NUL reach the log, a 4 MB
statement is a 4 MB line, and a 900,000-interval GTID set is decoded and
printed for the log before any check, which holds the listener thread for
11 s (over 60 s on ASan), in position mode too (ticket 92, PBS-166). Every query or
ping adds two info lines with no limit: one connection grows the log by
about 0.5 MB/s, 40-60 GB a day (ticket 93). The adapter itself has a
property test, `tests/logger_repeat_adapter_property_test.cpp` (6 cases
pass; a disabled seventh shows that group id `SIZE_MAX` overflows
`group_id + 1`, ticket 95, which no caller reaches today).

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/log_hygiene_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/log-hygiene \
  [--mode gtid]
```

### Hostile source binlog names — `hostile_source_test.py`

A man-in-the-middle proxy between a real MySQL source and `fetch` rewrites
the binlog name in the ROTATE events that announce the next binlog (fixing
the event size, log_pos, CRC32 and packet length), as a compromised source
or an attacker on a connection without TLS verification could. `fetch`
reads the dump without TLS (the server's cached password hash, as in
`fetch_ssl_disabled_caching_sha2`). After each case every stored binlog must
keep its data, `metadata.json` must be unchanged and the storage must still
open (`list`). On 2026-11-16 four of six names fail (ticket 59, PBS-131). With
`binlog.000001\0.000002`, `binlog.000001` loses its data. With
`metadata.json\0...`, `metadata.json` is overwritten ("unable to extract
mode"). With `binlog.index\0...` and a name containing a newline,
`binlog.index` gets an invalid entry and the storage no longer opens. In all
four cases `fetch` exits 0. `..` as a base name stays inside the storage, and
a 3000-byte name makes `fetch` fail cleanly. The same four names fail in GTID
mode (`--mode gtid`) and against an S3 storage on the local gateway
(`--storage s3`). Two resource attacks pass: `size_lie` (an event that claims
almost 4 GB is refused at once) and `gtid_bloat` (a PREVIOUS_GTIDS_LOG of
900,000 intervals is stored in 0.9 s with 32 MB; what it later costs is not
measured).

Three more names were added on 2026-11-16, and all fail in position mode.
`reannounce` renames the next binlog to the current one: `fetch` exits 0 and
the stored binlog gets a second FORMAT_DESCRIPTION and the next binlog's
events after its ROTATE (ticket 69 (PBS-144); in GTID mode `fetch` refuses it).
`lower_sequence` uses `binlog.000002` on a storage that starts at
`binlog.000004`: `fetch` exits 0, and every later start fails with "binlog
index contains a reference to a non-existing object" (ticket 70, PBS-158, also in
GTID mode). `case_variant` uses `BINLOG.000001` and runs only with
`--case-insensitive-dir <directory>`, a directory on a file system that
ignores case (a vfat loop mount, for example), where the storage is then
placed: the stored `binlog.000001` is replaced (ticket 71, PBS-157). The nightly has
no such mount, so it does not run this case.

`reannounce_midstream` (2026-11-16, GTID mode only) is the variant of
ticket 69 (PBS-144) that GTID mode does not refuse: after a complete transaction the
proxy injects an artificial ROTATE that names the open binlog at position 4
and copies of its FORMAT_DESCRIPTION and PREVIOUS_GTIDS_LOG, and lowers the
log_pos of what follows. `fetch` exits 0 and the stored binlog has a second
preamble in the middle (2330 bytes for the source's 2176). In position mode
the same sequence is refused ("unexpected binlog position in artificial
rotate event"; `--midstream-in-position-mode` runs it there).

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/hostile_source_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/hostile-source
```

### S3 uploads changed on the way — `s3_upload_tamper_test.py`

With an `http://` S3 endpoint the uploads carry `x-amz-content-sha256:
UNSIGNED-PAYLOAD` and no checksum header, so the request signature does not
cover the body. The test puts its own proxy between `fetch` and the local S3
server and changes bodies without touching anything signed. On 2026-11-16
three cases fail (ticket 78, PBS-154). `put_body_changed`: a row value in the PUT of
the binlog is replaced (event CRC32 recomputed); the server answers 200,
`fetch` exits 0 and the object keeps the attacker's bytes. `put_metadata_changed`:
the `size` in the PUT of the binlog's metadata is changed; set to the start
of the last transaction, the next `fetch` in GTID mode truncates the object
and exits 0 without that transaction, while in position mode it fetches it
again. `get_body_changed`: a changed GET response is taken by the resumed
`fetch`, appended to and uploaded again. Two cases are informational:
`server_checks_signed_hash` (the gateway refuses a body that does not match
a signed hash, so the gap is the client's choice) and `https_endpoint` (from
the code: certificate verification is left at the SDK default, on).

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/s3_upload_tamper_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/s3-tamper \
  --s3-bucket-dir ~/ws/s3/data/pbs-mtr
```

### What a kill loses at each checkpoint setting — `checkpoint_loss_test.py`

Runs `pull` and `fetch` against a source committing about 2 MB/s in
transactions of 10 to 100 KB, with no checkpointing, `checkpoint_size` 64K,
1M and 16M, and `checkpoint_interval` 1s; kills them with SIGKILL at a
random moment (five times per setting) and once with SIGTERM, and compares
what the source had committed with what the storage recorded. A setting
passes when the loss stays under its bound (the checkpoint size plus the
largest transaction plus one second of workload, or the interval plus one
second of workload) and the next start ends as an exact copy; without
checkpointing the loss is recorded only. On 2026-11-16 all 20 cases pass on
file storage (median loss after a kill: 7 MB without checkpoints, 12 KB at
64K, 150 KB at 1M, 0.9 MB at 1s). With `--storage s3` two fail: at 64K
`pull` cannot keep up, because every checkpoint uploads the whole binlog
again (ticket 17 (PBS-82); 375 MB uploaded for an 8.5 MB binlog), and a kill or even
SIGTERM loses 3 to 7 MB. The numbers depend on the host's load. With
`--no-s3-proxy` some kills at 64K leave the local gateway with a cut-off
object shorter than its metadata says, which the next start refuses; that is
how this gateway treats an interrupted PUT, and was not checked on AWS.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/checkpoint_loss_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/checkpoint-loss
```

### Rewrite mode under attack — `rewrite_attack_test.py`

GTID mode with `replication.rewrite`, where the stored files are not copies
of the source's, so the cases are judged by the stored stream of GTIDs,
`list` / `search_by_gtid_set`, and a MySQL replica of `pull` in
auto-position mode. 31 of 49 cases pass on 2026-11-16 (the same on the
ASan and UBSan builds, without sanitizer reports). The hostile binlog names
of `hostile_source_test.py` have no effect here: the source's ROTATE events
are dropped. What fails:

- nothing compares a GTID from the source with what is stored: sent twice
  it is stored twice, two swapped transactions are stored swapped, and
  edits of `added_gtids` / `previous_gtids` in the last binlog's metadata
  make `fetch` skip transactions for good or store them again, with exit 0
  (ticket 81 (PBS-147); without rewrite a position check refuses these);
- a `transaction_length` lowered on the way: `fetch` exits 1, but GTID and
  BEGIN are stored and the GTID recorded; the next `fetch` exits 0 without
  the rows, and a MySQL 8.4 replica of `pull` aborts (ticket 82, PBS-148);
- the encryption-record and `size` edits of tickets 62 and 63 behave as
  without rewrite;
- after a kill the bytes past the recorded size are written again with the
  same key and IV, and because the generated events carry a new timestamp
  the two plaintexts differ (ticket 32, PBS-104, here without any attacker).

`--random-kills` sets the number of kills of the last case (default 10).

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/rewrite_attack_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/rewrite-attack
```

### A hostile source against a running `pull` — `hostile_source_pull_test.py`

`hostile_source_test.py` and `rewrite_attack_test.py` attack `fetch`. This
one sends the same man-in-the-middle attacks to a long-running `pull` while
a MySQL replica and a `mysqlbinlog --raw --stop-never` client are connected
to its listener, in position mode and GTID mode (which always runs with
rewrite now that upstream `4717df9` makes `replication.rewrite` mandatory
there; `--rewrite` is kept for compatibility), and adds what only a long-running process meets: the source
stalling in the middle of an event, dropping the connection in the middle
of a transaction, replaying stored events after a reconnect, and ERR or EOF
packets at odd points. A case passes when `pull` refuses cleanly and neither
the storage nor the replica gets anything that is not the source's; a
stored ROTATE with a name the source never used counts as a failure here.
On 2026-11-16: 9 of 20 cases fail in position mode, 11 of 21 in GTID mode
and 6 of 23 with rewrite, the same on the ASan build, with no exit by
signal. The hostile names (tickets 59, 69, 70) do the same damage as with
`fetch`, and `pull` keeps serving from the damaged storage: a binlog is
truncated while a replica reads it, and the replica then stalls without an
error in its status. In GTID mode `pull` also accepts the plain
re-announcement that `fetch` refuses. With rewrite, an EOF after GTID +
BEGIN left fragments in the stored binlog that `pull` could no longer serve
(ticket 83, PBS-150; fixed upstream by `0e45db5`, which discards the
incomplete transaction and flushes the complete ones on an EOF too, and
`eof_mid_transaction` passes since 2026-11-23), and duplicated, swapped or replayed transactions are stored
(ticket 81, PBS-147). A lowered `transaction_length` leaves a stub in every mode
(ticket 82, PBS-148). `pull` reconnects once a second without back-off; an ERR
packet from the source ends it with exit 1. Since rewrite mode is
mandatory, the GTID-mode cases without it are gone: rewrite mode names its
own binlogs, so the hostile names no longer reach the storage in GTID mode.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/hostile_source_pull_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/hostile-pull \
  --mode gtid --rewrite
```

### Purge and search after edited metadata — `purge_search_tamper_test.py`

Builds a storage of six binlogs with three transactions each, one second
apart, plain and encrypted, and checks `purge_binlogs`,
`search_by_timestamp` and `search_by_gtid_set` against what the source's
binlogs hold: first untampered at every boundary (all of those cases pass),
then after one edit of the metadata or the index at a time. Where a search
returns a wrong list, the test replays that list with `mysqlbinlog | mysql`
into a scratch server and compares the rows. On 2026-11-16, 22 of 64 cases
fail in GTID mode and 18 of 54 in position mode, all consequences of
unauthenticated metadata (ticket 53, PBS-126): with the head of `binlog.index`
removed, any purge, also a refused one, deletes the binlogs no longer
listed; with a reordered index a purge removes the index prefix, the
newest binlog included; a lowered `size` of the newest binlog makes any
purge truncate it; edited timestamps, GTID sets or index order make the
searches return lists that restore 3 or 6 rows of 11 with exit 0; and a
binlog with unreadable metadata is left out of a "success" answer (ticket
80). In GTID mode the storage is written in rewrite mode (mandatory since
upstream `4717df9`), which cuts binlogs by size: the harness sets an 8K
`file_size` and lets the last transaction of each source binlog also write
10 000 bytes into a side table, so the stored binlogs switch where the
source's do and the source's binlogs stay the truth; the time range of a
stored binlog is that of its transactions, as rewrite mode writes its own
preamble and ROTATE events. `clean_metadata` fails in GTID mode because of
ticket 84 (PBS-151): the `max_timestamp` of every closed binlog is the time of the
fetch, which the generated closing ROTATE carries.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/purge_search_tamper_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/purge-search \
  --mode gtid
```

### A transaction cut off by its declared length — `truncated_transaction_test.py`

Follows up ticket 82 (PBS-148) in position mode, GTID mode and GTID mode with rewrite
(`--rewrite`), for `fetch` and a running `pull`. A proxy lowers
`transaction_length` in one GTID event so the transaction seems to end
after the GTID, after BEGIN, after TABLE_MAP or after the first ROWS event
(also in a multi-statement, a DDL and a compressed transaction), or inside
an event; other cases end or cut the stream after the false end. The test
then looks at what is stored and at what the next run without the proxy
does. It also covers what needs no attacker: connection drops and timeouts
in the middle of a transaction, SIGTERM, SIGKILL, a killed source, and six
kinds of damaged events. On 2026-11-16, 14 of 38 cases fail in each mode.
Every lie that ends at an event boundary leaves a stub in the storage; in
position and GTID mode every later run then exits 1, and with rewrite the
next run exits 0 without the rows. None of the honest faults stores a
partial transaction. `--replica-crash` adds two slow cases: replicas of
several MySQL versions reading the stub (8.4 and 9.7 abort, 8.0 skips the
transaction silently), and a reproducer of that abort with a hand-edited
binlog on a MySQL source, without Percona Binlog Server.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/truncated_transaction_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/truncated-trx \
  --mode gtid --rewrite
```

### Who can still read a damaged binlog — `damaged_binlog_consumers_test.py`

Produces the malformed binlogs the attack tests leave in a storage (a
second preamble, ticket 69 (PBS-144); duplicated, swapped, foreign or missing
GTIDs, ticket 81 (PBS-147); a cut-off transaction, ticket 82 (PBS-148); a forged row and a
plaintext swap, tickets 53 and 62; a lower sequence number, ticket 70 (PBS-158); the
rewrite-mode layouts) and gives each to every consumer: `mysqlbinlog` on
the files with position and GTID options, a replay into a scratch server,
`mysqlbinlog --read-from-remote-server` through `pull`, position and GTID
replicas from the start and from part way, and `list`, the searches and a
new `fetch`. Cases are named `<state>/<consumer>`; a case passes when the
consumer ends with the source's data or fails loudly. On 2026-11-16, 15 of
90 cases fail in position mode and 98 of 344 in GTID mode. A second
preamble is tolerated by a replica from the start, but a `--raw` copy
through `pull` keeps only what follows it, and position-based starts and
stops lose transactions. Swapped transactions give every consumer other
row values without an error. In rewrite mode `--stop-datetime` can stop
before any transaction, because the generated events carry the time of the
fetch (ticket 84, PBS-151); that cell of the rewrite control state depends on
timing. `--replica-basedirs` adds replicas of other MySQL versions.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/damaged_binlog_consumers_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/consumers \
  --mode gtid
```

### The keyring and the life of a key — `keyring_test.py`

Attacks the keyring file instead of the storage, and walks through what an
operator does with keys. The test decrypts every stored binlog itself (it
unwraps the file key with a given keyring), so it can say which bytes the
operator, an attacker or nobody can read. On 2026-11-17, 5 of 19 cases fail
in both modes, the same on the ASan and release builds and on the build
with mandatory rewrite mode. A KEK id is not bound to its key bytes
(ticket 85, PBS-161): with the bytes of the active CTR KEK overwritten in the
keyring (write access is enough), `list` and `fetch` exit 0 and everything
stored afterwards is readable with the attacker's key only; a GCM KEK
catches that on the open binlog, but not once the operator rotates to a
KEK whose bytes were already replaced; a cipher name changed from CBC to
CTR is accepted too. A wrong keyring backup (right id, other bytes) is the
same by accident: three `fetch` runs exit 0 and store data the right
keyring cannot read. Turning encryption on for a plain storage leaves the
open binlog in plaintext until the next rotation (ticket 86, PBS-162). What works:
rotation to a new KEK (the open binlog stays under the old one, nothing is
re-wrapped; with the old KEK removed every command refuses the whole
storage until its binlogs are purged), 93 malformed keyrings (clean exits,
no key bytes in any message), key and IV uniqueness over 200 binlogs, and
no secret outside the process's memory. The keyring is read once at start.
`c_hygiene` records how many copies of each secret a memory image of
`pull` holds (nothing is wiped or locked; informational).

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/keyring_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/keyring
```

### Memory and disk a source can make PBS use — `resource_exhaustion_test.py`

A proxy turns a real transaction into one that never ends, floods
rotations, sends GTIDs with gaps or events of 64 to 256 MiB, and 300
replicas stall their dumps, while the test records resident memory, bytes
written and start-up times. Every `binlog_server` runs under an
address-space cap (`--memory-cap-mib`), and a run needs about 1.3 GB of
disk. On 2026-11-17, 7 of 18 cases fail in position mode and 8 in GTID
mode. An unfinished transaction is buffered whole, about one byte of
memory per byte streamed, whatever the checkpoint settings, until
`std::bad_alloc` ends `fetch` or `pull` (ticket 87, PBS-163); without checkpoint
settings the whole binlog being received is held the same way.
Transactions whose GTIDs skip numbers make every new rewrite-mode binlog
carry the whole GTID set: 0.4 MiB of stream became 14 MiB in about 1,000
binlogs, and with 6,000 such transactions `list` fails with
`std::bad_alloc` at 3 GiB (ticket 88, PBS-164). `binlog.index` is rewritten in full
at every rotation (ticket 91), and the info log outgrows the stream
(ticket 45, PBS-118; measured before upstream `dd6b598` moved the
per-event messages to debug level). Bounded: an event header that claims 3.5 GiB allocates
nothing ahead of the data, a transaction longer than declared is refused
at once, sequence number 1,000,000 is refused, and a stalled replica costs
about 4 MiB.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/resource_exhaustion_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/resources \
  --mode gtid
```

### A hostile source and hostile clients at once — `pull_two_sided_attack_test.py`

Runs one `pull` for 90 s per case (`--duration`, `--seed`) with a
man-in-the-middle on its source connection (drops, stalls, replays, ERR
packets, rotations in a loop) and, at the same time, six client threads
sending malformed commands, dumps from odd positions, aborted and unread
dumps, wrong logins, TLS and plain; honest readers, a replica and `list` /
search loops run next to them, and a SIGTERM ends each case. Attacks of
known tickets are kept out of the mix. It passes: with a ThreadSanitizer
build of `binlog_server` (not built by the nightly; the test detects one
and runs it under `setarch -R`) about 40 minutes of this gave no report,
and the ASan and debug builds none either; memory stays flat, no wrong
login is accepted, and after a clean restart replicas match the source.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_two_sided_attack_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/two-sided
```

### Repairing a stuck storage — `stuck_storage_recovery_test.py`

Several tickets end in a storage no run can continue. This test produces
each such state (hostile names, a second preamble, a lower sequence
number, a cut-off transaction, a short last binlog, a reset or replaced
source, binlogs the source purged, an interrupted rotation, a deleted
last binlog, metadata that lists GTIDs the files do not hold) and tries
the repairs an operator would, each on a copy: run again (R1), delete the
last binlog (R2), trim it to a transaction boundary (R3), delete from the
damage on (R4), purge (R5), rebuild the index or `metadata.json` (R6), a
fresh storage next to the old one (R7), edit the GTID sets back (R8),
empty the last binlog (R9). Cases are `<state>/<repair>` and one line per
state; a repair fails when it leaves the backup silently wrong or destroys
more. On 2026-11-17, 15 of 208 lines fail in position mode and 45 of 147
in GTID mode, the same with and without mandatory rewrite mode. In
rewrite mode, after an interrupted rotation (a kill at the wrong moment is
enough: 22 of 40 random kills), the next `fetch` appends transactions
behind the closing ROTATE of the previous binlog, and deleting the last
binlog and fetching again does the same and moves transactions to other
positions (ticket 89, PBS-165); only an in-place trim, an emptied last binlog or a
fresh storage are clean there. A reset or replaced source (ticket 25, PBS-141) has
no in-place repair. Rebuilding `binlog.index` from a directory listing is
wrong when a binlog with a lower number was planted (ticket 70, PBS-158).

What worked, by the message `fetch` and `pull` exit with (stop the writer
and copy the storage first; verify with two `fetch` runs, `list`, and a
scratch replica of `pull`):

| Message | Repair that worked |
| --- | --- |
| `cannot perform recovery - size from the binlog metadata is bigger than the actual binlog file size` | truncate the last binlog at the end of its last complete transaction (`mysqlbinlog --base64-output=DECODE-ROWS`, the `end_log_pos` of the last `Xid`), set `size`, `last_sequence_number` and in GTID mode `added_gtids` in its `.json`, then `fetch`; or empty the last binlog (below) |
| `binlog index contains a reference to a non-existing object`, the last index line names files that are gone | position mode: remove that index line, `fetch`; GTID mode: empty the last binlog instead |
| the same, with the index not ascending | remove the planted binlog and its index line, cut the ROTATE that names it off the binlog before, set that binlog's `size`, `fetch`; do not rebuild the index from `ls` |
| `binlog index contains an entry that has an invalid path` | rebuild `binlog.index` (`./binlog.NNNNNN` per line), cut the hostile ROTATE off the last binlog, set `size`, `fetch` |
| `unable to extract "mode"` | write `metadata.json` again (`{"version":1,"mode":"position"}`), then as the line above |
| `Client requested source to start replication from position > file size`, `transaction group must start from one of the gtid log events` | cut the last binlog at its last complete transaction, set `size` and `last_sequence_number`, `fetch` |
| `Could not find first log file name in binary log index file`, `Replica has more GTIDs than the source has`, `the source purged required binary logs` | a fresh storage next to the old one; the old one still answers `list` |
| no error, `list` shows `added_gtids` the last binlog does not hold | before the next `fetch`: set its `added_gtids` to what the file holds, then `fetch` |
| no error, GTID mode, a warning "left after an interrupted binlog rotation" | before `fetch`: empty the last indexed binlog |

Emptying the last binlog: write the 4-byte magic as the data file (on an
encrypted storage truncate it to 4 bytes and keep its `encryption`
record), and a `.json` with `size` 4, `added_gtids` empty and
`previous_gtids` the union of the previous binlog's two sets; keep its
index line. `purge_binlogs` never repairs a stuck storage. PBS has no
`verify` or repair command, and no message names the damaged object.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/stuck_storage_recovery_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/stuck \
  --mode gtid
```

### Secrets in output — `secrets_exposure_test.py`

Runs `fetch`, `pull` (with right and wrong client logins), `list` and
`search_by_timestamp` at log level `trace`, with distinctive secrets: the
source password (and a wrong one), the listener password, a wrong client
password, KEK bytes, the bytes of a malformed keyring record, and S3
credentials in the storage URI, against an unreachable and a refusing S3
endpoint. Then it searches the log, stdout / stderr and telemetry for each
secret. On 2026-11-16 nothing was found in 7 runs. Three runs added later
that day (`s3_unencoded_*`) use a storage URI whose S3 secret key holds a `/`
that is not percent-encoded: such a URI parses without user info, and the
"binlog storage backend URI (masked)" log line carries the secret in all
three shapes (ticket 64, PBS-159).

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/secrets_exposure_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/secrets
```

### Upstream TLS verification — `upstream_tls_test.py`

A test CA signs the source's certificate. A proxy that understands the MySQL
TLS upgrade sits between `fetch` and the source. It either terminates TLS with
an attacker certificate (self-signed, or issued by the trusted CA for
`evil.example`) and opens its own TLS to the source, or it clears CLIENT_SSL
in the greeting. Each case logs in as a new user, so the source performs a
caching_sha2_password full authentication. On 2026-11-16 every case behaves
as MySQL's client does. `verify_ca` and `verify_identity` refuse the
self-signed certificate, `verify_identity` also refuses the CA's certificate
for another host, and `required` / `verify_*` refuse a stripped greeting. An
HTTPS S3 endpoint with a self-signed certificate is refused before any
request. Without an `ssl` section, and with `preferred` or `required`, the man
in the middle is accepted and captures the replication password in clear;
without a section, or with `preferred`, a stripped greeting is accepted too
(ticket 60, PBS-134). Those cases pass, as this is the MySQL client's design, and the
summary lists them.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/upstream_tls_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/upstream-tls
```

### Denial of service by logged-in replicas — `listener_replica_dos_test.py`

Hostile but authenticated clients against `pull` (GTID mode, about 28 MB
stored). While they run, the test measures how long an ordinary `mysqlbinlog`
takes to be served, and `pull`'s memory. On 2026-11-16, `many_dumps` (300
open blocking dumps) and `stalled_readers` (20 dumps that never read) pass:
the ordinary client is served in about 0.2 s. `huge_gtid_set` fails (ticket
61, PBS-135): one COM_BINLOG_DUMP_GTID with 1,000,000 intervals takes about 12 s, no
other client is served meanwhile, and about 100 MB stays resident;
`huge_gtid_sets_parallel` shows 8 such requests answered one after another.

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/listener_replica_dos_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/listener-replica-dos
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
  through the proxy's byte counters (this measurement surfaced ticket 17, PBS-82, the
  whole-binlog re-upload at every checkpoint).

The `fetch_crash_test.py` state machine also runs against S3 with
`--storage s3`. The nightly runs it on S3 storage every night (5 examples
per mode) and `s3_fault_sweep.py` on Mondays (position) and Fridays (gtid);
see [the nightly run](#the-nightly-run).

## The `pull` replication source

`pull_replica_test.py` sets up a second, real MySQL server as a **replica of
`pull`** (`CHANGE REPLICATION SOURCE TO ... ; START REPLICA`), in both position
and GTID auto-position mode, and requires the replica's data to match the
source's. When it does not, it reports the replica's I/O-thread error and the
statements `pull` rejected. It is the end-to-end check behind tickets 20-22 (PBS-120, PBS-86);
tickets 21 (resuming from a position) and 22 (`mysqlbinlog --raw`) are fixed
upstream in `a338dde` (PBS-42 part 5). Since PBS-29 part 2 (`7cd6e24`) a
GTID auto-position replica replicates too, so both cases must pass. Ticket 20 (PBS-120)
stays open for a replica that lacks GTIDs `pull` no longer holds (it gets
EOF where MySQL answers ER 1236) and for TLS on the listener; heartbeats
exist since PBS-30. The nightly runs it (see below).

`pull_heartbeat_test.py` checks the heartbeats of PBS-30 with a real replica:
`SOURCE_HEARTBEAT_PERIOD=1`, `replica_net_timeout=4`, `pull` checkpointing
after every transaction, and the source quiet for 20 s (`--quiet`) once the
replica has caught up. The replica must get about one heartbeat per period
and must not reconnect. Case `quiet` (the source just stays quiet) passes;
case `quiet_after_rotation` (a `FLUSH BINARY LOGS` first) gets no heartbeats
and reconnects every few seconds on MySQL 8.0, 8.4 and 9.7, because the new
binlog holds only the binlog magic until its first transaction (ticket 40,
PBS-113). It prints the heartbeats, the replica's reconnections and the
connections `pull` saw during the quiet time.

`pull_load_test.py` puts `pull` under load: a steady insert workload with a
binlog rotation every few seconds, `--clients` (default 30)
`mysqlbinlog --raw --stop-never` copies and two MySQL replicas with heartbeats.
One client is frozen (`SIGSTOP`) for the middle half of the run. Then `pull`
gets `SIGTERM` with everyone connected. Cases: `many_readers` (every client's
copy of every closed binlog is exact), `slow_reader` (the others keep
receiving while one is frozen, and it catches up), `replicas` (both replicas
match the source) and `shutdown` (`pull` exits 0 within 10 s, every connection
closes, and a `fetch` on the same storage resumes to exact copies). The first
three pass (30 clients, 316 binlogs). `shutdown` fails: with the source idle,
`pull` exits only when its blocking read from the source times out
(`connection.read_timeout`), and the clients stay connected until then
(ticket 43, PBS-116); the storage is left consistent.

All three harnesses start `pull` and point the replica at it through the same
helpers in `pull_replica_test.py` (`start_pull()`, `replicate_from_pull()`).

```sh
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_replica_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/pull-replica
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_heartbeat_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/pull-heartbeat
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_load_test.py \
  --mysql-basedir <dist> --binsrv <asan-binary> --workdir ~/ws/qa/pull-load
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_soak_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/pull-soak \
  --duration 900
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_tls_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/pull-tls
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_gtid_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/pull-gtid
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/pull_gtid_set_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/pull-gtid-set
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/telemetry_test.py \
  --mysql-basedir <dist> --binsrv <binary> --workdir ~/ws/qa/telemetry
PYTHONPATH=~/ws/qa/pylib python3 extra/qa/upgrade_test.py \
  --mysql-basedir <dist> --old-binsrv ~/ws/pbs-0.4.1-build/binlog_server \
  --binsrv <binary> --workdir ~/ws/qa/upgrade --mode gtid
```

`pull_gtid_test.py` drives GTID auto-position replicas (PBS-29 part 2,
`SOURCE_AUTO_POSITION=1`) of `pull` at the edges. `catch_up`: `pull` starts
on an empty storage and the replica connects at once while the source
writes for `--duration` seconds (default 60) with a `FLUSH BINARY LOGS`
every 25 transactions; the replica must end with the source's data.
`restart`: during that workload the replica is stopped and started twice;
it must still end with the source's data and GTID_EXECUTED. `purged`: `pull`
is stopped, `purge_binlogs` removes the older half of its binlogs (nothing
else uses the storage, see PBS-75), `pull` is restarted and a replica with
an empty GTID set connects. `ahead`: a replica holding every GTID the
source wrote plus one of its UUID beyond them (`:1000000`) connects. Since
PBS-120 (1659166) the purged replica is refused with ER 1236 as a MySQL
source does (the replica reports 13114, 'the source purged required binary
logs'). The replica that is ahead is accepted by design (PBS never
originates transactions, so its own UUID never appears in a GTID): the case
expects its I/O thread to stay up with no error and to receive the next
transaction the source writes. Before PBS-120 (ticket 20, PBS-120) the purged replica
was silently streamed what was left. The nightly runs it every night (`QA_PORT`+230, about 3 min).

`pull_gtid_set_test.py` checks where `pull` starts an auto-positioned dump
(PBS-29 part 3) for replicas with uneven GTID sets. The source writes six
binlogs, each with ten transactions of its own UUID, three of a second UUID
(`GTID_NEXT`) and, on 8.3 and later, two tagged GTIDs (`<uuid>:qa:N`); every
transaction inserts a row with a known id. For each case a replica is given
every GTID but some (`GTID_PURGED`) and connects; it must end with the
source's GTID_EXECUTED, exactly the rows of the transactions it lacked and
no thread error. Cases: `full`, `gap_spanning` (ten across a binlog
boundary), `mid_binlog`, `two_gaps`, `single_first`, `single_last`,
`other_uuid` (none of the second UUID), `tagged` and `nothing_missing`
(transactions written after it connects must still arrive). On 2026-11-03
all pass on 8.0, 8.4 and 9.7 (see below). The nightly runs it every night
(`QA_PORT`+250, about 2 min).

`telemetry_test.py` covers the telemetry reports of `pull` and the
instance_id (PBS-41) beyond the MTR tests. `pull` cases (`pull` runs for 8 s
with its own `telemetry` section and must keep serving a binlog to
mysqlbinlog): `reports` (names, one tag, mode 0644, fields, growing uptime),
`no_network_egress` (under strace every outbound `connect()` goes to the
MySQL source, none elsewhere and no DNS lookup, and no report holds the host
name, a password, a user name or the storage path; passes on 2026-11-16),
`scrape_zero`, `scrape_negative` and `scrape_huge` (`200000d`, past the ~292
years of int64 nanoseconds: no tight loop), `grace_huge` (no report),
`path_is_file` and `path_unwritable` (no report, a logged failure),
`cleanup_odd_names` (own old reports and stale foreign ones removed, files
that only look like reports kept), `shared_directory` (two instances keep
each other's recent reports), `settings_in_storage` (`local_settings_path`
is the storage directory) and `damaged_id` (regenerated, as the README
says; whether that is logged is reported). instance_id cases on `list`:
`readonly_without_id` (fails, naming the instance id: the directory must be
writable), `readonly_with_id`, `padded_id`, `uppercase_id`,
`id_is_directory`, `leftover_tmp` and `no_instance_section` (a config of an
earlier release is refused naming `instance`). With `--fsfault-lib` (the
disk fault sweep's LD_PRELOAD library) six more `pull` cases make calls
inside the telemetry directory fail: every write (`fault_write_enospc`),
open (`fault_open_erofs`), fsync (`fault_fsync_eio`), the rename of a
finished report (`fault_rename`), the removal of expired reports
(`fault_unlink`), and only the first two writes (`fault_transient`). In each
`pull` keeps serving and logs every failure; a failed write or rename leaves
its temporary file, which the history cleanup removes later; after the
transient failure complete reports are written again. All six pass on
2026-11-04. On 2026-11-03 four fail:
`scrape_zero`, `scrape_huge` and `grace_huge` (ticket 49, PBS-122: intervals are not
validated; 0 writes 100-170 reports a second, more than ~292 years overflows
the timer and expires at once) and `settings_in_storage` (ticket 50: the
instance_id file makes a fresh storage directory "not empty but does not
contain metadata"). A replaced damaged id is not logged. Since PBS-122
(upstream `2e9648c`, 2026-11-05) `pull` clamps the intervals (grace
20 s..2 d, scrape 10 s..7 d, history 60 s..7 d) and logs the values it uses,
so ticket 49 (PBS-122) is fixed: the cases now run `pull` for 25 to 60 s around those
minimums (reports at 20 s and then every 10 s), check the logged intervals
of the out-of-range ones (`scrape_zero`: 20 s / 10 s, 3 reports in 45 s;
`scrape_negative` and `scrape_huge`: 7 days, one report; `grace_huge`: 2
days, none in 25 s), and the fault case `fault_unlink` plants an 8-day-old
foreign report to be removed, as no own report expires within the 60 s
history minimum. All pass but `settings_in_storage` (ticket 50). The nightly
runs it every night (`QA_PORT`+270, about 12 min).

`pull_variety_test.py` checks the data a MySQL replica of `pull` ends with.
The source writes one database per kind: `types` (every column type at its
limits, NULLs, 4-byte UTF-8, a 1 MB blob, JSON, GEOMETRY, generated
columns), `row_images` (MINIMAL, NOBLOB and FULL, on a table with a primary
key and one without any key), `partial_json`, `compression`, `statement`
(STATEMENT and MIXED), `ddl` (ALTER, RENAME, TRUNCATE, CREATE ... SELECT, a
view, a function, a procedure and a trigger), `xa` (committed, rolled back
after PREPARE, ONE PHASE), `large` (20000 rows in one transaction, an 8 MB
row), `savepoints` (with a MyISAM table), `charsets`, and in GTID mode
`tagged` (8.3+) and `empty` (empty transactions of another UUID), then
`vector` (9.0+: VECTOR columns at the float limits, NULLs, a 16383-dimension
64 KB value) and `vector_index` (Percona Server 9.7 HNSW indexes, Euclidean
and cosine, through every DDL that rebuilds the table or moves the hidden
aux table: the first index added in place to a table with rows, OPTIMIZE,
drop and re-add in one ALTER, a COPY rebuild, one beside FULLTEXT, the last
index dropped, renames across schemas, TRUNCATE). The aux tables are never
binlogged, so each replica builds its own graph; besides matching the
source, a replica's vector index must pass CHECK TABLE, be used for
`ORDER BY DISTANCE(...) LIMIT k` and find a few stored vectors themselves.
On 2026-11-06 both pass on Percona Server 9.7.2-2 (built from the 9.7
branch, c6af080d) in GTID, position, encrypted and rewrite mode, and
`vector` passes on MySQL 9.7.2 (`vector_index` skipped). Phase
`live`: a replica of `pull` applies it as it is written; phase `replay`: a
reset replica replicates everything again from the storage. After each the
replica's tables (SHOW CREATE TABLE, row count, CHECKSUM TABLE), views,
routines and triggers must equal the source's, with no replication error and,
in GTID mode, the same GTID_EXECUTED. A kind the MySQL version does not
support is reported as skipped; any other SQL error fails it. On 2026-11-04
every kind passes in both modes on 8.4, and in GTID mode on 8.0 (`tagged`
skipped) and 9.7; a row changed and a trigger dropped on the replica behind
its back are both reported. `--encrypted` (KEK `alpha`, AES-256-CTR) and
`--storage s3` serve the replicas from an encrypted and an S3 storage; every
combination passes in both modes on 8.4. `--rewrite` (GTID mode) stores the
binlogs in rewrite mode (`rewritten.NNNNNN`, 32 KB each) and the `replay`
replica then starts by name and position (`rewritten.000001`, 4) instead of
auto-positioning, so it follows the ROTATE events and positions rewrite mode
generates; on 2026-11-05 it passes on 8.4 (41 rewritten binlogs, every kind
identical, GTID_EXECUTED equal). The nightly runs it in GTID mode too (cases
`gtid/<kind>/rewrite`). The nightly runs it for both modes
and once more in GTID mode on an encrypted S3 storage (cases
`gtid/<kind>/encrypted-s3`; `QA_PORT`+310, about 3.5 min each).

`pull_replica_fault_test.py` puts Toxiproxy between `pull` and a MySQL
replica (the replica connects to the proxy) and, while the source keeps
writing, injects one fault after another into the replica's connection:
`reset`, `cut_1k`, `cut_100k`, `cut_mid_event` (inside an 8 MB row event),
`cut_handshake` (the replica's side limited to 200 bytes while it logs in
again after a reset), `stall` (longer than `replica_net_timeout`, 8 s),
`refused` (6 s), and without an outage `sliced` (1-byte pieces), `latency`
and `bandwidth` (100 KB/s with an 8 MB row). After each the replica must
reconnect on its own and end with the source's data (and GTID_EXECUTED) with
no applier error, its I/O thread running and `pull` up without leaking file
descriptors. The replica clears its connection error once it reconnects, so
each outage fault must show lost connections in the replica's error log, or
the case fails as one that tested nothing. On 2026-11-04 every fault passes
in both modes on 8.4. The nightly runs it for both modes (`QA_PORT`+330,
needs Toxiproxy).

`pull_restart_test.py` connects a MySQL replica straight to `pull`
(auto-positioning in GTID mode, binlog name and position in position mode)
and, while the source keeps writing, stops and restarts `pull`:
`term_idle` (SIGTERM while caught up, rows written while it is down),
`kill_idle`, `kill_while_writing` (a stream of rows and rotations),
`kill_mid_big_event` (as soon as part of an 8 MB row is stored; the case
fails if the kill missed the event), `down_with_rotations` (three FLUSH
BINARY LOGS, rows and two more, so the source has binlogs without
transactions: before `rewrite` mode became mandatory the GTID-mode resume of
`pull` lost those, ticket 14, PBS-80, which the replica must not notice), `kill_during_replica_catchup` (100 rows
behind) and `flapping` (five SIGKILL / restart cycles a second apart). After
each the replica must reconnect on its own (`SOURCE_CONNECT_RETRY`) and end
with the source's data with no applier error, its error log must show the
lost connection, and a SIGTERM must end `pull` with exit code 0. On
2026-11-05 every case passes in both modes on 8.4. The nightly runs it for
both modes (`QA_PORT`+350, about 4.5 min each).

`upgrade_test.py` checks that the current build takes over a storage
written by the previous release (`--old-binsrv`, `pbs-0.4.1`, built once on
the host: `git worktree add ~/ws/pbs-0.4.1-src pbs-0.4.1`, then `cmake -S
~/ws/pbs-0.4.1-src -B ~/ws/pbs-0.4.1-build --preset release_gcc14` and
`cmake --build ~/ws/pbs-0.4.1-build --target binlog_server`). The release
gets the same settings without the sections it does not know (`instance`,
`replication_source`, `telemetry`). Cases: `old_config` (the release's
configuration given to the current build must fail naming what is missing;
it also reports what the next missing section is), `plain_takeover` (the
release fetches five binlogs; the current build must answer `list`,
`search_by_timestamp` and, in GTID mode, `search_by_gtid_set` exactly as
the release does, resume the fetch to a byte-identical copy and serve all of
it to a MySQL replica through `pull`; whether the release can still list
the storage afterwards is reported), `encrypted_takeover` (the same with
KEK `alpha` and AES-256-CTR, checked through the replica; every build
rejects the repository's sample `keyring_data.json`, whose AES-256-GCM
record `beta` holds 31 bytes (ticket 52, PBS-160), so the harness writes a keyring
with `alpha` only) and
`killed_takeover` (the release is killed with SIGKILL after 1.5 MB of a
fetch with `checkpoint_size` 64K; the current build must recover the
storage and complete the copy), `s3_takeover` (with `--s3-bucket-dir`:
`plain_takeover` on S3 storage of the local S3-compatible server; its
objects are removed when it passes) and, in GTID mode, `rewrite_takeover`
(`plain_takeover` with a `replication.rewrite` section, binlogs named
`rewritten.NNNNNN` of 32K, checked through the replica). On 2026-11-04 every
case passes in both modes against `pbs-0.4.1`, and the release still lists a
storage the current build wrote. `old_config` shows the upgrade is two failed starts
long: `unable to extract "instance"`, then, with `instance` added, `unable
to extract "replication_source"`, a section only `pull` uses (ticket 51, PBS-123).
The nightly runs it for both modes every night (`QA_PORT`+290, about 5 min
each).

`pull_tls_test.py` checks TLS on the listener (PBS-31). It starts `pull`
with one `replication_source` configuration after another on the same
storage (no `ssl` section; the MTR suite's test certificate `rs_cert.pem` /
`rs_priv.pem`; `require_secure_transport`; no RSA key) and connects
`mysqlbinlog --raw` clients with `--ssl-mode` DISABLED, the default
(PREFERRED), REQUIRED and VERIFY_CA (against `rs_ca_cert.pem`), and a MySQL
replica with `SOURCE_SSL=1`; each must copy every closed binlog exactly, and
under `require_secure_transport` a plain client must be refused with an
error while `pull` keeps serving. Four more cases start `pull` with a
missing certificate, a missing key, a key that is not the certificate's and
a certificate that is not PEM: it must exit at once, naming the file. Since
PBS-31 parts 2 and 3 (upstream `6b0960f`) the listener performs the TLS
handshake and every TLS client and the TLS replica pass (on PBS-31 part 1
they failed with `wrong version number`, ticket 46, PBS-119). Still open: a
plain client under `require_secure_transport` is refused, but `pull` closes
the connection without an error packet, so the client sees only `Lost
connection to MySQL server during query` instead of ER_SECURE_TRANSPORT_REQUIRED
(3159) (ticket 48). On 2026-11-04 the same holds with MySQL 9.7.2 (source,
replica and clients): a 9.7 replica with `SOURCE_SSL=1` replicates over TLS,
and only ticket 48's case fails. The limits (2026-11-05): clients limited
to `--tls-version=TLSv1.2` and `TLSv1.3`, and `VERIFY_IDENTITY` to
127.0.0.1 (a name of the certificate), copy exactly; TLS 1.0 and 1.1
handshakes (`openssl s_client -starttls mysql`, which still offers them) and
a TLS 1.2 handshake with only NULL, RC4, 3DES or export ciphers fail, while
`pull` keeps serving; `VERIFY_IDENTITY` to the host's own address (not in the
certificate) is refused by the client in time; with an expired certificate
signed by the test CA, `pull` starts, `VERIFY_CA` clients are refused and
`REQUIRED` ones copy exactly. All pass on 8.4. The listener sets no protocol
or cipher limits of its own (a plain Boost.Asio `tls_server` context): TLS
1.0 / 1.1 and weak ciphers are refused because of the host's OpenSSL 3.0
defaults, which `old_tls_refused` and `weak_cipher_refused` would show
changing. The nightly runs it every night (8.4).

`pull_soak_test.py` runs `pull` for a long time (`--duration`, 2 h by
default) under churn: a steady workload with rotations and an occasional
large transaction, three long-lived `mysqlbinlog --stop-never` clients, a
replica stopped and started every 10 minutes, and a short client every 15 s
(most copy from a random closed binlog to the end and exit, some are killed
during the dump). `pull` has an `ssl` section, and TLS and plain connections
are mixed: the second long-lived client, about half the short clients and
the replica (`SOURCE_SSL=1`) use TLS. Every minute it samples `pull`'s resident memory, open
file descriptors and threads (`/proc`), its log size and the storage size
(`samples.json`). Cases: `alive`; `memory`, `file_descriptors`, `threads`
(the last quarter of the run against the second, after a quarter of
warm-up); `log` (growth per hour at info level, and no error lines but the
known ones of ticket 42, PBS-115); `copies`, `replica`, `still_serving` (exact data at
the end). The first 15-minute run (2026-10-29, debug build) was flat (about
23 MB, 16 descriptors, 2 threads) and exact, but the log grew 67 MB per hour:
at info level `pull` logs every event it streams, per client (89% of the
lines; ticket 45, PBS-118, fixed upstream by `dd6b598`, which moves the
per-event messages to debug level: `position/log` passes since), and a
client that dies during a dump is logged as an
error (EPIPE, part of ticket 42, PBS-115). A 15-minute run with TLS clients
(2026-11-02, upstream `6b0960f`: 36 of 65 connections over TLS) was flat and
exact as well, and the TLS clients killed during a dump added no new error
lines.

`--mode gtid` runs the same soak with GTIDs: the replica follows `pull` with
`SOURCE_AUTO_POSITION`, and the storage is in rewrite mode (mandatory in GTID
mode since upstream `4717df9`) with a 32K `rewrite.file_size`, so `pull` cuts
its own binlogs and the clients copy the storage's closed binlogs; a copy
must equal the stored file and the stored transactions the source's first
ones. The first 15-minute run (2026-11-18, the merge of upstream `dd6b598`)
passed every case: 1,552 transactions in 139 stored binlogs, memory flat at
about 24 MB, 16 descriptors, 2 threads, 2.4 MB of log per hour, exact
copies for 3 long-lived and 43 churn clients (16 killed during a dump, 27
over TLS), and the replica at most 26 rows behind with the source's data at
the end. The nightly runs it on Fridays.

The `replica_lag` case (2026-11-05) samples how many rows of `qa.t` the
replica is behind the source (`MAX(id)` on both) once it has run 30 s since
its last restart: the worst must stay within 200 rows (some 50 s of the
workload) and the worst of the last quarter within twice that of the second
quarter plus 20, so a replica of `pull` that falls further and further behind
fails even when it catches up at the end.

`repro_broken_checkpointing.py` is a self-contained reproducer kept from an
earlier finding (checkpoint settings having no effect); it drives an existing
server rather than starting one.

## The nightly run

[`nightly/`](nightly) holds the nightly test runner and its data:

- **`run_nightly.sh`** — runs seven stages:
  1. **C++** (`cpp`): builds the `debug_gcc14`, `asan_gcc14` and
     `tsan_gcc14` trees in place, runs the unit tests, then runs **every
     Hegel property-test case separately** with many more cases than the
     default (the profiles in `hegel.toml`), and compares each failure
     against `known_failures.txt`. Builds use 12 compile jobs
     (`BUILD_JOBS`) and, when `ccache` is installed (test-infra since
     2026-11-05, 4 GB cache), compile through it: the first build of a tree
     sets `CMAKE_*_COMPILER_LAUNCHER`, and after that a rebuild after a
     merge recompiles only what changed. Property cases run 8 at a time
     (`JOBS`), ThreadSanitizer ones 4 (`TSAN_JOBS`); the S3 variants one at
     a time.
  2. **MTR** (`mtr`): the whole `binlog_streaming` suite with the debug build
     on MySQL 8.0.45, 8.4.8 and 9.7.2 (`MTR_DISTS`), one version after the
     other, each with `--parallel=4` (`MTR_PARALLEL`), a 10-minute limit per
     test (`--testcase-timeout=10`; the slowest passing test takes about 5,
     and a hung one, such as `rs_heartbeat` on 9.7 for ticket 44, PBS-117, cost 30
     minutes with the default), `--build-thread=500`
     (`MTR_BUILD_THREAD`; the `rs_*` tests also use ports 1000 above the
     server's, so 500 keeps clear of the automatic build threads 300-399) and var directories `$WS/mtr-var-nightly-<series>`,
     so it does not collide with manual MTR runs. Every test (and
     combination) is a case of the build `mtr-<series>`, compared against
     `mtr_known_failures.txt`. Skipped tests are recorded as `skipped`.
  3. **QA harnesses** (`qa`), against MySQL 8.4 (`QA_DIST`), ports from 24470
     (`QA_PORT`): `pull_replica_test.py` (cases `position` and `gtid`),
     `query_race_test.py` (case `list_next_to_pull`, 500 `list` runs,
     `QA_RUNS`), `pull_heartbeat_test.py` (cases `quiet` and
     `quiet_after_rotation`, port `QA_PORT`+120), `pull_tls_test.py`
     (TLS on the listener, 13 cases, port `QA_PORT`+190) and
     `pull_gtid_test.py` (GTID auto-position replicas, cases
     `gtid/catch_up`, `gtid/restart`, `gtid/purged` and `gtid/ahead`, port
     `QA_PORT`+230), `pull_gtid_set_test.py` (auto-positioning with
     uneven replica GTID sets, 10 `gtid/...` cases, port `QA_PORT`+250) and
     `telemetry_test.py` (telemetry and instance_id, 25 `position/...`
     cases with the file system fault cases, port `QA_PORT`+270) and
     `upgrade_test.py` per mode (a storage of the release in `OLD_BINSRV`
     taken over: 5 position and 6 gtid cases, port `QA_PORT`+290; skipped
     when that binary is missing) and `pull_variety_test.py` per mode (a
     replica of `pull` gets the source's data for a broad workload, 14
     `gtid/...` and 14 `position/...` cases plus `gtid/.../encrypted-s3`
     and `gtid/.../rewrite`,
     port `QA_PORT`+310), `pull_replica_fault_test.py` per mode
     (network faults between `pull` and a replica, 11 cases each, port
     `QA_PORT`+330) and `pull_restart_test.py` per mode (a replica across
     restarts of `pull`, 7 cases each, port `QA_PORT`+350), all with the
     debug build,
     and a short
     `fetch_crash_test.py` run per mode (cases `gtid` and `position`,
     10 examples of 8 steps, `QA_EXAMPLES` / `QA_STEPS`) with the ASan build,
     and the same on **S3 storage** against the local S3 server (cases
     `gtid/s3` and `position/s3`, 5 examples each, `QA_S3_EXAMPLES`), which
     reaches the S3 backend's crash recovery. Then **one heavier harness by
     day of the week** (`QA_ROTATION`, weekday from `date -u +%u`), so each
     runs at least weekly:

     | Day | Harness (mode) | Build |
     | --- | --- | --- |
     | Mon | `s3_fault_sweep.py` (position), through `s3_fault_proxy.py` the stage starts; `pull_versions` on 9.7; `hostile_source_test.py` (position and gtid, about 1 min each) and `tls_config_test.py` (position, about 5 min); `hostile_source_pull_test.py` (position, about 5 min, and again with `--storage s3`, cases `position/<case>/s3`); `stuck_storage_recovery_test.py` (position, about 4 min) | ASan; debug |
     | Tue | `disk_fault_sweep.py` (position) and `listener_exhaustion_test.py` (position, about 2 min); `pull_versions` on 8.0; `metadata_tamper_test.py` (position, about 4 min) and `checkpoint_loss_test.py` (position, file storage, about 5 min); `purge_search_tamper_test.py` (gtid, about 2 min); `keyring_test.py` (position, about 4 min) and `pull_two_sided_attack_test.py` (position, about 5 min); `source_reply_test.py` (position, 10 s) | debug (`fsfault` is an `LD_PRELOAD`; ASan must come first; the exhaustion test measures descriptors and memory) |
     | Wed | `network_fault_test.py` (position, parts fetch and pull; the nightly starts Toxiproxy) and `large_binlog_test.py` (about 20 GB of disk while it runs, removed at the end); `secrets_exposure_test.py` (position, about 2 min) and `listener_command_fuzz_test.py` (position, about 4 min); `rewrite_attack_test.py` (gtid, about 7 min) and `truncated_transaction_test.py` (position, about 3 min, and gtid with `--storage s3`, about 5 min); `resource_exhaustion_test.py` (position, about 5 min); `log_hygiene_test.py` (position, about 6 min); `shadow_test.py` (position, about 31 min, needs the patched Shadow and versitygw in `$WS/shadow/install`, `SHADOW_BIN`) | ASan; debug |
     | Thu | `listener_fuzz_test.py` (position and gtid, 600 connections each, `QA_FUZZ_CONNECTIONS`, seed = day of the year) and `source_change_test.py` (position and gtid); `pull_versions` on Percona Server 8.4; `upstream_tls_test.py` (position, about 2 min) and `replica_tamper_test.py` (position, about 4 min); `keyring_test.py` (gtid, about 4 min) | ASan; debug |
     | Fri | `s3_fault_sweep.py` (gtid) and `large_binlog_test.py --storage s3` (position, `binlog_over_4gib` only, about 30 GB of disk while it runs; cases `position/<case>/s3`); `file_security_test.py` (position, about 7 min, with the local S3 server), `s3_upload_tamper_test.py` (position, about 1 min) and `listener_command_fuzz_test.py --tls` (gtid, about 5 min); `truncated_transaction_test.py` (gtid, about 3 min) and `damaged_binlog_consumers_test.py` (position, about 3 min); `stuck_storage_recovery_test.py` (gtid, about 9 min); `pull_soak_test.py` (gtid, `QA_SOAK_SECONDS`, 2 h by default); `source_reply_test.py` (gtid, 10 s) | ASan; debug |
     | Sat | `disk_fault_sweep.py` (gtid) and `pull_load_test.py` (position); `pull_versions` on Percona Server 9.7; `listener_replica_dos_test.py` (gtid, about 5 min); `final_flush_test.py` (position, about 3 min); `hostile_source_pull_test.py --rewrite` (gtid, about 9 min); `resource_exhaustion_test.py` (gtid, about 9 min); `metadata_tamper_test.py` (gtid, about 9 min); `log_hygiene_test.py` (gtid, about 6 min); `clock_test.py` (position, about 35 min) | debug / ASan; debug |
     | Sun | `network_fault_test.py` (gtid) and `pull_soak_test.py` (position, `QA_SOAK_SECONDS`, 2 h by default); `listener_login_test.py` (position, about 7 min) and `replica_tamper_test.py` (gtid, about 4 min); `truncated_transaction_test.py --rewrite` (gtid, about 5 min) and `damaged_binlog_consumers_test.py` (gtid, about 11 min); `pull_two_sided_attack_test.py` (gtid, about 5 min) and `purge_search_tamper_test.py --storage s3` (gtid, about 22 min); `clock_test.py` (gtid, about 25 min); `shadow_test.py` (gtid, about 31 min) | ASan / debug; debug |

     `pull_versions/<series>` runs the replica harnesses of the nightly 8.4
     stage against another MySQL version of `MTR_DISTS`: `upgrade_test.py`,
     `pull_variety_test.py` and `pull_restart_test.py` per mode and
     `pull_replica_fault_test.py` in GTID mode, recorded as `qa-9.7` /
     `qa-8.0` (ports `QA_PORT`+370 to +430, about 40 min). It also takes a
     Percona Server label of `PS_DISTS` (`<label>:<series>:<distribution>`):
     `ps-8.4` is the 8.4.11-11 release tarball (glibc 2.35, minimal) and
     `ps-9.7` a RelWithDebInfo build of the `9.7` branch made by
     `~/ws/ps-9.7-build.sh` and installed in `~/ws/dist/percona-server-9.7`
     (it has the HNSW vector indexes the `vector_index` kind of
     `pull_variety_test.py` needs), recorded as `qa-ps-8.4` / `qa-ps-9.7`.
     A label whose distribution is missing is skipped with a log line.
     `QA_ROTATION_DAY` overrides the day (`1`-`7`, a comma list such as
     `2,4`, `all` or `none`), e.g. to try Tuesday's set now:
     `QA_ROTATION_DAY=2 STAGES=qa OUT_DIR=~/ws/qa/nightly-try ~/ws/nightly/run_nightly.sh`.
     `run.json` records the night's set as `qa_rotation`.
     Harnesses with one JSON line per case get one nightly case each, named
     `<mode>/...`: `position/PUT-binlog.index/error500/x6` (S3 sweep: method,
     object, fault, consecutive requests), `position/rename/binlog-index/EIO/x1`
     (disk sweep), `position/fetch/reset-after-50-ms` and
     `position/pull/r1-stall` (network), `position` (listener fuzzer),
     `gtid/reset` (source change), `position/binlog_over_4gib` (large
     binlog); a run that stops early adds
     `<mode>/timeout`. ASan runs get `ASAN_OPTIONS=exitcode=134`, so a
     memory error is not mistaken for the clean failure exit code 1 the
     harnesses accept.
     All of these are cases of the build `qa-<series>` (program: the harness),
     compared against `qa_known_failures.txt`. A failing harness keeps its
     workdir under `qa/` in the run directory, minus the MySQL data
     directories (and the S3 objects it created, moved out of the bucket);
     a multi-case harness writes its whole output once
     (`qa-<series>-<harness>-<mode>-full.log`) and one short log per failing
     case.
  4. **Release** (`release`): the `release_gcc14` preset (RelWithDebInfo:
     `-O2 -g -DNDEBUG`, against release builds of Boost and the AWS SDK,
     `$WS/{boost,aws-sdk-cpp}-install-release_gcc14`) in its own tree
     `$WS/percona-binlog-server-build-release_gcc14`, configured by the
     nightly the first time (`-DWITH_PROPERTY_TESTS=ON`). It runs the unit
     tests and every property-test case separately like the C++ stage
     (profile `nightly_release`, 500 cases), but each case under a
     wall-clock limit of 600 s (`RELEASE_CASE_TIMEOUT`; `timeout -k 30`), so
     a hang is recorded as a failure with the reason `TIMEOUT` (and
     `[TIMEOUT]` in the summary line) instead of stalling the night, and
     under an address-space limit of 4 GB (`RELEASE_CASE_MEMORY_MB`,
     `ulimit -v`), so runaway allocation fails the case instead of the
     host. Then one short `fetch_crash_test.py` run (`gtid`, 5 examples,
     `RELEASE_QA_EXAMPLES`, port `QA_PORT`+50) uses the release binary. All of it is the build `release_gcc14`. It is a stage of its own
     because release behaviour differs exactly where known bugs live: an
     `assert` that aborts the debug build is compiled out, so the bug runs on
     (ticket 7, PBS-73 runs away, ticket 6, PBS-72 keeps wrong timestamps). Property failures are
     compared against `known_failures.txt`; a line
     `release_gcc14:<test case>  ticket N: ...` takes precedence for that
     build and records how the release build fails differently. Found so
     far: ticket 6 (PBS-72) keeps the wrong timestamps instead of aborting at
     `storage.cpp:138`. Before PBS-29, ticket 7 (PBS-73) did not hang in release but
     never advanced past an event size of 0, so `indexed_event_block`'s
     index grew to 8+ GB until `std::bad_alloc` (hence the memory limit),
     and a misaligned dump position streamed an event cut out of the stored
     data; since PBS-29 both terminate the process as in debug (ticket 39,
     PBS-112), so they have no release-only line any more. The
     dependencies are built once with `setup_test_environment.sh deps` (or
     `build_deps.sh release_gcc14`; about 40 min, 1.9 GB), the project tree
     takes about 1.4 GB. The stage takes 15-30 min (property cases 14-29 min,
     the first night longest, then the failure database replays the known
     failures quickly; `fetch_crash_test` 1-2 min) plus the incremental
     build (a clean build is about 8 min).
  5. **UBSan** (`ubsan`): the `debug_gcc14` preset with
     UndefinedBehaviorSanitizer (`-fsanitize=undefined,float-divide-by-zero,float-cast-overflow`)
     in its own tree, `percona-binlog-server-build-ubsan_gcc14`, which the
     stage configures the first time. Boost and the AWS SDK are the debug
     installs, so only the project's code is instrumented. GCC reports
     `-Wconversion` in three places only with this instrumentation
     (`byte_span_inserters.hpp`, `flag_set.hpp`, `gtid_set.cpp`), so in this
     tree those warnings are not errors. The stage runs the unit tests and
     every property-test case (profile `nightly_ubsan`, 500 cases). A report
     does not stop a test (`halt_on_error=0`): a property case with a
     `runtime error:` line in its output fails, and the unit tests' reports
     are collected through `log_path` into `unit-ubsan_gcc14-reports.log`
     (ctest keeps a passing test's output) and compared with the
     `ubsan_gcc14:unit_tests` line of `known_failures.txt`. First run on
     2026-11-16: one report, a signed overflow in
     `util::microseconds_to_high_resolution_time_point` for a 2^60 µs
     timestamp in `event_test` (ticket 79 (PBS-145); only tests call the getters that
     use it). The QA harnesses were run once by hand against this build's
     `binlog_server` on the same day (25 runs, no report); the nightly does
     not repeat that.
  6. **Fuzzing** (`fuzz`), last: the libFuzzer targets of
     [`fuzz/`](../../fuzz) in their own Clang 20 build
     (`$WS/percona-binlog-server-build-fuzz_clang20`, configured by the
     nightly the first time), each for 10 min (`FUZZ_SECONDS`) with 12
     workers (`FUZZ_JOBS`) against a corpus kept between nights. Each
     distinct finding is a case `<target>/<signature>` of the build `fuzz`,
     compared with `fuzz_known_failures.txt`; see
     [`fuzz/README.md`](../../fuzz/README.md).
  7. **Mutation testing** (`mull`), weekly on Sundays (`MULL_DAY`, `all` for
     every run): [Mull](https://github.com/mull-project/mull) builds the
     unit and property tests of the GTID, util and event libraries, the
     configuration validators and the units with its compiler plugin in
     `$WS/percona-binlog-server-build-mull_clang20` (rebuilt from clean each
     time), then `mull/run_mull.sh` runs them once per mutant. The case
     `mull/run` of the build `mull` records the mutation score; each mutant
     no test kills that is not triaged in `mull/mull_known_survivors.txt` is
     a new case `mull/<file>:<line>:<column>-<mutator>`. About 30 min; see
     [`mull/README.md`](mull/README.md).

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
  when the ASan build fails, the release `fetch_crash_test` when the release
  build fails. Tonight's whole run takes about 40-50 min for the
  C++ stage plus 35-50 min of MTR (9-26 min per version) and 20-35 min of QA harnesses: about 11 min for `pull_replica_test`, `query_race_test` and `fetch_crash_test` on file storage, about 5 min for `pull_heartbeat_test`, about 7 min for `fetch_crash_test` on S3, plus the night's rotation (measured on MySQL 8.4.8: `s3_fault_sweep` 15 min per mode, 84 cases; `disk_fault_sweep` 3-6 min, 80 cases (plus 6 recovery cases, about 1.5 min); `network_fault_test` 4-5 min, 37 cases; `source_change_test` 3-4 min per mode, 4 cases; `large_binlog_test` about 20 min on the debug build, mostly MySQL writing the large transactions; `pull_load_test` about 6 min, 4 cases; `listener_fuzz_test` about 11 min per mode at 600 connections: about 1 s per connection since the listener always does full `caching_sha2_password` authentication (PBS-10), 2-3 min for 3000 before).

  To try a stage without touching the nightly's own results, select stages
  with `STAGES` (default `cpp mtr qa release ubsan fuzz mull`; without `cpp` the debug,
  ASan and TSan trees are not built) and
  write elsewhere with `OUT_DIR` (then `history.tsv`, `latest` and old runs
  are left alone):

  ```sh
  STAGES="mtr qa" OUT_DIR=~/ws/qa/nightly-try ~/ws/nightly/run_nightly.sh
  ```

  `STAGES=rotation` runs only the QA rotation (part of `qa`).

  Before building, the run checks the free space on `~/ws`. With less than
  `MIN_FREE_GB` (default 10) it builds and tests nothing and records one
  build failure, `NOT ENOUGH DISK`, naming the free space, the largest
  directories and any process holding more than 1 GB of deleted files. From
  2026-11-07 to 2026-11-09 a full disk instead showed up as every build
  failing.

  Servers left running are stopped before their files are deleted. A
  harness killed by a timeout used to leave its servers running, and a
  `mysqld` whose data directory was then deleted logged InnoDB errors in a
  loop into its deleted error log (99 GB in three days), which no file
  listing shows. Now `MySQLServer` and `BinlogServer` of the harness module
  (`fetch_crash_test.py`) stop any process whose command line names a path
  in their work directory when they are created
  (`stop_leftover_processes` in `leftovers.py`), leaving the run's own processes and their
  ancestors alone. `nightly_stages.py` does the same before it deletes a
  harness's workdir and after the harness exits. The harness's log notes
  each process it stopped.
- **`nightly_stages.py`** — runs one MTR suite, one QA harness, one fuzz
  target or the mutation testing for `run_nightly.sh`, parses its outcome
  into cases, matches them against the stage's known-failure list and
  appends the result lines.
- **`hegel.toml`** — the Hegel settings profiles (`nightly_debug`,
  `nightly_asan`, `nightly_tsan`, `nightly_release`, `nightly_ubsan`) selected per build via
  `HEGEL_DEFAULT_PROFILE`; each has its own failure database so a failure found
  one night is replayed first the next. Before each build's run,
  `run_nightly.sh` copies into its database every input the other builds have
  saved (without overwriting), so a failure one build found is replayed in all
  of them, and the log says how many it took. They stay separate databases
  because Hegel drops a saved input that no longer fails: one shared database
  would let a build that cannot hit a failure (an `assert` compiled out of
  release) delete it for the build that found it. Before this, a rarely hit
  failure could fail every night in one build and never in another only
  because the other build never generated the input. The database paths are
  templated (`@NIGHTLY_DIR@`) and filled in for the workspace by the setup
  script.
- **`known_failures.txt`** — maps property-test case names to the ticket that
  explains them; a listed failure is reported as *known*, any other as *new*.
  Remove a line once its bug is fixed. CI uses the same list (see below).
  A line `<build>:<test case>  ...` applies to that build only and wins over
  the plain line (used for `release_gcc14`; CI ignores such lines).
- **`mtr_known_failures.txt`** — the same for the MTR stage, one line per
  `<series> <test>  ticket N: ...`, where `<series>` is `8.0`, `8.4`, `9.7` or
  `*` (every version) and `<test>` is a test name (every combination) or
  `<test>/<combination>`; e.g. `9.7 rs_heartbeat` (ticket 44, PBS-117: the
  test leaves `SOURCE_SSL` at its default, which a 9.7 replica takes as
  required, so it never connects and the test waits for its 30-minute
  timeout).
- **`qa_known_failures.txt`** — the same for the QA-harness stage:
  `<harness> <case>  ticket N: ...` (`<case>` may be `*` or a shell-style
  pattern such as `position/PUT-*/reset_after/*`; a name without `/` also
  covers every `<name>/...` case), e.g.
  `query_race_test list_next_to_pull` (ticket 29, PBS-101),
  `pull_heartbeat_test quiet_after_rotation` (ticket 40, PBS-113),
  `large_binlog_test position/transaction_over_4gib` (ticket 41, PBS-114),
  `pull_load_test position/shutdown` (ticket 43, PBS-116),
  `pull_tls_test position/plain_refused_when_required` (ticket 48: no error
  packet for a plain client under `require_secure_transport`),
  `telemetry_test position/settings_in_storage` (ticket 50:
  `local_settings_path` in the storage directory). GTID-mode
  `upgrade_test` runs pass `--tolerate-ticket-14`: the old binary still has
  ticket 14 (PBS-80; a closed binlog stored without its closing ROTATE,
  binlogs without transactions skipped), so they accept only those effects
  in the storage it writes and any other GTID-mode failure is new. Mandatory
  `rewrite` mode (upstream `4717df9`) fixes ticket 14 (PBS-80) in the current binary,
  so `fetch_crash_test` no longer needs the option and the MTR test
  `gtid_resume_after_rotation` was removed.
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
  label: `debug_gcc14`, ..., `release_gcc14`, `mtr-8.4`, `qa-8.4`), a description of every
  build label, and the entries of all three known-failure lists (each with
  its `kind` and `scope`).

- **`fuzz_known_failures.txt`** — the same for the fuzzing stage:
  `<target> <case pattern>  ticket N: ...`, where a case is
  `<target>/<signature>` (see [`../../fuzz/README.md`](../../fuzz/README.md)).

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
`reader_context_property_test`, `config_value_property_test` (including
the `instance` section and its instance_id file and the optional
`telemetry` section, PBS-41),
`event_generation_property_test` (the artificial ROTATE / FORMAT_DESCRIPTION /
PREVIOUS_GTIDS generators), `search_helpers_property_test`,
`purge_selection_property_test`, `sender_context_property_test` (the
`pull` COM_BINLOG_DUMP streamer: the stream preamble, reading each stored byte
once, partial events at EOF, polling an empty storage, the heartbeat an
idle dump reports, ticket 40, PBS-113, and in a GTID-mode storage the auto-positioned
start at the first binlog holding a GTID the replica lacks, PBS-29 part 3) and `replication_source_property_test` (the
real `pull` listener in-process with a test client: handshake statements,
authentication, dump positions, heartbeat timing, and `COM_BINLOG_DUMP_GTID`,
written ahead of PBS-29 part 2; it passes since PBS-120, which refuses a
replica that lacks purged GTIDs with ER 1236, ticket 20, PBS-120).
Since PBS-41 every configuration these tests write has an
`instance.local_settings_path` (a directory in the test's scratch tree).
`storage_property_test`'s concurrent property runs a second reader that
resumes from a position mid-run and checks every artificial event and
heartbeat; the heartbeat checks are shared in `tests/heartbeat_test_helpers.hpp`.
Two of the properties are security checks. `ReplicationSourceServesOnlyIndexedBinlogNames`
sends COM_BINLOG_DUMP with names a client could use to get out of the
storage: paths out of it, other storage objects, files planted beside or
inside it that the binlog index does not list, NUL bytes, case variants and
oversized names. Every one must be refused with no event streamed and no
file created, changed or removed; it passes since PBS-120 looks a named
binlog up in the index. `StorageDetectsTamperingWithEncryptedBinlogData`
changes bytes of an encrypted binlog's ciphertext as someone with write
access to the storage but no key could, optionally patching the event's
CRC32 footer to match (AES-CTR is malleable, CRC32 is affine). It fails
(ticket 53, PBS-126): the changed event is read back without an error.
Build them with
`-DWITH_PROPERTY_TESTS=ON` and run them with `ctest`. Their design, settings profiles (`hegel.toml`), how to
replay a failure, and a per-test rundown are documented in
[`../../tests/PROPERTY_TESTS.md`](../../tests/PROPERTY_TESTS.md).

## Coverage

[`COVERAGE_GAPS.md`](COVERAGE_GAPS.md) records the last line and branch
coverage measurement of `src/` (2026-11-03: 90.0% of lines and 80.3% of
branches over the unit, property, MTR 8.4 and QA layers; 91.0% with every
layer), how it was taken, and a table of the code
no test reaches yet, each with the cheapest kind of test that would. It is
measured with a gcov build in its own tree (`~/ws/build_coverage.sh`,
`~/ws/run_coverage.sh`); see the document for the steps and for the pitfalls
(one crashing property case hides the rest of its program; property layers
vary between runs). It includes PBS-41 (telemetry and instance_id) and PBS-29
part 3 (upstream `2d7dc80`), with a `telemetry` layer for `telemetry_test.py`.
