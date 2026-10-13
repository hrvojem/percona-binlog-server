# Focus 7: Existing test strategy

Commit 99de682. Everything below comes from reading the test sources, harness docstrings, CI YAML and the nightly script. Nothing was run.

## Findings

**F1. CI covers formatting, example-based unit tests and the MTR suite, on the file backend only.** In `.github/workflows/cmake.yml`, the matrix (GCC/Clang x Debug/RelWithDebInfo/ASan, plus GCC Valgrind) runs `ctest` and MTR against MySQL 8.0, 8.4 and 9.7. No `-DWITH_PROPERTY_TESTS=ON` is set anywhere in the workflow, and that CMake option defaults to OFF (`CMakeLists.txt:78`), so CI does not run the Hegel property tests. No `MTR_BINSRV_AWS_*` variables are set either, so `identify_storage_backend.inc` falls back to `file` and CI never exercises S3. `tests/PROPERTY_TESTS.md` says "CI / local: built with -DWITH_PROPERTY_TESTS=ON", which the YAML does not support.

**F2. The nightly run (`extra/qa/nightly/run_nightly.sh`) only runs C++ tests.** It runs unit tests and every Hegel case under the debug, ASan and TSan builds. Under TSan it runs only `storage_property_test` (lines 140-186). It does not call MTR or any of the Python black-box harnesses. `extra/qa/README.md` says the crash test and `s3_fault_sweep.py` are "nightly/coverage layers", but the script does not run them, so in practice they run by hand. `known_failures.txt` lists 18 property cases that are known to fail: tickets 1, 3, 5, 7-13 and 26.

**F3. Unit tests** (`tests/*_test.cpp`, Boost.Test) are pure component tests: gtid/gtid_set, uuid, tag, event, cipher/digest/EVP/crypto_rnd, and byte_span encoding. None of them do I/O, run processes or use threads.

**F4. The Hegel property tests** (`tests/*_property_test.cpp`) are the deepest in-process layer:
- **Model-based storage test** (`storage_property_test.cpp`). It runs write, flush, rotate, restart, "crash", purge and damage operations against a list-of-binlogs model, with optional encryption and an S3 variant, and a concurrent `sender_context` reader thread. The "crash" (lines 1845-1856) copies the storage directory **between operations**. So a crash can only happen at an operation boundary, never inside a storage call (for example between the data write and the metadata rename). Because the copy reads the live page cache, it also cannot model losing data that was written but not fsynced (ticket 19).
- **Purge**: purge from a second storage instance in the same process is tested; this is how ticket 9 was found.
- **Parsers and codecs**: round-trip and robustness tests for events, reader_context, rewriter, the minimysql codec, config values and cipher CTR.
- **sender_context**: an in-memory COM_BINLOG_DUMP stream, including polling an empty storage (PBS-91/96).
- Everything runs in a single process, with no real network and no real MySQL. Time is wall-clock only.

**F5. MTR suite** (`mtr/binlog_streaming/t`, about 50 tests): deterministic functional scenarios. They cover fetch/pull, checksum combinations, encryption, GTID renumbering and rewrite, search_by_gtid_set/timestamp, purge, list, SSL/auth, large and huge events, 1000 binlogs, an 8.0->8.4 datadir upgrade, and replica/mysqlbinlog round-trips against `pull` (`rs_*`).
- binlog_server is started in the background by `start_proc_in_background.inc` and stopped **only with SIGTERM** (`terminate_proc.inc`), or ends on its own through `read_timeout` or a `KILL CONNECTION` of the Binlog Dump thread (`kill_binlog_dump_connection.inc`).
- Crash states are **hand-made rather than produced by a real crash**. For example, `storage_interrupted_binlog_creation.test` writes a stray magic-only binlog.
- `kill_and_restart.test` SIGKILLs **mysqld**, not binlog_server.

**F6. `fetch_crash_test.py` is the main randomized black-box test.** It is a Hypothesis `RuleBasedStateMachine` with these rules: workload (rows up to 17 MB, DDL), FLUSH, checkpoint changes, fetch-to-end, `fetch_and_kill`, and optional `fetch_with_fault` (S3 proxy) and `fetch_with_disk_fault` (fsfault).
- **The kill is coarse.** It polls `stored_data_size()` every 5 ms and SIGKILLs once N bytes have landed or after 5 s (lines 607-634). Kill points therefore follow file growth rather than code points.
- **It covers only `fetch`.** There is no pull, encryption, rewrite mode or purge, there is one binlog_server process at a time, and the config is fixed (lines 350-379).

**F7. Other fault harnesses** each inject **one fault per fetch run**, and each fault is cleared before the healing fetch:
- `disk_fault_sweep.py` and fsfault: open-for-write, write, fsync, rename and truncate failing with ENOSPC or EIO, selected by nth/count. There are no short writes, no unlink/mkdir/read faults and no latency.
- `s3_fault_sweep.py` and `s3_fault_proxy.py`: 500, 503, reset before or after, stall, or truncated body, per method and key. The backend is Versity S3 Gateway POSIX, which is strongly consistent, and the proxy never serves stale LIST/GET results or reorders requests.
- `network_fault_test.py` (Toxiproxy) only sits **between binlog_server and the source**: limit_data, reset_peer, timeout, slicer, latency, bandwidth. The pull part also checks liveness and fd growth across outage episodes.
- **Nothing injects faults between `pull` and its replicas.**

**F8. Other process-level scenarios:**
- `concurrent_storage_test.py`: N fetches on one storage (ticket 24).
- `source_change_test.py`: RESET, identity swap, PURGE, and a **graceful** source restart via `mysqladmin shutdown` (ticket 25).
- `event_variety_test.py`: compression, MINIMAL, PARTIAL_JSON, XA and tagged GTIDs, with kill/resume.
- `pull_replica_test.py`: one real replica, position and auto-position (ticket 20). No kills or restarts.
- `listener_fuzz_test.py`: malformed client traffic sent **sequentially**, one connection at a time, seeded.

**F9. Ticket provenance confirms the layers above.** Tickets 9 and 13 came from property tests, 24 and 25 from the concurrency and source-change harnesses, 17 and 18 from the S3 and disk sweeps. Ticket 19 (no data fsync) says outright that "a power-loss test would need a VM or ... dm-flakey" (snapshot line 1068), so that failure mode has never been tested for real. Ticket 15 (crash during binlog creation) is covered only by a hand-made MTR state.

## Coverage gaps (where Antithesis adds value)

1. **Crashes at arbitrary points during multi-step storage updates.** Real SIGKILL or node crash between data write, metadata write and index rename, during rotation (ticket 15), during checkpointing, and during purge. Existing crashes are either at operation boundaries (Hegel) or size-polled (Python).
2. **Losing data that was never fsynced** (ticket 19). Nothing tests a power-loss or node-hang style crash.
3. **Faults in combination.** Every harness injects one fault at a time and clears it before verifying. Nothing tests, say, a network cut plus a disk fault plus a restart, or an S3 503 storm during rotation followed by SIGKILL.
4. **Pull mode under faults.** Nobody crashes or restarts `pull`, and no network faults sit between pull and replicas. Several concurrent replicas while the collector writes and rotates are untested; only an in-process TSan reader thread exists. Disconnect or reconnect of the replica-facing listener mid-handshake or mid-dump is untested, as are tickets 10 and 26 timing (a dump before the first pull).
5. **Purge racing with fetch/pull across processes** (ticket 9 is in-process only), and a purge interrupted by a crash.
6. **Source-side timing.** Source mysqld SIGKILL or failover while fetch is mid-transaction (only a graceful stop is tested). FLUSH or rotation while binlog_server is caught up or disconnected (ticket 14). A network partition during the handshake or the COM_BINLOG_DUMP setup, rather than only after N bytes.
7. **Time.** `checkpoint_interval`, `idle_time`, read/connect timeouts and `search_by_timestamp` all depend on wall-clock time. No test applies clock skew or jumps. Timeouts are only exercised by real stalls.
8. **S3 semantics.** Eventual or stale LIST/GET, multipart-upload interleaving with crashes, and partial object visibility are all untested.
9. **Features with no fault coverage.** Encryption and rewrite/renumber mode (MTR only, including resume) have never been run under crash or fault harnesses.
10. **CI blind spots.** CI never runs the property tests or S3 MTR, and the nightly never runs any black-box harness. Antithesis would be the only continuous fault-and-crash layer.

## Reusable pieces

- **Oracles in `extra/qa/fetch_crash_test.py`:**
  - `storage_holds_prefixes()`: the index matches a prefix of the source's binlog list, each stored file is a byte prefix of `binlog_as_sent()`, and the metadata `size` is at a transaction boundary.
  - `check_complete_copy()`: an exact copy after a clean fetch, allowing a magic-only tail.
  - Both are directly usable as Antithesis `Always` and eventually-style checks.
- **Helpers from the same file:** `MySQLServer.binlog_as_sent()` (clears the IN_USE flag and recomputes the CRC), `parse_events` and `transaction_boundaries`. These are the core of a byte-exact oracle.
- **Workload and config:** the Hypothesis strategies (`transactions`, `workload_sql`) and `BinlogServer.write_config()` can serve as the workload generator and config template.
- **Fault shims:** `fsfault.c` (LD_PRELOAD, rule language) and `s3_fault_proxy.py` (HTTP control endpoint) are optional add-ons for faults that Antithesis does not provide natively, such as ENOSPC and S3 5xx.
- **Pull-side checks:** the pull liveness and fd-growth checks in `network_fault_test.py`, the clean-handshake end check in `listener_fuzz_test.py`, and the replica data-equality check in `pull_replica_test.py`.
- **Specs:** the C++ model in `storage_property_test.cpp` (complete transactions per binlog, checkpoint-size bound) is a ready-made specification of on-disk invariants. The `sender_context` properties (artificial ROTATE/FDE preamble, byte-exact stream, partial-tail hold-back) are a ready-made spec for what a replica should see.

## Areas examined

- `tests/` (all files listed, `PROPERTY_TESTS.md`, storage test header and crash function)
- `mtr/README`, `mtr/binlog_streaming/{t,include}`: the start, terminate and kill includes and the test headers
- `extra/qa/README.md` and the docstrings and fault tables of every `*.py` harness
- `fsfault.c` header
- `nightly/run_nightly.sh` and `known_failures.txt`
- `.github/workflows/cmake.yml`
- ticket snapshot overview

## Assumptions

- I did not trace CMake presets. I assume that, because the YAML never sets `WITH_PROPERTY_TESTS`, CI does not build the property tests.
- I assume no external cron or job outside the repo runs the Python harnesses or S3 MTR on a schedule.

## Open questions

- [open] Are the Python harnesses or S3 MTR run on a schedule anywhere outside the repo (Jenkins, QA host cron)? The README implies they are; `run_nightly.sh` does not run them.
- [open] Should the Antithesis harness reuse the Python oracles (Hypothesis dependency) or re-implement them in a lighter client?
- [open] Is losing data that was never fsynced in scope for Antithesis here, given ticket 19 and whether the platform can simulate it?
