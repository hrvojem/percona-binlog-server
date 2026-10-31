# Coverage gaps

- **Date:** 2026-10-30 (previous measurement: 2026-10-29, commit `6800b5d`)
- **Commit:** `bd8e2f6` (branch `pbt-init`). Since 2026-10-29 this adds the upstream merge of PBS-31 part 1 (`b49f8ce`: an optional `ssl` section and `require_secure_transport` for the listener), the coverage-gap tests of `e698aaf` (garbled encrypted passwords, a damaged PREVIOUS_GTIDS_LOG, the S3 range checks, the `fetch` TLS errors), the GTID-mode sender properties (`GtidModeStreamsExactlyTheMissingTransactions`, `GtidModeRejectsDamagedTransactionsCleanly`, `GtidModeFollowsANewBinlog`) and the harnesses `pull_tls_test.py` and `pull_gtid_test.py`. The measured tree is a clean worktree of that commit; md5 sums of `src/` before and after the run match.
- **Host output (kept on the QA host only):** `test-infra:~/ws/coverage-20261030/` (the earlier data stays in `~/ws/coverage-20261029/` and `~/ws/coverage-20261028/`)
  - `html/index.html`: line/branch HTML report for all layers (base + extra).
  - `combined.json`, `summary-combined.json`: the four base layers merged.
  - `combined-all.json`, `summary-all.json`: all layers merged.
  - `layers/*.json`: one tracefile per layer. `summary-<layer>.json`: one summary per layer.
  - `logs/`: test output. `logs/property-results.txt` has one PASS/FAIL line per property case.
  - `all.sh` (the driver), `build.sh`, `run.sh`, `run_faults.sh`, `run_property_serial.sh`, `merge.sh`: the scripts that produced the data.
  - `src-md5-before.txt`, `src-md5-after.txt`: md5 sums of `src/` before the build and after the last layer.

## How it was measured

1. **Build.** `build.sh` is a copy of `~/ws/build_coverage.sh` with its own paths. It uses preset `debug_gcc14`, `-DWITH_PROPERTY_TESTS=ON`, `--coverage -fprofile-update=atomic -fprofile-abs-path` and linker `--coverage`, with `--parallel 4`.
   - Source: a detached git worktree `~/ws/cov-src-20261030` at `bd8e2f6`. Build dir: `~/ws/percona-binlog-server-build-coverage-20261030` (kept).
   - Nothing else built in this tree, so the MD5 merge problem of 2026-10-14 cannot happen.
2. **Capture.** The `.gcda` counters were reset before each layer. Each layer was captured with gcovr 8.6 and gcov-14, using the flags of `~/ws/run_coverage.sh`: `--filter src/ --exclude-unreachable-branches --exclude-throw-branches`.
3. **Isolation from other jobs on the host.** Every harness used its own port (25xxx, clear of the nightly's 24470 and up). MTR used `--build-thread=600` (S3 MTR: 610). The S3 layers used their own bucket `pbs-cov-20261030` on the shared versitygw, and their own fault proxy on port 25575. The serial property layer now runs with `LAYERS=property`: on 2026-10-29 its script, a copy of `run.sh`, also re-ran every other layer (same data, about an hour lost).
4. **Base layers.** These four are comparable with earlier measurements:

| Layer | What ran |
|---|---|
| unit | `ctest -E _property_test$` (13 Boost unit tests, new: `util_test`). All pass. |
| property | All 14 Hegel property programs (125 cases, 116 before) at default Hegel settings, with `PBS_TEST_S3_*` set so the `...OnS3` variants ran. Each case ran in its own process, 4 at a time. 24 cases fail, all listed in `known_failures.txt`. |
| mtr-8.4 | `binlog_streaming` suite (84 tests, 82 before; new: `fetch_ssl_errors`, `fetch_connection_error_reason`) on MySQL 8.4.8, filesystem storage. 13 tests fail, all listed in `mtr_known_failures.txt`. |
| qa | `pull_replica_test.py` (position + gtid), `query_race_test.py --runs 200` (position + gtid) and `fetch_crash_test.py --examples 5 --steps 8` (gtid + position), all against 8.4.8. `pull_replica_test` passes in both modes now (GTID auto-position since PBS-29 part 2); the `query_race_test` failures are ticket 29. |

5. **Extra layers.** These are not in the base totals. They are cheap and reach code that the base layers do not.

| Layer | What ran |
|---|---|
| mtr-s3-8.4 | `binlog_streaming` on 8.4.8 with S3 storage (82 tests). 12 tests fail, all known. |
| crash-s3 | `fetch_crash_test.py --storage s3` (gtid + position, 5 examples, 8 steps). Passes. |
| listener-fuzz | `listener_fuzz_test.py --connections 2500 --seed 7` (position + gtid). Passes, with no crash. |
| s3-faults | `s3_fault_sweep.py --mode position` (87 cases, new: `pull_read_shrunk_range`). 10 cases fail, all matching `qa_known_failures.txt` (ticket 18 `PUT-.json`/`PUT-binlog.0` x6; ticket 37 `PUT-binlog.index … then DELETE-.json`). |
| disk-faults | `disk_fault_sweep.py --mode position` with `libfsfault.so` (86 cases, 19 fail). The failures all match ticket 18 (`*/binlog-metadata/*`) and ticket 37 (`binlog-index … then unlink binlog-metadata`). |
| pull-heartbeat | `pull_heartbeat_test.py` (new since 2026-10-28) with a real replica. Its known failure is `quiet_after_rotation` (ticket 40, PBS-113). |
| property-serial | The property programs again, one case at a time (101 pass, 24 fail, all known). The two runs fail different random-input cases: `FilesystemStorageUriMatchesModel` (ticket 12) only in parallel, `GtidModeRejectsDamagedTransactionsCleanly` (ticket 39) only serially. |
| pull-tls | `pull_tls_test.py` (new): the listener with and without an `ssl` section, TLS clients and a TLS replica. 7 of 13 cases fail, all known (ticket 46, PBS-119). |
| pull-gtid | `pull_gtid_test.py` (new): GTID auto-position replicas through rotations, restarts, a purge and a replica ahead of the source. `purged` and `ahead` fail, both known (ticket 20, PBS-120). |

**No unexpected failures** in any layer. Some known property failures did not fire at default settings, which is expected, because they depend on what Hegel generates. Examples are tickets 8 (split), 36, 37 (fs) and 39 (`ReplicationSourceSurvivesAnyDumpPosition`).

> **Note: one property run is not deterministic.** Hegel draws new inputs every run. A file reached by only a few cases can differ a lot between runs. For example, `config_value_property_test/ConfigFileMatchesModel` was run 6 more times on its own. `replication_source_config.cpp` came out at 66.7–81.5% lines, and `rewrite_config.cpp` at 50–100%. In the measured property layer, `rewrite_config.cpp` had 0% and the `replication_source_config` defect lines had none (see gap 10). Treat a ±1–2 line difference in a config validator as noise. A gap that a single run reliably misses needs explicit examples (`hegel` `@example`-style fixed cases) or more test cases.
>
> The older note still applies: one crash disables the rest of a property program, so each case is run in its own process.

## Totals (all `src/` files, headers included)

Base layers. "Δ" is the change in line % against 2026-10-29. There are 118 more instrumented lines (11764 → 11882), mostly the TLS listener of PBS-31 part 1 (`network_service.cpp` 245 → 266 lines, `replication_source_config.cpp`, the new `source_ssl_config.cpp`).

| Layer | Lines | Line % | Δ | Branches | Branch % | Functions % |
|---|---|---|---|---|---|---|
| unit | 3134 / 11882 | 26.4 | +1.0 | 1263 / 6536 | 19.3 | 25.2 |
| property | 8560 / 11882 | 72.0 | +1.4 | 4077 / 6536 | 62.4 | 71.9 |
| mtr-8.4 | 8307 / 11882 | 69.9 | −0.3 | 3956 / 6536 | 60.5 | 71.0 |
| qa | 5712 / 11882 | 48.1 | −0.4 | 2654 / 6536 | 40.6 | 50.3 |
| **combined** | **10700 / 11882** | **90.1** | **+1.1** | **5248 / 6536** | **80.3** (+1.1) | **90.5** (+1.1) |

Extra layers (each measured on its own), and everything merged:

| Layer | Lines | Line % | Δ | Branches | Branch % | Functions % |
|---|---|---|---|---|---|---|
| mtr-s3-8.4 | 8523 / 11882 | 71.7 | −0.4 | 4074 / 6536 | 62.3 | 72.3 |
| crash-s3 | 3767 / 11882 | 31.7 | +0.6 | 1771 / 6536 | 27.1 | 32.9 |
| listener-fuzz | 4820 / 11882 | 40.6 | 0.0 | 2177 / 6536 | 33.3 | 41.8 |
| s3-faults | 3348 / 11882 | 28.2 | 0.0 | 1536 / 6536 | 23.5 | 29.5 |
| disk-faults | 3153 / 11882 | 26.5 | 0.0 | 1438 / 6536 | 22.0 | 28.2 |
| pull-heartbeat | 4573 / 11882 | 38.5 | 0.0 | 1898 / 6536 | 29.0 | 42.0 |
| property-serial | 8531 / 11882 | 71.8 | +0.9 | 4066 / 6536 | 62.2 | 71.6 |
| pull-tls | 1790 / 11882 | 15.1 | new | 756 / 6536 | 11.6 | 17.2 |
| pull-gtid | 5619 / 11882 | 47.3 | new | 2523 / 6536 | 38.6 | 50.0 |
| **all layers** | **10787 / 11882** | **90.8** | **+1.0** | **5319 / 6536** | **81.4** (+1.1) | **91.2** (+1.2) |

The small drops in mtr and qa are the larger denominator (the TLS code is reached only by pull-tls). The extra layers add 87 lines over the base. `pull-tls` is the only layer that reaches 11 of them: the certificate and key loading of `network_service::initialize_optional_ssl_context` (1173-1189). Its 15.1% understates it: the test killed `pull` with SIGKILL, so only the startup-failure cases were counted (see below). `pull-gtid` reaches no line that another layer misses; its value is the end-to-end checks (it found the ticket 20 gaps, PBS-120).

**Not in this measurement:** MTR on 8.0 and 9.7, `network_fault_test.py`, `source_change_test.py`, `concurrent_storage_test.py`, `large_binlog_test.py`, `pull_load_test.py`, gtid-mode fault sweeps, the libFuzzer targets (a separate Clang build), Antithesis runs.

## Top gaps

Coverage is the combined figure of the base layers, unless "extra" is named. Every line listed was checked as uncovered (count 0) in `combined.json`. Where an extra layer is named, its tracefile was checked too. "Test kind" is the cheapest kind of test that would reach the code.

### Rows carried over from 2026-10-14

| # | Where (file:lines) | What is not exercised | Test kind |
|---|---|---|---|
| 1 | `easymysql/connection.cpp` fetch error → `operations/pull_operation.cpp` | ~~Fetch error other than CR_SERVER_LOST in pull mode.~~ **Covered (2026-10-28): `pull_operation.cpp` 52/53 lines (only the `jthread` declaration line 125 shows 0). Addressed (2026-10-15) by MTR `pull_source_purged_past_resume_point`.** | — |
| 2 | `binsrv/s3_storage_backend.cpp:766-809` (`do_resize_object`) | ~~S3 crash-recovery truncation.~~ **Covered by extra layers (2026-10-28): crash-s3 runs it 2 times (`storage_core.cpp:1155` recovery warning, `s3_storage_backend.cpp:776-799`) and mtr-s3-8.4 3 times. The base layers still have 0, because the property S3 variants never truncate.** Still open: the error raises at 786, 795 and 805 (resize, reopen and remove of the temp file fail). | Property: fs-fault on the S3 temp dir; low value |
| 3 | `s3_storage_backend.cpp:452,482-484` | ListObjectsV2 pagination. **452 and 482-483 are reached by mtr-s3-8.4 (6 hits).** Open: 484, the empty-continuation-token error, and the 465/472 malformed-listing raises. | Fault proxy: a `truncate_body`/rewrite fault on `list-type` that drops the token |
| 4 | `s3_storage_backend.cpp:384,410,513` | Error outcomes of put / delete / get. **Reached by s3-faults (384: 14, 410: 2, 513: 15).** Still open in every layer: size and length checks at 283, 298, 304, 313, 349 and 359 (object too large, range length mismatch, short read). | Property with a fake backend that lies about `Content-Length`; S3 proxy `truncate_body` with a wrong length header |
| 5 | `binsrv/storage_core.cpp:446-457` | ~~`purge_binlogs` when `remove_objects` fails.~~ **Covered: the catch is reached by the property layer (`StoragePurgeSurvivesAFault`). Addressed (2026-10-15).** | — |
| 6 | `storage_core.cpp:838,860`; `binsrv/file_keyring.cpp:39,46,66`; `binsrv/encryption_config.cpp:31,37` | Encryption and keyring errors that are still open: encryption configured but no keyring (838); KEK cipher block size incompatible with the data key length (860); keyring URI that does not parse, or has a wrong scheme or a query (39, 46, 66); non-generic format and non-CTR cipher (31, 37). **`keyring_factory.cpp` is now 100%.** The KEK-missing and no-keyring paths are covered (2026-10-15 MTR `encryption_keyring_removed`). | Property: `KeyringUriMatchesModel` (add unparsable / scheme / query URIs) and `ConfigFileMatchesModel` (31/37 are drawn but missed in this run, see the note above) |
| 7 | moved to `operations/sender_single_file_context.cpp` (PBS-29) | See new row N3. The old `sender_context.cpp` lines no longer exist. The remaining `sender_context.cpp` gaps are 104 (empty next binlog name after a non-active EOF, a logic error) and 121-122 (heartbeat before the first file is resolved). | Property: `sender_context_property_test` with an empty storage + heartbeat |
| 8 | `minimysql/network_service.cpp:246-261,275-281` (`handle_exception`) | **2026-10-29: file at 95.1% (90.8%); the open lines are the two unreachable branches.** Before: listener-fuzz enters `handle_exception` 3738 times. Every time, the category test at 243-244 fails, so the error goes to the generic "system error caught" **error-level** log at 268-272. The `timed_out`, `operation_aborted` and `eof`/`connection_reset` cases (246-261) and the `std::exception` / `...` catches (275-281) never run. **Confirmed bug (ticket 42, PBS-115):** `boost::asio::error::eof` is in the `asio.misc` category, not the system category, so the "connection closed by peer" info branch is unreachable for EOF and every client disconnect is logged at error level (`system error caught in session ...: asio.misc:2 ... End of file`, 11 times in one `pull_heartbeat_test.py` run). **Test added (2026-10-28):** `ReplicationSourceLogsDisconnectsAsRoutine` disconnects at four points of a session with a FIN or a reset and checks the log levels; it fails today (known failure) and passes with the fix, reaching the EOF and `connection_reset` branches. **Also added (2026-10-28):** a client silent past `read_timeout` (the `timed_out` branch) and `ReplicationSourceLogsAFailedDumpAndKeepsListening` (the `std::exception` catch). `operation_aborted` and the `...` catch are unreachable: `pull` stops by `io_context::stop()`, which destroys the session coroutines without an exception, nothing cancels a session's socket or timer, and every exception the session code raises derives from `std::exception`. | Done, except the two unreachable branches |
| 9 | `minimysql/connection_context.cpp:440-458` | **2026-10-29: covered; file at 92.4% (89.8%).** COM_RESET_CONNECTION and COM_CHANGE_USER decoding. Still open, even with listener-fuzz (its "random command" scenario never sends 0x1f / 0x11 after a valid handshake). **`generate_encoded_access_denied` and `generate_encoded_full_auth` are now covered (PBS-10).** **Addressed (2026-10-28):** both commands are in the generated command sequences of `replication_source_property_test`, and `ReplicationSourceAnswersResetConnectionAndChangeUser` sends each every run. Both are decoded and then refused as unknown commands (ERR 1047): `network_service` has no case for them. | Done |
| 10 | `binsrv/replication_source_config.cpp:34,42,46,57,62,67,79`; `binsrv/rewrite_config.cpp:26-33` | **2026-10-29: both files at 100% (74.1% and 50%).** On 2026-10-28: regressed in that run, not in the code. UUID length, separator and non-hex rejection, port = 0, `read_timeout`/`write_timeout` = 0 and an empty `auth_rsa_private_key` (79) were all missed. `rewrite_config::validate` was never called by the property layer (mtr reaches 50%). A solo rerun of `ConfigFileMatchesModel` reached 34, 62 and all of `rewrite_config.cpp`. Line 79 (empty RSA key, new with PBS-34) is not among the drawn defects at all. **Addressed (2026-10-28):** `ConfigFileRejectsEveryDefect` forces every defect (each UUID damage separately) on 10 files each; the new `auth_rsa_private_key_empty` defect covers line 79, and the rewrite defects call `rewrite_config::validate` every run. | Done |
| 11 | `binsrv/events/reader_context.cpp:553,571,701,836,869,876,886,943,952,972,983,990` | Addressed (2026-10-15) by `ReaderContextRejectsTargetedDefectsCleanly`; the file is now at 93.7%. Still open: an artificial ROTATE with a position mismatch (553, 571), pseudo-FDE next-position ≠ 0 (701), a second FDE in a binlog (836), a last event that is not ROTATE/STOP or is artificial (869, 876), a non-artificial ROTATE position (886), non-empty or empty GTIDs and oversized transaction lengths in (anonymous / tagged) GTID events (943-990). | Property: extend the defect list of `ReaderContextRejectsTargetedDefectsCleanly` |
| 12 | `operations/collector_context.cpp:155,263-267,530,563`; `storage_core.cpp:295,300,914-919` | Unexpected packet prefix (155). `switch_to_*_replication` failing with `core_error` (263-267, new). Rewrite-mode ROTATE position mismatch (530). The `opened_empty` reopen (563 / `storage_core.cpp:914-919`) still looks unreachable: 295/300 raise first. | MTR for 530 (rewrite + source restart); 263 needs a source that refuses COM_BINLOG_DUMP (QA with a fake source); 155 likewise |

### New rows (code added since 2026-10-14)

| # | Where (file:lines) | What is not exercised | Test kind |
|---|---|---|---|
| N1 | `binsrv/events/heartbeat_log_v2_body_impl.cpp:65,73,82,88,103,108-115,121-122,141,171-174` (PBS-30) | The decoder's rejection paths are never run: malformed field type (73), truncated length (82), filename longer than the body (88), truncated position (103) and a body without an end marker (65). The same goes for skipping an unknown field type (108-115), `get_parsed_binlog()` (121-122), the encode-buffer-too-small raise (141) and `operator<<` (171-174). The file was at 77.6%; **2026-10-29: 92.5%, open only `get_parsed_binlog()` (121-122), the encode-buffer raise (141) and `operator<<` (171-174).** | Unit (`heartbeat_log_v2_event_test`: add truncated / unknown-field bodies) or a property round-trip with mutated bodies in `event_property_test` **Addressed (2026-10-28): property `HeartbeatLogV2BodyDecodesLikeMysql` (`event_property_test`) decodes generated and corrupted bodies against a model of MySQL's `Heartbeat::decode`; passes, and catches a dropped filename length check, unknown fields not skipped, and a position length check MySQL does not make.** |
| N2 | `minimysql/heartbeat_sender.cpp:66` (PBS-30) | Only `wait_cap` when a beat is already overdue. The rest of the sender is 94.7%, reached by mtr (94.7%) and qa (84.2%). The property layer reaches only 47.4%. Ticket 40 (PBS-113) is the known failure of the idle path. | Property: `sender_context_property_test` with a period shorter than the poll interval |
| N3 | `operations/sender_single_file_context.cpp:380,393,399` (PBS-29; since part 2 at 404-424) | **Addressed (2026-10-29):** `CorruptedPreviousGtidsLogFailsCleanly` reaches 'cannot determine size' (a binlog ending inside the event's header) and 'remainder fetch failed' (an event_size larger than the binlog); the third check ('still corrupted') needs a consistent size and a wrong event count at once, which one event cannot give. The second preamble fetch, when the first one does not hold the whole PREVIOUS_GTIDS_LOG event, **is covered**: lines 374-398 run 160 times in the property layer (the large generated GTID sets of `FromPositionStreamsArtificialPreambleAndRemainingEvents`; a resumed read first fetches only about FORMAT_DESCRIPTION + 2 KiB). 386 is a continuation line. Open: only the corruption raises, "cannot determine size" (380), "remainder fetch failed" (393) and "still corrupted" (399). | Property: a damaged stored PREVIOUS_GTIDS_LOG (size field larger than the file) under a resumed read |
| N4 | `operations/sender_single_file_context.cpp:71,75,144-148,159-162,250,297,358-359,408-409,517` | Constructor checks (block size too small, empty binlog name). `get_event()` after EOF and an unknown FSM state. A partial `discard_from_carry_buffer` (159-162; only whole-buffer discards happen). A preamble whose first/second event is not FDE / PREVIOUS_GTIDS (250, 297). A non-active-file EOF while reading the preamble (358-359: a non-last binlog of 4 bytes). The corrupted preamble default (408-409). An empty block after retry (517). | Property: `sender_context_property_test` with a storage of damaged / magic-only non-last binlogs (cheap: the in-process storage is already built there) |
| N5 | `minimysql/network_service.cpp:114-115,715` (PBS-90 / PBS-30 handshake handlers) | The regex SET/SELECT handlers all run. The `};` lambda exits at 674, 683 and 696 are gcov artifacts. Open: an out-of-range `@master_heartbeat_period` value (715, "disables heartbeats") and the swallowed exception in `scope_tracer`'s destructor (114-115, low value). | Property: `replication_source_property_test`, add a 25-digit period to the statement generator |
| N6 | `operations/sender_single_file_context.cpp` / `operations/sender_context.cpp` | Overall the PBS-29 code is well covered (91.4% and 94.2%). See N3/N4 for the open paths. | — |
| N7 | `binsrv/storage_core.cpp:657-659,664,715-719,1149,1205` (PBS-59 recovery) | The recovery collectors are well covered. Per-layer hits: first-binlog garbage at 670/676 (property 41/99, mtr 2); rotation garbage at 735/742 (property 2000/2488, mtr 12/13, qa 25/39, disk-faults 22/25, s3-faults 5/5); interrupted purge at 794/806 (property 29/25, mtr 2/2); temporary objects at 597 (property 3576, qa 244). Open: a single data object whose name does not parse as a binlog name (657-659); two objects without the matching metadata (664); the last binlog at the maximum sequence number, so `next()` throws (715-719); recovery refused because the metadata size is larger than the file (1149); "found metadata for a non-existing binlog" (1205). 1205 is the ticket 37 symptom; it was not reached in the base layers, and the ticket-37 fault-sweep cases were not checked line by line. | Property: `StorageOpeningOfDamagedFilesFailsCleanly` (+OnS3), add the four layouts (cheap; the damaged-storage generator exists) |
| N8 | `minimysql/connection_context.cpp:137-139`; `minimysql/caching_sha2_password_authenticator.cpp:89,108` (PBS-10) | **Addressed (2026-10-29):** `ReplicationSourceRefusesGarbledEncryptedPasswords` sends random bytes that do not decrypt (137-139), half an encryption, and an encryption without the terminating NUL (108); each is refused like a wrong password and the next login still succeeds. Line 89 (an empty scramble) cannot come from a client. The full-auth path works (RSA decrypt + SHA256 check), and the fast path and auth switch are covered. Open: a full-auth attempt whose RSA-encrypted password does not decrypt (137-139, 108: not null-terminated) and an empty nonce (89, a logic error). `caching_sha2_password_authenticator.cpp` is at 89.3% (line 71 is a continuation-line artifact). | Property: `replication_source_property_test`, send random bytes of key size as the encrypted password |
| N9 | `binsrv/s3_storage_backend.cpp:210-226,258-272,676-677` | The bucket-region lookup constructor (`bucket_tag`), used for an `s3://` URI without an endpoint (real AWS). Every test uses `http://host:port`. Also open: the default temp directory and the "not a directory" checks (564-590). | Manual / Antithesis against real S3; a property for 582/588 with a file as `tmp_file_directory` |
| N10 | `binsrv/indexed_event_block.cpp:52-59,173,188,196-197,201,256` (PBS-29 part 2) | **2026-10-30: 92.1% (74.0%), measured after the GTID-mode properties.** Transactions larger than a block (85-106) and tagged GTIDs (182-195) are now reached. Open: the by-event constructor alias (52-59, unused by the tests) and some rejection checks: a transaction whose first event is not a GTID event (173), an empty or wrong GTID (188, 196-197, 201) and a too-short tail (256). `GtidModeRejectsDamagedTransactionsCleanly` draws these defects but stops at the first damaged transaction, and ticket 39 (PBS-112) terminates the process for a too-long one. | Property: a defect list forced per case, like `ConfigFileRejectsEveryDefect` |

**2026-10-29, later:** the S3 download range checks (`s3_storage_backend.cpp:348`, 'range does not match the length') are reached by the new `pull_read_shrunk_range` case of `s3_fault_sweep.py` (the proxy's `shrink_body` answers ranged GETs with half the range): the client fails rather than receiving wrong bytes, and `pull` keeps serving. The TLS failures of the `fetch` side (a CA that did not sign the server certificate, a missing CA file, an invalid TLS version, a REQUIRE X509 user without a client certificate) are refused with exit 1 (MTR `fetch_ssl_errors`), but the log does not say why (MTR `fetch_connection_error_reason`, ticket 47, PBS-121); the `mysql_options` error raises of `easymysql/connection.cpp:240-298` stay unreachable (the client library accepts any value there and fails at connect). `connection_context.cpp:197-202` (the heartbeat-v2 flag of a COM_BINLOG_DUMP) is only used in a debug log line for GTID dumps and cannot be reached.

**2026-10-30:** the TLS listener of PBS-31 part 1. In the base layers `network_service.cpp` falls to 89.8% (95.1%), because none of them configures an `ssl` section; with pull-tls it is at 94.0%. The layer understates TLS: `pull_tls_test.py` stopped `pull` with SIGKILL, so only the cases where `pull` exits by itself (the four bad-certificate / bad-key startups, which reach the raises at 1179 and 1185) wrote their counters, and the successful load (1188) and the TLS context deleter (139-145) show 0. Stopping `pull` with SIGTERM, which `pull` handles, would let the next measurement count those runs too. No TLS handshake runs yet (ticket 46, PBS-119), so the session side of TLS has nothing to measure. Noise: `byte_span_extractors.hpp` 98.1 → 92.3 (the packed-integer markers at 161-178 were reached by random property inputs on 2026-10-29 and not this time); `empty_body.cpp` and `heartbeat_log_v2_body_impl.cpp` lost a line each the same way.

**Checked and adequately covered** (combined %, more than one layer reaches each):
- **PBS-30 heartbeat sender:** `heartbeat_sender.cpp` 94.7%. `event_generation_helpers.cpp` 95.8% (150 and 192 are continuation lines).
- **PBS-29 sender split:** `sender_context.cpp` 95.0%, `sender_single_file_context.cpp` 92.9%; `indexed_event_block.cpp` 92.1% (N10).
- **PBS-90/PBS-30 handshake regex handlers** in `network_service.cpp`: every SET/SELECT handler runs (file at 95.1%).
- **PBS-10 `caching_sha2_password`:** fast and full paths. Only the decrypt-failure branches are open (N8).
- **PBS-59 recovery collectors:** see N7. `storage_core.cpp` 93.0% (was 90.9%).
- **Rewrite mode:** `rewriter.cpp` 96.2%.

Known-unreachable code that should not drive new tests:
- **AEAD tags** in `storage_core.cpp:87,119-121,1363-1364`: only CTR is accepted (`encryption_config.cpp:35`).
- **`cipher_context.cpp`:** the cipher-mode mapping.
- **`easymysql/connection.cpp:240-318`:** SSL option setters, plus `mysql_binlog_open` failures at 136 and 160. Optional; an MTR with TLS would reach the setters.
- **`connection_context.cpp` encode-failure raises** (206, 259, 316, 334, 358, 398, 600, 614, 635, 762-823): `classic_protocol::encode` into a dynamic buffer does not fail.

## Per-source numbers (src/*.cpp, combined base layers)

The last four columns are line % per layer.

| File | Lines | L% | Branches | B% | unit | prop | mtr | qa |
|---|---|---|---|---|---|---|---|---|
| app.cpp | 7/14 | 50.0 | 5/29 | 17.2 | 0.0 | 0.0 | 50.0 | 50.0 |
| binsrv/authentication_config.cpp | 8/8 | 100.0 | 6/6 | 100.0 | 0.0 | 100.0 | 62.5 | 62.5 |
| binsrv/basic_keyring.cpp | 7/8 | 87.5 | 0/0 | - | 0.0 | 87.5 | 87.5 | 0.0 |
| binsrv/basic_logger.cpp | 21/22 | 95.5 | 13/13 | 100.0 | 0.0 | 95.5 | 95.5 | 95.5 |
| binsrv/basic_storage_backend.cpp | 44/47 | 93.6 | 11/14 | 78.6 | 0.0 | 93.6 | 74.5 | 53.2 |
| binsrv/binlog_file_metadata.cpp | 16/17 | 94.1 | 6/7 | 85.7 | 0.0 | 94.1 | 94.1 | 94.1 |
| binsrv/cout_logger.cpp | 4/4 | 100.0 | 0/0 | - | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/encryption_config.cpp | 5/6 | 83.3 | 3/4 | 75.0 | 0.0 | 83.3 | 66.7 | 0.0 |
| binsrv/events/common_header.cpp | 42/47 | 89.4 | 24/25 | 96.0 | 74.5 | 89.4 | 89.4 | 85.1 |
| binsrv/events/common_header_view.cpp | 77/85 | 90.6 | 7/22 | 31.8 | 80.0 | 88.2 | 89.4 | 89.4 |
| binsrv/events/composite_binlog_name.cpp | 30/33 | 90.9 | 20/24 | 83.3 | 81.8 | 87.9 | 81.8 | 78.8 |
| binsrv/events/empty_body.cpp | 5/6 | 83.3 | 1/2 | 50.0 | 0.0 | 83.3 | 83.3 | 0.0 |
| binsrv/events/empty_post_header.cpp | 5/6 | 83.3 | 1/2 | 50.0 | 50.0 | 83.3 | 83.3 | 83.3 |
| binsrv/events/event.cpp | 124/137 | 90.5 | 38/47 | 80.9 | 69.3 | 89.1 | 75.9 | 56.2 |
| binsrv/events/event_view.cpp | 57/58 | 98.3 | 39/40 | 97.5 | 70.7 | 87.9 | 81.0 | 72.4 |
| binsrv/events/footer.cpp | 4/12 | 33.3 | 2/6 | 33.3 | 16.7 | 33.3 | 33.3 | 33.3 |
| binsrv/events/footer_view.cpp | 19/22 | 86.4 | 7/10 | 70.0 | 59.1 | 86.4 | 86.4 | 86.4 |
| binsrv/events/format_description_body_impl.cpp | 25/26 | 96.2 | 8/10 | 80.0 | 88.5 | 88.5 | 88.5 | 61.5 |
| binsrv/events/format_description_post_header_impl.cpp | 72/82 | 87.8 | 20/25 | 80.0 | 70.7 | 87.8 | 86.6 | 69.5 |
| binsrv/events/gtid_log_body.cpp | 100/110 | 90.9 | 66/76 | 86.8 | 61.8 | 90.9 | 29.1 | 29.1 |
| binsrv/events/gtid_log_post_header.cpp | 45/48 | 93.8 | 20/23 | 87.0 | 54.2 | 93.8 | 81.2 | 62.5 |
| binsrv/events/gtid_tagged_log_body_impl.cpp | 268/293 | 91.5 | 125/152 | 82.2 | 75.1 | 90.4 | 37.5 | 0.0 |
| binsrv/events/heartbeat_log_v2_body_impl.cpp | 61/67 | 91.0 | 20/22 | 90.9 | 77.6 | 91.0 | 43.3 | 0.0 |
| binsrv/events/previous_gtids_log_body_impl.cpp | 25/26 | 96.2 | 9/10 | 90.0 | 73.1 | 96.2 | 96.2 | 53.8 |
| binsrv/events/protocol_traits.cpp | 31/41 | 75.6 | 26/39 | 66.7 | 75.6 | 73.2 | 73.2 | 73.2 |
| binsrv/events/reader_context.cpp | 239/255 | 93.7 | 175/196 | 89.3 | 40.0 | 93.7 | 84.3 | 78.4 |
| binsrv/events/rewriter.cpp | 128/132 | 97.0 | 59/74 | 79.7 | 33.3 | 97.0 | 44.7 | 0.0 |
| binsrv/events/rotate_body_impl.cpp | 20/21 | 95.2 | 5/6 | 83.3 | 76.2 | 95.2 | 95.2 | 95.2 |
| binsrv/events/rotate_post_header_impl.cpp | 13/14 | 92.9 | 3/4 | 75.0 | 78.6 | 85.7 | 85.7 | 85.7 |
| binsrv/events/unknown_body.cpp | 2/2 | 100.0 | 0/0 | - | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/events/unknown_post_header.cpp | 2/2 | 100.0 | 0/0 | - | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/exception_handling_helpers.cpp | 24/27 | 88.9 | 15/20 | 75.0 | 0.0 | 0.0 | 88.9 | 0.0 |
| binsrv/file_keyring.cpp | 30/33 | 90.9 | 30/42 | 71.4 | 0.0 | 90.9 | 72.7 | 0.0 |
| binsrv/file_logger.cpp | 8/10 | 80.0 | 3/7 | 42.9 | 0.0 | 0.0 | 80.0 | 80.0 |
| binsrv/filesystem_storage_backend.cpp | 98/115 | 85.2 | 79/110 | 71.8 | 0.0 | 85.2 | 80.0 | 67.8 |
| binsrv/gtids/gtid.cpp | 18/19 | 94.7 | 20/20 | 100.0 | 84.2 | 78.9 | 89.5 | 73.7 |
| binsrv/gtids/gtid_set.cpp | 379/399 | 95.0 | 233/256 | 91.0 | 90.5 | 94.0 | 75.4 | 60.4 |
| binsrv/gtids/tag.cpp | 30/34 | 88.2 | 17/19 | 89.5 | 88.2 | 76.5 | 47.1 | 14.7 |
| binsrv/gtids/uuid.cpp | 24/25 | 96.0 | 5/7 | 71.4 | 96.0 | 84.0 | 56.0 | 56.0 |
| binsrv/indexed_event_block.cpp | 117/127 | 92.1 | 78/90 | 86.7 | 0.0 | 90.6 | 72.4 | 69.3 |
| binsrv/keyring_factory.cpp | 8/8 | 100.0 | 6/6 | 100.0 | 0.0 | 100.0 | 75.0 | 0.0 |
| binsrv/keyring_record.cpp | 11/11 | 100.0 | 8/8 | 100.0 | 0.0 | 100.0 | 63.6 | 0.0 |
| binsrv/keyring_record_collection.cpp | 41/42 | 97.6 | 28/28 | 100.0 | 0.0 | 97.6 | 88.1 | 0.0 |
| binsrv/logger_factory.cpp | 5/5 | 100.0 | 4/4 | 100.0 | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/main_config.cpp | 14/14 | 100.0 | 8/8 | 100.0 | 0.0 | 100.0 | 92.9 | 92.9 |
| binsrv/models/error_response.cpp | 8/8 | 100.0 | 3/3 | 100.0 | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/models/search_response.cpp | 12/20 | 60.0 | 2/3 | 66.7 | 0.0 | 0.0 | 60.0 | 60.0 |
| binsrv/replication_config.cpp | 7/7 | 100.0 | 4/4 | 100.0 | 0.0 | 100.0 | 85.7 | 57.1 |
| binsrv/replication_source_config.cpp | 33/33 | 100.0 | 38/38 | 100.0 | 0.0 | 100.0 | 66.7 | 66.7 |
| binsrv/rewrite_config.cpp | 6/6 | 100.0 | 5/5 | 100.0 | 0.0 | 100.0 | 50.0 | 0.0 |
| binsrv/s3_error.cpp | 7/13 | 53.8 | 4/5 | 80.0 | 0.0 | 53.8 | 0.0 | 0.0 |
| binsrv/s3_error_helpers_private.cpp | 20/20 | 100.0 | 14/15 | 93.3 | 0.0 | 100.0 | 0.0 | 0.0 |
| binsrv/s3_storage_backend.cpp | 351/427 | 82.2 | 239/337 | 70.9 | 0.0 | 82.2 | 0.0 | 0.0 |
| binsrv/size_unit.cpp | 36/39 | 92.3 | 22/26 | 84.6 | 0.0 | 92.3 | 69.2 | 66.7 |
| binsrv/source_ssl_config.cpp | 6/6 | 100.0 | 4/4 | 100.0 | 0.0 | 100.0 | 0.0 | 0.0 |
| binsrv/storage.cpp | 144/145 | 99.3 | 57/64 | 89.1 | 0.0 | 80.7 | 97.9 | 93.1 |
| binsrv/storage_backend_factory.cpp | 19/22 | 86.4 | 10/12 | 83.3 | 0.0 | 86.4 | 54.5 | 54.5 |
| binsrv/storage_config.cpp | 11/11 | 100.0 | 7/7 | 100.0 | 0.0 | 100.0 | 90.9 | 81.8 |
| binsrv/storage_core.cpp | 608/653 | 93.1 | 500/589 | 84.9 | 0.0 | 88.2 | 89.4 | 57.0 |
| binsrv/storage_metadata.cpp | 15/16 | 93.8 | 6/7 | 85.7 | 0.0 | 93.8 | 93.8 | 93.8 |
| binsrv/time_unit.cpp | 37/40 | 92.5 | 22/26 | 84.6 | 0.0 | 92.5 | 67.5 | 0.0 |
| easymysql/connection.cpp | 227/268 | 84.7 | 88/139 | 63.3 | 0.0 | 0.0 | 84.7 | 57.5 |
| easymysql/connection_config.cpp | 22/27 | 81.5 | 26/36 | 72.2 | 0.0 | 33.3 | 77.8 | 77.8 |
| easymysql/core_error.cpp | 9/13 | 69.2 | 4/5 | 80.0 | 0.0 | 0.0 | 69.2 | 0.0 |
| easymysql/core_error_helpers_private.cpp | 12/12 | 100.0 | 6/7 | 85.7 | 0.0 | 0.0 | 100.0 | 0.0 |
| easymysql/library.cpp | 8/12 | 66.7 | 1/2 | 50.0 | 0.0 | 0.0 | 66.7 | 66.7 |
| minimysql/caching_sha2_password_authenticator.cpp | 26/28 | 92.9 | 19/22 | 86.4 | 0.0 | 92.9 | 89.3 | 42.9 |
| minimysql/connection_context.cpp | 390/423 | 92.2 | 190/218 | 87.2 | 0.0 | 88.7 | 79.9 | 65.5 |
| minimysql/heartbeat_sender.cpp | 18/19 | 94.7 | 7/8 | 87.5 | 0.0 | 94.7 | 94.7 | 84.2 |
| minimysql/network_io_operations.cpp | 6/6 | 100.0 | 3/3 | 100.0 | 0.0 | 100.0 | 100.0 | 100.0 |
| minimysql/network_service.cpp | 239/266 | 89.8 | 142/201 | 70.6 | 0.0 | 85.7 | 81.2 | 64.3 |
| opensslpp/cipher_context.cpp | 209/247 | 84.6 | 106/150 | 70.7 | 82.2 | 71.3 | 70.4 | 0.0 |
| opensslpp/core_error.cpp | 23/23 | 100.0 | 22/26 | 84.6 | 100.0 | 95.7 | 0.0 | 0.0 |
| opensslpp/crypto_rng.cpp | 7/9 | 77.8 | 4/6 | 66.7 | 77.8 | 66.7 | 66.7 | 66.7 |
| opensslpp/digest_context.cpp | 46/57 | 80.7 | 22/36 | 61.1 | 80.7 | 78.9 | 78.9 | 0.0 |
| opensslpp/evp_pkey.cpp | 187/216 | 86.6 | 109/152 | 71.7 | 86.6 | 76.9 | 53.7 | 53.7 |
| operations/basic_operation.cpp | 6/9 | 66.7 | 2/7 | 28.6 | 0.0 | 0.0 | 66.7 | 66.7 |
| operations/collector_context.cpp | 226/231 | 97.8 | 207/219 | 94.5 | 0.0 | 0.0 | 97.8 | 66.2 |
| operations/event_generation_helpers.cpp | 46/48 | 95.8 | 23/26 | 88.5 | 0.0 | 95.8 | 93.8 | 20.8 |
| operations/fetch_operation.cpp | 39/40 | 97.5 | 22/22 | 100.0 | 0.0 | 0.0 | 97.5 | 80.0 |
| operations/list_operation.cpp | 22/22 | 100.0 | 14/15 | 93.3 | 0.0 | 0.0 | 100.0 | 100.0 |
| operations/logger_helpers.cpp | 306/312 | 98.1 | 205/233 | 88.0 | 0.0 | 0.0 | 98.1 | 54.5 |
| operations/model_helpers.cpp | 12/12 | 100.0 | 10/20 | 50.0 | 0.0 | 0.0 | 100.0 | 91.7 |
| operations/operation_factory.cpp | 14/18 | 77.8 | 6/9 | 66.7 | 0.0 | 0.0 | 77.8 | 61.1 |
| operations/pull_operation.cpp | 52/53 | 98.1 | 34/35 | 97.1 | 0.0 | 0.0 | 98.1 | 84.9 |
| operations/purge_binlogs_operation.cpp | 25/27 | 92.6 | 16/19 | 84.2 | 0.0 | 0.0 | 92.6 | 0.0 |
| operations/search_by_gtid_set_operation.cpp | 25/25 | 100.0 | 16/17 | 94.1 | 0.0 | 0.0 | 100.0 | 0.0 |
| operations/search_by_timestamp_operation.cpp | 27/27 | 100.0 | 18/19 | 94.7 | 0.0 | 0.0 | 100.0 | 0.0 |
| operations/search_helpers.cpp | 33/33 | 100.0 | 31/31 | 100.0 | 0.0 | 100.0 | 97.0 | 0.0 |
| operations/sender_context.cpp | 57/60 | 95.0 | 30/35 | 85.7 | 0.0 | 95.0 | 95.0 | 90.0 |
| operations/sender_single_file_context.cpp | 236/254 | 92.9 | 133/157 | 84.7 | 0.0 | 92.9 | 87.0 | 78.3 |
| operations/version_operation.cpp | 0/6 | 0.0 | 0/3 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| util/byte_range.cpp | 18/18 | 100.0 | 16/16 | 100.0 | 100.0 | 83.3 | 83.3 | 83.3 |
| util/command_line_helpers.cpp | 22/26 | 84.6 | 14/18 | 77.8 | 0.0 | 0.0 | 84.6 | 84.6 |
| util/crc_helpers.cpp | 4/4 | 100.0 | 0/0 | - | 100.0 | 100.0 | 100.0 | 100.0 |
| util/ctime_timestamp.cpp | 26/30 | 86.7 | 11/16 | 68.8 | 6.7 | 86.7 | 86.7 | 56.7 |
| util/ctime_timestamp_range.cpp | 23/25 | 92.0 | 16/20 | 80.0 | 0.0 | 92.0 | 84.0 | 84.0 |
| util/file_operations_helpers.cpp | 39/54 | 72.2 | 40/66 | 60.6 | 50.0 | 59.3 | 63.0 | 59.3 |
| util/hex_value.cpp | 47/52 | 90.4 | 19/22 | 86.4 | 0.0 | 90.4 | 71.2 | 0.0 |
| util/native_file_operations_helpers_posix.cpp | 6/14 | 42.9 | 4/10 | 40.0 | 0.0 | 42.9 | 42.9 | 42.9 |
| util/semantic_version.cpp | 27/28 | 96.4 | 26/27 | 96.3 | 78.6 | 96.4 | 78.6 | 78.6 |
| util/timestamp_helpers.cpp | 10/10 | 100.0 | 4/4 | 100.0 | 60.0 | 100.0 | 40.0 | 40.0 |

Notable moves against 2026-10-29 (combined base layers):
- Up: `indexed_event_block.cpp` 74.0 → 92.1 (the GTID-mode sender properties, N10), `s3_storage_backend.cpp` 78.6 → 82.1, `file_operations_helpers.cpp` 63.0 → 72.2, `byte_range.cpp` 83.3 → 100, `connection_context.cpp` 91.5 → 92.9 (garbled passwords, N8), `reader_context.cpp` 92.5 → 93.7. New: `source_ssl_config.cpp` 100.
- Down: `network_service.cpp` 95.1 → 89.8 (new TLS code reached only by the pull-tls extra layer; 94.0 with all layers). `byte_span_extractors.hpp`, `empty_body.cpp` and `heartbeat_log_v2_body_impl.cpp` are property-run noise.

Low-percentage but low-value: `footer.cpp`, `version_operation.cpp`, the `app.cpp` top-level catch, the `file_operations_helpers.cpp` / `native_file_operations_helpers_posix.cpp` I/O-error raises (disk-faults reaches some), and the `util/*.hpp` exception-location templates. The headers are in the totals but not in this table.
