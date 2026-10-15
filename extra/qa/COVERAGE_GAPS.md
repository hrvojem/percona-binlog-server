# Coverage gaps

- **Date:** 2026-10-14
- **Commit:** `2ebba09655d41e8cb82899089c32b68a39091d9e` (branch `pbt-init`), plus the uncommitted working tree at that time. That tree had test-only changes, including the new `tests/replication_source_property_test.cpp`. `src/` was identical to HEAD.
- **Host output (kept on the QA host only):** `pbs-infra:~/ws/coverage-20261014/`
  - `html/index.html`: combined line/branch HTML report.
  - `combined.json`: merged gcovr tracefile.
  - `layers/*.json`: one tracefile per layer.
  - `summary-*.json`: per-layer and combined summaries.
  - `logs/`: test output.
  - `run.sh`, `property_per_case.sh`: the scripts that produced the data.

## How it was measured

1. **Build.** `~/ws/build_coverage.sh` (preset `debug_gcc14`, `-DWITH_PROPERTY_TESTS=ON`, `--coverage -fprofile-update=atomic`) built into `~/ws/percona-binlog-server-build-coverage_gcc14`.
   - md5 sums of `src/` were taken before the build and after the last run, and they match.
   - A first attempt was discarded. Another agent had briefly synced a different `network_service.cpp` during that build, which caused a gcovr MD5 merge error.
2. **Capture.** The `.gcda` counters were reset before each layer. Each layer was captured with gcovr 8.6 using the same flags as `~/ws/run_coverage.sh`: `--filter src/ --exclude-unreachable-branches --exclude-throw-branches`.
3. **Layers.**

| Layer | What ran |
|---|---|
| unit | `ctest -E _property_test$` (10 Boost unit tests) |
| property | All 14 Hegel property programs (97 cases) at default Hegel settings, with `PBS_TEST_S3_*` set so the `...OnS3` variants ran against the local versitygw. Each Boost case ran in its own process (see note below). 21 cases fail. 20 of them are in `known_failures.txt`. The other one is the new `ReplicationSourceMatchesStatementsExactlyOrByPrefix`, which is still being written. |
| mtr-8.4 | `binlog_streaming` suite (69 tests) on MySQL 8.4.8, filesystem storage, `--build-thread=600`. 12 tests fail, all listed in `mtr_known_failures.txt`. |
| qa | `pull_replica_test.py` (position + gtid), `query_race_test.py --runs 200` (position + gtid), and `fetch_crash_test.py --examples 5 --steps 8` (gtid + position), all against 8.4.8. Failures are known: ticket 20 (gtid pull_replica) and ticket 29 (query_race). |

> **Note: one crash disables the rest of a property program.** When the whole `storage_property_test` binary runs under ctest, the caught SIGSEGV in `StorageOpeningOfDamagedFilesFailsCleanly` (ticket 13) makes every later property in that program return immediately and "pass". That includes all 7 `...OnS3` variants. In that run `s3_storage_backend.cpp` showed 47% instead of 79%, and the known failures for tickets 6, 28 and 29 did not fire.
>
> `nightly/ci_property_tests.sh` (CI) originally ran whole binaries and had the same blind spot. It now runs one process per case, like the nightly (fixed after this measurement; 70 cases pass, 27 known failures skipped, 37 s with 8 jobs on the QA host).

## Totals (all `src/` files, headers included)

| Layer | Lines | Line % | Branches | Branch % | Functions % |
|---|---|---|---|---|---|
| unit | 2867 / 10993 | 26.1 | 1112 / 6120 | 18.2 | 24.6 |
| property | 7217 / 10993 | 65.7 | 3369 / 6120 | 55.0 | 66.3 |
| mtr-8.4 | 7557 / 10993 | 68.7 | 3597 / 6120 | 58.8 | 70.8 |
| qa | 5211 / 10993 | 47.4 | 2438 / 6120 | 39.8 | 50.3 |
| **combined** | **9575 / 10993** | **87.1** | **4666 / 6120** | **76.2** | **87.8** |

**Not in this measurement:**
- MTR with S3 storage. In the MTR, qa and unit layers, `s3_storage_backend.cpp` is at 0%; all of its coverage comes from the property layer.
- MTR on 8.0 and 9.7.
- `s3_fault_sweep.py`, `disk_fault_sweep.py`, `listener_fuzz_test.py`, `network_fault_test.py` and the other harnesses.
- Antithesis runs.

Some of the gaps listed below may already be reached by those layers. The existing `~/ws/run_coverage.sh` runs them.

## Top gaps

Coverage is the combined figure. "Test kind" is the cheapest kind of test that would reach the code.

| # | Where (file:lines) | What is not exercised | Test kind |
|---|---|---|---|
| 1 | `easymysql/connection.cpp:556-558` → `operations/pull_operation.cpp:154-156,165` | `fetch_binlog_event` gets a fetch error other than CR_SERVER_LOST (e.g. ER 1236 "binlog purged on source"). It raises, the operation thread catches it, and `pull` exits. This whole fatal path in pull mode never runs. | MTR: purge the source's binlogs while PBS is down, then restart `pull` **Addressed (2026-10-15): MTR `pull_source_purged_past_resume_point` (pull twice + fetch exit 1 with ER 1236 text, storage unchanged, `list` works; passes on 8.0.45/8.4.8/9.7.2).** |
| 2 | `binsrv/s3_storage_backend.cpp:765-800` (`do_resize_object`) | S3 crash-recovery truncation. On S3 the storage never reopens with metadata size < object size, while the filesystem backend does (242 times, `storage_core.cpp:979`). | QA harness: `fetch_crash_test.py --storage s3` (or S3 MTR); Antithesis S3 kill |
| 3 | `s3_storage_backend.cpp:449-483` | ListObjectsV2 pagination: continuation token, empty-token error. Never more than 1000 objects. | Property (S3 variant with >1000 binlogs, or a small max-keys) / QA `s3_upload_volume.py` |
| 4 | `s3_storage_backend.cpp:383,409,512,283-312,347-358` | Error outcomes of put / delete / get, and size mismatches on download. | QA `s3_fault_sweep.py` (not in this run); Antithesis S3 faults |
| 5 | `binsrv/storage_core.cpp:421-430` | `purge_binlogs` when `remove_objects` fails, giving the "warning" result. | Property: backend fault injection in `purge_selection`/storage property test **Addressed (2026-10-15): property `StoragePurgeSurvivesAFault` (+`OnS3`); fails today (failed removal → unopenable storage, lead N3, no ticket yet).** |
| 6 | `storage_core.cpp:637,642,659,812,822,895,900`; `binsrv/file_keyring.cpp:39-77`; `binsrv/encryption_config.cpp:31,37`; `binsrv/keyring_factory.cpp:34,43` | Encryption and keyring errors: KEK missing from the keyring, binlog metadata encrypted but no keyring configured, encryption format changed, non-CTR cipher, bad keyring URIs, missing or non-regular key file. | Property (`config_value_property_test` model of keyring/encryption configs) + one MTR for "metadata encrypted, keyring removed" **Addressed (2026-10-15): property `KeyringUriMatchesModel` (file_keyring/keyring_factory URI checks; the non-CTR cipher is the existing `encryption_cipher` defect of `ConfigFileMatchesModel`) and MTR `encryption_keyring_removed` (KEK missing from the keyring, no keyring section, encryption KEK missing; fetch and list fail cleanly, storage unchanged; passes on 8.0/8.4/9.7).** |
| 7 | `operations/sender_context.cpp:227,240,247,252,362,473,537` | Validation of a stored FDE when the dump starts from an offset; event too large; non-magic offset with an empty binlog name (COM_BINLOG_DUMP `""`, pos ≠ 4). | Property (sender_context: damaged storage, empty name + offset); QA `listener_fuzz_test.py` |
| 8 | `minimysql/network_service.cpp:245-279` (`handle_exception`) | Session errors: timeout, operation_aborted, eof/connection_reset, `std::exception`, and unknown exceptions. Clients in these layers always quit cleanly. | QA `listener_fuzz_test.py` / `network_fault_test.py`; Antithesis network partition |
| 9 | `minimysql/connection_context.cpp:394-413` | COM_RESET_CONNECTION and COM_CHANGE_USER decoding; `generate_encoded_access_denied` (320-323) and `generate_encoded_full_auth` (256-259) are never called. | Property (minimysql frame generator) / QA `listener_fuzz_test.py` |
| 10 | `binsrv/replication_source_config.cpp:34,42,46,62,72` | UUID length and separator rejection; `read_timeout`/`write_timeout` = 0. Port = 0 and non-hex UUIDs are covered. | Property: `replication_source_property_test` / `config_value_property_test` |
| 11 | `binsrv/events/reader_context.cpp:535,539,553,562,571,625-649,701-997` (about 30 single-line raises) | Rejection of malformed streams: an artificial ROTATE with a non-zero timestamp or next-position, an artificial FDE, a wrong binlog version or header length, an FDE version newer than the server, and so on. | Property: `reader_context_property_test` with mutated events **Addressed (2026-10-15): property `ReaderContextRejectsTargetedDefectsCleanly` (18 single-event defects, each asserted to be rejected at the defective event with its own message; passes).** |
| 12 | `operations/collector_context.cpp:155,530,563`; `storage_core.cpp:276,281,713-718` | Unexpected packet prefix. In rewrite mode, an artificial ROTATE whose position mismatches the storage. `opened_empty` reopen: `storage_core.cpp:280` raises first, so lines 713-718 look unreachable. | MTR for 530 (rewrite + source restart); 155 needs a fake source (QA, using minimysql as the source) |

**Checked and adequately covered** (combined %; more than one layer reaches each):
- **PBS-90 handshake handlers** in `network_service.cpp:499-676`: every SET/SELECT handler runs, and only lambda-exit lines are uncovered.
- **COM_REGISTER_SLAVE** in `connection_context.cpp:424-436` (241 hits) and the `register_replica` reply in `network_service.cpp:768-788`.
- **`select_purge_victim_count` / `purge_binlogs`**, apart from gap 5.
- **`sender_context` file switch.** The known tickets 26 and 27 make it fail, but the code runs.
- **`collector_context` case (3) EOF** at `collector_context.cpp:184-186`: 207 hits (MTR 186, qa 21; the property layer has 0, as no in-process collector test exists). The reconnect and timeout case (2) at 195-204 has 13 hits.
- **Rewrite mode:** `rewriter.cpp` at 96% lines.

Known-unreachable code that should not drive new tests:
- **AEAD tags** in `storage_core.cpp:87,119-121,1150-1151`: only CTR is accepted (`encryption_config.cpp:35`).
- **`cipher_context.cpp:73-84`:** cipher-mode mapping.
- **`easymysql/connection.cpp:240-318`:** SSL option setters. Optional; an MTR with TLS would reach them.

## Per-source numbers (src/*.cpp, combined)

The last four columns are line % per layer.

| File | Lines | L% | Branches | B% | unit | prop | mtr | qa |
|---|---|---|---|---|---|---|---|---|
| app.cpp | 7/14 | 50.0 | 5/29 | 17.2 | 0.0 | 0.0 | 50.0 | 50.0 |
| binsrv/authentication_config.cpp | 6/8 | 75.0 | 4/6 | 66.7 | 0.0 | 75.0 | 62.5 | 62.5 |
| binsrv/basic_keyring.cpp | 7/8 | 87.5 | 0/0 | - | 0.0 | 62.5 | 87.5 | 0.0 |
| binsrv/basic_logger.cpp | 21/22 | 95.5 | 13/13 | 100.0 | 0.0 | 95.5 | 95.5 | 95.5 |
| binsrv/basic_storage_backend.cpp | 35/47 | 74.5 | 8/14 | 57.1 | 0.0 | 68.1 | 74.5 | 74.5 |
| binsrv/binlog_file_metadata.cpp | 16/17 | 94.1 | 6/7 | 85.7 | 0.0 | 94.1 | 94.1 | 94.1 |
| binsrv/cout_logger.cpp | 4/4 | 100.0 | 0/0 | - | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/encryption_config.cpp | 4/6 | 66.7 | 2/4 | 50.0 | 0.0 | 66.7 | 66.7 | 0.0 |
| binsrv/events/common_header.cpp | 42/47 | 89.4 | 24/25 | 96.0 | 74.5 | 89.4 | 89.4 | 85.1 |
| binsrv/events/common_header_view.cpp | 77/85 | 90.6 | 7/22 | 31.8 | 77.6 | 88.2 | 89.4 | 89.4 |
| binsrv/events/composite_binlog_name.cpp | 30/33 | 90.9 | 21/24 | 87.5 | 45.5 | 90.9 | 81.8 | 78.8 |
| binsrv/events/empty_body.cpp | 5/6 | 83.3 | 1/2 | 50.0 | 0.0 | 83.3 | 83.3 | 0.0 |
| binsrv/events/empty_post_header.cpp | 5/6 | 83.3 | 1/2 | 50.0 | 50.0 | 83.3 | 83.3 | 83.3 |
| binsrv/events/event.cpp | 119/131 | 90.8 | 38/47 | 80.9 | 68.7 | 89.3 | 75.6 | 58.8 |
| binsrv/events/event_view.cpp | 56/58 | 96.6 | 38/40 | 95.0 | 69.0 | 86.2 | 81.0 | 69.0 |
| binsrv/events/footer.cpp | 4/12 | 33.3 | 2/6 | 33.3 | 16.7 | 33.3 | 33.3 | 33.3 |
| binsrv/events/footer_view.cpp | 19/22 | 86.4 | 7/10 | 70.0 | 59.1 | 86.4 | 86.4 | 86.4 |
| binsrv/events/format_description_body_impl.cpp | 24/26 | 92.3 | 7/10 | 70.0 | 73.1 | 92.3 | 88.5 | 61.5 |
| binsrv/events/format_description_post_header_impl.cpp | 72/82 | 87.8 | 20/25 | 80.0 | 70.7 | 87.8 | 86.6 | 50.0 |
| binsrv/events/gtid_log_body.cpp | 101/110 | 91.8 | 67/76 | 88.2 | 61.8 | 91.8 | 29.1 | 29.1 |
| binsrv/events/gtid_log_post_header.cpp | 45/48 | 93.8 | 20/23 | 87.0 | 54.2 | 93.8 | 81.2 | 62.5 |
| binsrv/events/gtid_tagged_log_body_impl.cpp | 267/293 | 91.1 | 121/152 | 79.6 | 74.1 | 91.1 | 37.5 | 0.0 |
| binsrv/events/previous_gtids_log_body_impl.cpp | 25/26 | 96.2 | 9/10 | 90.0 | 73.1 | 96.2 | 96.2 | 53.8 |
| binsrv/events/protocol_traits.cpp | 30/41 | 73.2 | 24/39 | 61.5 | 39.0 | 73.2 | 73.2 | 73.2 |
| binsrv/events/reader_context.cpp | 225/255 | 88.2 | 161/196 | 82.1 | 38.8 | 85.1 | 85.9 | 78.4 |
| binsrv/events/rewriter.cpp | 127/132 | 96.2 | 58/74 | 78.4 | 33.3 | 96.2 | 44.7 | 0.0 |
| binsrv/events/rotate_body_impl.cpp | 20/21 | 95.2 | 5/6 | 83.3 | 76.2 | 95.2 | 95.2 | 95.2 |
| binsrv/events/rotate_post_header_impl.cpp | 12/14 | 85.7 | 2/4 | 50.0 | 71.4 | 85.7 | 85.7 | 85.7 |
| binsrv/events/unknown_body.cpp | 2/2 | 100.0 | 0/0 | - | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/events/unknown_post_header.cpp | 2/2 | 100.0 | 0/0 | - | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/exception_handling_helpers.cpp | 19/27 | 70.4 | 10/20 | 50.0 | 0.0 | 0.0 | 70.4 | 0.0 |
| binsrv/file_keyring.cpp | 24/33 | 72.7 | 23/42 | 54.8 | 0.0 | 66.7 | 72.7 | 0.0 |
| binsrv/file_logger.cpp | 8/10 | 80.0 | 3/7 | 42.9 | 0.0 | 0.0 | 80.0 | 80.0 |
| binsrv/filesystem_storage_backend.cpp | 93/115 | 80.9 | 73/110 | 66.4 | 0.0 | 73.0 | 80.0 | 80.9 |
| binsrv/gtids/gtid.cpp | 18/19 | 94.7 | 20/20 | 100.0 | 84.2 | 73.7 | 89.5 | 73.7 |
| binsrv/gtids/gtid_set.cpp | 377/399 | 94.5 | 231/256 | 90.2 | 89.7 | 94.0 | 73.2 | 59.4 |
| binsrv/gtids/tag.cpp | 30/34 | 88.2 | 17/19 | 89.5 | 88.2 | 76.5 | 47.1 | 14.7 |
| binsrv/gtids/uuid.cpp | 24/25 | 96.0 | 5/7 | 71.4 | 96.0 | 84.0 | 56.0 | 56.0 |
| binsrv/indexed_event_block.cpp | 17/17 | 100.0 | 8/8 | 100.0 | 0.0 | 100.0 | 100.0 | 94.1 |
| binsrv/keyring_factory.cpp | 6/8 | 75.0 | 4/6 | 66.7 | 0.0 | 75.0 | 75.0 | 0.0 |
| binsrv/keyring_record.cpp | 11/11 | 100.0 | 8/8 | 100.0 | 0.0 | 100.0 | 63.6 | 0.0 |
| binsrv/keyring_record_collection.cpp | 41/42 | 97.6 | 28/28 | 100.0 | 0.0 | 97.6 | 88.1 | 0.0 |
| binsrv/logger_factory.cpp | 5/5 | 100.0 | 4/4 | 100.0 | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/main_config.cpp | 14/14 | 100.0 | 8/8 | 100.0 | 0.0 | 100.0 | 92.9 | 92.9 |
| binsrv/models/error_response.cpp | 8/8 | 100.0 | 3/3 | 100.0 | 0.0 | 0.0 | 100.0 | 100.0 |
| binsrv/models/search_response.cpp | 12/20 | 60.0 | 2/3 | 66.7 | 0.0 | 0.0 | 60.0 | 60.0 |
| binsrv/replication_config.cpp | 6/7 | 85.7 | 3/4 | 75.0 | 0.0 | 85.7 | 85.7 | 57.1 |
| binsrv/replication_source_config.cpp | 19/23 | 82.6 | 22/26 | 84.6 | 0.0 | 82.6 | 69.6 | 69.6 |
| binsrv/rewrite_config.cpp | 6/6 | 100.0 | 5/5 | 100.0 | 0.0 | 100.0 | 50.0 | 0.0 |
| binsrv/s3_error.cpp | 7/13 | 53.8 | 4/5 | 80.0 | 0.0 | 53.8 | 0.0 | 0.0 |
| binsrv/s3_error_helpers_private.cpp | 20/20 | 100.0 | 14/15 | 93.3 | 0.0 | 100.0 | 0.0 | 0.0 |
| binsrv/s3_storage_backend.cpp | 339/427 | 79.4 | 236/337 | 70.0 | 0.0 | 79.4 | 0.0 | 0.0 |
| binsrv/size_unit.cpp | 35/39 | 89.7 | 21/26 | 80.8 | 0.0 | 89.7 | 69.2 | 66.7 |
| binsrv/storage.cpp | 142/143 | 99.3 | 57/64 | 89.1 | 0.0 | 80.4 | 97.9 | 93.7 |
| binsrv/storage_backend_factory.cpp | 12/22 | 54.5 | 6/12 | 50.0 | 0.0 | 54.5 | 54.5 | 54.5 |
| binsrv/storage_config.cpp | 11/11 | 100.0 | 7/7 | 100.0 | 0.0 | 100.0 | 90.9 | 81.8 |
| binsrv/storage_core.cpp | 502/552 | 90.9 | 404/491 | 82.3 | 0.0 | 85.9 | 87.3 | 58.5 |
| binsrv/storage_metadata.cpp | 15/16 | 93.8 | 6/7 | 85.7 | 0.0 | 93.8 | 93.8 | 93.8 |
| binsrv/time_unit.cpp | 33/40 | 82.5 | 19/26 | 73.1 | 0.0 | 82.5 | 67.5 | 67.5 |
| easymysql/connection.cpp | 223/268 | 83.2 | 85/139 | 61.2 | 0.0 | 0.0 | 83.2 | 57.5 |
| easymysql/connection_config.cpp | 21/27 | 77.8 | 20/36 | 55.6 | 0.0 | 29.6 | 77.8 | 77.8 |
| easymysql/core_error.cpp | 7/13 | 53.8 | 4/5 | 80.0 | 0.0 | 0.0 | 53.8 | 0.0 |
| easymysql/core_error_helpers_private.cpp | 12/12 | 100.0 | 6/7 | 85.7 | 0.0 | 0.0 | 100.0 | 0.0 |
| easymysql/library.cpp | 8/12 | 66.7 | 1/2 | 50.0 | 0.0 | 0.0 | 66.7 | 66.7 |
| minimysql/caching_sha2_password_authenticator.cpp | 13/14 | 92.9 | 10/11 | 90.9 | 0.0 | 92.9 | 92.9 | 92.9 |
| minimysql/connection_context.cpp | 315/365 | 86.3 | 170/222 | 76.6 | 0.0 | 78.6 | 75.9 | 68.5 |
| minimysql/network_io_operations.cpp | 6/6 | 100.0 | 3/3 | 100.0 | 0.0 | 100.0 | 100.0 | 100.0 |
| minimysql/network_service.cpp | 163/187 | 87.2 | 97/154 | 63.0 | 0.0 | 81.3 | 81.8 | 67.4 |
| opensslpp/cipher_context.cpp | 211/247 | 85.4 | 107/150 | 71.3 | 82.2 | 72.5 | 70.4 | 0.0 |
| opensslpp/core_error.cpp | 23/23 | 100.0 | 22/26 | 84.6 | 100.0 | 0.0 | 0.0 | 0.0 |
| opensslpp/crypto_rng.cpp | 7/9 | 77.8 | 4/6 | 66.7 | 77.8 | 66.7 | 66.7 | 66.7 |
| opensslpp/digest_context.cpp | 46/57 | 80.7 | 22/36 | 61.1 | 80.7 | 78.9 | 78.9 | 78.9 |
| opensslpp/evp_pkey.cpp | 187/216 | 86.6 | 109/152 | 71.7 | 86.6 | 0.0 | 0.0 | 0.0 |
| operations/basic_operation.cpp | 6/9 | 66.7 | 2/7 | 28.6 | 0.0 | 0.0 | 66.7 | 66.7 |
| operations/collector_context.cpp | 226/231 | 97.8 | 207/219 | 94.5 | 0.0 | 0.0 | 97.8 | 66.2 |
| operations/event_generation_helpers.cpp | 35/36 | 97.2 | 19/20 | 95.0 | 0.0 | 97.2 | 94.4 | 27.8 |
| operations/fetch_operation.cpp | 38/40 | 95.0 | 20/22 | 90.9 | 0.0 | 0.0 | 95.0 | 80.0 |
| operations/list_operation.cpp | 22/22 | 100.0 | 14/15 | 93.3 | 0.0 | 0.0 | 100.0 | 100.0 |
| operations/logger_helpers.cpp | 306/312 | 98.1 | 205/233 | 88.0 | 0.0 | 0.0 | 98.1 | 54.5 |
| operations/model_helpers.cpp | 12/12 | 100.0 | 10/20 | 50.0 | 0.0 | 0.0 | 100.0 | 91.7 |
| operations/operation_factory.cpp | 14/18 | 77.8 | 6/9 | 66.7 | 0.0 | 0.0 | 77.8 | 61.1 |
| operations/pull_operation.cpp | 46/53 | 86.8 | 32/35 | 91.4 | 0.0 | 0.0 | 86.8 | 84.9 |
| operations/purge_binlogs_operation.cpp | 25/27 | 92.6 | 16/19 | 84.2 | 0.0 | 0.0 | 92.6 | 0.0 |
| operations/search_by_gtid_set_operation.cpp | 25/25 | 100.0 | 16/17 | 94.1 | 0.0 | 0.0 | 100.0 | 0.0 |
| operations/search_by_timestamp_operation.cpp | 27/27 | 100.0 | 18/19 | 94.7 | 0.0 | 0.0 | 100.0 | 0.0 |
| operations/search_helpers.cpp | 33/33 | 100.0 | 31/31 | 100.0 | 0.0 | 100.0 | 97.0 | 0.0 |
| operations/sender_context.cpp | 197/209 | 94.3 | 131/153 | 85.6 | 0.0 | 94.3 | 90.0 | 61.2 |
| operations/version_operation.cpp | 0/6 | 0.0 | 0/3 | 0.0 | 0.0 | 0.0 | 0.0 | 0.0 |
| util/byte_range.cpp | 15/18 | 83.3 | 10/16 | 62.5 | 0.0 | 83.3 | 83.3 | 83.3 |
| util/command_line_helpers.cpp | 22/26 | 84.6 | 14/18 | 77.8 | 0.0 | 0.0 | 84.6 | 84.6 |
| util/crc_helpers.cpp | 4/4 | 100.0 | 0/0 | - | 100.0 | 100.0 | 100.0 | 100.0 |
| util/ctime_timestamp.cpp | 26/30 | 86.7 | 11/16 | 68.8 | 6.7 | 86.7 | 86.7 | 56.7 |
| util/ctime_timestamp_range.cpp | 23/25 | 92.0 | 16/20 | 80.0 | 0.0 | 92.0 | 84.0 | 84.0 |
| util/file_operations_helpers.cpp | 35/54 | 64.8 | 35/66 | 53.0 | 0.0 | 61.1 | 64.8 | 61.1 |
| util/hex_value.cpp | 46/52 | 88.5 | 18/22 | 81.8 | 0.0 | 88.5 | 71.2 | 0.0 |
| util/native_file_operations_helpers_posix.cpp | 6/14 | 42.9 | 4/10 | 40.0 | 0.0 | 42.9 | 42.9 | 42.9 |
| util/semantic_version.cpp | 27/28 | 96.4 | 26/27 | 96.3 | 78.6 | 96.4 | 78.6 | 78.6 |
| util/timestamp_helpers.cpp | 10/10 | 100.0 | 4/4 | 100.0 | 40.0 | 100.0 | 40.0 | 40.0 |

Low-percentage but low-value: `footer.cpp`, `version_operation.cpp`, `app.cpp` top-level catch, `file_operations_helpers.cpp` / `native_file_operations_helpers_posix.cpp` I/O-error raises (reachable with the `fsfault` LD_PRELOAD in `disk_fault_sweep.py`), and the `util/*.hpp` exception-location templates. The headers are in the totals but not in this table.
