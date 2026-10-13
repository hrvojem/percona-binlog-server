# Focus 6: Bug history and density

Commit: 99de682 (branch pbt-init). Sources: `git log` (upstream keys PS-/PBS-), `.tickets-snapshot.txt` (26 tickets), source code. Tickets are treated as leads. Each was checked against the code path it names.

## Findings

**F1. The storage layer is the main hotspot, both historically and today.** Since 2026-01 the most-changed source files are `storage.cpp` (32 commits), `app.cpp` (30), `storage.hpp` (24) and `s3_storage_backend.cpp` (16). Fixed upstream bugs in storage include PS-11136 (twice, checkpointing bypass), PS-69/3804c25 (checkpoint config dropped by refactor 7f8cb69), PS-10320 (no flush on read_timeout), PS-11002, PBS-27 (list during fetch), PBS-21 (recovery) and PBS-91. Of the 23 open tickets, 8 (6, 9, 13, 15, 18, 19, 24, 25) are storage consistency or crash bugs, and 6 of those are High priority.

**F2. The replication source (`sender_context`, `minimysql`) is the newest cluster.** Six commits in one week (a338dde, d3bc7fb, f0730b1 PBS-64, bb9a9c7 PBS-65, 0f56bd6 PBS-91) rewrote `sender_context.cpp`: 502 lines changed in a338dde and 190 in PBS-65. Four open tickets (7, 10, 20, 26) remain. Ticket 26 is a gap left by the PBS-91 fix. This code is unsettled and is the best regression target.

**F3. Recurring pattern: state is read once and never refreshed or recorded.** Ticket 2 (config never copied), ticket 9 (`load_binlog_index()` runs only in the constructor, storage_core.cpp:214, and the stale list is written back at :701) and ticket 25 (`storage_metadata.hpp:35-37` holds only version, mode and encryption).

**F4. Recurring pattern: multi-object updates are not atomic and recovery covers only one direction.** Examples:
- The new-binlog write order is magic, then metadata, then index (storage_core.cpp:680-701). This is ticket 15.
- `write_event_block` appends data, then increments size, then saves metadata (:307-321), and the caller erases its buffer only after success (storage.cpp:276-302). This is ticket 18.
- No data fsync is ever done (filesystem_storage_backend.cpp:229 TODO). This is ticket 19.
- Recovery only truncates a binlog that is longer than its metadata (storage_core.cpp:972-976).
- `purge_binlogs` commits the index first and then removes objects on a best-effort basis. Its own comments (storage_core.cpp:388-395, 405-409) say a failure there "leaves the storage in an inconsistent state ... that the constructor's validators will refuse to open". So a crash or fault in purge step 3 makes the storage unopenable by design. This is not ticketed. It needs a crash or fault, so it fits Antithesis well.

**F5. Recurring pattern: `assert`s and `noexcept` stand in for input validation.** Examples: indexed_event_block.cpp:43 (ticket 7), storage.cpp:138 (ticket 6), storage_core.cpp:219 (ticket 13), and `noexcept` on a throwing parser at format_description_post_header_impl.cpp:167 (ticket 1). Release (NDEBUG) and debug builds behave differently, so the Antithesis build type matters.

**F6. NEW (hypothesis, strong): starting `purge_binlogs` truncates the live binlog that fetch or pull is appending to.**
- Purging mode is not `querying_only`, so its constructor runs the full `load_and_validate_binlog_metadata_set`.
- If the last binlog's file is larger than its metadata size, the constructor "recovers" by calling `backend_->resize_object(...)` (storage_core.cpp:977).
- During fetch, `write_event_block` appends data before it saves metadata (:307 then :321). A purge that starts inside that window truncates the writer's file.
- The writer's `ofstream` is opened `app|ate` (filesystem_storage_backend.cpp:201-205), so later writes go to the new end of file. The in-memory size is still the old size plus n, so the file is now permanently shorter than its metadata.
- Result: the next start fails with "size from the binlog metadata is bigger ..." or "does not match", and pull readers read past the end of the file.

Ticket 24 describes the same truncation for two streaming instances. Purge is a documented routine operation (README), which makes this worse. It is purely a timing bug, so it is an ideal Antithesis target.

**F7. NEW (hypothesis): purge or a second instance deletes another process's in-flight `.tmp` objects.** `remove_temporary_objects` (storage_core.cpp:597-629) removes every `*.tmp` in non-querying modes. Temp names are deterministic (`<name>.tmp`, basic_storage_backend_fwd.hpp:60; filesystem_storage_backend.cpp:141-157). If purge starts while fetch is inside `put_object` for `binlog.NNNNNN.json`, the fetch rename fails. That throws out of `save_binlog_metadata` after data was appended and the size was incremented, which is ticket 18's permanent-duplicate path, triggered by purge instead of ENOSPC. Filesystem backend only.

**F8. NEW (confirmed by reading): the pull listener does synchronous storage I/O on one `io_context` thread while holding the storage `shared_mutex`.**
- `handle_binlog_dump_command` (network_service.cpp:279-334) calls `sender_ctx.get_event()`. That call reaches `storage_core::fetch_event_block`, which holds a `shared_lock` (storage_core.cpp:461) across `backend_->get_object` and decryption.
- `pull_operation.cpp:150` runs everything with a single `io_ctx.run()`.
- Consequences:
  - A stalled S3 GET blocks every replica session.
  - It blocks the collector, whose `write_event_block` takes a `unique_lock` (:305), so the collector stops reading from the source.
  - It delays SIGTERM handling, which is posted on the same `io_ctx`.
- The same holds in reverse: a writer holds the `unique_lock` for a full S3 re-upload (ticket 17, s3_storage_backend.cpp:862), and the upload grows with binlog size.

These are liveness and timing issues, which Antithesis's network faults can drive.

**F9. NEW (confirmed by reading): ticket 1 can also be reached from pull's sender.** `sender_context::transform_fde_to_artificial` (sender_context.cpp:323-325) builds a `generic_post_header_impl<format_description>` from the stored FDE bytes. Its constructor calls the `noexcept` `get_encoded_server_version()` (format_description_post_header_impl.cpp:130). `fetch_fde_from_storage` checks only the type code and size (:238-253). A stored FDE with a corrupted version string, requested from a position other than 4, therefore aborts the whole pull process: all sessions and the collector, not just one session. This needs storage corruption, which is a fault-injection fit.

**F10. Regression check on 3804c25 (ticket 2 fix).** The fix is present (storage.cpp:54-63), but interval checkpointing is evaluated only when an event arrives (storage.cpp:178-199). The collector requests `MYSQL_RPL_SKIP_HEARTBEAT` (easymysql/connection.cpp:205). On an idle source, a committed transaction therefore stays in memory: pull readers cannot see it, and a crash loses it. It is flushed only by the next event or a read_timeout disconnect (collector_context.cpp:200). This is not a bug in the fix, but a bounded-staleness property should account for it.

**F11. Quiet components.**
- Encryption and keyring: an 8-part feature (PBS-39) with only ticket 8 open.
- The rewriter: a 5-part PS-11080 fix, no open ticket, and a passing property test. A regression target only for rewrite mode.
- `easymysql`: only PS-11054 and ticket 23.
- Purge victim selection: covered by a model property.

Encryption and the rewriter are quiet because they are well tested, not because they were never exercised. They are low priority for Antithesis.

## Ticket validation table

Fit: **A** = timing, crash or fault dependent (good Antithesis fit). **D** = deterministic input bug (deprioritise). **E** = environment or toolchain.

| # | Verdict | Discriminating code detail | Fit / notes |
|---|---|---|---|
| 1 | Confirmed | `get_encoded_server_version() const noexcept` (format_description_post_header_impl.cpp:166-169) calls `util::semantic_version`, which raises `invalid_argument` (semantic_version.cpp:42-63). Called from the ctor at :130. Not config: the code is unconditional. | D for fetch. A in pull via F9 (stored corruption). |
| 3 | Confirmed | `// TODO: add overflow checks` at gtid_set.cpp:211. `add_interval` validates only the lower bound (:622) before `upper+1` (:625). Decoder checks only `upper <= lower` (:768). | D |
| 4 | Partially | `ICL_USE_BOOST_MOVE_IMPLEMENTATION` is defined nowhere (CMakeLists.txt). The miscompile itself rests on the reporter's macOS repro and was not reproducible here. | E. Not relevant on a Linux/GCC Antithesis image. |
| 5 | Confirmed | Unchecked `resize(extracted_tag_length)` into a 32-byte `static_vector`: gtid_set.cpp:370 and gtid_tagged_log_body_impl.cpp:386. | D (error message quality only) |
| 6 | Confirmed (latent) | `close_binlog()` clears `event_buffer_` (storage.cpp:210) but not `incomplete_transaction_timestamps_`. Assert at :138. The only mid-transaction close path (collector_context.cpp:547) is preceded by a discard on disconnect (:195). | D, latent |
| 7 | Confirmed | `assert(event_size >= 19)`, then `offset += event_size` (indexed_event_block.cpp:43-51). It is reachable from the sender: `compute_fetch_size_requirements` returns `ready` when `size(buffer) >= first_event_size` (sender_context.cpp:364). | A (needs storage corruption) |
| 8 | Confirmed | `counter += offset / block_size` on the low 64 bits only (cipher_context.cpp:459-518). | D, probability about 2^-38 per GB |
| 9 | Confirmed | Index loaded only in the ctor (storage_core.cpp:214) and rewritten from the in-memory list (:701, :789-799). No cross-process notification. | A (purge timing against rotation). See also F6 and F7. |
| 10 | Confirmed | A single frame with length `size+1` (connection_context.cpp:348-350). `async_write_mysql_frame` rejects `>= max_payload_size` (network_io_operations.cpp:144-146). | D (payload size) |
| 11 | Confirmed | Uses `from_iso_extended_string` with no field range check (ctime_timestamp.cpp:34). | D |
| 12 | Confirmed | `root_path_ = uri.path()` with no NUL check (filesystem_storage_backend.cpp:91). | D, config-only |
| 13 | Confirmed | Unconditional `binlog_records_.front()` (storage_core.cpp:1001). The querying mode can erase every record (:949), and the assert at :219 does the same. | A-ish (needs outside damage). D otherwise. |
| 14 | Confirmed | On an artificial ROTATE naming a different binlog, the else branch calls `close_binlog()` with no synthesized ROTATE and no gap check (collector_context.cpp:543-551). | A (source rotates while PBS is disconnected or caught up) |
| 15 | Confirmed | Magic, then metadata, then index (storage_core.cpp:680-701). An unindexed object is rejected (:776-781). `remove_temporary_objects` handles only `.tmp` (:603). | **A, top** (kill at rotation, ENOSPC, S3 5xx) |
| 16 | Confirmed (code) | `configuration_{}` default ctor (s3_storage_backend.cpp:189, 196). No IMDS disable. The delay itself is environment-dependent. | E. Harness: set `AWS_EC2_METADATA_DISABLED=true`. |
| 17 | Confirmed | Each `do_write_data_to_stream` uploads the whole tmp file (s3_storage_backend.cpp:850-863, 933-938). | A as a liveness issue (combines with F8 lock hold) |
| 18 | Confirmed | Buffer erased only after `write_event_block` returns (storage.cpp:285-302). Size incremented before metadata save (storage_core.cpp:310, 321). Destructor re-flushes (storage.cpp:66-75). | **A, top** (fault at metadata save) |
| 19 | Confirmed | `// TODO: ... use fsync()` (filesystem_storage_backend.cpp:229-230). Metadata is fsynced (:147, :159). Shorter-than-metadata is rejected (storage_core.cpp:972). | A, but needs page-cache loss. Simulated power loss may not be available, so check the Antithesis fault model. |
| 20 | Confirmed | `known_queries` holds no `SELECT @@GLOBAL.SERVER_ID` and so on (network_service.cpp:503-550). `binlog_dump_gtid` is parsed (connection_context.cpp:446) but falls to `default:` "unknown command" (network_service.cpp:608-611). | D (feature gap). Blocks MySQL-replica workloads. |
| 23 | Confirmed | No `MYSQL_OPT_GET_SERVER_PUBLIC_KEY` or `MYSQL_SERVER_PUBLIC_KEY` anywhere in `src/easymysql`. Only SSL options are set (connection.cpp:228-253). It is the client library's documented behaviour, not the environment. | D. Harness: use `ssl.mode` preferred or required. |
| 24 | Confirmed | No `flock`, `O_EXCL` or lock file in `src/`. Fixed `.tmp` suffix (basic_storage_backend_fwd.hpp:60). Shared truncation through recovery (storage_core.cpp:977). | **A, top** (two writers) |
| 25 | Confirmed | `storage_metadata` fields are only `version`, `mode`, `encryption` (storage_metadata.hpp:35-37). | A (source reset or failover during operation) |
| 26 | Confirmed | `fetch_event_block` returns `true` with an empty buffer on empty storage (storage_core.cpp:474-478). `fetch_fde_from_storage` then returns `false` on `size < 19` (sender_context.cpp:231-233). | **A** (client connects before the first ROTATE is stored). A PBS-91 regression gap. |

Fixed tickets 2, 21 and 22 were re-checked. The ticket 2 block is present at storage.cpp:54-63. The sender now takes the name and position at sender_context.cpp:61-72, and artificial ROTATE and FDE are generated at :557-587.

## Areas examined

- Git history: 384 commits, fix and feature commits since 2024, and per-file churn since 2026-01.
- `storage.cpp`, `storage_core.cpp` (ctor, open, write, close, purge, fetch, index and metadata load), `filesystem_storage_backend.cpp`, `s3_storage_backend.cpp` (context, stream, upload).
- `sender_context.cpp` (whole file), `indexed_event_block.cpp`, `network_service.cpp` (dump handler, known queries), `connection_context.cpp:343-359`, `pull_operation.cpp`.
- `collector_context.cpp` (receive loop, artificial ROTATE).
- `format_description_post_header_impl.cpp`, `semantic_version.cpp`, `gtid_set.cpp`, `gtid_tagged_log_body_impl.cpp`, `cipher_context.cpp`, `ctime_timestamp.cpp`, `storage_metadata.hpp`, `easymysql/connection.cpp`.
- `tests/PROPERTY_TESTS.md`.

## Assumptions

- Upstream fix commits reflect bug density. Ticket counts reflect where QA looked, which biases toward storage.
- F6 and F7 are derived from code order and the semantics of `app` mode and `rename`. They were not executed.
- Antithesis can supply process kill, network faults and S3-proxy faults. Page-cache loss for ticket 19 is assumed not to be available.

## Open questions

- [open] Can F6 (purge truncating the live binlog) be reproduced with fetch writing plus purge in a loop? This would extend ticket 24 to the purge process.
- [open] Does `event_view` construction in `extract_fields_from_fde` also touch the server version? F9 already holds through `transform_fde_to_artificial`.
- [open] Should the Antithesis build be NDEBUG (release presets) or debug? Tickets 6, 7 and 13 change from an abort to a hang or undefined behaviour depending on this.
- [open] Is the single-threaded `io_ctx` (F8) intended? Is there a requirement on replica session isolation or shutdown latency?
