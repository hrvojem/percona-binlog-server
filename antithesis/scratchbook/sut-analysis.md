---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server (local snapshot antithesis/scratchbook/.tickets-snapshot.txt); open tickets used as leads for failure-prone areas, each validated against the code before use
---

# SUT Analysis: Percona Binlog Server (binlog_server)

Synthesised from twelve discovery passes (`.discovery/focus-01` … `focus-12`). Provenance is given as "(focus N, M)". "Confirmed in code" means an agent (or this synthesis) traced the mechanism to a file:line; nothing here was executed. Guarantees are written as claims to test, never as facts.

## Summary

`binlog_server` is a single C++ binary run as one of six CLI modes (`src/app.cpp:25-48`, `operation_factory.cpp:43-82`). Two modes write: `fetch` (copy the MySQL source's binlogs until EOF, then exit) and `pull` (long-lived: a collector thread keeps reconnecting to the source, and a home-grown MySQL-protocol "replication source" serves the stored binlogs to downstream `mysqlbinlog`/replica clients). Four modes read or administer: `list`, `search_by_timestamp`, `search_by_gtid_set`, `purge_binlogs`. Storage is a flat object namespace on a local directory or an S3 bucket, optionally AES-CTR encrypted, optionally re-split ("rewrite", GTID mode only). There is no daemon cluster, no consensus and no cross-process lock. The distributed system is: MySQL source → PBS writer process → storage ← other PBS processes (query/purge), plus pull → downstream clients (focus 1, 10).

The highest-value Antithesis surface is **crash- and fault-consistency of multi-object storage updates**: rotation (magic → `.json` → `binlog.index`), checkpoint writes (data → in-memory size → `.json`), purge (index → best-effort deletes) and startup recovery (tail truncation, `.tmp` deletion). These run in every mode, including purge. Second is **pull's single io thread**, which does synchronous storage I/O under a storage-wide `shared_mutex`. Third is **error classification**: only errno 2013 is retried mid-stream, and anything else ends pull and its replication source. Most open tickets in the target components were confirmed in code. Several new timing-dependent leads have no ticket (purge truncating a live writer, `.tmp` deletion by a concurrent purge, the sender skipping the artificial ROTATE at a file switch, the list-vs-rename stat race, listener death on EMFILE).

## Architecture & data flow

**Modes and storage construction.** `fetch`/`pull` build storage in `streaming` mode (`fetch_operation.cpp:80-81`, `pull_operation.cpp:109-110`). `list`/`search_*` use `querying_only` (`list_operation.cpp:51`, `search_by_*_operation.cpp:57/60`). `purge_binlogs` uses `purging` (`purge_binlogs_operation.cpp:57`). Query and purge modes print one JSON object with `status` ∈ {success, warning, error} (`purge_binlogs_operation.cpp:66-86`). Exit contract: `fetch` exits 0 only on reaching EOF (`fetch_operation.cpp:107-115`), and `pull` exits 0 only after a signal (focus 1, 10).

**Ingress (source → storage), shared by fetch and pull.**
1. `collector_context::receive_binlog_events` (`collector_context.cpp:125-205`) connects with libmysqlclient. `open_connection_and_switch_to_replication` (`:207-272`) then issues COM_BINLOG_DUMP_GTID (GTID set = tail `previous_gtids ∪ added_gtids`; on an empty storage it reads `SELECT @@GLOBAL.gtid_purged`, `:229-240`) or a position dump (tail name + `size`, `:253-261`). Dump flags always include `MYSQL_RPL_SKIP_HEARTBEAT` (`connection.cpp:205`). PBS sets no heartbeat period and no replica UUID.
2. Each packet is parsed by `reader_context`. Its FSM is ROTATE(artificial) → FDE → PREVIOUS_GTIDS? → (GTID …)* → ROTATE|STOP, with 32 `logic_error` raise sites. It enforces `next_event_position == position + size` (`reader_context.cpp:915-920`) and, in position mode only, artificial-ROTATE position == stored position (`:547-556`).
3. The optional rewrite path regenerates ROTATE/FDE/PREVIOUS_GTIDS and recomputes positions and CRCs (`collector_context.cpp:274-429`).
4. `process_binlog_event` (`:431-491`): an artificial ROTATE opens or reuses a binlog (`:493-572`); a real ROTATE/STOP closes it (flush).
5. The `storage` facade buffers events in RAM and flushes only up to the last transaction boundary (`storage.cpp:276-302`). Flushes happen at a size/interval checkpoint (evaluated only inside `write_event`, `storage.cpp:175-200`), on close, on a timeout disconnect (`collector_context.cpp:195-200`), and in `~storage` (`storage.cpp:68-77`).
6. `storage_core::write_event_block` (`storage_core.cpp:299-322`) holds a `unique_lock` while it encrypts and appends to the backend stream, bumps the in-memory `size`, GTIDs and timestamps, and then calls `save_binlog_metadata` (focus 1, 2, 3).

**Egress (pull's replication source).** `minimysql::network_service` listens on `replication_source.port` (IPv4 any, no TLS, `network_service.cpp:680`). All sessions are coroutines on the **single** `io_ctx.run()` thread, which also hosts the SIGINT/SIGTERM `signal_set` (`pull_operation.cpp:98-104`). `handle_binlog_dump_command` (`network_service.cpp:279-334`) reads in 1 MiB blocks. A blocking dump polls storage every 500 ms at EOF and sends no heartbeats. `sender_context` synthesises an artificial ROTATE + FDE and serves stored bytes through `storage_core::fetch_event_block` (shared lock, `storage_core.cpp:461`). Readers see only **flushed** bytes: reads are clamped to `record.size` (`:513-538`). Decryption happens on this path (`:544-557`), and it is the **only in-product way to read an encrypted storage back as plaintext** (focus 1, 3, 10).

**Wire and format boundaries.** MySQL framing is in `minimysql/network_io_operations.cpp`: a frame with payload ≥ 0xFFFFFF is rejected (`:97`, `:144`). The JSON config is parsed via nv_tuple. GTID set binary encoding is at `collector_context.cpp:242-251`. Events between 256 MiB (backend read cap, `basic_storage_backend.hpp:35`) and 1 GiB (sender max) cannot be served (focus 1, 11).

## State & persistence

**Layout** (focus 1, 2). Storage root contains:
- `metadata.json`: storage format version, `mode` (gtid/position) and encryption format only (`storage_core.cpp:828-834`, `storage_metadata.hpp:35-37`). There is no source identity, no rewrite flag and no `base_file_name`.
- `binlog.index`: text lines `./<name>` (`:789-799`).
- Per binlog: `<name>` holds the raw or AES-CTR ciphertext bytes, and `<name>.json` holds size, previous/added GTIDs, min/max timestamps, last_sequence_number and the encryption envelope (`:907-925`).
- Sequence numbers are 6 digits; values above 999,999 are rejected (`composite_binlog_name.hpp:30`) (focus 12).

**Persistence primitives.**
- Filesystem `put_object` writes `<name>.tmp`, fsyncs it, renames it and fsyncs the directory (`filesystem_storage_backend.cpp:127-160`). The `.tmp` name is deterministic (`basic_storage_backend_fwd.hpp:56-60`).
- Data streams use `ofstream` opened `app|ate`, with write and `flush()` but **no fsync** (TODO `:229-230`). `resize_object` fsyncs the file but not the directory. Creating a new file does not fsync the directory (focus 9).
- S3: `put_object` is a single PutObject. The data "stream" is a local temp file (`/tmp/<uuid>` or `fs_buffer_directory`), and **every** `write_data_to_stream` re-uploads the whole file (`s3_storage_backend.cpp:850-863, 933-938`). An append-mode open downloads the object first (`:828-832`), `resize_object` downloads, truncates and re-uploads (`:765-808`), and `do_fsync` is a no-op that relies on read-after-write consistency (`:815-819`). Timeouts and retries are left at SDK/CRT defaults (focus 2, 9).

**Write orderings (the attack surface).**
- *New binlog* (`storage_core.cpp:680-703`): `open_stream` (create, truncating) → write magic (on S3 this uploads the object) → push record → `put_object(<name>.json)` → `put_object(binlog.index)`. It is not atomic (focus 1, 2, 4, 6, 8, 11).
- *Checkpoint* (`:299-322`): data append → `size += n`, `added_gtids +=`, timestamps → metadata PUT. The caller erases the buffer only after this returns (`storage.cpp:286-296`) (focus 2, 6, 8, 11).
- *Purge* (`:369-433`): rewrite the index (commit point) → best-effort `remove_objects` (metadata, then payload). Leftovers are reported as `warning`. The code comment says leftovers "will trip the constructor's validators on next startup" (`:391-397`) (focus 2, 4, 6, 8).

**Startup validation and recovery** (all non-`querying_only` modes, which includes `purging`). These steps were checked against code by this synthesis:
1. `remove_temporary_objects` deletes every `*.tmp` (`:597-630`). Only `querying_only` skips the delete (`:619-623`).
2. A non-empty storage without an index is rejected (`:187-198`).
3. Any object not in the index is rejected (`:779-783`), and so is metadata with no index entry (`:993-996`).
4. Per binlog: a size mismatch on a non-tail binlog is fatal (`:968-972`). On the tail, metadata > actual is fatal (`:974-977`), and actual > metadata triggers `resize_object` (truncate) (`:979`).
5. `purged_gtids_ = front().added_gtids` (`:1001-1004`). This should arguably be `previous_gtids`. It is harmless today because it is read only when the storage is empty (focus 2, 4, 12).
6. Data bytes are never parsed on open (TODO `:785`). Metadata is the single source of truth (focus 2).

`querying_only` tolerates a concurrent writer by design. It accepts a missing index (`:189-195`), skips index/object reconciliation (`:766-770`), drops records whose metadata cannot be read (`:942-950`) and skips the size check (focus 3).

**Resume derivation.** Resume reads only the tail record's metadata: in position mode, name + `size`; in GTID mode, `previous_gtids ∪ added_gtids` (`storage_core.hpp:313-337`). Without checkpointing, data reaches the backend only at rotation, disconnect or shutdown. A kill loses the unflushed tail, which resume re-requests from the source; this works only while the source still has it (focus 2, 5). Encrypted appends reuse the file's own envelope (`:715`, `:308`), so the old KEK must remain in the keyring (focus 2).

## Concurrency model

**In-process** (focus 3).
- `fetch`: the main thread runs `io_ctx.run()` for signals only, and the collector runs on a `std::jthread`.
- `pull`: the same, plus every replica session on the main thread.
- Query and purge modes are single-threaded.
- The S3 CRT client has its own event-loop threads.
- Shared between the two app threads: `storage` (via `shared_ptr`), the logger mutex, and `io_ctx.stopped()`.
- The facade's buffer and checkpoint state are unsynchronised and safe only because the collector is the sole caller. Sessions touch only `storage_core::fetch_event_block` (shared lock), and writers take the unique lock (`storage_core.cpp:231, 261, 305, 327, 372`).
- Because data is flushed before `size +=` under the exclusive lock, a reader should never see a size beyond the readable data. The exception is the window after a failed metadata save, where the size is already incremented (focus 2).
- No deadlock was found. The only lock nesting is storage lock → logger mutex (focus 3).

**Blocking I/O on the event loop** (focus 1, 3, 5, 6, 8, 9, 11, 12; the most-repeated finding). `fetch_event_block` holds the shared lock across a backend GET (S3 GetObject) plus decryption, on the single io thread. The collector holds the unique lock across a whole-object S3 PUT (which grows with binlog size, ticket 17) plus the metadata PUT. Consequences:
- A slow PUT freezes every session, the acceptor, the read/write `steady_timer`s and the **signal handler**.
- N replicas serialise 1 MiB reads and delay the collector.
- Hypothesis (focus 8, 12): the collector stops reading → the source's `net_write_timeout` fires → reconnect → a bigger file → a longer upload. This could be a degradation spiral.

**Cross-process** (focus 1, 2, 3, 6, 11, 12). There is no flock, lock file, O_EXCL or pid file anywhere in `src/`. Hazards confirmed in code:
- (i) A shared deterministic `<name>.tmp` lets one writer's rename publish another's torn content, or fail with ENOENT.
- (ii) Any second streaming **or purging** constructor deletes every `*.tmp`, including a live writer's in-flight `.json.tmp`.
- (iii) `app` mode means two writers append duplicates at the real EOF.
- (iv) Constructor recovery truncates the tail to the metadata size, which can cut a live writer's data (verified at `storage_core.cpp:974-983`, which runs in `purging` mode).
- (v) A streaming instance loads the index once (`:214`) and rewrites it from its stale in-memory list at every rotation (`:695-701, 789-799`).
- On S3, the temp files are UUID-named. The hazard there is last-writer-wins on whole objects.

README:473 says purge + fetch on one storage "may cause data corruption and should be avoided". It does not forbid fetch + fetch, and it does not forbid query + writer, which the code explicitly tolerates.

## Claimed safety guarantees

- **SG1 Resume continuity.** The system claims a second run "results in resuming streaming from the position at which the previous run finished" (README:711; README:3). Property: across any number of runs, reconnects and signals, the stored transactions are neither duplicated nor missing relative to the source. Built-in self-checks (position continuity, rotate position) turn some violations into non-zero exits. Known code-confirmed exception: GTID-mode resume across a source rotation (ticket 14) (focus 4, 2).
- **SG2 Byte-prefix fidelity** (implicit; asserted by MTR `diff_with_storage_object.inc` and QA `storage_holds_prefixes`). Each stored binlog is expected to be a byte prefix of the source binlog as sent (IN_USE cleared, CRC recomputed). After catch-up it should be an exact copy, except that a magic-only tail is allowed. This does not apply in rewrite mode or to the ciphertext of encrypted storages; compare the event stream served by pull instead (focus 4, 7, 10).
- **SG3 Consistent storage after error or signal.** The system claims "Any error … results in immediate termination … making sure that storage is left in consistent state" (README:373) and that a graceful signal leaves the storage consistent (README:715). README:721 allows `kill -9` to "lose some progress" but does not allow corruption. Implicit expectation: the next run opens the storage, and the tail metadata size sits on a transaction boundary (focus 3, 4, 10).
- **SG4 Crash recovery is tail-only, by truncation** (claimed in code, `storage_core.cpp:960-985`). `put_object` is claimed atomic: readers see old or new in full (`basic_storage_backend.hpp:49-51`). Property: a kill at any instant leaves a storage that opens, and the next fetch restores byte identity (focus 4).
- **SG5 Purge removes a strict prefix, never the tail** (README:374-376, 420-424, 441; `storage_core.cpp:331-366`). Properties: the surviving index is a suffix of the old one; the resume position and GTIDs are unchanged; a `warning` status never leaves the index referencing a deleted file. Implicit: the storage stays openable after any purge outcome. The code comment says it does not (focus 4, 2).
- **SG6 Encryption at rest** (README:673): data files are encrypted before they are written, and file keys are never stored in plaintext. Weak spots found by reading the code: enabling encryption on an existing storage leaves the existing tail growing **in plaintext** (`storage_core.cpp:172-178`, with the record's own `encryption = nullopt`); removing encryption from the config creates new plaintext files while `metadata.json` still says `generic` (focus 4). Whether these are intended is open.
- **SG7 GTID bookkeeping.** For consecutive records, `prev[i+1] == prev[i] ∪ added[i]`; the `added` sets are disjoint and equal the GTIDs present in the bytes; `search_by_gtid_set` returns files covering the target, in index order (`search_helpers.cpp:22-52`) (focus 4).
- **SG8 Checkpoint durability** (README:619, 626): data is written "permanently" at size or interval boundaries. Property: after a process kill, the tail is no more than one checkpoint plus one transaction behind. On the filesystem this does not hold against page-cache loss (ticket 19) (focus 4, 5).
- **SG9 Faithful serving** (`sender_context.cpp:81-140` contract; `tests/PROPERTY_TESTS.md:213`, "every stored byte fetched exactly once", partial tail held back). Property: a pull dump with the artificial events stripped equals the stored (decrypted) bytes (focus 4, 7).
- **Implicit exit-code contract:** `fetch` exit 0 ⇒ the storage is an exact copy of the source at that time. Ticket 25's QA reproduction reports exit 0 while bytes differ from offset 4, and `~storage` swallowing flush errors weakens the contract (focus 3, 10).

## Claimed liveness guarantees

- **LG1 Pull reconnects after network errors** (README:378, "another reconnection attempt is made"). The mechanism covers only connect/switch failures (`collector_context.cpp:209-216, 263-267`) and a mid-stream `CR_SERVER_LOST` (`connection.cpp:546-551`, verified). Any other mid-stream errno, a server error packet (1236), a parse error, or a storage error ends the whole process, including the replication source (`pull_operation.cpp:154-157`). Retries use a fixed `idle_time` with no backoff. Which errnos Antithesis faults produce is open (focus 1, 5, 8, 9, 10, 11).
- **LG2 Bounded graceful shutdown** (README:719 says the worst case is `read_timeout + 1` s). The bound omits `connect_timeout`, the setup queries, an in-flight S3 PUT under the lock (SDK defaults, no app timeout), the destructor flush, and S3 client construction (ticket 16, about 6 s of IMDS). The signal handler itself is blocked while the io thread waits on the storage lock (focus 3, 5, 8).
- **LG3 Freshness.** No bound is claimed. The interval checkpoint fires only on the next event (`storage.cpp:178-199`), and heartbeats are skipped. So the last transaction of a burst reaches the backend only on the next event, a read_timeout disconnect, rotation or shutdown. With the **default config (no checkpointing), pull clients see nothing of the active binlog until the source rotates** (focus 5, 6, 12). `rs_blocking_dump.test:17-26` sets `checkpoint_interval=1s`, which hides this (focus 12).
- **LG4 Blocking dump eventually delivers** (PBS-91 claim at `storage_core.hpp:216-224`; MTR `rs_blocking_dump_empty_storage.test`). A blocking client at EOF or on an empty storage polls every 500 ms. A request for a name that is not stored yet, or for an offset past the flushed size, returns false and the session `co_return`s **without an error packet** (`network_service.cpp:308-313`). A dead client is noticed only on the next write (focus 5, 8).
- **LG5 Restart recoverability** (implicit, README:3). Expectation: after any crash or fault, a restarted fetch/pull resumes without manual repair. Tickets 9, 13, 15, 18, 19 and 25 and the purge-interrupt lead describe wedge states (focus 5).
- **LG6 Replica progress.** A ≥ 16 MiB event stops replicas permanently (ticket 10). An `event_size == 0` in storage hangs the io thread forever in NDEBUG builds (ticket 7, `indexed_event_block.cpp:31-50`), which blocks every session and SIGTERM (focus 5, 8).

## Failure & degradation modes

- **Fail-stop pull.** Pull is "down" for both collection and serving at once. No degraded mode keeps serving while the source is gone. A purged source binlog (1236) produces a crash-loop on every restart. That may be intentional, so a property should assert "exits non-zero and storage still opens" (focus 8, 10).
- **Disconnect mid-transaction.** On a false fetch the collector calls `discard_incomplete_transaction_events()` and then flushes. On an exception the discard is skipped, but the flush still stops at the boundary, so no partial transaction should land (focus 1, 8).
- **Failed flush, retried by the destructor (ticket 18).** If the metadata PUT fails after the data append, `~storage` appends the same bytes again and saves size = old + 2n, which makes the duplicate permanent. **Resolved contradiction:** focus 2 and 4 say a failed S3 *data* upload also leaves a duplicate. Focus 8 says recovery repairs it. Checked in the code: `do_write_data_to_stream` appends to the local temp file before uploading (`s3_storage_backend.cpp:850-862`), and `size +=` runs only after a successful write (`storage_core.cpp:307-310`). The retry therefore uploads old + 2n with metadata old + n, and startup truncation keeps the first copy. That case is transient and self-healing on restart, and only the metadata-failure case is permanent. On the filesystem, sticky failbits make the retry fail (focus 2, 8, 11).
- **Swallowed final-flush errors.** `~storage` catches everything (`storage.cpp:68-77`, verified). Pull still logs "successfully shut down" and exits 0 (focus 3, 5).
- **Listener death.** Any accept error other than aborted or bad_descriptor (EMFILE, ENFILE, ENOBUFS) ends the listener coroutine (`network_service.cpp:647-667`, verified). The acceptor fd stays open, so TCP health checks still pass, while the collector keeps running. Session count is unbounded, a pre-auth session holds an fd for `read_timeout`, and S3 temp files also consume fds and disk (focus 8, 9, 11, 12).
- **Resource leaks.** Each SIGKILL on S3 leaks a full local binlog copy (a UUID directory is removed only in the destructor, `s3_storage_backend.cpp:563-571, 640-651`), so repeated crashes fill the local disk, which then turns into ticket 18 or 15 paths (focus 2, 8, 9, 12). `file_logger` truncates on open (`file_logger.cpp:30`), so a restart loop erases the crash log, and logging stops silently after ENOSPC (focus 12).
- **Assert-only invariants.** `storage_core.cpp:219, 707`, `indexed_event_block.cpp:43`, `storage.cpp:138`, `network_io_operations.cpp:91-92, 128-129, 175`, and the `release` preset (RelWithDebInfo + NDEBUG) removes 163 asserts. The build type changes tickets 6, 7 and 13 from an abort into a hang or UB (focus 6, 11, 12).
- **Harness-relevant brittleness.** `do_list_objects` throws on any non-regular entry (`filesystem_storage_backend.cpp:108-112`), so a mount root with `lost+found` fails in every mode. S3 listing uses no delimiter, so nested storages break (`s3_storage_backend.cpp:419-487`) (focus 9, 11, 12).

## External dependencies and Antithesis stand-ins

Pinned (`CMakeLists.txt:80-85`): Boost 1.90.0 (asio, json, url), AWS SDK C++ 1.11.774 (s3-crt), libmysqlclient, OpenSSL, zlib (focus 9).

- **MySQL source (mysqld 8.0 / 8.4 / 9.x containers)** with binlog + GTID on, distinct `server_id`s, and **TLS enabled**. Ticket 23 is confirmed: no `GET_SERVER_PUBLIC_KEY` is set (`connection.cpp:225-321`), so `caching_sha2_password` over non-TLS fails full auth, and on 9.x non-TLS is the only problem path. Event layouts are hard-coded for 8.0 and ≥ 8.3 (`protocol_traits_fwd.hpp:30-37`); a newer event type or an FDE server version above the client version is fatal (`reader_context.cpp:646-657`). Timeouts are unvalidated: `read_timeout: 0` is passed straight to `MYSQL_OPT_READ_TIMEOUT`, and its semantics are open. Duplicate `server_id` dump-thread eviction ping-pong is an unverified lead (focus 9).
- **S3-compatible server (MinIO or Versity S3 Gateway)** with strong read-after-write consistency for overwrite + read. Use the endpoint URI form `http://key:secret@s3:9000/bucket/path`; path-style is forced (`:252`), credentials come only from the URI, and there is no CA-file option. Set `AWS_EC2_METADATA_DISABLED=true` unless ticket 16 is deliberately under test. The local filesystem under `/tmp` or `fs_buffer_directory` is a hidden dependency of the S3 backend (focus 9).
- **Filesystem volume:** a dedicated *subdirectory* of a volume, never the mount root (focus 12). Ticket 19 needs page-cache loss, which a container kill does not cause.
- **Downstream clients for pull:** `mysqlbinlog --read-from-remote-server [--raw] [--stop-never]` is the working client. MySQL replicas fail today: ticket 20 is confirmed (missing `known_queries`, and COM_BINLOG_DUMP_GTID falls to "unknown command", `network_service.cpp:503-550, 608-611`). A replica container therefore exercises a known failure rather than finding new ones (focus 9, 7).
- **Optional fault shims** already in the repo: `extra/qa/fsfault.c` (LD_PRELOAD ENOSPC/EIO by op and nth) and `s3_fault_proxy.py` (5xx, reset, stall, truncated body per method and key). They cover faults Antithesis does not inject natively (focus 7).

## Product context & user-visible failures

Packaging ships only the binary and a config file, with no systemd unit, so cron `fetch` and supervised `pull` are left to the operator (focus 10). Workflows, ranked by oracle budget (focus 10):

| Workflow | User-visible failure |
|---|---|
| W1 continuous backup (pull / repeated fetch) | storage not a byte prefix of the source; GTIDs in `added_gtids` absent from the bytes; size off a boundary; exit 0 while diverged. **Silent, catastrophic.** |
| W6 PITR planning (`search_*`) | GTID search returns files that do not cover the set, a false "cannot be covered", or files out of order; timestamp search omits a needed file. **Silent.** Clock steps backward hide later binlogs (`search_helpers.cpp:72-77`, focus 11). |
| W2/W4 resume after stop/kill | storage unopenable (wedge), duplicate or skipped data, never converges. Losing un-checkpointed data is allowed. |
| W5 serve via pull (only restore path for encrypted storage) | bytes differ from the stored/decrypted bytes, wrong artificial ROTATE/FDE, gaps or duplicates at file switches, a valid request dropped, a hang. |
| W7 purge | over-deletion, tail deleted, `success` with orphans, unresumable after. |
| W3 graceful stop | non-zero exit, exceeds the bound, received data lost. |
| W8 list | listing disagrees with the objects. |

Rewrite-mode-specific risks (focus 12):
- Generated FDE, PREVIOUS_GTIDS and ROTATE events carry PBS's **wall clock** (`event_generation_helpers.cpp:48, 117, 149`). This skews `max_timestamp` and may make `mysqlbinlog --stop-datetime` output nothing for a file; unverified.
- `rewrite` and `base_file_name` are not recorded in the storage, so toggling them is undetected and can wedge the storage (`storage_core.cpp:274-278`).
- Recomputed CRCs mask corruption in transit.
- The 999,999 sequence limit can be reached in about 17 minutes with `file_size=1024` at 1 MB/s.

The listener also contains test scaffolding: canned `select * from tbl` rows and `SELECT VERSION()` returning "9.7.0" (`network_service.cpp:504, 530`). It serves decrypted data over cleartext TCP behind one shared password (focus 12).

## Existing test coverage and where Antithesis adds value

- **CI** (`.github/workflows/cmake.yml`) runs format checks, Boost unit tests and MTR (about 50 tests) on MySQL 8.0, 8.4 and 9.7, on the **file backend only**. It does not run the Hegel property tests (`WITH_PROPERTY_TESTS` defaults to OFF, `CMakeLists.txt:78`) or S3 (focus 7).
- **The nightly run** (`extra/qa/nightly/run_nightly.sh`) runs only the C++ unit and Hegel tests under debug, ASan and TSan. Under TSan it runs only `storage_property_test`. `known_failures.txt` lists 18 cases (tickets 1, 3, 5, 7-13, 26). No black-box harness runs on a schedule in the repo (focus 7).
- **The Hegel model test** (`storage_property_test.cpp`) "crashes" by copying the directory **between operations** (`:1845-1856`). It never crashes inside a storage call and cannot drop un-fsynced data (focus 7).
- **MTR:** crash states are hand-made (`storage_interrupted_binlog_creation.test`), binlog_server is stopped only with SIGTERM, and `kill_and_restart.test` kills mysqld, not PBS (focus 7).
- **Python harnesses:**
  - `fetch_crash_test.py`: Hypothesis state machine, fetch only, SIGKILL triggered by polling file size.
  - `disk_fault_sweep.py` / `s3_fault_sweep.py`: one fault per run, cleared before healing.
  - `network_fault_test.py`: Toxiproxy between PBS and the source only.
  - `concurrent_storage_test.py`: ticket 24.
  - `source_change_test.py`: graceful restart, RESET, swap.
  - `pull_replica_test.py`: one replica, no kills.
  - `listener_fuzz_test.py`: sequential malformed traffic (focus 7).

**Gaps Antithesis fills** (focus 7):
- kills at arbitrary instruction points inside rotation, checkpoint, purge and recovery;
- combined faults (partition + S3 5xx + kill);
- pull under faults, with several concurrent replicas, and network faults on the replica side;
- purge racing writers across processes;
- source-side timing (source kill mid-transaction, rotation while PBS is disconnected);
- clock jumps (none tested);
- S3 latency, stalls and stale reads;
- encryption and rewrite under crash;
- the absence of any continuous fault layer.

**Reusable oracles** (focus 7, 10):
- `storage_holds_prefixes()` and `check_complete_copy()` in `extra/qa/fetch_crash_test.py:636-697`;
- helpers `MySQLServer.binlog_as_sent()`, `parse_events` and `transaction_boundaries`;
- the Hypothesis `transactions`/`workload_sql` strategies and `BinlogServer.write_config()`;
- pull liveness and fd-growth checks (`network_fault_test.py`);
- the replica equality check (`pull_replica_test.py`);
- the clean-handshake check (`listener_fuzz_test.py`);
- the C++ storage model and the sender_context properties, as specs.

The codebase has no Antithesis SDK instrumentation (see `existing-assertions.md`).

## Bug history

Hotspots by churn since 2026-01: `storage.cpp` (32 commits), `app.cpp` (30), `storage.hpp` (24), `s3_storage_backend.cpp` (16). The replication source (`sender_context.cpp`) was rewritten in one week (a338dde, d3bc7fb, PBS-64/65/91) and is the least settled code. Recurring patterns:
- state read once and never refreshed (tickets 2, 9, 25);
- non-atomic multi-object updates whose recovery handles only one direction (15, 18, 19, purge);
- `assert`/`noexcept` used in place of validation (1, 6, 7, 13).

Encryption and the rewriter are quiet because they are well tested (focus 6).

Validation column: "Yes" means an agent cited the discriminating code detail. Fit: high = timing, crash or fault dependent; med = needs a fault plus a specific workload, or is liveness-only; low = deterministic input, environment or config.

| # | Ticket (short) | Component | Validated? (discriminating detail) | Antithesis fit |
|---|---|---|---|---|
| 1 | unparsable FDE server version terminates | events parser | Yes: `noexcept get_encoded_server_version()` calls a throwing `semantic_version` (`format_description_post_header_impl.cpp:130,166-169`) | low (fetch); med via pull reading corrupted stored FDE (`sender_context.cpp:323-325`, focus 6 F9) |
| 2 | checkpoint settings ignored (FIXED) | storage | Fix present (`storage.cpp:54-65`); interval evaluated only on event arrival | med (regression; freshness) |
| 3 | gtid_set no GNO bound check | gtids | Yes: `add_interval` checks lower bound only (`gtid_set.cpp:622-625`, TODO `:211`) | low |
| 4 | Boost.ICL wrong with Apple clang | build | Partial: macro unset (CMakeLists); miscompile is reporter's macOS repro | low (env) |
| 5 | oversized GTID tag reported as bad_alloc | events | Yes: unchecked `resize` into 32-byte static_vector (`gtid_set.cpp:370`, `gtid_tagged_log_body_impl.cpp:386`) | low |
| 6 | close mid-transaction leaves stale timestamps | storage | Yes, latent: `close_binlog` clears buffer not timestamps (`storage.cpp:210`, assert `:138`) | low |
| 7 | infinite loop on event_size < 19 | sender | Yes: assert-only then `offset += event_size` (`indexed_event_block.cpp:43-51`); reachable via `compute_fetch_size_requirements` (`sender_context.cpp:358-370`) | med (needs corruption; NDEBUG hangs io thread) |
| 8 | CTR counter no carry into IV high half | crypto | Yes: low 64 bits only (`cipher_context.cpp:457-461`) | low |
| 9 | purge during fetch/pull wedges storage | storage | Yes: index loaded only in ctor (`storage_core.cpp:214`), rewritten from stale list (`:701, 789-799`) | high (timing), but documented misuse (README:473) |
| 10 | events ≥ 16 MiB cannot be sent | minimysql | Yes: single frame `size+1` (`connection_context.cpp:348-350`), reject `>= max_payload_size` (`network_io_operations.cpp:144`) | low (deterministic; include big events in workload) |
| 11 | search_by_timestamp accepts out-of-range time | query | Yes: `from_iso_extended_string`, no range check (`ctime_timestamp.cpp:34`) | low |
| 12 | `%00` in file:// URI | config | Yes: `root_path_ = uri.path()` (`filesystem_storage_backend.cpp:91`) | low |
| 13 | empty index → segfault | storage | Yes: unconditional `binlog_records_.front()` (`storage_core.cpp:1001`) | low–med (needs external damage) |
| 14 | GTID resume after rotation loses ROTATE, skips empty binlogs | collector | Yes: name-mismatch branch just `close_binlog()`, no synthesized ROTATE or gap check (`collector_context.cpp:541-553`); no GTID-mode position check (`reader_context.cpp:566-573`) | high |
| 15 | crash during binlog creation wedges storage | storage | Yes: magic → .json → index (`storage_core.cpp:680-703`); unindexed object rejected (`:779-783`) | **high (top)** |
| 16 | ~6 s IMDS wait outside EC2 | S3 | Yes (code): default `ClientConfiguration` (`s3_storage_backend.cpp:189-196`); delay env-dependent | low (harness env var) |
| 17 | S3 full re-upload per checkpoint | S3 | Yes: whole-file PutObject per write (`s3_storage_backend.cpp:850-863, 933-938`); README:670-671 documents it | med (liveness, lock hold) |
| 18 | failed write duplicates data; permanent if metadata failed | storage | Yes: `size +=` before metadata save (`storage_core.cpp:310, 321`); buffer erased only on success (`storage.cpp:286-296`); dtor re-flush (`:68-77`) | **high (top)** |
| 19 | data never fsynced, metadata is | fs backend | Yes: TODO fsync (`filesystem_storage_backend.cpp:229-230`); recovery rejects metadata > file (`storage_core.cpp:974-977`) | med/low: only if page-cache loss is simulated |
| 20 | MySQL replicas cannot replicate from pull | minimysql | Yes: missing `known_queries`; GTID dump falls to unknown command (`network_service.cpp:503-550, 608-611`) | low (feature gap; blocks replica workloads) |
| 21 | source ignores requested binlog/position (FIXED) | sender | Fix present (`sender_context.cpp:61-72`) | med (regression) |
| 22 | `mysqlbinlog --raw` fails (FIXED) | sender | Fix present (artificial ROTATE/FDE `sender_context.cpp:557-587`) | med (regression) |
| 23 | no non-SSL caching_sha2 auth | easymysql | Yes: no `GET_SERVER_PUBLIC_KEY` (`connection.cpp:225-321`) | low (harness: use TLS) |
| 24 | two fetches corrupt storage, no lock | storage | Yes: no lock primitive; fixed `.tmp` suffix (`basic_storage_backend_fwd.hpp:60`); recovery truncation (`storage_core.cpp:977`) | **high (top)** |
| 25 | storage has no source identity | storage | Yes: `storage_metadata` fields only version/mode/encryption (`storage_metadata.hpp:35-37`) | med–high (needs source reset/failover action) |
| 26 | dump from position on empty storage disconnects (PBS-96) | sender | Yes: empty storage returns true + empty buffer (`storage_core.cpp:474-478`); `fetch_fde_from_storage` false on `size < 19` (`sender_context.cpp:231-233`) | high (client-before-first-flush timing) |

**New code-confirmed leads with no ticket (each is a lead, not reproduced):**
- **N1. purge_binlogs truncates a live writer's tail.** In `purging` mode the constructor runs tail recovery (`storage_core.cpp:974-983`, checked by this synthesis). A purge that starts between a writer's data append and its metadata save truncates the file. The writer's `app` stream keeps appending at the new EOF while its in-memory size is too large, so the next start fails and pull readers read past the end. README:473 discourages purge alongside a writer, but purge is routine (focus 6 F6).
- **N2. A concurrent purge or second instance deletes the writer's in-flight `.json.tmp`** (`storage_core.cpp:597-630`; only `querying_only` skips this at `:619-623`, verified). The writer's rename then fails after its data was appended, which is ticket 18's permanent-duplicate path triggered by purge. Filesystem only (focus 3, 6, 11, 12).
- **N3. An interrupted purge leaves the storage unopenable by every mode, including purge itself.** The index is committed first and the deletes are best-effort; the code comment admits the outcome (`storage_core.cpp:387-430`) (focus 2, 4, 6, 8, 11).
- **N4. Query racing a writer fails spuriously.** `do_list_objects` calls `dir_entry.file_size()` (a fresh stat) per entry (`filesystem_storage_backend.cpp:114`), and a `.json.tmp` renamed away between readdir and stat throws `filesystem_error`. Querying alongside a writer is a design intent (focus 1, 2, 11).
- **N5. The sender skips the artificial ROTATE and FDE refresh when the next binlog is still magic-only.** `fetch_event_block` moves to N+1 with a read length of 0 and updates `binlog_name` (`storage_core.cpp:520-541, 559-561`). `populate_event_block` takes the EOF path before `handle_binlog_file_switch` (`sender_context.cpp:484-491`), and the next call's `saved_binlog_name == N+1`, so `start_from_beginning` is never set. Verified by this synthesis. The window lasts from rotation to the first flush of N+1, which is long without checkpointing (focus 5 L9).
- **N6. The listener dies permanently on EMFILE/ENFILE while pull looks healthy** (`network_service.cpp:647-667`) (focus 8, 9, 11, 12).
- **N7. Any mid-stream errno other than 2013 kills pull and its replication source** (`connection.cpp:546-551`) (focus 1, 5, 8, 9, 10, 11).
- **N8. Exit 0 after a failed final flush** (`storage.cpp:68-77`) (focus 3, 5, 8).
- **N9. Encryption toggling can write plaintext into an "encrypted" storage** (`storage_core.cpp:172-178`) (focus 4).
- **N10. S3 temp copies leak per crash and fill the local disk** (focus 2, 8, 9, 12).
- **N11. Rewrite-mode issues:** wall-clock timestamps, an unrecorded rewrite config, and the 999,999 limit (focus 11, 12).
- **N12. A dump request ahead of the flushed position closes silently with no error packet** (`network_service.cpp:308-313`) (focus 5, 8).
- **N13. 32-bit position cast:** resuming a binlog over 4 GiB in position mode raises a fatal error (`collector_context.cpp:146, 378`) (focus 11).

## Unproven assumptions (focus 11, cross-referenced)

- A1: single writer per storage, with nothing enforcing it.
- A2: the source never resets or changes identity.
- A3: data is durable before metadata (false on the filesystem).
- A4: binlog creation and purge are atomic.
- A5: a failed write did not happen.
- A6: only 2013 is transient.
- A7: storage I/O is fast enough for the event loop.
- A8: `accept()` never fails transiently.
- A9: a directory listing is a quiescent snapshot.
- A10: every S3 endpoint is strongly consistent with atomic overwrite. A short range response is a hard `out_of_range` (`s3_storage_backend.cpp:296-300`), so a stale read disconnects a replica.
- A11: assert-guarded invariants hold in release builds.
- A12: offsets fit in 32 bits.
- A13: events fit in one frame and in memory; dump positions are never checked to fall on an event boundary, which makes ticket 7 reachable through a crafted position.
- A14: source timestamps are monotonic for `search_by_timestamp`.
- Further assumptions that no agent verified: libmysqlclient maps a read timeout or RST to 2013; `directory_entry::file_size()` re-stats; S3 CRT sync calls are thread-safe; an aborted CRT multipart upload never replaces the object.

## Wildcard observations (focus 12, plus singletons)

- **W1 / LG3.** Default-config pull serves nothing from the active binlog. MTR hides this by setting checkpointing.
- **W3.** Downstream load can propagate upstream through lock starvation (hypothesis).
- **W8.** A mount root or stray file breaks every mode. The harness must use a subdirectory.
- **W9.** S3 storages cannot nest or share a bucket root.
- **W11.** The truncating logger destroys crash evidence on restart, and each idle replica logs every 500 ms under the logger mutex.
- **W12.** Production listener scaffolding.
- **W13.** Build with the debug or ASan preset, or the asserts never fire.
- **Singleton, focus 8.** The S3 `tmp_fstream_` may keep eof/fail bits after a failed PutObject; no `clear()` or `seekp` was seen. This would make every later write fail.
- **Singleton, focus 3.** In pull, the last `storage_ptr` owner can be a suspended session frame, so the final flush may run during `~io_context`. It is fragile, but no current UAF was found.
- **Singleton, focus 9.** `util::fsync` reopens the path read-only, which may miss earlier writeback errors.

## Attack surfaces ranked for Antithesis

1. **Kill or fault inside binlog rotation** (ticket 15): magic → .json → index. Drive frequent `FLUSH BINARY LOGS` and small `max_binlog_size`, then kill PBS or fault the index/metadata PUT. Oracle: the storage reopens and converges (LG5, SG4).
2. **Fault at the checkpoint metadata write, then shutdown or exception** (ticket 18, N2, N8). S3 5xx or ENOSPC on the `.json` only. Oracle: byte-prefix fidelity, no duplicate transaction, exit 0 ⇒ consistent.
3. **Cross-process races on one storage** (ticket 24, N1, N2, ticket 9). Overlapping fetch/fetch and purge/writer. Oracle choice is a human decision: fail closed and stay detectable, versus out-of-contract.
4. **Interrupted purge** (N3): kill or DELETE failure between the index commit and the removals. Oracle: the storage still opens and the resume position is unchanged.
5. **Pull serving during collector rotation and checkpoints with several concurrent replicas** (N5, ticket 26, LG4). Oracle: the stream from each client, with artificial events stripped, equals the stored bytes, and every file switch carries a ROTATE + FDE.
6. **Network faults and source restarts mid-dump** (N7, LG1, ticket 14). Partition, RST, source SIGKILL, rotation while disconnected, GTID mode. Oracles: pull stays alive or exits cleanly and reopens; storage is a prefix with no skipped binlogs.
7. **Slow or stalled S3 and disk under pull** (single io thread, ticket 17). Latency injection plus N blocking replicas. Oracles: SIGTERM latency bound (LG2), replica freshness, no upstream drop spiral.
8. **Crash/restart loops on S3** (N10, temp leaks → local ENOSPC → paths 1 and 2) and on encrypted or rewrite storages, which have never been crash-tested.
9. **Query modes concurrent with a writer** (N4): tight `list`/`search_*` loops against fetch with a small `checkpoint_size`. Oracle: never errors, and returns a prefix of the quiescent listing.
10. **Listener resource exhaustion** (N6): many idle or pre-auth connections. Oracle: a new client is eventually served.
11. **Clock faults:** `search_by_timestamp` monotonicity (A14), rewrite wall-clock stamps (N11), and checkpoint interval (steady_clock, so expected to be robust).
12. **Source identity change** (ticket 25): RESET BINARY LOGS or failover to another server. Oracle: never exit 0 with diverged bytes.
13. **Power-loss durability** (ticket 19): only if the platform drops un-fsynced page cache.

## Assumptions

- Agent line numbers refer to commit 99de682 and are within a few lines of each other where they differ (for example, `pull_operation.cpp` ranges). The synthesis re-checked the S3 data-write path, the purge-mode temp deletion and recovery truncation, sender file switching, the listener and `~storage`.
- Antithesis provides process kill/pause, network partition/latency/reset and probably clock and node faults. It does not inject disk-error codes or S3 5xx natively, so the repo's `fsfault` and `s3_fault_proxy` shims, or a fault-injecting S3 stand-in, are needed for surfaces 2 and 4.
- The SUT for Antithesis should be a debug or ASan build so that assert-only invariants are live. A release build is needed only to observe the NDEBUG hang of ticket 7.
- "Consistent state" (README:373, 715) means openable by the next run and resumable without loss or duplication.
- Byte-identity oracles apply only to non-rewrite, unencrypted storages. Other storages are compared through the event stream that pull serves.

## Open Questions

- [needs-human] Are **node-termination** (whole-VM) faults and **clock** faults enabled for the tenant? Does any Antithesis fault discard un-fsynced page cache? If not, ticket 19 is unreachable and must not become a property.
- [needs-human] Is purge (or fetch + fetch) concurrent with a writer in contract? README:473 says "avoid" for purge only. This decides whether surface 3 is asserted as "never corrupts", as "fails closed and detectable", or is left out of the workload.
- [needs-human] Is "storage unopenable after an interrupted rotation or purge" accepted fail-stop behaviour or a bug? This decides whether LG5 is a safety property or a recoverability expectation.
- [needs-human] Is pull expected to run under a supervisor? If yes, exiting on non-2013 errors is acceptable, and only restart-recoverability is asserted.
- [needs-check] Which libmysqlclient errnos do Antithesis resets and partitions, a graceful source shutdown, and a KILL of the dump thread produce mid-stream (2013 vs 2006, 1053, 1317, 1236)?
- [needs-check] libmysqlclient semantics of `MYSQL_OPT_READ_TIMEOUT=0`, and the client `max_allowed_packet` for `mysql_binlog_fetch` (CR_NET_PACKET_TOO_LARGE would be fatal).
- [needs-check] S3 CRT default retry, backoff and request-timeout values in SDK 1.11.774. Does a blackholed endpoint hang `write_event_block`, and with it the io thread, indefinitely?
- [needs-human] Is "byte-identical to the source" an official guarantee or only an MTR convention? This decides whether ticket 14 is a safety violation.
- [needs-human] Are encryption toggling (N9), `purged_gtids_ = added_gtids`, and the interval checkpoint firing only on the next event intended?
- [needs-repro] N1, N2, N4 and N5 (all timing leads), the duplicate-`server_id` eviction ping-pong, and the S3 fstream failbit stickiness.
- [open] Should the Antithesis workload reuse the Python/Hypothesis oracles from `extra/qa` or re-implement them in a lighter client? Should the C++ SDK be vendored as a header or fetched like Hegel?
