# Property discovery 04 — Protocol Contracts

Focus: the MySQL replication protocol as served by pull's replication source (artificial ROTATE/FDE sequencing, positions, checksums, terminal packets), as consumed by fetch/pull from the source, and the JSON outputs of `list`/`search_*`/`purge_binlogs` that PITR and retention tooling consume. Commit 99de682. Evidence files are in `antithesis/scratchbook/properties/`.

Several protocol leads were already taken by other agents when this pass wrote its files, and are not duplicated here. They are referenced under "Cross-property assumptions": `sender-file-switch-emits-artificial-rotate` (N5), `pull-fde-switch-across-format-change`, `pull-client-reconnect-receives-each-event-once`, `served-bytes-stable-across-restart`, `large-event-served-or-rejected-cleanly`, `fetch-exit-zero-implies-tail-flushed`, `query-concurrent-with-writer-never-errors`, `query-sees-binlogs-flushed-before-start`, `interrupted-purge-leaves-openable-storage`, `purge-retry-idempotent`.

## Replication source (pull egress)

### pull-served-stream-self-consistent — Served events carry a log_pos chain and CRC consistent with where they were served

| | |
|---|---|
| **Type** | Safety |
| **Property** | Every non-artificial event pull serves has `next_event_position == its offset in the file + event_size`, and verifies its CRC32 when the governing FDE says CRC32. |
| **Invariant** | Workload `Always` (two messages: "pull: served event next_event_position equals file offset plus size", "pull: served event CRC32 verifies under the governing FDE"), checked per event by the raw dump client, which tracks the offset from the artificial ROTATE and the checksum mode from the last FDE. `Always` because both must hold for every served event. SUT-side `ALWAYS` twins in `handle_fetch_event_from_storage_state` (missing). `Sometimes(carry splice used)` and `Sometimes(encrypted file served)` against vacuity. |
| **Antithesis Angle** | Egress never re-validates what it serves (`indexed_event_block.cpp:29-55` TODO). Duplicates (ticket 18), truncate-then-append (N1), two writers (ticket 24), wrong carry splices (`sender_context.cpp:438-524`) and wrong CTR offsets all show up as a broken chain or a CRC failure. They need narrow write-side windows and read-side races that Antithesis explores. The check needs no source access and works for encrypted and rewrite storages. |
| **Why It Matters** | Clients derive resume coordinates from log_pos and detect corruption by CRC. A broken chain means wrong replica coordinates, double-applied transactions (position mode) or corrupt `--raw` copies, and pull is the only in-product way to read an encrypted storage. |

Priority: P0. Confidence: high (the mechanism is clear; the oracle is standard MySQL format). Fault requirements: pull process kill, network faults on pull ↔ S3 (or `s3_fault_proxy`/`fsfault` for `.json` write failures), CPU throttling.

**Open Questions:**

- Does the oracle correctly tolerate files that end without ROTATE/STOP (source crash, contract step 4c)?
- Which FDE governs the checksum of the artificial ROTATE between files of different checksum mode (previous file per the code comment; does MySQL agree)?
- Should the SUT-side CRC check be Antithesis-only or a product feature?

### dump-session-ends-with-eof-or-err-packet — Unservable dump requests get an ERR packet, not a silent close

| | |
|---|---|
| **Type** | Safety |
| **Property** | When pull's replication source cannot serve a COM_BINLOG_DUMP request (unknown/purged binlog, position past the flushed size, unreadable FDE, storage exception), it sends a MySQL ERR packet before closing. |
| **Invariant** | SUT-side `UNREACHABLE("replication source: dump session closed without ERR or EOF packet")` at `network_service.cpp:312`, plus `UNREACHABLE("replication source: storage exception ended a dump session")` in a new catch around `get_event` (both missing). `Unreachable` because every reach of these sites is a contract violation, and SUT-side placement avoids confusing them with injected network drops. Workload `Sometimes(session ended with ERR 1236)` as the positive goal. |
| **Antithesis Angle** | The unservable states are timing-produced: a client ahead of a recovery-truncated tail after a pull kill, a request before the first flush (ticket 26), an external purge with pull's stale index (ticket 9), S3 GET faults or short reads. |
| **Why It Matters** | A silent close looks like a network blip, so replicas reconnect forever with no visible error and `mysqlbinlog` reports "Lost connection" instead of the real cause. This masks errors. |

Priority: P1. Confidence: high (code path verified: `network_service.cpp:305-313`, session `catch (...)` at `:621-623`). Fault requirements: pull kill/restart, network faults on pull ↔ S3, a purge task.

**Open Questions:**

- For a blocking dump, should "past flushed size" or "file not yet created" wait instead of failing (splitting this into a liveness twin)?
- Which error code and message should be used (1236 expected)?
- Does the ticket 26 known-failure entry imply silent close is already considered a bug in general?

### misaligned-dump-position-rejected-cleanly — A dump from a non-boundary position is refused, not served, looped on, or allowed to stop pull

| | |
|---|---|
| **Type** | Safety |
| **Property** | A COM_BINLOG_DUMP at a position that is not an event boundary ends that session with an error and does not abort pull, hang its io thread, or stream garbage; other sessions and the collector keep running. |
| **Invariant** | Workload `Always` (message "dump: misaligned request rejected without affecting other sessions"): the misaligned session receives no non-artificial event, and concurrent tailing sessions keep advancing while the pull process stays alive. `Always` because one bad request must never take the server down. SUT-side `ALWAYS(event_size >= 19)` replacing the assert at `indexed_event_block.cpp:43` (missing). `Sometimes(misaligned request while other sessions tail)`. |
| **Antithesis Angle** | The parse is deterministic per input (Hegel could cover it). The Antithesis value is the cross-session consequence on a single-io-thread server, and misaligned positions that arise from earlier faults (duplicates, truncation, source swap, rewrite-mode re-splitting) that Antithesis composes. |
| **Why It Matters** | In debug/ASan builds an `event_size < 19` header aborts the whole pull (collector included). In release, `event_size == 0` loops forever on the io thread (ticket 7), blocking all sessions and SIGTERM. Any authenticated client can trigger it. |

Priority: P1. Confidence: high for the mechanism (ticket 7 validated, positions never validated: `sender_context.cpp:52-76`, `storage_core.cpp:436-562`), medium for how often realistic clients send such positions. Fault requirements: none for the direct case. Kills and storage faults for the diverged-storage variants.

**Open Questions:**

- Validate only the first header (size, type, CRC) or keep an event index?
- Is a debug-build abort acceptable as fail-stop here?

### dump-preamble-matches-request — Every dump starts with an artificial ROTATE for the requested file and position, then that file's FDE

| | |
|---|---|
| **Type** | Safety |
| **Property** | For every dump session, the first event is an artificial ROTATE (timestamp 0, next_event_position 0) whose binlog and position equal the request (or the oldest indexed file for an empty name), and the second is that file's FDE (artificial with zeroed next_event_position/create_timestamp iff position ≠ 4, valid CRC). |
| **Invariant** | Workload `Always` (three messages: "dump: first event is artificial ROTATE", "dump: artificial ROTATE matches request", "dump: FDE follows ROTATE and matches position mode"). `Always` because the preamble is required on every session. `Sometimes` markers for requests on an empty storage, on a magic-only file, and shortly after a purge. SUT-side `ALWAYS` in `handle_start_states` (missing). |
| **Antithesis Angle** | Regression guard for fixed tickets 21 and 22 and open ticket 26, but against a storage that is changing: first flush, file creation, rotation, external purge with a stale in-memory index (ticket 9), pull restart. The Hegel spec covers only static storages. |
| **Why It Matters** | `mysqlbinlog --raw` names output files from this ROTATE, and clients reset their parsing from this FDE. A wrong or missing preamble produces misnamed or corrupt local copies. |

Priority: P2. Confidence: medium (regression plus race leads, some scenarios unverified). Fault requirements: none beyond workload concurrency. Pull kills and a purge task widen the windows.

**Open Questions:**

- If the requested file is magic-only and a later file appears first, should the ROTATE still name the requested file?
- Is "oldest in pull's stale in-memory list" acceptable for empty-name requests after an external purge, or is that out of contract?

## Machine-readable CLI outputs (list / search_* / purge_binlogs)

### query-read-error-not-reported-as-success — Query modes never answer "success" from a view with a hole caused by a failed metadata read

| | |
|---|---|
| **Type** | Safety |
| **Property** | On a quiescent storage, `list`/`search_*` either return a result computed from every indexed record or return `status: error`; a transient metadata read failure is never silently turned into a missing file in a `success` response. |
| **Invariant** | Workload `Always` ("query: success output has no missing binlog", "search_by_timestamp: success result equals reference selection"), comparing with the authoritative `binlog.index` read directly from the backend. SUT-side `UNREACHABLE("querying_only: dropped a binlog record that is not the leading one")` at `storage_core.cpp:946-949` and `ALWAYS(!binlog_records_.empty())` before `:1001` (missing). `Sometimes(status error during an S3 fault)`. |
| **Antithesis Angle** | Needs one metadata GET to fail while the others succeed during the multi-GET load loop. Antithesis network faults on the query-container ↔ MinIO link do this naturally. |
| **Why It Matters** | `querying_only` erases any record whose metadata load throws (`storage_core.cpp:933-950`) and still reports `success`. For `search_by_timestamp` a middle gap means a PITR replay silently skips a whole binlog. For `search_by_gtid_set` it causes a false "cannot be covered". |

Priority: P0. Confidence: high for the code path, medium for reachability (depends on SDK retries). Fault requirements: network faults between the query task's container and S3, or `fsfault` EIO on `.json` reads.

**Open Questions:**

- Is the silent drop meant for transient I/O errors, or only for semantically invalid metadata?
- How long must faults last to beat the SDK's GET retries?

### purge-error-status-means-index-unchanged — purge_binlogs reports "error" only when nothing was committed

| | |
|---|---|
| **Type** | Safety |
| **Property** | On a quiescent storage, `status: error` from `purge_binlogs` implies `binlog.index` is unchanged; `success` implies the index is the expected suffix with no victim objects left; `warning` implies the index is the expected suffix. |
| **Invariant** | Workload `Always` (three messages: "purge: error status implies index unchanged", "purge: success status implies no leftovers", "purge: warning status implies index committed"), comparing index/object snapshots before and after. SUT-side `REACHABLE("purge: index put_object threw")` (missing). `Always` because the status is the API's only signal. |
| **Antithesis Angle** | The ambiguous commit window is a lost S3 PutObject response after the server applied it (CRT retries then fail), or the moment between `rename` and the directory `fsync` on the filesystem (`filesystem_storage_backend.cpp:127-160`). Network faults timed by Antithesis hit the S3 variant. |
| **Why It Matters** | An operator told "error" assumes the storage is untouched, while the committed index plus leftover objects wedges every later fetch/pull start (N3), and a retry also fails. |

Priority: P1. Confidence: medium (code path verified: the exception from `save_binlog_index` at `storage_core.cpp:398` propagates after the commit; S3 reachability depends on SDK behaviour). Fault requirements: network faults between the purge container and S3; `fsfault` directory-fsync EIO for the filesystem variant.

**Open Questions:**

- What are the CRT client's default retry count and request timeout?
- Is "error" meant to mean "nothing changed", or "unknown state"?
- Should a post-rename fsync failure be reported as success-with-warning?

### listed-binlog-size-is-stable-readable-prefix — The object at each listed URI holds at least the listed size, and listed prefixes never change

| | |
|---|---|
| **Type** | Safety |
| **Property** | For every record that `list` reports, the object at `uri` has length ≥ `size`, the bytes `[0, size)` equal those of every earlier listing of that file, and for plain storages `[0, size)` ends on an event boundary. |
| **Invariant** | Workload `Always` (three messages: "list: object at uri holds at least the listed size", "list: previously listed binlog prefix never changes", "list: listed size falls on an event boundary"). Optional SUT-side `ALWAYS(object size >= metadata size)` inside `querying_only` construction (missing). |
| **Antithesis Angle** | Violations come from cross-process interleavings: a purge constructor truncating a live tail between append and metadata save (N1), two writers (ticket 24), and non-consistent S3 reads (A10). Concurrent `list` calls at random moments catch them. |
| **Why It Matters** | PITR tooling downloads `uri` and replays `size` bytes. A shorter object, or bytes that changed after being listed, means a failed or silently wrong restore. |

Priority: P1. Confidence: medium (oracle sound; violations depend on misuse-ish concurrency that README discourages). Fault requirements: workload concurrency (writer, purge or second writer, list). Kills widen the windows.

**Open Questions:**

- Is purge concurrent with a writer in contract (README:473)? If not, gate those variants.
- Should README tell consumers to read only `size` bytes of the tail object?

### cli-json-envelope-and-exit-code-agree — Every list/search/purge run prints one JSON response whose status agrees with the exit code

| | |
|---|---|
| **Type** | Safety |
| **Property** | Under any storage state and backend fault, each `list`, `search_*` and `purge_binlogs` run prints exactly one JSON object with `status ∈ {success, warning, error}` (and `message` on error), exits 0 iff status is not `error`, and never dies by a signal. |
| **Invariant** | Workload `Always` ("cli: stdout is one JSON response", "cli: exit code agrees with JSON status") and `Unreachable("cli: query or purge crashed")` for a signal exit. SUT-side `ALWAYS(!binlog_records_.empty())` before `storage_core.cpp:219/1001` and a top-level `catch (...)` with `UNREACHABLE` (missing). |
| **Antithesis Angle** | The crash paths (empty record list after every metadata load failed, an empty index after an interrupted rotation, ticket 13) need storage states produced by kills and backend faults at specific moments. The envelope check is a cheap always-on oracle across all CLI calls. |
| **Why It Matters** | Scripts parse stdout and branch on the exit code. No JSON, or an exit code that disagrees with the status, breaks retention and PITR automation silently. |

Priority: P2. Confidence: medium (UB path verified at `storage_core.cpp:1001`; other escape paths are speculative). Fault requirements: writer kills, S3 network faults, optional `fsfault`.

**Open Questions:**

- Should `warning` exit 0?
- Is `list` on an index-less storage (`querying_only` early return) meant to report `success` with `[]`?

### search-by-timestamp-includes-all-early-files — search_by_timestamp returns every file with an event at or before t

| | |
|---|---|
| **Type** | Safety |
| **Property** | `search_by_timestamp t` returns, in index order, every stored binlog whose `min_timestamp ≤ t`, as README documents, even when timestamps are not monotonic across files. |
| **Invariant** | Workload `Always("search_by_timestamp: every file with an event at or before t is returned")` against a `list` snapshot of a quiescent storage. `Sometimes(list shows a record with min_timestamp lower than its predecessor's)` so the case is reached. SUT-side `SOMETIMES` marker in `select_records_by_timestamp` (missing). |
| **Antithesis Angle** | The implementation stops at the first file with `min_timestamp > t` (`search_helpers.cpp:75-77`). Non-monotonic timestamps come from source clock steps, `SET TIMESTAMP`, rewrite-mode wall-clock stamps (N11) and source swaps. Antithesis clock faults produce realistic skews. |
| **Why It Matters** | A PITR plan that omits a file containing events before t restores less than requested, and does so silently. |

Priority: P2. Confidence: low-medium (the code and README clearly disagree, but the intended semantics are unclear; the implemented prefix may match `mysqlbinlog --stop-datetime` usage). Fault requirements: none for the `SET TIMESTAMP` variant. Clock faults on the source/PBS containers for the realistic variant (may be disabled on the tenant).

**Open Questions:**

- Which semantics is the contract: README's "every file with an event ≤ t", or the implemented leading run?
- Should rewrite mode stamp generated events with source time instead of wall clock?

## Areas examined

- **Egress FSM and serving path:** `sender_context.cpp` in full (contract comment `:81-140`, start states, carry, file switch, EOF), `storage_core::fetch_event_block` (`:436-562`), `indexed_event_block.cpp`, and `network_service.cpp` (`handle_binlog_dump_command` `:279-334`, the `known_queries` checksum handshake `:487-550`, the command loop and the session/listener catches). Confirmed N5 independently; it was already filed by another agent, so it is not duplicated.
- **Checksum handshake:** the session checksum flag is set only by exact-string `SET @source_binlog_checksum/@master_binlog_checksum = 'NONE'|'CRC32'` queries (`network_service.cpp:539-550`). Any other spelling gets a syntax error and the default (no CRC on the first artificial ROTATE). Deterministic, so not a property. Noted for the workload's client choice.
- **Ingress protocol:** `collector_context::receive_binlog_events` / `open_connection_and_switch_to_replication` (`:125-272`), `process_binlog_event` / `process_artificial_rotate_event` (`:431-572`), `reader_context` state handlers (`:510-930`), `easymysql::connection` dump flags and the 2013-only tolerance (`connection.cpp:180-225, 535-551`). Checked GTID-mode resume into the same file: the reader keeps `position_` at the stored size because the name matches, so the source's skipped (excluded) transactions are consistent with the log_pos check. No new ingress property beyond those already filed (ticket 14 → `gtid-resume-*`, N7, `fetch-exit-zero-implies-tail-flushed`, `source-swap-never-silently-accepted`). The exit-0 ⇔ EOF-packet mapping was verified (`collector_context.cpp:172-187`, libmysqlclient EOF → empty portion) and is covered by `fetch-exit-zero-implies-tail-flushed`.
- **Not turned into properties (deterministic, Hegel/MTR territory or known gaps):** COM_BINLOG_DUMP_GTID unsupported and missing replica queries (ticket 20); 16 MiB frames (ticket 10, owned by `large-event-served-or-rejected-cleanly`); no heartbeats on idle blocking dumps (a MySQL replica would reconnect every `replica_net_timeout`; liveness only, and blocked by ticket 20 anyway); the canned `select * from tbl` / `SELECT VERSION()` = 9.7.0 scaffolding; `search_by_gtid_set` selection logic (Hegel-covered; metadata-vs-bytes agreement belongs to SG7 properties).
- **JSON outputs:** `list_operation.cpp`, `search_by_*_operation.cpp`, `purge_binlogs_operation.cpp`, `model_helpers.cpp`, `models/search_response.cpp`, `search_helpers.cpp`, `app.cpp` exit mapping, and the README sections for each mode.

## Cross-property assumptions

- The workload has a raw Python COM_BINLOG_DUMP client (or parses `mysqlbinlog --raw` output) that exposes per-event headers, the last packet type before close, and the session's checksum mode. `pull-served-stream-self-consistent`, `dump-preamble-matches-request`, `misaligned-dump-position-rejected-cleanly` and `dump-session-ends-with-eof-or-err-packet` share it. It must send the exact checksum `SET` strings PBS recognises (`network_service.cpp:539-550`).
- Real MySQL replicas cannot be used as pull clients (ticket 20), so every egress property is checked through the workload client.
- The SUT is a debug/ASan build (sut-analysis). That makes `misaligned-dump-position-rejected-cleanly` observable as an abort rather than a hang.
- Oracles that need an "authoritative" storage state (`query-read-error-not-reported-as-success`, `purge-error-status-means-index-unchanged`, `search-by-timestamp-includes-all-early-files`) run against a quiescent storage: no writer or purge in flight. The workload must sequence that. `listed-binlog-size-is-stable-readable-prefix` is the only CLI property meant to run concurrently with writers.
- Relationships to other agents' properties:
  - `pull-served-stream-self-consistent` complements `pull-client-reconnect-receives-each-event-once` and `served-bytes-stable-across-restart` (those check byte stability across sessions; this one checks per-event internal consistency) and `sender-file-switch-emits-artificial-rotate` / `pull-fde-switch-across-format-change` (file-switch sequencing).
  - `dump-session-ends-with-eof-or-err-packet` generalises the error-packet clause of `large-event-served-or-rejected-cleanly`.
  - `query-read-error-not-reported-as-success` covers the fault-induced drop; `query-sees-binlogs-flushed-before-start` covers the writer-race drop; `query-concurrent-with-writer-never-errors` covers the spurious error and empty-records crash.
  - `purge-error-status-means-index-unchanged` is the status-reporting side of `interrupted-purge-leaves-openable-storage` and `purge-retry-idempotent`.
- S3 fault properties assume Antithesis network faults on the client ↔ MinIO link surface as SDK errors after retries. The CRT defaults are unverified (sut-analysis open question).
