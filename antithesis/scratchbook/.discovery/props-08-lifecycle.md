# Property discovery 08 — Lifecycle transitions

Focus: start-up (storage open/recovery, signal installation, listener bind), clients arriving before the collector has data (PBS-91/96), graceful termination and the final flush, rotation as a transition, config changes between runs (encryption, rewrite, replication mode). Commit 99de682. Every SUT-side assertion named here is **missing** (no Antithesis instrumentation exists).

A ninth candidate, `query-modes-never-write-storage` (the encryption-format stamp at `storage_core.cpp:175-178` runs in `querying_only` mode), was found independently by another agent, who wrote the evidence file at the same moment. Their file is the canonical one, and this catalog does not repeat it.

## Shutdown

### graceful-stop-leaves-recovery-free-storage — A signal-initiated stop leaves nothing for start-up recovery to fix

| | |
|---|---|
| **Type** | Safety |
| **Property** | After fetch or pull stops on a single SIGTERM/SIGINT (and pull exits 0), the storage has no `*.tmp` objects and every binlog's object size equals its `.json` size, so the next start performs no recovery. |
| **Invariant** | Workload-side `Always(no *.tmp && ∀ binlog: object_size == metadata.size)`, checked by listing the storage between the graceful exit and the next start, gated on "workload sent exactly one signal and the process exited by itself". `Always` fits because a graceful stop must never leave recovery work. A SUT-side `ALWAYS(!final_flush_failed || !result)` at the end of `pull_operation::execute` / `fetch_operation::execute` requires surfacing the swallowed `~storage` failure. |
| **Antithesis Angle** | The final flush runs in `~storage` (`storage.cpp:68-77`, exceptions swallowed) *after* "successfully shut down" is logged and `result = true` (`pull_operation.cpp:168-171`). Antithesis times S3 partitions, latency or ENOSPC to the instant of shutdown, and pauses PBS so the signal lands mid-flush. In pull the last `storage_ptr` owner may be a session frame destroyed in `~io_context`, after `signal_set` is gone. |
| **Why It Matters** | README:713-721 promises that signals leave a consistent storage. A pull that exits 0 with an unflushed or half-flushed tail misleads supervisors and backup audits. Related: `fetch-exit-zero-implies-tail-flushed` (fetch EOF path), `failed-flush-never-reappends-data` (ticket 18), `sigterm-mid-write-leaves-boundary-storage`. |

Priority: P1. Confidence: high (mechanism verified in code; the violation needs a storage fault at the shutdown moment). Fault requirements: network partition or latency PBS↔S3, or the S3 5xx / ENOSPC shims; process pause helps. Node termination is not needed.

**Open Questions:**

- Is exit 0 after a failed final flush intended (best-effort) or a bug? If intended, the property weakens to "storage opens and resumes" and loses the exit-code gate.
- Should a second SIGTERM during the final flush be ignored or escalate? Today this depends on which object owns the last `storage_ptr`, not on a design decision.

### sigterm-during-startup-is-honoured — A signal sent while fetch/pull is still starting is acted on promptly, and the storage stays openable

| | |
|---|---|
| **Type** | Liveness |
| **Property** | A SIGTERM delivered at any point between process start and the first streamed event makes the process exit within a start-up bound, and the next start opens the storage and resumes. |
| **Invariant** | Workload-side `Always(exit observed within B_startup after SIGTERM)` with `B_startup = storage-open bound + connect_timeout + read_timeout + slack`, plus `Always(next start opens)`. `Sometimes(SIGTERM delivered before io_ctx.run())` makes sure the property is not vacuous. A time-bounded `Always` is used because "eventually exits" alone cannot fail in a finite run. |
| **Antithesis Angle** | `signal_set` is registered (`pull_operation.cpp:98-104`) before storage construction (`:109`), but its handler runs only once `io_ctx.run()` starts (`:160`), after S3 client construction, listing, `.tmp` deletion, tail truncation (on S3, a whole-object download and re-upload) and the listener bind. The first `open_connection_and_switch_to_replication` (`collector_context.cpp:132`) does not check `io_ctx.stopped()`. A second signal is queued by asio and ignored. Antithesis picks the signal instant and stretches start-up with S3 latency or a source outage. |
| **Why It Matters** | README:719's bound (`read_timeout + 1`) does not cover start-up. Supervisors escalate to SIGKILL after a grace period, which turns a graceful stop into a crash in the middle of recovery. Complements `sigterm-not-starved-by-storage-lock` (steady state). |

Priority: P2. Confidence: medium (the ordering is verified; the size of the bound is not stated by the project). Fault requirements: process pause, S3 latency, source container stop. Node termination is not needed.

**Open Questions:**

- What stop latency does the team commit to during start-up? Without a stated bound, the `Always` becomes plain eventual exit.
- Should the collector check `io_ctx.stopped()` before its first connect? If yes, the bound can drop `connect_timeout`.
- Is PBS expected to run as PID 1 in containers? If yes, a SIGTERM before handler installation is ignored outright.

## Start-up ordering and restart overlap

### bind-failure-leaves-storage-untouched — A pull that cannot bind its listener port must not have changed the storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | A pull instance that exits because its replication-source port is already in use leaves every pre-existing storage object (including another instance's in-flight `.tmp` objects and the tail) unchanged. |
| **Invariant** | Workload-side end-to-end: while pull A streams with frequent checkpoints, repeatedly launch pull B with the same config. B exits non-zero. Assert `Always(A never fails with rename/ENOENT && after A's graceful stop the storage is recovery-free and a byte prefix of the source)`. SUT-side `SOMETIMES(true, "startup recovery modified storage")` in `remove_temporary_objects` and before `resize_object` confirms the dangerous branch was reached. |
| **Antithesis Angle** | Storage construction (`pull_operation.cpp:109`) deletes every `*.tmp` (`storage_core.cpp:625`) and truncates the tail to its metadata size (`:979`) **before** `network_service` binds the port (`network_service.cpp:679-681`, `pull_operation.cpp:117`). The port is the only de-facto mutual exclusion. Antithesis pauses A between its data append and its metadata save, which is the window in which B's recovery cuts A's data or deletes A's `.json.tmp`. |
| **Why It Matters** | A supervisor restart that overlaps the old process's shutdown, or a crash-looping duplicate, looks like a harmless "address in use" failure while it damages the live writer's storage (N1/N2 via a different trigger). Related: `second-writer-on-storage-fails-closed`, `startup-tmp-cleanup-spares-live-writer`, `purge-ctor-never-truncates-live-tail`. |

Priority: P2. Confidence: medium (the ordering is verified; hitting the window needs timing; same-port co-location is a deployment assumption). Fault requirements: process pause, plus a second PBS process sharing the storage and port. Node termination is not needed.

**Open Questions:**

- Is a second pull on the same storage in contract at all (README:473 warns only about purge + fetch)?
- Can two pulls share a host and port in the target deployment? With different ports, B becomes a full second writer and `second-writer-on-storage-fails-closed` applies instead.
- Is the encryption-format stamp (`storage_core.cpp:175-178`) meant to run before the rest of the storage is validated?

## Config changes between runs

### pull-stream-survives-encryption-config-changes — Pull serves the exact source stream across restarts that turn encryption on, turn it off, or change the KEK

| | |
|---|---|
| **Type** | Safety |
| **Property** | For any sequence of restarts that switches between no encryption, KEK A + cipher X, and KEK B + cipher Y (all KEKs kept in the keyring), interleaved with kills, the stream that pull serves (artificial events stripped) is a prefix of the source events as sent. |
| **Invariant** | Workload-side `Always(served stream == prefix of binlog_as_sent())`, with every event CRC-valid. `Always(files with an envelope have ciphertext ≠ source bytes)`. `Sometimes(one session crossed an encrypted↔plaintext file boundary)` and `Sometimes(an encrypted tail was appended to under a different active KEK)` as non-vacuity anchors. |
| **Antithesis Angle** | An append reuses the tail's own envelope (`storage_core.cpp:307-309`), and new files take the current config's envelope (`generate_binlog_encryption_record`). Decryption is per record (`:544-557`). `encryption.test` changes the config only after `FLUSH BINARY LOGS`, never kills PBS and never reads through pull. Antithesis adds kills during the first encrypted binlog's creation, truncation recovery on an encrypted tail, and metadata-PUT failures. |
| **Why It Matters** | Pull is the only in-product decryption path, so a misaligned keystream after a transition makes the backup unrestorable. Per-file mixing appears intended (`encryption.test`), so this property checks readability rather than "never plaintext". Related: `mixed-encryption-storage-served-correctly`, `encrypted-storage-decrypts-to-source`, `ctr-keystream-not-reused-across-truncation`. |

Priority: P1. Confidence: medium (the paths are verified; no concrete defect was found, and the value lies in the combinations no existing test drives). Fault requirements: process kill or pause, S3 faults on the metadata PUT. Node termination is not needed.

**Open Questions:**

- Is per-file mixing (a plaintext tail after encryption is enabled, plaintext new files after it is disabled) the intended design? If not, add an "encrypted storage never gains plaintext" property, which fails today.
- Must every historical KEK stay in the keyring? If operators may remove old KEKs, start-up fails closed for the whole storage.

### rewrite-toggle-between-runs-keeps-storage-resumable — Changing replication mode, rewrite or base_file_name between runs is either refused at start-up or leaves a resumable storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any change to `replication.mode`, the presence of `rewrite`, `rewrite.base_file_name` or `rewrite.file_size` between runs, a start either exits non-zero before modifying any non-`.tmp` object, or it streams and every later run (with the new or the original config) keeps converging. |
| **Invariant** | Workload-side `Always(mode mismatch ⇒ exit ≠ 0 ∧ binlog objects unchanged)` and `Always(after any toggle sequence, each start either refuses with a config-mismatch error or makes progress)`. The failure signature is a repeated "cannot open an existing binlog that is not the latest one for append" across restarts. SUT-side `UNREACHABLE("writer asked to append to a non-tail binlog")` at `storage_core.cpp:275-278`. |
| **Antithesis Angle** | `rewrite` and `base_file_name` are not stored. Turning rewrite on → off in GTID mode makes the source resume at an older source file name that the rewriter already created, so `open_binlog` throws on every restart (traced: `collector_context.cpp:352-374`, `:555-557`, `storage_core.cpp:268-279`). Changing `base_file_name` on a non-empty storage is silently ignored. Antithesis combines toggles with kills just after a rewritten file is created. |
| **Why It Matters** | A routine config edit wedges the backup writer permanently, with no hint that the config is at fault (lead N11, now traced). Mixed base names also stop purge (`storage_core.cpp:339-344`). |

Priority: P2. Confidence: high for the wedge mechanism (traced in code, not reproduced); low for whether toggling is supported at all. Fault requirements: none for the base case; process kill makes the combinations richer.

**Open Questions:**

- Is toggling `rewrite` on an existing storage supported? If not, the expected behaviour is a start-up refusal, which needs the settings recorded in `metadata.json`.
- Should a `base_file_name` change on a non-empty storage be rejected or ignored?
- How does this interact with a fix for ticket 14, which goes through the same name-mismatch branch?

## Clients arriving before the collector has data

### blocking-dump-before-first-flush-eventually-served — A blocking client that connects from the beginning before pull has stored anything is kept open and eventually served (PBS-91 regression)

| | |
|---|---|
| **Type** | Liveness |
| **Property** | A blocking COM_BINLOG_DUMP from `("",4)` or `(<first stored name>,4)` that arrives while the storage is empty, holds only a magic-only first binlog, or while the collector is reconnecting, is not disconnected by PBS and receives the first FDE and transactions once they are flushed. |
| **Invariant** | `Sometimes(client connected in state S ∧ later received FDE + ≥1 transaction)` for S ∈ {empty, magic-only, collector-reconnecting}. Workload-side `Always(no PBS-initiated close of such a session while PBS runs)`. `Sometimes` fits because this is eventual delivery and each state must be shown reachable and served. |
| **Antithesis Angle** | The fix (`storage_core.cpp:463-478`) is tested in one interleaving (`rs_blocking_dump_empty_storage.test`) and by a Hegel model with no real collector. Antithesis varies the client's arrival time against the collector's states: no records, the record created by `open_new_binlog_file_internal` with size 4 (`binlog_name` left empty, `:523-531`), and the first flush. It also adds source flaps and PBS restarts. |
| **Why It Matters** | A replica or `mysqlbinlog --stop-never` started together with PBS (typical in compose, k8s or after a host reboot) must not depend on start order. |

Priority: P1. Confidence: high that the mechanism should work (traced); expected to pass, so this is a regression guard. Fault requirements: source stop/start, partition PBS↔source, PBS kill, process pause.

**Open Questions:**

- Should a request for a name older than the oldest stored file get MySQL error 1236 instead of a silent close? This decides whether the "source purged before PBS's first start" case belongs here.
- With no checkpointing, the magic-only state lasts until the source rotates. Is that acceptable latency for PBS-91's intent?

### blocking-dump-ahead-of-flushed-position-eventually-served — A blocking client that asks for a position PBS will store later waits instead of being silently disconnected

| | |
|---|---|
| **Type** | Liveness |
| **Property** | A blocking dump from (name, position) that the source has but PBS has not yet flushed (empty storage with position > 4; a magic-only binlog; position > the flushed size; a name not yet created) is eventually served from that position in the same session. |
| **Invariant** | `Sometimes(client requested a position beyond PBS's flushed size ∧ later received the event at that position in the same session)`. A supporting workload-side `Always(PBS never closes such a session without sending events or an ERR packet)`. A SUT-side anchor `SOMETIMES(true, "dump requested offset beyond flushed size")` at `storage_core.cpp:513-515`. |
| **Antithesis Angle** | Ticket 26/PBS-96 (`sender_context.cpp:231-233`, validated) and N12 (`storage_core.cpp:513-515` → `network_service.cpp:308-313`, a silent `co_return`). The outcome depends on *when* the client arrives relative to PBS's first record, first flush and rotations, which Antithesis explores. PBS restarts and source lag widen the gap. |
| **Why It Matters** | Switchover (pointing a client that read from the source at PBS) and clients reconnecting with saved positions after PBS starts both hit this. Clients see a dropped connection with no error and retry forever or exit. Expected to fail today. Related: `dump-session-ends-with-eof-or-err-packet`. |

Priority: P1. Confidence: high (mechanism verified, ticket 26 validated). Fault requirements: none for the base case; PBS restarts and source flaps add timing variety.

**Open Questions:**

- Is the intended behaviour to wait, or to answer with error 1236? If error, replace the `Sometimes` with `Always(ERR sent)`.
- How should PBS tell "will exist" from "never existed" for an unknown name?
- Is switchover a supported workflow? Ticket 20 blocks real replicas today, but `mysqlbinlog` reaches this path.

## Coverage

### lifecycle-outcomes-all-reached — Every distinct start-up, rotation and shutdown outcome is exercised

| | |
|---|---|
| **Type** | Reachability |
| **Property** | Each lifecycle outcome the workload is designed to reach does occur at least once. The outcomes are: fresh init, metadata-only open, `.tmp` removed, tail truncated, encryption format stamped, GTID `gtid_purged` bootstrap, tail reopened for append (data present / magic-only), same-process reuse, rotation via ROTATE and via STOP, empty-storage and magic-only dump polls, empty name resolved after waiting, silent session close, stop at EOF / by signal mid-stream / by signal while idle / by read_timeout / by exception, final flush failed, listener stopped vs. died. |
| **Invariant** | One `REACHABLE("<outcome>")` per outcome at the lines listed in the evidence file. Until the SDK is linked, the workload asserts `Sometimes` on the existing distinct log lines. The swallowed final-flush failure and the metadata-only open have no log line, so they need new code. |
| **Antithesis Angle** | The conditional lifecycle properties above are vacuous unless their branch is reached. These anchors also guide Antithesis' exploration towards rare transitions such as a kill during recovery or a signal before `io_ctx.run()`. |
| **Why It Matters** | Without them, a run that never exercised recovery or the shutdown-flush failure looks identical to a passing run. Related: `recovery-paths-exercised`, `resume-replay-paths-exercised`. |

Priority: P1. Confidence: high (each location is verified). Fault requirements: the union of the faults used by the properties above.

**Open Questions:**

- Should SDK markers live in `storage_core` (shared with unit and Hegel tests, so the SDK must compile out) or only in the operation layer?
- Which outcomes are deliberately out of scope (for example, rewrite rotation)? Drop them rather than leaving `Reachable`s that always fail.

## Areas examined

- `pull_operation.cpp` / `fetch_operation.cpp`: the order of logger → config → `signal_set` → storage → collector → `network_service` → jthread → `io_ctx.run()`; destructor order and where `~storage` runs; exit-code decisions.
- `storage_core.cpp` constructor: `.tmp` removal before metadata validation; mode and encryption validation; encryption stamp; index and metadata reconciliation; tail truncation; `purged_gtids_`.
- `storage.cpp`: checkpoint state reset at `open_binlog`; `~storage` swallow; flush-to-boundary.
- `collector_context.cpp`: first-connect bootstrap (GTID `gtid_purged`, position from the first binlog); artificial ROTATE reuse/close/open; rewrite naming on a fresh vs. a non-empty storage; the signal check only between events.
- `reader_context.cpp:520-600`: info-only preamble on resume (`cycle_number_ == 0`, the name matches, position > 4). A rewrite resume within the same process is safe because each `receive_binlog_events` builds a new reader.
- `sender_context.cpp` / `network_service.cpp`: empty-storage, magic-only and ahead-of-flushed states; acceptor binds in the constructor after storage open; listener exit conditions.
- `s3_storage_backend.cpp`: zero-length range GET returns empty (no S3 call), and resize is download + truncate + single PUT (atomic per object, temp copy leaks on kill).
- MTR `encryption.test` (config changes are intended to mix per-file envelopes), `rs_blocking_dump_empty_storage.test`, the PROPERTY_TESTS.md sender entry.
- Checked and **not** turned into properties: changing checkpoint settings between runs (buffer state is per process, so harmless); a first start in GTID mode racing a source `PURGE` between `SELECT @@gtid_purged` and the dump (gives 1236, transient, cured by restart); a crash during the empty-storage `metadata.json` init (`put_object` is atomic); start-up recovery idempotence under kills (covered by `startup-recovery-is-restartable` and `restart-after-kill-converges-to-source`); the sender skipping ROTATE when the next binlog is magic-only (covered by `sender-file-switch-emits-artificial-rotate`); fetch EOF exit 0 before the final flush (covered by `fetch-exit-zero-implies-tail-flushed`).

## Cross-property assumptions

- The workload controls the PBS process lifecycle directly (start, SIGTERM, restart with a different config). Several properties require the workload, not docker or Antithesis, to send signals, so that "graceful" is known.
- PBS logs to a file that the workload preserves across restarts (`file_logger` truncates on open). The log-based `Sometimes`/`Reachable` checks depend on this until the SDK is linked.
- `AWS_EC2_METADATA_DISABLED=true` is set unless ticket 16 is under test. Otherwise start-up latency dominates `sigterm-during-startup-is-honoured`.
- Clients for the PBS-91/96 properties are `mysqlbinlog` or a light Python COM_BINLOG_DUMP client, not MySQL replicas (ticket 20).
- Exit 0 from pull means "stopped by signal without a collector exception". This is assumed as the graceful marker; fetch stopped by a signal exits 1 by design.
- These properties overlap with other agents' slugs as noted inline. Synthesis should merge `graceful-stop-leaves-recovery-free-storage` with `fetch-exit-zero-implies-tail-flushed` and `sigterm-mid-write-leaves-boundary-storage` if it wants one "exit code honesty" property, and `pull-stream-survives-encryption-config-changes` with `mixed-encryption-storage-served-correctly`.
