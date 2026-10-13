# Property discovery, focus 5: resource boundaries

Commit 99de682. All SUT-side assertions suggested here are **missing** (existing-assertions.md: no Antithesis instrumentation exists). Evidence files are in `antithesis/scratchbook/properties/<slug>.md`.

## Category: local resource leaks and exhaustion

### s3-local-buffer-bounded-across-crashes — S3 local buffer does not grow per crash

| | |
|---|---|
| **Type** | Safety |
| **Property** | Across any number of PBS crashes and restarts on an S3 storage, the local buffer directory holds no more than a small constant multiple of the largest binlog size. |
| **Invariant** | Workload `Always(buffer_bytes ≤ C·max_stored_binlog_size + slack)`, checked after every kill/restart cycle. It must hold every time, so `Always` is the right type. Add `Sometimes(kill happened while a temp file existed)` to show the window was reached. |
| **Antithesis Angle** | The workload SIGKILLs fetch or pull subprocesses at random instants: during the append-download in `do_open_stream`, mid-upload, or during startup `do_resize_object`. Each kill skips `~s3_storage_backend` (`s3_storage_backend.cpp:640-652`) and leaves a full binlog copy. Partial-download failures leak even without a kill (`:828-832`). No node termination is needed. |
| **Why It Matters** | The README sample config sets an explicit `fs_buffer_directory`, which is never cleaned. Leaks accumulate until the local disk fills, and then every run fails at startup or at the first checkpoint (a wedge), feeding the ENOSPC paths. Lead N10. |

Priority: P1. Confidence: high (the mechanism was read in code; the growth rate depends on kill timing). Fault requirements: workload-driven SIGKILL of the PBS subprocess, plus an S3 stand-in (MinIO). Optionally, network faults on the PBS↔S3 link for the partial-download path.

**Open Questions:**

- Is leaking a full copy per crash acceptable, given that README promises cleanup only "when the server exits"?
- Can `fs_buffer_directory` be shared by several live PBS processes, which would constrain any startup cleanup?
- Does the CRT client buffer anything else on local disk?

### listener-survives-fd-exhaustion — the replication-source listener outlives a transient accept() error

| | |
|---|---|
| **Type** | Safety |
| **Property** | After a burst of connections that exhausts file descriptors has drained, pull either serves new clients again or has exited. It never stays alive and collecting with a dead listener. |
| **Invariant** | SUT-side `UNREACHABLE` at the throw for a non-shutdown accept error (`network_service.cpp:648-651`): no execution may reach it. Workload `Always(!(pbs_alive && probe_no_greeting_within_T))` after the flood ends. `Sometimes(accept hit EMFILE)` confirms the boundary was reached. |
| **Antithesis Angle** | A low `ulimit -n` in the PBS container, plus workload-held pre-auth connections. The race is whether the collector's next `.tmp`/S3 temp open (which exits pull) or the next `accept()` (which kills only the listener) hits EMFILE first. Node hang or throttling of flood clients lengthens how long pre-auth sessions live. |
| **Why It Matters** | The replication source fails silently: the TCP port still accepts handshakes, so health checks pass while no client is ever served. Lead N6, assumption A8. |

Priority: P1. Confidence: high (the code path is verified; reaching it requires a lowered fd limit). Fault requirements: none from Antithesis. Needs a workload connection flood and a container fd limit.

**Open Questions:**

- Is pull expected to run under a supervisor with a protocol-level health check, or should PBS retry accept or exit?
- What fd limit will the Antithesis PBS container have (a 1M default makes this unreachable)?
- Should concurrent sessions be capped?

### idle-dump-session-reclaimed — PBS reclaims sessions of dump clients that have gone away

| | |
|---|---|
| **Type** | Liveness |
| **Property** | The session of a dump client that disconnected or vanished is released within `write_timeout` after the next flushed data arrives, so stale sessions do not accumulate. |
| **Invariant** | Workload `Always(PBS established connections on the listener port ≤ live clients)`, evaluated after a flush plus `write_timeout` plus δ. It is a bounded-time check that must hold at each evaluation point. `Sometimes(stale sessions > 0 before the flush)` documents the idle window. |
| **Antithesis Angle** | Client container kills, hangs and partitions while the session sits at EOF. At EOF a blocking dump never reads or writes the socket (`network_service.cpp:314-320`), there are no heartbeats and no keepalive. Long idle source periods (and LG3 without checkpointing) stretch the window. |
| **Why It Matters** | fds, memory and info-level log lines every 500 ms (`sender_context.cpp:384`) grow linearly with client reconnects while the source is idle. This eventually feeds fd exhaustion. |

Priority: P2. Confidence: medium (the mechanism is verified; the impact depends on client reconnect behaviour). Fault requirements: kill, hang or partition of the client container. Network faults on the PBS↔client link.

**Open Questions:**

- Should PBS detect dead idle clients without new data (heartbeats, keepalive, socket read)?
- Does the EOF poll do backend I/O on S3? (Reading the code says no.)

### enospc-write-fails-closed-without-duplicates — running out of disk never corrupts the storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | When the storage volume or the S3 buffer volume runs out of space at any point in a write, rotation or startup recovery, the storage, once space is available again, opens, resumes and holds a duplicate-free byte prefix of the source. |
| **Invariant** | Workload `Always` after each PBS exit (with ballast freed, then fetch to EOF): exit 0, `storage_holds_prefixes()` and `check_complete_copy()` hold, no GTID occurs twice in the stored bytes, and `added_gtids` matches the bytes. `Always` because corruption must never happen. SUT-side `SOMETIMES(metadata save failed after a successful data append)` marks the dangerous window. |
| **Antithesis Angle** | A size-limited volume plus a workload "filler" that allocates and frees ballast at random times. The interesting interleaving: data fits, the `.json.tmp` fails, then space is freed **before** the `~storage` retry, so the retry re-appends and persists `size = old + 2n` (ticket 18 mechanism, `storage_core.cpp:307-321`, `storage.cpp:68-77, 286-296`). |
| **Why It Matters** | Disk-full is the most common real resource fault for a backup tool. Permanent duplicate transactions silently corrupt point-in-time recovery. |

Priority: P0. Confidence: medium (the duplicate path needs a narrow timing window; short-write semantics of `ofstream` are not verified). Fault requirements: a size-limited tmpfs or loop volume in the PBS container (fallback: the `extra/qa/fsfault.c` LD_PRELOAD shim). No Antithesis-native fault needed, but process kill widens coverage.

**Open Questions:**

- Is the "ENOSPC, then space freed" duplicate the same defect as ticket 18 or a separate finding?
- Does a short `ofstream` write leave partial block bytes on disk?
- Can the Antithesis environment provide a size-limited volume in the PBS container?

### fetch-exit-zero-implies-tail-flushed — fetch exits 0 only if the buffered tail actually reached storage

| | |
|---|---|
| **Type** | Safety |
| **Property** | A fetch run that exits 0 has persisted every transaction it received before the source EOF. |
| **Invariant** | Workload `Always(fetch_exit == 0 ⇒ storage holds a complete copy up to the source position snapshotted before the run)`. It is an implication checked on every fetch run. SUT-side `UNREACHABLE` inside the swallowing `catch (...)` of `~storage` (`storage.cpp:72-75`). |
| **Antithesis Angle** | At the source EOF, `receive_binlog_events` returns `true` without flushing (`collector_context.cpp:184-187`). `fetch_operation.cpp:112-115` sets success, and the real flush happens later in `~storage`, which swallows errors. A full volume, a PBS↔S3 partition, or MinIO latency at that instant produces exit 0 with the tail missing. Antithesis network faults on the S3 link reach this naturally. |
| **Why It Matters** | Cron-driven backups trust exit 0. With the default config the whole active binlog is lost behind a success status. Lead N8 plus the implicit exit-code contract. |

Priority: P0. Confidence: high (the code path is verified line by line; the trigger is an ordinary fault at the end of a run). Fault requirements: network faults between PBS and S3, or a full storage volume. No process faults.

**Open Questions:**

- Is "fetch exit 0 ⇒ complete copy at EOF" an intended contract?
- Is pull's exit 0 after a failed final flush on SIGTERM acceptable?

## Category: throughput, backpressure and capacity

### stalled-dump-client-isolated — a stalled downstream client delays neither other clients nor the collector beyond write_timeout

| | |
|---|---|
| **Type** | Liveness |
| **Property** | A downstream client that stops reading delays no other client or the collector beyond `write_timeout`, and its own session is dropped within `write_timeout`. |
| **Invariant** | Workload `Always`: healthy clients receive every flushed transaction within `write_timeout + checkpoint_interval + δ`, and the stalled session disappears within `write_timeout + δ`. Both are evaluated only while storage and source links are fault-free. `Sometimes(source dropped PBS while ≥ 4 sessions were catching up)` reports the upstream spiral if it occurs. |
| **Antithesis Angle** | Node hang of a client container, or congestion on its link, fills its TCP window. All sessions share one io thread with a synchronous `fetch_event_block` under the shared lock (`storage_core.cpp:461`). The collector needs the unique lock (`:305`). This explores reader/writer lock interleavings under load. |
| **Why It Matters** | One bad replica must not degrade backup ingestion or other replicas. W3 hypothesis: downstream load propagates upstream. |

Priority: P1. Confidence: medium (the async write timeout looks correct; lock-preference starvation is a hypothesis). Fault requirements: node hang or throttling on client containers, network congestion on the PBS↔client links.

**Open Questions:**

- Does `std::shared_mutex` on the target libc prefer readers (risk of collector starvation)?
- What latency bound δ is acceptable?

### s3-pull-converges-despite-reupload-growth — pull on S3 catches up after load stops, even with whole-object re-uploads

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After source writes stop and faults stop, pull on S3 eventually stores and serves everything the source wrote, even when the active binlog is large and checkpoints are small. |
| **Invariant** | Workload `Sometimes(within T of quiescence: stored tail == source position and live dump clients have it)`, evaluated in each quiet period. `Sometimes` because it is an eventual-progress goal. SUT-side `SOMETIMES(single checkpoint upload ≥ 64 MiB)` and `SOMETIMES(source dropped the dump during an upload)` as exploration anchors. |
| **Antithesis Angle** | Each checkpoint re-uploads the whole file under the exclusive lock (`s3_storage_backend.cpp:850-863, 933-938`, `storage_core.cpp:305`). MinIO throttling or latency stretches uploads past the source's `net_write_timeout`, which causes drop → flush (another full upload) → `idle_time` sleep → resume. This tests whether lag can grow without bound. |
| **Why It Matters** | The amplification is documented (README:671). Whether it can make pull fall permanently behind, or exit through a non-2013 error, is not. Ticket 17. |

Priority: P1. Confidence: medium (the mechanism is verified; the spiral is a hypothesis). Fault requirements: node throttling or latency on the S3 container, network latency on the PBS↔S3 and PBS↔source links, and a quiet period at the end (stop faults).

**Open Questions:**

- What are the CRT default request timeout and retry policy (could a PUT hang forever)?
- Does a source-side drop of a stalled dump surface as 2013 (reconnect) or another errno (exit)?
- Is any catch-up bound expected by the product?

### large-event-served-or-rejected-cleanly — large binlog events are stored and served, or refused with an explicit error, never a silent loop

| | |
|---|---|
| **Type** | Safety |
| **Property** | A large binlog event (≥ 16 MiB, up to the source's `max_allowed_packet`) is ingested and either delivered intact to dump clients or refused with an explicit MySQL error packet. A session is never closed silently in a way that traps a reconnecting client forever. |
| **Invariant** | Workload `Always`: each dump session spanning a large event ends with the event received or an error packet observed, and fetch/pull never exits 0 short of the source. SUT-side `UNREACHABLE` at the frame-limit throw (`network_io_operations.cpp:144`). Liveness half: `Sometimes(≥ 16 MiB event delivered)`, a known failure today (ticket 10). |
| **Antithesis Angle** | Network resets make several clients re-fetch the same big event concurrently. Each session tops up its buffer to the event size (`sender_context.cpp:371-378`), so RSS ≈ N × event. With a PBS memory limit and throttling, OOM interacts with the reconnect loop. |
| **Why It Matters** | Big BLOB rows and bulk statements are routine. Today a replica stops permanently at the first ≥ 16 MiB event with no error, and events above 256 MiB cannot be read back at all (`basic_storage_backend.hpp:35`). |

Priority: P2. Confidence: high for egress (ticket 10 validated); low for ingress (the libmysqlclient packet limit is unverified). Fault requirements: none essential. Network resets on client links and a memory limit add the resource angle.

**Open Questions:**

- What is libmysqlclient's effective `max_allowed_packet` for `mysql_binlog_fetch` when the option is unset?
- Is an explicit error packet acceptable instead of multi-frame support?

### collector-memory-bounded-by-checkpoint — collector buffer memory is bounded by the checkpoint size plus the open transaction

| | |
|---|---|
| **Type** | Safety |
| **Property** | With size checkpointing enabled, the collector's in-memory event buffer never exceeds `checkpoint_size` plus the open transaction plus one event. PBS within its memory limit is never OOM-crash-looped by transactions below a documented size. |
| **Invariant** | SUT-side `ALWAYS(event_buffer_.size() ≤ checkpoint_size_bytes_ + open-transaction bytes + one event)` after each `storage::write_event` (`storage.cpp:143-202`). It must hold on every call. Workload `Always(no OOM kill of PBS when the largest source transaction < X)`. `Sometimes(EOF reached after a ≥ 64 MiB transaction)`. |
| **Antithesis Angle** | A memory-limited PBS container, varied transaction sizes, and kills or restarts that cause re-delivery of the same big transaction. The interval checkpoint fires only on event arrival, so source pauses (node hang of the source) keep data buffered. |
| **Why It Matters** | Without checkpointing (the default), a whole binlog (up to 1 GiB) sits in RAM and is lost on a kill. A transaction larger than memory turns into an OOM crash loop with no progress. Ticket 2 was a checkpoint-ignored regression. |

Priority: P2. Confidence: medium (buffering is verified; whether default-config whole-binlog buffering is intended is unknown). Fault requirements: a container memory limit. Process kills and node hang on the source are optional.

**Open Questions:**

- Is whole-binlog RAM buffering under the default config intended?
- What memory limit will the PBS container have?

### s3-no-orphaned-multipart-uploads — killed or failed checkpoint uploads do not leave incomplete multipart uploads in the bucket

| | |
|---|---|
| **Type** | Safety |
| **Property** | Repeated crashes or failed uploads do not accumulate incomplete multipart uploads under the storage prefix, and an interrupted upload never changes the visible object to anything other than the old or the new full content. |
| **Invariant** | Workload `Always(incomplete multipart uploads older than T ≤ k)` via `ListMultipartUploads` after each cycle. `Always(GetObject returns the old or the new full content)`. `Sometimes(an incomplete upload observed)`, to confirm the property is not vacuous. |
| **Antithesis Angle** | Workload SIGKILLs and PBS↔MinIO partitions during whole-object re-uploads of large binlogs (`s3_storage_backend.cpp:369-386, 933-938`), which the CRT likely splits into multipart. |
| **Why It Matters** | Hidden bucket growth and cost on AWS, and the atomic-replace assumption behind SG4 on S3. |

Priority: P2. Confidence: low (CRT multipart use for these bodies is not verified). Fault requirements: workload-driven kill, network partition of the S3 link.

**Open Questions:**

- Does `S3CrtClient::PutObject` (SDK 1.11.774) use multipart for an iostream body, and above what size?
- Is relying on an operator lifecycle rule acceptable?

## Areas examined

- **S3 temp files** (`s3_storage_backend.cpp:554-652, 765-863, 928-946`): per-process UUID directories versus an explicit `fs_buffer_directory`; leaks per kill, on partial download, and on resize failure. Properties: `s3-local-buffer-bounded-across-crashes`, plus the ENOSPC follow-on.
- **Whole-object re-upload per checkpoint** (ticket 17, README:671): documented, so it is not asserted as a bug. Turned into the convergence property and a multipart-orphan property.
- **accept() EMFILE** (`network_service.cpp:629-669`): verified; `listener-survives-fd-exhaustion`.
- **Session lifetime and backpressure**: pre-auth and command-loop reads are bounded by `read_timeout` per frame (`network_io_operations.cpp:63-117`). Writes are bounded by `write_timeout`. Blocking-dump EOF has no bound (`idle-dump-session-reclaimed`). One io thread with synchronous storage reads (`stalled-dump-client-isolated`).
- **16 MiB frames, the 256 MiB backend read cap, the 1 GiB sender cap**: `large-event-served-or-rejected-cleanly`.
- **Collector RAM buffering** (`storage.cpp:143-302`): `collector-memory-bounded-by-checkpoint`.
- **Disk-full**: the filesystem stream, `put_object` `.tmp`, the S3 buffer, `~storage` swallowing. Properties: `enospc-write-fails-closed-without-duplicates` and `fetch-exit-zero-implies-tail-flushed` (the latter found while tracing the fetch EOF path: `collector_context.cpp:184-187` returns without flushing).
- **Binlog-number exhaustion** (`composite_binlog_name.cpp:54-58`, 6 digits, < 1,000,000): **no property**. It needs about 10⁶ rotations, or a hand-forged storage. Also, MySQL itself names its 1,000,000th file `binlog.1000000`, which `parse()` rejects (the separator-position check). This is deterministic and impractical to reach in an Antithesis run, so it is left as a note for a unit or Hegel test.
- **4 GiB positions** (`collector_context.cpp:146, 378`, `static_cast<std::uint32_t>`): **no property**. It needs a single binlog above 4 GiB, which is impractical in a simulation run and deterministic. Suited to a unit test with a forged record size.
- **Logger** (`file_logger.cpp:29-40`): it truncates on open and silently stops after a write error (sticky failbit, no reopen). Each idle session logs at info every 500 ms, and the sample config uses debug. This causes log growth, but it is not a correctness property for Antithesis. Mentioned in `idle-dump-session-reclaimed`.
- **libmysqlclient ingress packet limit**: no `MYSQL_OPT_MAX_ALLOWED_PACKET` is set (`easymysql/connection.cpp:225-310`). It remains an open question in the large-event property.

## Cross-property assumptions

- PBS runs as a workload-controlled subprocess (fetch) or a container whose process the workload can signal. Kills are workload-driven, so Antithesis node termination is not required for any property here.
- Size-limited volumes (tmpfs `size=` or a loop device) and a lowered `ulimit -n` can be configured in the PBS container. Without them, three properties (ENOSPC, fetch-exit, fd exhaustion) fall back to `extra/qa/fsfault.c` or become unreachable.
- MinIO (or Versity) is the S3 stand-in and supports `ListMultipartUploads`. Network faults on the PBS↔S3 link are enabled.
- A debug or ASan build is used, so the asserts in `network_io_operations.cpp` are live.
- Oracles reuse `extra/qa/fetch_crash_test.py` (`storage_holds_prefixes`, `check_complete_copy`) and the fd counter in `extra/qa/network_fault_test.py:198`.
- Relationships to other agents' slugs (seen in `properties/`): `enospc-write-fails-closed-without-duplicates` triggers the same mechanism as `failed-flush-retry-never-duplicates` and `failed-flush-never-reappends-data`, reached through disk exhaustion. `s3-pull-converges-despite-reupload-growth` overlaps `blocking-dump-catches-up-after-io-stall` and `sigterm-not-starved-by-storage-lock`. `fetch-exit-zero-implies-tail-flushed` complements `backend-error-exit-leaves-openable-storage`.
