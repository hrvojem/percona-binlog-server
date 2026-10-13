# Property discovery: evaluation gaps G2, G3, G4, G6 (restore, durability, fetch liveness, S3 round trips)

This is a targeted pass on gaps found by catalog evaluation (`evaluation/wildcard.md` W5, W6, W8; `evaluation/coverage-balance.md` F4, F7, F11). It adds four properties. Each extends, by reference, existing properties that own overlapping assertion sites. None re-proposes an existing check:

- the complete-copy and exactly-once `Always`s stay with `graceful-exit-leaves-flushed-storage` and `stored-transactions-exactly-once-across-resumes`;
- the prefix-hash and stale-PUT checks stay with `stored-state-never-regresses`;
- the memory `ALWAYS` on `event_buffer_.size()` stays with `collector-memory-bounded-by-checkpoint`;
- the generic tail-truncation marker stays with `recovery-paths-exercised`.

Two questions were investigated in this pass: the effective libmysqlclient read timeout, and the S3 sticky-failbit hypothesis. Both have Investigation Logs in their evidence files. Neither is fully resolved.

## End-to-end restorability

### stored-binlogs-replay-to-source-state — Replaying the stored binlogs reproduces the source's tables

| | |
|---|---|
| **Type** | Safety |
| **Property** | At quiescence, `mysqlbinlog --verify-binlog-checksum <stored binlogs in index order> \| mysql` into a freshly initialised scratch `mysqld` (same version as the source) exits 0. It yields the same per-table checksums and pending XA set as the reference. The reference is the source itself when the storage has converged, or an identical replay of the source's own binlogs cut at the stored GTID set / tail position when it has not. This holds for plain, encrypted (decrypted offline) and rewrite-mode storages, across all event kinds the load generator produces. |
| **Invariant** | Workload `Always`, one message per storage kind: "replay of stored binlogs matches the source state (position)", "(gtid)", "(rewrite)", "(encrypted)". The replay's exit status is part of the check. `Always` fits because any divergence means a restore from this backup produces wrong data. A secondary strict variant, `--skip-gtids`, gets its own message, "strict replay of stored binlogs matches the source state". It catches duplicates that GTID auto-skip hides in the realistic variant. Vacuity: `Sometimes("replay restore covered a storage containing <kind> transactions")` for each of compressed payload, XA prepare/commit, PARTIAL_JSON, MINIMAL row image, DDL, and tagged GTID (8.4 or later), plus `Sometimes("replay restore ran on a storage that saw at least one writer kill")`. No SUT instrumentation is required. Optional anchor (missing): `SOMETIMES("rewrite split a binlog inside an XA prepare/commit pair")`. |
| **Antithesis Angle** | Kills, rotations, checkpoints and rewrite `file_size` splits land next to boundary-sensitive event kinds: an XA PREPARE without its COMMIT at a cut, a compressed `TRANSACTION_PAYLOAD`, DDL with no BEGIN/XID. Network and S3 faults force resumes at those points. The oracle judges the result through the consumer the backup exists for. That is where the byte oracles are weakest: in rewrite mode (CRCs, preamble, `transaction_length` regenerated), on encrypted storages, and for events after a terminal ROTATE/STOP. |
| **Why It Matters** | PITR via `mysqlbinlog \| mysql` is the product's purpose (README:3, 711). Today no oracle applies a backup. The check also separates cosmetic file differences (ticket 14's dropped ROTATE) from restore-breaking ones across the many expected-to-fail properties. It answers `stored-rotate-or-stop-is-last-event`'s "what does a downstream tool do" question empirically. Prior art: `mtr/binlog_streaming/t/rs_tail_reuse.test:70-110` replays a pull dump with a row-signature compare, in one fault-free scenario. |

**Priority:** P1 · **Confidence:** medium (the oracle is standard and the MTR precedent exists; the reference construction for non-converged storages and the replay cost are unverified) · **Found by:** evaluation gap G2 (wildcard W5, W8) · **Fault requirements:** workload SIGKILL of PBS, network faults (native); **custom:** `s3_fault_proxy` for S3 write failures (optional). Node termination is not needed. · **Runs in:** needs new driver: `pbs/finally_replay_restore.py` for `fetch_fs`, `pull_fs` (after pull stops gracefully) and `s3`. It requires `bin/mysqld` and `bin/mysql` from the same generic tarball in `runtime-base` (today only `mysqlbinlog` is planned), the offline decryptor from W2, and an event-kind dimension in `workload/parallel_driver_source_load.py` reusing the `extra/qa/event_variety_test.py` generators.

**Evidence:** `properties/stored-binlogs-replay-to-source-state.md`

**Open Questions:**

- What does `mysqlbinlog` in local-file mode do with events after a ROTATE/STOP inside a file?
- Is the prefix-form reference (source binlogs cut by `--include-gtids` / `--stop-position`) exactly the source state at that point, including XA and compressed groups?
- Is the per-timeline replay cost (scratch `mysqld` init plus replay) affordable, or must the check be sampled?
- Should the strict `--skip-gtids` variant be asserted or only logged, given that it re-reports known duplicates?
- Is offline decryption a supported restore path, or only reading through pull?

## Recovery-point durability

### checkpoint-bounds-unpersisted-data — With checkpointing on, received data is persisted within a bounded size and time

| | |
|---|---|
| **Type** | Safety |
| **Property** | With `checkpoint_size` set, the complete-transaction bytes PBS has received but not persisted stay below `checkpoint_size` after every received event. With checkpointing on and PBS connected and caught up on healthy links, a transaction is persisted, and so survives a SIGKILL, within the configured bound. The bound is: `read_timeout + δ` if the source goes quiet; `checkpoint_interval + inter-event gap + δ` under steady traffic; or once `checkpoint_size` more bytes have followed it. δ covers transfer and the backend write, including S3's whole-tail re-upload. |
| **Invariant** | SUT-side (missing): `ALWAYS("unflushed complete-transaction bytes stay below checkpoint_size")` at the end of `storage::write_event` when size checkpointing is on (`get_ready_to_flush_position() − get_flushed_position() < checkpoint_size_bytes_`, `storage.cpp:143-202`). This property owns that message; `collector-memory-bounded-by-checkpoint` keeps its distinct buffer-size `ALWAYS`. Anchor (missing): `SOMETIMES("a flush wrote complete transactions older than checkpoint_interval")` in `flush_event_buffer_internal`, which marks the interval-gap state as reached. Workload: a bounded `Always("with checkpointing on, a marker transaction is persisted within the configured bound")`, evaluated in an `ANTITHESIS_STOP_FAULTS` window. The driver waits for catch-up, commits a marker, picks a follow-up pattern (quiet / steady filler / ≥ `checkpoint_size` filler), optionally SIGKILLs PBS at `t + B + ε`, then reads the persisted `.json` (`size` / `added_gtids`). `Always` fits because the bound is a durability promise that must hold on every evaluation. Liveness phrasing would hide a single late flush. |
| **Antithesis Angle** | The checkpoint baselines are reset in several places: open/rotation (`storage.cpp:118-131, 204-215`), reconnect reusing an open binlog, rewrite's local rotation at `file_size`, and the read-timeout flush that does not reset the baseline (`collector_context.cpp:189-203`). Kills and partitions drive every one of those transitions. Exploration of event timing finds the gap where the interval trigger fires only on the next event (`storage.cpp:179-201`). The S3 re-upload time grows with the tail size (ticket 17), so the flush itself can outlast the interval under latency. |
| **Why It Matters** | SG8 (README:619, 626) is the recovery-point promise: how much received data a crash loses. It is asserted nowhere today, and its best check sat in a P2 blocked on `mem_limit` (W6). The interval half has a real expectation gap. The actual bound is about `max(checkpoint_interval + gap, read_timeout)`, not `checkpoint_interval`, so with the README sample (`read_timeout` 60, `checkpoint_interval` 30s) a quiet period after a burst leaves up to 60 s unpersisted. Ticket 2 (checkpointing silently ignored, fixed in 3804c25) shows this logic can regress unnoticed. Its Hegel test covers the size bound only at the model level, without reconnects or kills. |

**Priority:** P1 · **Confidence:** high for the size `ALWAYS` (mechanism traced, Hegel precedent); medium for the time bound (contract wording and the effective read timeout are open) · **Found by:** evaluation gap G3 (wildcard W6, coverage-balance F11) · **Fault requirements:** workload SIGKILL of PBS and network faults to drive the baseline transitions; `ANTITHESIS_STOP_FAULTS` for the quiet-window check; `checkpoint_size` / `checkpoint_interval` drawn on (not off) in the timelines that evaluate it. No custom faults or memory limit are needed. · **Runs in:** `pull_fs` and the pull-like arm of `s3` (SUT `ALWAYS` in every template, including `fetch_fs`). Needs new driver: `pbs/parallel_driver_checkpoint_rpo.py` (quiet window, marker, follow-up pattern, optional kill, metadata read; reuses the shape of `extra/qa/repro_broken_checkpointing.py`).

**Evidence:** `properties/checkpoint-bounds-unpersisted-data.md`

**Open Questions:**

- Is the interval contract "flushed within `checkpoint_interval`" (README:626 reading) or "flushed on the first event after it" (ticket 2's expected behaviour)?
- Does the size guarantee's "+ one transaction" slack count the open transaction?
- Does the effective read timeout equal `connection.read_timeout` or three times it? `(partial: 1× in the MySQL 9.7.1 client source, only EINTR is retried; the 8.0 libmysqlclient21 the image links was not checked)`
- Should PBS flush on a local timer (or request heartbeats) instead of only on event arrival?

## Fetch liveness

### fetch-converges-after-faults-stop — After faults stop, repeated fetch runs reach exit 0 within N attempts

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After faults stop, the harness has cleared custom fault rules and stragglers, the source is reachable and the source load has stopped, at most N (= 3) consecutive `fetch` runs, each within a timeout `T_a`, include one that exits 0. That success then lets the existing complete-copy and exactly-once checks run. |
| **Invariant** | Workload `Always("fetch reaches exit 0 within N attempts after faults stop")` in `eventually_converge.py`. The details carry mode, encryption, rewrite, backend, and each attempt's exit code and last error line, so triage buckets ticket 15, ticket 18/`CR_1236`, lead (b) aborts and hangs without new assertions. This is a bounded `Always`, not `Sometimes`: a `Sometimes` passes when any one timeline converges and would hide a wedge in another, which is the silence this property removes. Vacuity: `Sometimes("fetch converged after a timeline with a writer kill during rotation")`, `Sometimes("fetch converged after an exhausted S3 PUT failure")`, `Sometimes("fetch converged on an encrypted storage")`, `Sometimes("fetch converged on a rewrite storage")`. Harness guard: `Unreachable("fetch convergence preconditions not met")`. No new complete-copy assertion; those stay with `graceful-exit-leaves-flushed-storage` and `stored-transactions-exactly-once-across-resumes`. Optional SUT anchor (missing): `REACHABLE("fetch reached EOF and is about to report success")` at `fetch_operation.cpp:112`. |
| **Antithesis Angle** | Any wedge left by the fault phase surfaces here as a red result instead of an unevaluated `Always`. That covers ticket 15 rotation kills, ticket 18 metadata-PUT failures in position mode (`CR_1236`, not a validator error, so outside `kill-anywhere-storage-reopens`), lead (b) source kills just after rotation, interrupted purges, and S3 hangs with no overall request timeout. It also catches wedges nobody has found yet. Each fault timing that leads to a wedge is a distinct, replayable timeline. |
| **Why It Matters** | Fetch is the cron backup workflow (sut-analysis W1/W2/W4: "silent, catastrophic"). Without this property, a timeline that wedges reports green on the P0 exactly-once property because the property never runs. Pull has `pull-chain-converges-after-heal`; fetch had nothing. **Expected to fail today** wherever ticket 15, ticket 18 (position mode) or lead (b) is hit, with the same root causes their owning properties report. |

**Priority:** P1 · **Confidence:** high (the gap is structural; known wedge routes are catalogued and code-traced by their owners) · **Found by:** evaluation gap G4 (coverage-balance F4) · **Fault requirements:** none during the check (faults stopped). The preceding phase needs the union of the wedge routes: workload SIGKILL of fetch during rotation, network faults on pbs↔s3 and source↔pbs, source SIGKILL via the restart wrapper; **custom:** `s3_fault_proxy` / `fsfault` for the ticket 18 route. · **Runs in:** `fetch_fs` and the fetch-like arm of `s3`, by extending the existing `pbs/eventually_converge.py` (adds a precondition step, an attempt bound and the assertion).

**Evidence:** `properties/fetch-converges-after-faults-stop.md`

**Open Questions:**

- What attempt bound N is the contract: "the next run after any crash succeeds" (N = 1) or "a few runs" (N = 3)?
- Are manual-repair fail-stops (ticket 15, the ticket 18 metadata variant) accepted, so that those buckets should be excluded rather than failed?
- How should `T_a` separate a hang from slow S3 progress under whole-object re-uploads?
- Do driver child processes outlive their driver when the `eventually_` command starts?

## S3 backend round trips

### s3-whole-object-put-publishes-intended-bytes — S3 startup truncation and checkpoint re-uploads publish exactly the intended bytes, including after a failed transfer

| | |
|---|---|
| **Type** | Safety |
| **Property** | On S3: (1) startup recovery's `resize_object` (GET whole tail, then local `resize_file`, then whole-object PUT) leaves the tail object exactly `metadata.size` long, with bytes equal to its previous prefix, and never zero-extended. (2) After a `PutObject` of `tmp_fstream_` fails, any later successful upload in the same process publishes the whole local buffer from offset 0, never a suffix or a partial body. |
| **Invariant** | SUT-side (all missing): `ALWAYS("S3 resize download covers the requested size")` before `resize_file` in `do_resize_object` (`s3_storage_backend.cpp:782`); `REACHABLE("S3 tail truncated by resize_object during startup recovery")` after its PUT (a backend-local site, distinct from `recovery-paths-exercised`'s marker at `storage_core.cpp:979`); `SOMETIMES("S3 upload succeeded on a stream whose previous upload failed")` in `upload_tmp_stream_internal`; `ALWAYS("S3 checkpoint upload published the whole local buffer")` after a successful upload, using a HEAD only in the Antithesis build (`ContentLength == local temp size`). Workload (`s3` template): `Always("S3 tail object equals its metadata size after startup recovery")`, evaluated when the recovery log line appeared and no append followed yet; `Sometimes("S3 startup truncation observed in a writer log")` as a log-line fallback for the `REACHABLE`. Content after appends is judged by the existing `stored-state-never-regresses` prefix hash and `stored-binlog-byte-prefix-of-source`, which this property feeds triggers to. `Always` fits because a wrong published body is silent corruption that later size checks cannot detect. |
| **Antithesis Angle** | Partitions and stalls on pbs↔s3 **mid-body** during the upload of a large tail are the only route to the dangerous outcome (a suffix published as the whole object). The `s3_fault_proxy` cannot produce it, because it reads the whole request before acting, and CRT's throughput monitor may abort a stalled transfer. Kills between the data PUT and the `.json` PUT, and ticket 18's data-upload variant (the destructor retry uploads old+2n, the next start must truncate), drive the resize path. `truncate_body` on startup GETs targets the download half. |
| **Why It Matters** | `s3_storage_backend.cpp` is a 16-commit churn hotspot, and its mechanics differ from the filesystem backend's, yet it had only one S3-specific property (F7). Ticket 18's S3 data-upload variant is called "self-healing" only because this truncation round trip is assumed exact. A zero-padded or suffix tail of the right length passes every size check, and on encrypted storages it decrypts to garbage that only content oracles see. |

**Priority:** P2 · **Confidence:** medium (resize mechanism traced; the sticky-failbit hypothesis refuted for HTTP-error failures by ticket 18's repro numbers; mid-body abort behaviour unverified) · **Found by:** evaluation gap G6 (coverage-balance F7; focus 8 lead) · **Fault requirements:** network partitions, latency and stalls on pbs↔s3 (native, timed inside uploads), workload SIGKILL of the writer; **custom:** `s3_fault_proxy` 5xx on six or more consecutive binlog PUTs and `truncate_body` on GETs. Node termination is not needed. · **Runs in:** `s3` (both arms), by extending `pbs/parallel_driver_s3_faults.py` with the triggers above. Needs new driver: `pbs/anytime_s3_tail_recovery.py` (reads the writer log and HEADs the tail through `s3:9000`). Config draw: a large `max_binlog_size` with a small `checkpoint_size` in some timelines, so bodies are large.

**Evidence:** `properties/s3-whole-object-put-publishes-intended-bytes.md`

**Open Questions:**

- Does a `PutObject` that fails mid-body (a transport abort, not an HTTP error) leave `tmp_fstream_` so that the next upload publishes a suffix or nothing? `(partial: HTTP-error failures leave the stream usable and the next upload is whole (ticket 18 repro: 308,063 = 154,699 + 153,364); mid-body aborts unverified, AWS SDK/CRT stream handling not available to read)`
- Does CRT upload PBS's un-sized stream body as multipart over 8 MiB, and can a failed multipart leave the stream mid-file?
- Should `do_resize_object` verify the downloaded length itself instead of trusting `resize_file`?
- Is a failed temp-file removal after a successful resize PUT (the open fails although the truncation landed) acceptable?
