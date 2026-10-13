# Property discovery 07: Distributed Coordination

binlog_server has no consensus, leader election or quorum. The coordination surface is the replication topology: the MySQL source acts as "leader" for PBS, PBS acts as "leader" for pull clients, and several PBS processes may share one storage without any fencing. The properties below cover source identity change and failover (tickets 25 and 14), GTID auto-position resume, the source → storage → client chain under pairwise partitions, and zombie writers as the split-brain analogue (ticket 24).

## Category: Source failover and identity

### source-swap-never-silently-accepted — Swapped or reset source is never silently continued

| | |
|---|---|
| **Type** | Safety |
| **Property** | After the source is replaced by a server with unrelated history, or the source's binlogs are reset, fetch never exits 0 and pull never appends bytes to the existing storage. |
| **Invariant** | Workload-side `Always(!(source_epoch_changed && (fetch_exit == 0 \|\| bytes_appended)))`, evaluated at every fetch exit and at periodic pull checks. The workload tracks the source epoch, because PBS records no source identity. `Always` fits because the property must hold for every run after a swap. |
| **Antithesis Angle** | The workload repoints a VIP/proxy container from source A to an independent server B, or runs RESET BINARY LOGS AND GTIDS. Antithesis varies how far B's binlog has grown relative to the stored offset, and partitions or hangs around the switch. The interleaving that leads to exit 0 is the one where B's file length or an event boundary lines up with the stored offset. |
| **Why It Matters** | This is ticket 25. The backup silently holds another server's lineage (a file that neither server wrote), and in the reset case the storage is wedged permanently. Validated: `storage_metadata.hpp:35-37` has no identity, and nothing in `src/` queries server_uuid. |

Priority: P0. Confidence: high (mechanism confirmed in code; the QA repro reports exit 0 with diverged bytes). Fault requirements: none beyond network faults. Needs a workload "source swap" action and a proxy/VIP container.

**Open Questions:**

- Is fail-closed expected on a swapped source, or is source continuity the operator's responsibility?
- How does the harness swap the source endpoint (a proxy/VIP container vs a hostname alias)?
- Which error does the new source return when the stored offset is not an event boundary in its file?

### gtid-failover-no-lost-transaction-merge — Lost-on-failover transactions are never merged with the new primary's history

| | |
|---|---|
| **Type** | Safety |
| **Property** | After PBS appends any transaction received from source S, every GTID in the storage is in S's `gtid_executed`. Otherwise PBS must have failed closed. |
| **Invariant** | Workload-side `Always(lost == ∅ \|\| nothing_appended_from_new_source)`, where `lost = storage_gtids − new_source.gtid_executed` is computed at failover from `list` output and from SQL. `Always` fits because a single merge permanently corrupts the history that PITR relies on. |
| **Antithesis Angle** | Asymmetric partitions of A↔B while PBS still receives from A create "PBS ahead of the promoted replica". The workload then kills or isolates A and promotes B behind the VIP. The test is most informative in rewrite mode, which accepts any source. |
| **Why It Matters** | This is the classic split-brain artefact. PBS's metadata (`storage_core.cpp:691`; in rewrite mode also the generated PREVIOUS_GTIDS) encodes a history that no server has. Pull then serves the deposed primary's lost transactions to downstream replicas, and PITR from the stored files replays a branch that conflicts with B. |

Priority: P1. Confidence: medium (the PBS side is confirmed in code; that the MySQL dump check accepts foreign-uuid excess GTIDs is not checked). Fault requirements: network partitions. Needs an async replica B with log_replica_updates and a promotion action in the workload.

**Open Questions:**

- Is keeping the deposed primary's lost transactions and continuing with the new primary acceptable, or must PBS refuse?
- Does MySQL 8.0/8.4/9.x accept a GTID dump whose set contains foreign-uuid GTIDs the source lacks?
- Is failover behind a VIP an in-scope deployment for binlog_server?

### gtid-failover-resume-progresses — GTID mode follows a clean failover

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After a clean GTID failover (the promoted replica's executed set includes every stored GTID) and a quiet period, PBS eventually stores transactions that were committed on the new source after the promotion. |
| **Invariant** | Workload-side `Sometimes(stored_gtids ∩ new_source_post_promotion_gtids ≠ ∅)`, with separate messages for rewrite mode and non-rewrite mode. It is checked after `ANTITHESIS_STOP_FAULTS`. `Sometimes` fits because this is progress after an event, not an invariant. |
| **Antithesis Angle** | Partitions and hangs decide how far B lagged, whether B rotated at promotion, and where the first missing GTID sits in B's files. Non-rewrite mode is predicted to fail closed when B's file name collides with a stored non-tail name (`storage_core.cpp:272-279`), and when B skips already-stored transactions, which breaks `next_event_position` continuity (`reader_context.cpp:905-920`). |
| **Why It Matters** | GTID auto-position exists so that replicas can follow failovers. A backup tier that wedges on every routine promotion needs manual re-seeding and leaves a backup gap. |

Priority: P2. Confidence: medium (PBS behaviour is confirmed in code; the MySQL skip/heartbeat behaviour still needs checking). Fault requirements: network partitions, plus node hang or kill of A (node termination is optional).

**Open Questions:**

- Is following a GTID failover in non-rewrite mode a supported feature, or is rewrite mode the intended answer?
- Does the source send a heartbeat for skipped GTID transactions, and does `MYSQL_RPL_SKIP_HEARTBEAT` hide it from PBS?
- Which of its binlogs does the promoted source choose as the dump start on 8.0/8.4/9.x?

## Category: GTID auto-position resume

### gtid-resume-preserves-source-rotations — GTID resume keeps every source rotation

| | |
|---|---|
| **Type** | Safety |
| **Property** | After every reconnect or restart in GTID mode (non-rewrite), the stored binlog names have no gaps from the first stored name to the tail, and every non-tail stored binlog is byte-identical to the source's closed binlog, including its closing ROTATE. |
| **Invariant** | Workload-side `Always(names_contiguous && non_tail_byte_equal)` after a quiet period, compared against `SHOW BINARY LOGS` and the source's files. SUT-side `UNREACHABLE("GTID resume closed the stored tail without its ROTATE event")` at `collector_context.cpp:540-553`. |
| **Antithesis Angle** | Partitions, source restarts, PBS read_timeout disconnects and PBS kills, combined with workload `FLUSH BINARY LOGS` repeated 0 to 3 times while PBS is disconnected and caught up. |
| **Why It Matters** | This is ticket 14 (validated: the name-mismatch branch only calls `close_binlog()`, with no ROTATE and no gap check). Stored copies are not exact. Downstream `mysqlbinlog --raw` and replica file tracking never see the switch. |

Priority: P1. Confidence: high (known failing). Fault requirements: network partitions. Node termination is optional (a source restart can be done by the workload).

**Open Questions:**

- Is byte identity with the source an official guarantee for GTID mode, or only an MTR convention?
- Are intermediate binlogs with no transactions expected to be materialised in the storage?

### collector-observes-source-binlog-switch-on-reconnect — Reconnect lands in a different source binlog

| | |
|---|---|
| **Type** | Reachability |
| **Property** | During a run, the first artificial ROTATE after a reconnect or restart sometimes names a binlog other than the stored tail. This is reported per mode, plus the refused non-tail case. |
| **Invariant** | SUT-side `REACHABLE` with three distinct messages: position-mode continuation into the next binlog (`collector_context.cpp:540`), the GTID-mode mismatch branch (the same site, with the mode in the details), and "resume named an existing non-tail binlog" (`storage_core.cpp:276`). `Reachable` fits because these are exploration anchors that keep the safety properties above from passing vacuously. |
| **Antithesis Angle** | The disconnect window has to overlap a source rotation. Partitions, node hangs, source restarts and a small `max_binlog_size` drive it. |
| **Why It Matters** | Without these assertions there is no evidence that the workload ever reaches the ticket 14 and ticket 25 branches. |

Priority: P2. Confidence: high. Fault requirements: network partitions.

**Open Questions:**

- None

## Category: Replication chain (source → storage → pull clients)

### pull-chain-prefix-consistency — Client ⊑ storage ⊑ source at every observation

| | |
|---|---|
| **Type** | Safety |
| **Property** | For each binlog (one source epoch, non-rewrite, unencrypted), the bytes received by every pull client are a prefix of the stored bytes, and the stored bytes are a prefix of the source's. GTID sets nest the same way. Bytes a client has received are never contradicted later. |
| **Invariant** | Workload-side `Always(client ⊑ stored ⊑ source)`, checked at random times while faults are active. The client is `mysqlbinlog --raw --stop-never` or a Python dump client that records the bytes it receives. `Always` fits because prefix order is a safety invariant that must hold under faults. |
| **Antithesis Angle** | Independent partitions on source↔PBS, PBS↔S3 and PBS↔client, PBS kills, `.json` PUT failures (to open the ticket 18 window), and clients resuming at (file, pos). Ways the property can be violated: retraction after recovery truncation (in-memory size runs ahead after a failed metadata save), duplicate delivery after the ticket 18 re-flush, missing ROTATE at a switch, and source divergence. |
| **Why It Matters** | Downstream replicas that apply bytes PBS later retracts or duplicates diverge from the source, and they diverge silently. |

Priority: P1. Confidence: medium (each way it can break is a lead built on code-confirmed mechanisms; the end-to-end chain has not been reproduced). Fault requirements: network partitions. Retraction needs node termination or a workload kill, and a fault on the metadata PUT (custom fault or `s3_fault_proxy`).

**Open Questions:**

- Is a position-mode client expected to tolerate duplicate delivery after a PBS write failure?
- Can the harness use a real MySQL replica downstream (ticket 20), or only mysqlbinlog or a custom client?
- In rewrite mode, is regenerating different ROTATE/FDE bytes for an already served range acceptable?

### pull-chain-converges-after-heal — Every hop converges after faults stop

| | |
|---|---|
| **Type** | Liveness |
| **Property** | After faults stop and the source stops writing, within T = read_timeout + idle_time + checkpoint_interval + margin, the storage holds every source transaction and every reconnected blocking client holds every stored byte. |
| **Invariant** | Workload-side `Sometimes(converged_within_T)` per heal cycle, checked mid-run after `ANTITHESIS_STOP_FAULTS` so that testing continues afterwards. Pull runs under a supervisor. `Sometimes` fits because this is eventual progress. |
| **Antithesis Angle** | Asymmetric partitions (a source dump thread stuck in write, a PBS session waiting on a write timeout, a collector blocked on S3 under the storage lock), followed by a heal. It also exercises same-server_id zombie dump eviction on the source. |
| **Why It Matters** | A backup or relay tier that does not catch up after a network blip silently falls behind. LG3 suggests that the default config never serves the active binlog. |

Priority: P1. Confidence: medium. Fault requirements: network partitions. A supervisor restart is needed if a fault produces a non-2013 error.

**Open Questions:**

- What is the intended freshness bound, and is the default no-checkpoint config expected to serve the active binlog?
- Is pull expected to run under a supervisor?
- Which errno does a partition produce mid-stream?

## Category: Split-brain writers

### zombie-writer-takeover-fails-closed — A takeover writer never corrupts storage held by a zombie writer

| | |
|---|---|
| **Type** | Safety |
| **Property** | When two writer instances are alive on one storage at overlapping times (a hung or partitioned "old" pull plus a "new" pull or fetch started by the supervisor), then after both stop and one clean fetch runs, the storage opens and every stored binlog is a byte prefix of the source's binlog that ends on a transaction boundary. |
| **Invariant** | Workload-side `Always(opens && all_prefix_at_boundary)`. A stricter variant is `Always(second_writer_refused)`. SUT-side `UNREACHABLE("tail metadata size exceeds data size at open")` (`storage_core.cpp:974-977`) and `REACHABLE("startup recovery truncated the tail binlog")` (`:979`). |
| **Antithesis Angle** | A node hang or partition of pull #1, the workload starts pull #2, and then #1 resumes. Explored windows: #2's constructor deletes #1's `.json.tmp` and truncates its tail, and the two instances interleave rotation and index rewrites. On S3 the data and `.json` objects are last-writer-wins independently. Same-config instances also evict each other's dump thread on the source (same server_id). |
| **Why It Matters** | This is ticket 24 (validated: no fencing primitive, deterministic `.tmp`, constructor truncation). It causes permanent silent corruption with exit 0. Overlapping cron runs and supervisor takeovers are routine. |

Priority: P0. Confidence: high (file backend, QA-reproduced); medium for S3. Fault requirements: node hang or network partition. No node termination is needed.

**Open Questions:**

- Is a second writer on a live storage in contract?
- What error does an evicted same-server_id dump connection receive (2013 vs 1317 vs other)?
- Does the harness S3 server provide atomic whole-object overwrite?

## Candidate SUT instrumentation points (coordination internals)

There is no leader election, so the redirect, stepdown and handoff concepts map to the following points:
- **Leadership-loss retry (upstream reconnect):** `pull_operation.cpp:131-150` (idle and reconnect loop, `iteration_number`); `collector_context.cpp:209-216, 263-267` (connect/switch failure → retry); `connection.cpp:546-551` (only 2013 is retried). Suggest `SOMETIMES("pull caught up after reconnect")` and `REACHABLE("mid-stream error other than 2013 ended pull")`.
- **Upstream "leader moved" detection:** `collector_context.cpp:518-553` (reuse branch vs name-mismatch branch); `reader_context.cpp:547-581` (per-mode artificial ROTATE checks); `storage_core.cpp:272-279` (refusal of a non-tail name). The FDE common-header server_id that the reader parses is the only source-identity signal PBS sees today.
- **Handoff of the storage between writers (fencing):** none exists. Candidate anchors are `storage_core.cpp:597-630` (tmp purge at open), `:974-983` (tail truncation), `:214` and `:789-799` (stale index load/save).
- **Downstream "redirect" / serving decisions:** `network_service.cpp:308-313` (a request past the flushed size closes silently with no error packet, so the client gets no redirect or reason); `storage_core.cpp:520-541` (file switch on the serving path).
- **Bootstrap coordination:** `collector_context.cpp:229-240` (`SELECT @@GLOBAL.gtid_purged` on an empty GTID storage, followed by a separate dump request; a source PURGE between the two leads to 1236 and a fatal pull exit, which is fail-closed and transient).

## Areas examined

- `src/operations/collector_context.cpp` (resume request, rewrite path, artificial ROTATE handling), `pull_operation.cpp` (reconnect loop), `src/easymysql/connection.cpp` (dump flags, 2013 handling).
- `src/binsrv/events/reader_context.cpp` (per-mode continuity checks, info-only preamble).
- `src/binsrv/storage_core.cpp`/`.hpp` (open_binlog, new-record GTIDs, resume derivation, recovery truncation, purge base-name check, fetch_event_block clamp), `storage_metadata.hpp`.
- `src/minimysql/network_service.cpp` (dump loop, polling, silent close).
- README (no failover or identity section; GTID/rewrite mode descriptions); tickets 14, 24 and 25 in the snapshot; the `extra/qa` harness list (`source_change_test.py` scenarios).
- Considered and not turned into properties: replication-lag visibility (PBS exposes no lag metric or status, so there is nothing to assert beyond convergence); the gtid_purged bootstrap racing a source PURGE (fail-closed, transient; noted above); the source losing un-synced binlog tail that PBS already captured (needs `sync_binlog=0` and page-cache loss, which a container kill does not cause; it is a variant of `gtid-failover-no-lost-transaction-merge` if node termination drops page cache).

## Cross-property assumptions

- The harness puts a VIP/proxy container between PBS and the source so that the workload can fail over without changing PBS's config, and so that Antithesis can partition PBS↔proxy and proxy↔source separately.
- The source and the promoted replica run with GTID on and log_replica_updates on, and use distinct server_ids. Both default to the `binlog` base name unless the workload varies it on purpose (name collisions drive `gtid-failover-resume-progresses`).
- Byte-level chain checks use non-rewrite, unencrypted storages. Rewrite and encrypted storages are compared through the event stream that pull serves.
- Pull runs under a supervisor (the workload restarts it) for the liveness properties.
- Overlap with other agents' slugs, to merge at synthesis: `gtid-resume-no-skipped-binlogs` (ticket 14) with `gtid-resume-preserves-source-rotations`; `second-writer-on-storage-fails-closed` (ticket 24) with `zombie-writer-takeover-fails-closed` (this one adds the node-hang takeover angle, S3 last-writer-wins and same-server_id dump eviction); `served-bytes-stable-across-restart` and `restart-after-kill-converges-to-source` with `pull-chain-prefix-consistency` and `pull-chain-converges-after-heal`.
