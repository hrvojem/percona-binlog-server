# Targeted property discovery: evaluation gaps G1, G5, G7 (source rewind, source retention, pull retention cycle)

Inputs: `sut-analysis.md`, `deployment-topology.md`, `property-catalog.md` (60 entries), `existing-assertions.md`, `evaluation/wildcard.md` (W4, W7, W13), `evaluation/coverage-balance.md` (F5, F9), ticket snapshot (tickets 18, 25 and the `source_change_test.py` purge scenario). Code checked in this repo at 99de682, plus the PXC tree's `sql/rpl_binlog_sender.cc` and `sql-common/client.cc` for server and client behaviour.

Each property below extends existing ones by reference and does not duplicate their assertions. The owners referenced are `source-swap-never-silently-accepted`, `gtid-failover-resume-progresses`, `interrupted-purge-leaves-openable-storage`, `dump-session-ends-with-eof-or-err-packet`, `dump-preamble-matches-request`, `stored-transactions-exactly-once-across-resumes` and `recovery-paths-exercised`.

## Source-side changes

### source-rewind-never-silently-continued — A source restored from its own backup is never silently continued past reused GTIDs

| | |
|---|---|
| **Type** | Safety |
| **Property** | After the source is restored from an earlier backup of itself (same `server_uuid`) behind what PBS has stored, PBS never appends further data to that storage. Fetch never exits 0 and pull never stores new bytes, including after the rewound source has re-used the stored GNOs for different transactions. |
| **Invariant** | Workload `Always("no PBS append after a source rewind behind the stored GTIDs")`. It is evaluated at every fetch exit and at periodic pull checks while `ahead = stored_gtids − restored.gtid_executed ≠ ∅` and the storage has not been re-seeded. The mode goes in the details. "Append" means a storage object changed beyond the restore-time snapshot, with recovery truncation tolerated. On violation the details carry per-GTID payload hashes of the reused GNOs, stored versus source. Vacuity markers: `Sometimes("rewound source refused PBS's dump with ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE")` and `Sometimes("rewound source committed past the stored GTID count while the storage still held reused GNOs")`. `Always` fits because one silent append permanently mixes two histories. The benign case (stored ⊆ restored set) is excluded by the precondition. |
| **Antithesis Angle** | Antithesis varies when the snapshot is taken relative to PBS's progress, how long the restored source runs (and rotates) before PBS reconnects, and where rotations fall. In rewrite mode silent continuation is predicted as soon as the GNO-count check passes. In GTID non-rewrite mode it needs the source's numbering to pass PBS's tail and a file whose PREVIOUS_GTIDS equals the stored set exactly; otherwise there is a name-collision wedge (`storage_core.cpp:276`) or a `validate_position` failure (`reader_context.cpp:917`). In position mode it needs X to land on an event boundary of the rewritten same-named file. Kills and partitions around the restart move PBS's tail. |
| **Why It Matters** | Restoring a source from xtrabackup/snapshot is the most common moment a binlog server is needed for PITR. The server checks only the GNO count for its own UUID (`rpl_binlog_sender.cc:918-921`), and PBS records no identity or lineage (`storage_metadata.hpp:30-37`). The storage then holds GTIDs whose content no server has, and pull serves them downstream. A `server_uuid`-based fix for ticket 25 would not catch this, which is why it is separate from `source-swap-never-silently-accepted` (different trigger, precondition and timing; no proxy needed). |

**Priority:** P1 · **Confidence:** medium (PBS side confirmed in code; server acceptance confirmed in MySQL source; the rewrite-mode silent branch is predicted, not run) · **Found by:** evaluation gap G1 (wildcard W4) · **Fault requirements:** **custom:** a mysqld restart-wrapper extension that snapshots the datadir while mysqld is stopped and restores it before a later restart, driven by a marker file; workload SIGKILL of PBS and network faults optional. **Runs in:** needs new driver: `mysql-source/parallel_driver_rewind_source.sh` (snapshot/restore via the restart-loop wrapper) in `fetch_fs` and `pull_fs`, behind a flag like the `source-swap-*` RESET action. It invalidates the exactly-once oracles for the timeline, so it must be excluded from them or re-seed afterwards.

**Evidence:** `properties/source-rewind-never-silently-continued.md`

**Open Questions:**

- Is PBS expected to detect a rewound source, or is lineage the operator's job (same contract as catalog OQ 5, but a `server_uuid` check cannot answer it)?
- Does a datadir snapshot model an xtrabackup restore well enough, given that xtrabackup does not copy binlogs and the name-collision outcomes differ?
- Is the predicted rewrite-mode silent continuation real, or does some check this pass missed refuse it?

### source-purge-past-resume-point-fails-closed — PBS that fell behind source retention fails closed, cleanly and repeatably

| | |
|---|---|
| **Type** | Safety |
| **Property** | When the source has purged binlogs or GTIDs that PBS's resume point still needs, every fetch exits non-zero and pull exits non-zero instead of collecting. No bytes are appended and no gap is stored. Repeated restarts leave the storage byte-identical and openable. |
| **Invariant** | Workload, with distinct messages: `Always("writer never exits 0 or appends after the source purged a binlog it still needed")`; `Always("repeated failed resumes against a purged source leave the storage byte-identical")` (hashes equal across attempts 2..k, and `list` succeeds); `Always("pull exits after its resume hits a purged source binlog")` (a supervisor outcome row with non-zero exit within `idle_time + connect_timeout + δ`); `Sometimes("resume hit a purged source binlog")`, split by mode (position: "Could not find first log file"; GTID: purged required GTIDs). The oracle classifies the position-mode "complete tail purged" case separately. SUT-side (missing): `ALWAYS_OR_UNREACHABLE(storage is empty, "gtid_purged bootstrap only on an empty storage")` at `collector_context.cpp:229-240` guards the silent-skip regression. |
| **Antithesis Angle** | A purge bites only at the next reconnect, because MySQL keeps files in use by a dump thread. So the interesting interleavings are a partition or kill that forces pull's 2013 reconnect, or a pull restart, landing after the workload's `PURGE BINARY LOGS TO`. Kills during the crash loop's startup recovery test that the loop is idempotent. Partitions between PBS and the source decide whether the refusal or a 2013 is seen first. |
| **Why It Matters** | PBS down for longer than `binlog_expire_logs_seconds` is routine. Code today fails closed: the refusal arrives at the first `mysql_binlog_fetch` (`mysql_binlog_open` reads no reply), and non-2013 errors are raised (`connection.cpp:546-550`) and end fetch/pull. That path is confirmed by the ticket 18 QA output "cannot fetch binlog event: … CR_1236". The property guards against a silent "resume from first available" regression, for example in a ticket 25 fix, and checks that the supervisor crash loop never drifts the storage. It also replaces the catalog's blanket "source retains its binlogs" assumption with a tested outcome. |

**Priority:** P2 · **Confidence:** high (fail-closed path traced in PBS and libmysqlclient and seen in QA output; the drift and pull-exit clauses are untested) · **Found by:** evaluation gap G5 (coverage-balance F5, wildcard W13) · **Fault requirements:** workload SQL `FLUSH BINARY LOGS` + `PURGE BINARY LOGS TO` on the source; workload SIGTERM/SIGKILL of PBS; network faults (native) to force reconnects. **Runs in:** needs new driver: `pbs/serial_driver_source_purge_past_pbs.py` in `fetch_fs` (under the writer lock) and `pull_fs` (removes `pull.enabled` and SIGTERMs pull, then re-enables it). The driver re-seeds the storage after its checks so the permanent wedge does not end the timeline, and flags the timeline for the exactly-once oracles.

**Evidence:** `properties/source-purge-past-resume-point-fails-closed.md`

**Open Questions:**

- Is a permanent crash loop the intended outcome, or should PBS offer an explicit re-seed that records the gap?
- Should position mode resume when only a complete (ROTATE-terminated) tail was purged, instead of failing?

### Investigation Log

#### Does the server's refusal surface at switch time (retry forever) or at the first fetch (process exits)?

- Examined: `src/easymysql/connection.cpp:130-180, 536-551`; PXC `sql-common/client.cc` `mysql_binlog_open` (`simple_command(..., 1)`); `rpl_binlog_sender.cc:900-1000`; `collector_context.cpp:207-272`; `pull_operation.cpp:125-165`; ticket 18 snapshot line 992.
- Found: the first fetch raises `core_error`, so fetch and pull exit non-zero. It is not swallowed by the switch-retry path.
- Conclusion: resolved and dropped from the open questions. The details are in the evidence file.

## Pull serving after retention

### pull-retention-cycle-keeps-serving-survivors — After stop → purge → restart, pull serves surviving binlogs unchanged, serves nothing purged, and resumes collecting

| | |
|---|---|
| **Type** | Safety |
| **Property** | After the in-contract retention cycle (SIGTERM pull, a `purge_binlogs` that reports success, restart pull), pull serves every surviving binlog exactly as it served it before the cycle, serves no event from a purged binlog, and resumes storing new source transactions. |
| **Invariant** | Workload, evaluated only after a purge that reported `success`: `Always("pull restarted after a retention purge serves every surviving binlog identical to what it served before the purge")` (the post-cycle served bytes per surviving file start with the pre-cycle served bytes, artificial events stripped; this covers encrypted storages without an offline decryptor); `Always("no event from a purged binlog is served after a retention cycle")`; `Sometimes("pull stored a new source transaction after a retention cycle")` (liveness, after faults stop); `Sometimes("empty-name dump after a retention purge started at the oldest surviving binlog")`, with mode and encryption in the details. ERR-vs-close and the preamble contents are scored by `dump-session-ends-with-eof-or-err-packet` and `dump-preamble-matches-request`. The purge outcome itself is scored by `interrupted-purge-leaves-openable-storage`. |
| **Antithesis Angle** | It exercises pull's streaming constructor and recovery on a freshly purged storage, the sender's empty-name resolution and file switches over a shortened index, and encrypted survivors whose neighbours' `.json` envelopes were just deleted. Antithesis varies the purge target, readers reconnecting at positions inside purged files, source load during the downtime (so the resume has catch-up work), and kills or partitions around the restart. |
| **Why It Matters** | For a 24/7 pull this is the only retention procedure README:473 leaves in contract, and no template runs it (`pull_fs` has no purge driver). A wrong served byte after routine retention breaks PITR from the binlog server. A pull that does not restart takes both backup and serving offline. |

**Priority:** P1 · **Confidence:** medium (paths read in code; the combination has never been run; predicted to pass except on encrypted or N3 leftovers) · **Found by:** evaluation gap G7 (wildcard W7; coverage-balance F9 for the encryption angle) · **Fault requirements:** workload SIGTERM of pull and purge; network faults and workload SIGKILL optional; `fsfault` on `remove`/`rename` optional (those outcomes are scored by the purge property). **Runs in:** needs new driver: `pbs/serial_driver_retention_cycle.py` in `pull_fs`, which captures the pre-cycle served stream, removes `pull.enabled`, SIGTERMs pull, purges, and re-enables pull. It purges only below every downstream client's current file unless the timeline marks chained readers as "purged past". The fetch half is already covered by `fetch_fs/serial_driver_purge.py`.

**Evidence:** `properties/pull-retention-cycle-keeps-serving-survivors.md`

**Open Questions:**

- Must retention respect the slowest downstream consumer, or may it purge past a lagging chained PBS/`mysqlbinlog`? The answer decides whether the chained wedge is a workload error or an expected exclusion from `pull-chain-converges-after-heal`.
- Is stop → purge → restart the documented retention procedure for pull, or do operators purge beside a running pull (catalog OQ 1)?

## Areas examined

- PBS resume request construction, artificial-ROTATE continuity checks, `open_binlog` non-tail refusal, `validate_position`, rewrite-mode skipping (`collector_context.cpp`, `storage_core.cpp`, `reader_context.cpp`).
- The libmysqlclient `mysql_binlog_open`/`fetch` error timing, and PBS error propagation in fetch and pull (`connection.cpp`, `fetch_operation.cpp`, `pull_operation.cpp`, `exception_handling_helpers.cpp`).
- MySQL `Binlog_sender::init` GTID and purge checks and the per-file fake ROTATE (`rpl_binlog_sender.cc:395-420, 900-1000`).
- Purge victim selection and commit order, sender empty-name resolution, and the purged-name lookup (`storage_core.cpp:332-500`, `sender_context.cpp`, `network_service.cpp:279-330`).
- QA `extra/qa/source_change_test.py` (the purge scenario covers only an empty storage), and tickets 18 and 25 in the snapshot.

## Cross-property assumptions

- The source always runs `gtid_mode=ON` (topology), so the workload can compute "PBS ahead of the source" from GTID sets in position mode too.
- Rewind and purge-past drivers leave a storage that is legitimately unusable against the current source. They must re-seed or flag the timeline so that `stored-transactions-exactly-once-across-resumes`, `stored-binlog-byte-prefix-of-source` and `pull-chain-converges-after-heal` skip it. Without that, they repeat the wedge-swamps-exploration problem (coverage-balance F-wedges, wildcard W1).
- Workload `Always` messages here are evaluated only under their stated preconditions (rewind epoch with `ahead ≠ ∅`; purge past a needed binlog; purge reported `success`). That keeps them from leaking across templates, per the scoping concern in wildcard W1.
