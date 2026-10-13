# gtid-failover-resume-progresses

## What led to it

GTID auto-position exists so that a replica can follow a failover without knowing file names. PBS uses COM_BINLOG_DUMP_GTID, so an operator may expect GTID mode to follow a promoted replica whose `gtid_executed` ⊇ the storage's GTIDs (no lost transactions — the clean failover). Reading the resume path shows several ways it cannot.

## Mechanism (confirmed in code unless marked)

When B (promoted, `gtid_executed` ⊇ storage GTIDs) receives the GTID dump, it starts at the last of its binlogs whose PREVIOUS_GTIDS is a subset of the requested set and skips transactions already in the set (MySQL behaviour, verified in server source for 8.0, 8.4.8 and 9.7.1; see Investigation Log). Then in PBS:

1. B's file name equals a stored non-tail name (both servers default to `binlog.NNNNNN`): `storage_core::open_binlog` raises "cannot open an existing binlog that is not the latest one for append" (`storage_core.cpp:272-279`). Fatal, and identical on every retry: a wedge.
2. B's name equals the stored tail: fresh fetch opens it in append mode, the reader marks FDE/PREVIOUS_GTIDS info-only (`reader_context.cpp:579-581`), and the first GTID event must satisfy `stored_size + event_size == log_pos` (`reader_context.cpp:905-920`). The position is from B's unrelated file, so it fails with "unexpected next event position", unless a coincidence makes it pass. If it does pass, B's bytes are appended to A's file (see `source-swap-never-silently-accepted`).
3. B's name is new (sorts after or simply not present): a new record is created. B sends its FDE + PREVIOUS_GTIDS from position 4, then skips transactions already in the set. PBS sees no events for skipped transactions (heartbeats, if any, are filtered by `MYSQL_RPL_SKIP_HEARTBEAT`, `connection.cpp:205`), so the first non-skipped GTID event's `log_pos` jumps, and `validate_position` fails. This progresses only if B's chosen file starts exactly at the first missing transaction (e.g. B ran FLUSH BINARY LOGS at promotion).
4. Mixed base names are not checked on the write path (`open_binlog` does not compare base names; only purge does, `storage_core.cpp:339-344`), so case 3 can append `mysql-bin.000004` after `binlog.000007`. A later `purge_binlogs` across the boundary is refused.
5. Rewrite mode ignores source names and positions (`collector_context.cpp:294-330`), so it follows the failover.

For a single unchanging source, skipping is always contiguous with the stored prefix, so none of this happens without a failover.

## Property

After a clean GTID failover (B ⊇ storage GTIDs) and a quiet period, PBS eventually stores a transaction committed on B after the promotion. `Sometimes(stored_gtids ∩ B_new_gtids ≠ ∅)` per failover, evaluated in a workload check after `ANTITHESIS_STOP_FAULTS`. Split by mode in the implementation (rewrite vs non-rewrite) with distinct messages, since the expected outcomes differ.

## Antithesis angle

The interesting interleavings decide which case occurs: whether B rotated at promotion, how far B lagged, and whether PBS was mid-transaction when A died. Partitions on A↔B, A↔PBS and node hang of A drive these.

## SUT-side instrumentation suggestions (all missing)

- `storage_core.cpp:272-279`: `REACHABLE("resume named an existing non-tail binlog")` — fail-closed path, an anchor for failover exploration.
- `reader_context.cpp:917-920` (position continuity failure): `SOMETIMES`/`REACHABLE("GTID resume broke next_event_position continuity")`, with the replication mode in details.
- `collector_context.cpp` reconnect: `SOMETIMES(fde_server_id != previous_fde_server_id, "collector followed a source whose FDE server_id changed")`.

## Open questions

- Is following a GTID failover in non-rewrite mode a supported feature? If not (rewrite is the answer), cases 1-3 are expected to fail closed and this property applies only to rewrite mode; the non-rewrite cases become "fails closed with a clear error" checks. `(needs human input)`

## Answers and implications (this pass)

- **Heartbeats on skip:** MySQL does report the skip. In `Binlog_sender::send_events`, skipped GTID groups set `exclude_group_end_pos`, and **before the next non-skipped event** the sender always sends a HEARTBEAT carrying the end position of the skipped group, whatever `source_heartbeat_period` is (`rpl_binlog_sender.cc:626-660`; also at the end of a read, `:680-686`). libmysqlclient's `mysql_binlog_fetch` drops HEARTBEAT_LOG_EVENT and HEARTBEAT_LOG_EVENT_V2 when `MYSQL_RPL_SKIP_HEARTBEAT` is set (`sql-common/client.cc:7600-7606` in 9.7.1). PBS always sets that flag (`easymysql/connection.cpp:205`). So case 3 is caused by PBS discarding information the server sends. A fix would drop the flag and advance the reader's position on a heartbeat. That is a code-level suggestion, not a property change.
- **Which file B chooses:** the **newest** binlog whose PREVIOUS_GTIDS ⊆ requested set (`find_first_log_not_in_gtid_set` iterates the index in reverse from `rbegin()` and returns the first subset match). This is the same in 8.0, 8.4.8 and 9.7.1. For a clean failover (B ⊇ S), B's chosen file starts with no transaction already in S only when S equals B's `gtid_executed` at that file's creation, i.e. B rotated at promotion **and** PBS had received everything B had at that moment. Otherwise B skips at least one transaction, the heartbeat is dropped, and `validate_position` fails. So non-rewrite progress needs all of: B rotated at promotion, PBS fully caught up with B at that point, and a new file name (case 3). That is narrow but reachable. Rewrite mode ignores positions and is unaffected.
- **Property change:** keep `Sometimes`, but split it explicitly. (a) Rewrite mode: `Sometimes(stored_gtids ∩ B_new_gtids ≠ ∅ after clean failover, rewrite)`. (b) Non-rewrite: `Sometimes(... non-rewrite)` plus a companion `Always(non-rewrite failover run either progresses or exits non-zero with no bytes appended to an existing stored file)`, which keeps case 2's silent append in view. The workload should rotate B at promotion in some fraction of runs (`FLUSH BINARY LOGS`), or the non-rewrite Sometimes is practically unreachable.

### Investigation Log

#### Does MySQL's dump thread send anything for skipped GTID transactions, and does `MYSQL_RPL_SKIP_HEARTBEAT` drop it?

- Examined: `sql/rpl_binlog_sender.cc` `send_events` and `skip_event` (9.7.1, percona-xtrabackup f1aa0691f76, `:600-690, 749-765`); the same lines via `git show` in PXC `upstream/8.0` (`:637, 657`) and `Percona-XtraDB-Cluster-8.4.8-8.1` (`:646, 666`); `sql-common/client.cc` `mysql_binlog_fetch` (`:7600-7606` in 9.7.1; `:7564` in 8.0; `:7641` in 8.4.8); PBS `easymysql/connection.cpp:185-206`.
- Found: the server sends a heartbeat with the skipped group's end position before the next sent event, and the client library drops it because PBS sets `MYSQL_RPL_SKIP_HEARTBEAT`.
- Conclusion: resolved. Case 3 is a PBS-side information loss and is fixable in principle.

#### Which file does B choose (first vs last file whose PREVIOUS_GTIDS ⊆ set)?

- Examined: `sql/binlog.cc` `MYSQL_BIN_LOG::find_first_log_not_in_gtid_set` (9.7.1 `:4384-4444`; 8.0 `:4914-4941`; 8.4.8 `:4989-5016`).
- Found: reverse iteration over the index, returning the first (newest) file whose PREVIOUS_GTIDS is a subset of the requested set, in all three versions.
- Conclusion: resolved. Newest qualifying file; consequences above.

#### Is following a GTID failover in non-rewrite mode supported?

- Examined: README (no failover section; grep for failover, promot, server_uuid), `collector_context.cpp:274-425` (rewrite path ignores source names and positions), ticket 25.
- Found: no documentation either way. The design of rewrite mode (its own names and positions) makes it the only mode that can follow, but no doc presents it as the failover answer.
- Conclusion: `(needs human input)`.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Rewrite mode is expected to progress after a clean failover, so its check becomes a bounded `Always("rewrite mode stores a post-promotion transaction within T after the quiet period starts")`, once per quiet window. The per-mode `Sometimes` stays as the vacuity guard and as the only progress check for non-rewrite mode (antithesis-fit F3). Quiet window = an `ANTITHESIS_STOP_FAULTS` window in which a harness quiet flag also pauses the harness's own fault drivers (kill drivers, proxy rules, fsfault rules, SIGSTOP), with the measured action started after the window begins and the window longer than the bound. Latency inside the window comes only from harness-bounded stimuli (an `s3_fault_proxy` stall of known seconds, SIGSTOP/SIGCONT of known duration) whose duration is added to the bound, and the bound is calibrated against a measured baseline of the coverage-instrumented debug build (lenses: antithesis-fit F2, implementability C3/C4, wildcard W10).
- Runs in: **Deferred** (`failover`).
