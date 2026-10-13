# gtid-failover-no-lost-transaction-merge

## What led to it

The split-brain analogue for a log-shipping system: under async MySQL replication, a failover can lose transactions the old primary A committed (and PBS already captured) but the promoted replica B never received. The new cluster history is B's; A's lost suffix is a diverged branch. Focus 7 asks what PBS does with that branch.

## Mechanism (from code + MySQL behaviour)

- PBS resumes with its full stored GTID set, `prev ∪ added` of the tail (`storage_core.hpp:313-337`; `collector_context.cpp:242-251`).
- MySQL's dump thread rejects a replica whose GTID set has more GTIDs *for the source's own server_uuid* than the source has (ER_SLAVE_HAS_MORE_GTIDS_THAN_MASTER family). A's lost GTIDs (A:101-105) have A's uuid, not B's, so B accepts the request and streams everything not in the set (B's own new transactions).
- PBS records `previous_gtids = get_gtids_unsafe()` for every new file (`storage_core.cpp:691`) — i.e. PBS's own set, including A:101-105 — not the PREVIOUS_GTIDS event B wrote in the bytes. So metadata claims a history (A:1-105 then B:1-N) that no server has, and `search_by_gtid_set`/PITR planning uses that metadata (`search_helpers.cpp:22-52`).
- In rewrite mode (`collector_context.cpp:274-425`) the generated PREVIOUS_GTIDS event is also `storage_->get_gtids()`, so even the bytes encode the merged history, and rewrite accepts any file names from any source. Rewrite is therefore the mode where the merge is silently completed. In non-rewrite GTID mode PBS often fails closed first because of name collisions / position continuity (`gtid-failover-resume-progresses`).
- Pull then serves A:101-105 to downstream replicas, which diverge from B.

The MySQL-side check is now verified in server source (see Investigation Log): the dump thread compares only the source's own-UUID GTIDs, so B accepts PBS's set containing A:101-105. The PBS side (no identity, set sent as-is, metadata from own set) is confirmed in code.

## Property

After PBS appends any transaction received from source S, every GTID in the storage (union over all files' `prev ∪ added`) is in S's `gtid_executed` at that time — otherwise PBS must have failed closed (non-zero exit, nothing appended). Workload-side `Always` after each post-failover fetch/pull progress check. In practice: workload records `lost = storage_gtids − B.gtid_executed` at failover; if `lost ≠ ∅` then `Always(no new file/bytes appended from B)`.

## Antithesis angle

Workload: source A with async replica B (log_replica_updates ON), PBS pulling from a VIP/proxy pointing at A. Antithesis partitions A↔B (replication lag builds up while PBS still receives from A), then the workload kills/isolates A and promotes B (STOP REPLICA; RESET REPLICA ALL; repoint VIP). The interleaving that produces `lost ≠ ∅` is exactly "PBS ahead of B at failover", which Antithesis' asymmetric partitions make likely.

## SUT-side instrumentation suggestions (all missing)

- `collector_context.cpp` after `switch_to_gtid_replication`: no in-process knowledge of the source's executed set without a new query; suggest (debug-only) `SELECT @@GLOBAL.gtid_executed` and `ALWAYS(storage_gtids ⊆ source_executed, "storage GTIDs are a subset of the source's executed GTIDs at resume")`.
- `storage_core.cpp:691`: `ALWAYS_OR_UNREACHABLE(previous_gtids == PREVIOUS_GTIDS event payload, ...)` in non-rewrite mode — would flag metadata/bytes disagreement (needs plumbing of the parsed event).

## Open questions

- Is keeping the deposed primary's lost transactions and then continuing with the new primary's history acceptable, or must PBS refuse? `(needs human input)`
- Is failover (promoted replica behind a VIP) an in-scope deployment for binlog_server at all? `(needs human input)`

## Answers and implications (this pass)

- MySQL accepts the dump. In `Binlog_sender::init` the check is `m_exclude_gtid->is_subset_for_sid(&gtid_executed_and_owned, server_tsid.get_uuid())`, which raises `ER_REPLICA_HAS_MORE_GTIDS_THAN_SOURCE` only for GTIDs with **B's own** UUID. The remaining checks are `lost_gtids ⊆ requested` and `find_first_log_not_in_gtid_set`, which picks the newest file whose PREVIOUS_GTIDS ⊆ requested. A's lost A:101-105 pass all three. Verified in `sql/rpl_binlog_sender.cc` for 9.7.1 (percona-xtrabackup f1aa0691f76, `:899-903, 931-944`), 8.4.8 (PXC tag `Percona-XtraDB-Cluster-8.4.8-8.1`, `:918-921`) and 8.0 (PXC `upstream/8.0` bb827a82eaf, `:910-914`). These are Percona forks, but this code is inherited from upstream MySQL.
- **Implication:** the server never makes PBS fail closed on a lost-transaction failover. Non-rewrite GTID mode fails closed only through the name and position coincidences in `gtid-failover-resume-progresses`. When those happen to line up (a new file name on B, B rotated at promotion, and no skipped transaction before the first new one), the merge completes silently in non-rewrite mode too. **Property change:** the property applies to both modes, not just "rewrite plus the metadata half". Keep one `Always` with mode in the details, or split the messages per mode. The assertion type is unchanged.

### Investigation Log

#### Does MySQL accept a GTID dump whose set contains foreign-uuid GTIDs the source lacks?

- Examined: `sql/rpl_binlog_sender.cc` `Binlog_sender::init` GTID branch in MySQL 9.7.1 (percona-xtrabackup f1aa0691f76) and via `git show` in PXC `upstream/8.0` and tag `Percona-XtraDB-Cluster-8.4.8-8.1`; `sql/binlog.cc` `find_first_log_not_in_gtid_set` (reverse iteration, first file with `previous_gtids ⊆ requested`).
- Found: the refusal is scoped to the source's own UUID in all three versions. Foreign-UUID GTIDs the source lacks are accepted, and the source streams everything not in the requested set, starting from the newest file whose PREVIOUS_GTIDS is a subset.
- Not found: an end-to-end run (not run, per brief).
- Conclusion: resolved. The server accepts the dump, so the property needs the full PBS-side check in both modes.

#### Must PBS refuse, or is a merged history acceptable?

- Examined: README (no failover or identity text), `storage_metadata.hpp`, ticket 25, `storage_core.cpp:685-695` (`previous_gtids = get_gtids_unsafe()`).
- Found: no design statement. The metadata already records PBS's own set rather than the source's PREVIOUS_GTIDS, which is consistent with either reading.
- Conclusion: `(needs human input)`.

#### Is failover an in-scope deployment?

- Examined: README (grep for failover, promot, server_uuid, replica: no deployment guidance), `deployment-topology.md` line 221 (the `failover` template is deferred).
- Found: nothing in scope or out of scope.
- Conclusion: `(needs human input)`.
