# pull-retention-cycle-keeps-serving-survivors

## What led to it

Evaluation gap G7 (wildcard W7). README:473 says purge alongside fetch or pull "may cause data corruption and should be avoided". For a 24/7 `pull`, the only in-contract retention workflow is therefore: stop pull (SIGTERM), run `purge_binlogs`, restart pull, and downstream clients reconnect. The topology's `pull_fs` template has no purge driver (`deployment-topology.md`, Template `pull_fs`). Purge appears with pull only in the misuse (`contention`) phase. `fetch_fs` has `serial_driver_purge.py` under the writer lock, so the fetch half of the cycle is already exercised by `interrupted-purge-leaves-openable-storage` plus the exactly-once oracle. Three things are untested:
- pull's startup on a freshly purged storage;
- the served stream for the surviving binlogs after the cycle;
- clients whose position lies in a just-purged file.

This property owns the **post-cycle pull behaviour**. It deliberately does not re-assert the claims owned elsewhere:
- the purge outcome itself: index suffix, tail and resume point unchanged, status honesty, reopen after interrupted purge (`interrupted-purge-leaves-openable-storage`);
- ERR versus silent close for a request naming a purged file (`dump-session-ends-with-eof-or-err-packet`);
- the empty-name preamble naming the oldest indexed file (`dump-preamble-matches-request`, whose "empty name after purge" marker this driver makes reachable in-contract);
- graceful SIGTERM flushing (`graceful-exit-leaves-flushed-storage`, `sigterm-honoured-within-bound`).

## Code paths

- Purge: `select_purge_victim_count` refuses the tail and foreign base names (`src/binsrv/storage_core.cpp:332-366`). `purge_binlogs` commits the index first and then best-effort removes metadata and then payload (`:368-433`).
- Pull startup on the purged storage: the streaming constructor loads `binlog.index`, validates every indexed record and refuses unindexed objects (`storage_core.cpp:214`, validators `:779-783, 899-903, 993-996`). It then runs tail recovery (`:960-985`). After a clean purge none of these should fire. After a `warning` purge they do (N3, owned elsewhere), and pull then crash-loops under the supervisor.
- Resume after the cycle: the tail is untouched by purge, so the GTID set and (name, size) are unchanged (`collector_context.cpp:227-261`). `purged_gtids_ = front().added_gtids` (`storage_core.cpp:1001-1004`, catalog OQ 11) is read only on an empty storage. Purge never empties the storage (tail refusal), so it should be inert here. This property is the first in-contract place where that claim gets exercised.
- Serving: `sender_context` resolves an empty name to the first indexed record (`src/operations/sender_context.cpp:66-71`; `storage_core::fetch_event_block` `:483-497`). A name not in the index returns `false` (`:490-496`), and `handle_binlog_dump_command` then logs "failed to fetch next event block" and closes without ERR (`src/minimysql/network_service.cpp:305-310`). File switches between surviving binlogs go through `handle_binlog_file_switch` (`sender_context.cpp:404-432`).

## Failure scenario

- **Served bytes differ for survivors.** After restart, a full dump (empty name, position 4, non-blocking) must reproduce exactly what the pre-cycle pull served for the surviving files. Encrypted storages are the case to watch: purge removes `<name>.json` (which holds the per-file envelope) before the payload. A survivor's envelope must be untouched, and the restarted pull must decrypt every survivor (coverage-balance F9).
- **Pull fails to start or to resume collecting after a clean purge.** This could come from an index/metadata validator tripping on a state a successful purge left (for example a directory fsync ordering on the filesystem backend), or from a GTID-mode resume that depends on the purged front record.
- **A purged binlog's bytes are served.** On S3, a deleted object might still be readable through a stale local buffer. A restarted pull has none, so this is unlikely, but it is cheap to check.
- **Chained downstream wedge (observation).** The workload's `parallel_driver_chained_fetch.py` storage (and any lagging reader) may have its tail in a file the cycle purged. It then gets a silent close. Prediction from the dump-session evidence: the downstream fetch exits 1, and a downstream pull retries forever. This collides with `pull-chain-converges-after-heal`. The driver must either purge only below every downstream client's current file (in-contract retention respects consumers), or mark the chain as "purged past", so that the convergence oracle skips it. Which retention policy operators are expected to follow is an open question.

## Property and oracle

New driver `pbs/serial_driver_retention_cycle.py` in `pull_fs`. It:
1. captures a non-blocking full dump from `127.0.0.1:3307` and keeps it as `served_before`, per file with artificial events stripped;
2. removes `pull.enabled`, SIGTERMs pull and waits for its exit;
3. snapshots the index;
4. runs `purge_binlogs` up to a random non-tail binlog, with optional fsfault on `remove`/`rename` and an optional self-kill (those outcomes are scored by `interrupted-purge-leaves-openable-storage`);
5. re-enables pull.

Then, only if the purge reported `success`:
- `Always("pull restarted after a retention purge serves every surviving binlog identical to what it served before the purge")`: for each surviving file, the post-cycle served bytes start with `served_before[file]`. A prefix relation is used because the tail may have grown.
- `Always("no event from a purged binlog is served after a retention cycle")`: a dump naming any purged file yields zero binlog events. The ERR versus close distinction is scored by `dump-session-ends-with-eof-or-err-packet`.
- `Sometimes("pull stored a new source transaction after a retention cycle")`: collection resumes. This is liveness, evaluated after faults stop.
- `Sometimes("empty-name dump after a retention purge started at the oldest surviving binlog")`: the vacuity marker that makes `dump-preamble-matches-request`'s clause reachable in-contract. The encryption on/off and mode go in the details, so per-config coverage is visible.

`Always` fits the first two because a single wrong served byte breaks PITR from the binlog server.

## SUT-side instrumentation suggestions (all missing; see existing-assertions.md)

- `storage_core.cpp` constructor, streaming mode: `SOMETIMES(first indexed record's previous_gtids non-empty, "streaming storage opened on a purged storage")`. This shows that pull starts on post-purge storages; the condition is meaningful because only a purge, or a start from a non-empty `gtid_purged`, makes the first record's previous set non-empty.
- `storage_core.cpp:1001-1004`: `ALWAYS_OR_UNREACHABLE(binlog_records_.empty() || <purged_gtids_ unused>, ...)` is not expressible as stated. Leave OQ 11 to the catalog-wide list. No new assertion.

## Open questions

- **What retention policy are operators expected to follow with downstream consumers?** If retention must respect the slowest downstream client, the driver purges only below every client's current file, and a chained wedge is a workload error. If retention may purge past a lagging consumer, the chained wedge is expected and `pull-chain-converges-after-heal` must exclude it, and ERR-vs-close becomes the consumer's only signal. That raises the priority of `dump-session-ends-with-eof-or-err-packet`. `(needs human input)`

## Resolved during investigation (see Investigation Log)

- **Stop → purge → restart is the only in-contract retention procedure for pull.** README:473 says purge beside a fetching/pulling instance on the same storage "may cause data corruption and should be avoided". The README documents no explicit procedure, but any in-contract retention of a pull storage must stop pull first. Whether operators violate this in practice is catalog Open Question 1, not this property's question. The property's scope is unchanged.

### Investigation Log

#### What retention policy are operators expected to follow with downstream consumers?

- Examined: README `purge_binlogs` section (`:380-473`), `pull` section (`:375-378`), `replication_source` config (`:595-605`), ticket snapshot (`grep -i retention|expire_logs`), `network_service.cpp:305-313` (a request naming a purged file is closed without ERR).
- Found: the README describes purge only in terms of the storage (sequence-number cut-off, tail refusal, warning status). It says nothing about downstream consumers of `pull`, and PBS keeps no record of which files connected clients still need. No ticket covers the question.
- Not found: any stated retention policy with respect to consumers.
- Conclusion: `(needs human input)`. Until answered, the driver purges only below every known downstream client's current file, and a separately marked variant purges past a lagging client with `pull-chain-converges-after-heal` excluded.

#### Is the stop → purge → restart cycle the documented retention procedure for pull, or do operators purge beside a running pull?

- Examined: README:443-473.
- Found: README:473: "running the utility in the `purge_binlogs` mode on a certain storage ... while another instance is fetching / pulling data to the same storage may cause data corruption and should be avoided." Stopping pull is therefore required for in-contract retention, even though the README spells out no step-by-step procedure.
- Conclusion: resolved. This property tests the only in-contract path. Purge beside a running pull remains a misuse scenario owned by the `contention` phase and catalog OQ 1.
