# rewrite-pitr-by-datetime-matches-source

## What led to it

This crosses rewrite mode, PITR tooling and catch-up lag. In rewrite mode PBS **writes** generated events stamped with its own wall clock at generation time:
- the non-artificial FORMAT_DESCRIPTION (`operations/event_generation_helpers.cpp:117` for the common-header timestamp, `:92` for the post-header create_timestamp);
- PREVIOUS_GTIDS_LOG (`:149`);
- the closing non-artificial ROTATE (`:48`, with `current_timestamp=true` from `collector_context.cpp:379-380`).

They are generated when a rewritten file is created, which happens when PBS *processes* the transaction that crosses `file_size` (`collector_context.cpp:352-420`), not when the source committed it. Whenever PBS lags the source (first backup of an existing server, or after any outage, partition, kill/restart or S3 slowness), every rewritten file created during catch-up starts with `FDE(now) PREVIOUS_GTIDS(now)` followed by transactions whose source timestamps are minutes or hours older.

`mysqlbinlog --stop-datetime=T` stops at the first event of **any** type whose common-header `timestamp >= T`, FORMAT_DESCRIPTION included. FDE is exempt only from the `--start-datetime`/`--offset` filter. Verified in `client/mysqlbinlog.cc` `process_event` for 8.0, 8.4, 9.6 and 9.7.1; see Investigation results. So a PITR to any time T inside the catch-up window stops at the PREVIOUS_GTIDS of the first file generated after T, and **silently omits every transaction from that file onward that committed before T (in fact it stops at that file's FDE(now), one event earlier)**. `search_by_timestamp` itself is conservative and returns the right files (I checked `search_helpers.cpp:64-85`: it stops only at a file whose minimum timestamp > T, which is a superset of what is needed). The loss happens in the standard replay tool the files are meant for.

sut-analysis lists this as W5/N11 ("unverified"). `binlog-metadata-matches-stored-bytes` covers metadata-vs-bytes fidelity and notes the skew, but no property asserts the user-visible PITR outcome.

## Oracle

At quiescence, pick a few T values from the source's transaction timestamps, including T values inside a window where PBS was partitioned or killed:
- Reference: `mysqlbinlog --stop-datetime=T` over the source's binlogs (or `--read-from-remote-server` against the source), extracting the GTID set printed.
- Candidate: `mysqlbinlog --stop-datetime=T` over the rewritten storage, either from the files directly (unencrypted) or via `--read-from-remote-server` against `pull` (all storages).
- `ALWAYS(candidate GTID set == reference GTID set)`. Running `--start-datetime` symmetrically checks the other edge.

Non-rewrite storages are a control: they contain no generated events, so the property should trivially hold there.

## Antithesis angle

No clock fault is needed. Lag is the trigger, and Antithesis partitions between source and PBS, PBS kills and S3 stalls all create lag. Clock jitter makes it worse: a forward jump stamps a rewritten file's preamble in the future, so even a no-lag PITR stops early.

## Instrumentation suggestions

- SUT-side (missing): in `rewrite_and_process_binlog_event` after generating the preamble, `SOMETIMES(generated_preamble_ts > first_source_event_ts_in_file + some_seconds, "rewritten file preamble stamped later than its first transaction")`. It marks the state; the oracle is workload-side.
- Workload (missing): `ALWAYS(rewrite-storage PITR-by-datetime GTID set equals source PITR-by-datetime GTID set)`.

## Open questions

- Is stamping generated FDE, PREVIOUS_GTIDS and ROTATE events with PBS's wall clock an intentional design choice? Using the timestamp of the first or last source event in the file would keep `min/max_timestamp` and PITR consistent. Whether to fix the code or to document "PITR by datetime is unsupported on rewritten storages" is the owners' decision. `(needs human input)`

## Investigation results (2026-10-13)

- **mysqlbinlog exempts nothing from `--stop-datetime`.** In `process_event`, the stop check `if ((my_time_t)(ev->common_header->when.tv_sec) >= stop_datetime) { retval = OK_STOP; ... }` sits *inside* the block entered for FDE events and for events passing the start filter. FDE is exempt only from `--start-datetime`/`--offset`, as the code comment says: "Format events are not concerned by --offset and such". It is **not** exempt from the stop check. Source locations: PXB trunk 9.7.1 `client/mysqlbinlog.cc:1400-1437`, PXC 9.6 `:1405-1442`, PXB `origin/8.4` `:1394-1426`, PXB `origin/8.0` `:1380-1412`. `OK_STOP` ends `dump_multiple_logs` for all remaining files (PXB trunk `:2466-2471`). Artificial ROTATEs carry timestamp 0 (`collector_context.cpp:390-395`) and never trigger the stop. The generated FDE gets `ctime_timestamp::now()` in its common header even when artificial (`event_generation_helpers.cpp:117-121`).
- **Consequence: the property is expected to fail on the first catch-up of ≥ 1 s.** For any T inside a lag window, the candidate stops at the FDE(now) of the first rewritten file created after T, or at the closing ROTATE(now) of the file before it, and drops every later transaction with source timestamp < T. The defect is confirmed by code reading on both sides: PBS generation and mysqlbinlog's stop rule. It has not been executed. The `Always` stays as is, and this property is now a **likely-failing** one rather than a regression guard. The `--start-datetime` direction also fails: the first PREVIOUS_GTIDS(now) ≥ T switches printing on for everything after it (`start_datetime = 0`), so the candidate returns a **superset** that includes transactions older than T. Keep both as distinct messages: "rewrite-storage PITR stop-datetime GTID set equals source" and "rewrite-storage PITR start-datetime GTID set equals source".
- **Oracle refinements required:**
  1. Run `mysqlbinlog` in **non-raw** mode, because `--stop-datetime` is ignored with `--raw` (PXC 9.6 `client/mysqlbinlog.cc:3192-3193`). Extract GTIDs from the `SET @@SESSION.GTID_NEXT=` lines.
  2. Timestamps are compared at **1 s** granularity (`when.tv_sec`). The lag window must be ≥ 2 s, and T values are whole seconds.
  3. The reference is not trivially comparable. Source events carry statement-start times, so they are not monotonic in commit order, and the source's own FDE, PREVIOUS_GTIDS and ROTATE (absent from the rewritten storage) can stop the reference earlier. Use short autocommit transactions, and choose T in a gap where every earlier source transaction's events are < T and every later one's are ≥ T, with no source rotation inside the gap. Alternatively, compute the reference directly from the parsed source as "GTIDs of transactions before the first transaction event with `when ≥ T`".
  4. Non-rewrite storages remain the control.

### Investigation Log

#### Does `mysqlbinlog --stop-datetime` exempt PREVIOUS_GTIDS_LOG and ROTATE as well as FDE?

- Examined: `~/work/percona-xtrabackup/client/mysqlbinlog.cc` (trunk, MYSQL_VERSION 9.7.1) `process_event` :1385-1440, `dump_multiple_logs` :2440-2480; the same function in `git show origin/8.0:` and `origin/8.4:`; `~/work/percona-xtradb-cluster/client/mysqlbinlog.cc` (9.6) :1395-1445 and raw-mode warnings :3180-3195. Also PBS `event_generation_helpers.cpp:84-133` and `collector_context.cpp:376-420`.
- Found: no event type is exempt from the stop check. FDE is exempt only from the start and offset filter. `OK_STOP` aborts all remaining files. The rule is identical in 8.0, 8.4, 9.6 and 9.7.1.
- Not found: an upstream Oracle MySQL tree. The Percona forks share this code unchanged, and no fork-specific change to this function was seen.
- Conclusion: resolved. The premise is worse than first assumed (even the FDE stops), so the property is expected to fail under lag.

#### Is the generated-event timestamp an intentional design choice?

- Examined: `event_generation_helpers.cpp:40-150` (all generated events use `now()` unless artificial), `collector_context.cpp:376-420`, README:592-595 (rewrite section), README `search_by_timestamp` sections, and the ticket snapshot.
- Found: there are no comments or documentation explaining the choice, and nothing about PITR on rewritten storages.
- Not found: the owners' intent.
- Conclusion: `(needs human input)`.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- `mysqlbinlog --start/--stop-datetime` read T in the client's local time zone, so the oracle pins `TZ=UTC` for both the computation of T and the `mysqlbinlog` call (implementability P11).
- Runs in `fetch_fs`.
