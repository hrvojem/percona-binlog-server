# rewrite-checksum-none-fail-stop — Rewrite mode fails stop on a checksum-less source file without corrupting the storage

## What led here
- `collector_context.cpp:300-321`: in rewrite mode an FDE without `checksum_algorithm` raises `logic_error("rewrite is supported in gtid replication mode only when all events received from the MySQL server have checksums")`. The reason given: `transaction_length` recomputation assumes event sizes do not change.
- `mtr/binlog_streaming/t/gtid_rewrite_enforce_checksums.test` checks this deterministically: it toggles `binlog_checksum` **before** the fetch.
- What is untested: the toggle happening **while** PBS (especially pull) is mid-stream, with buffered but unflushed events of the CRC32 file, plus kills and reconnects. The source's `SET GLOBAL binlog_checksum` rotates. Rewrite mode skips the real ROTATE/STOP (`:290-334`), so PBS's open rewritten file stays open. The exception then propagates out of `receive_binlog_events`, which ends pull and its replication source (`pull_operation.cpp:154-157`, LG1). `~storage` flushes the buffer to the last transaction boundary.
- After that, every restart reconnects with the stored GTID set, the source starts from (or before) the NONE file, and the run fails again. This is a permanent wedge until the source purges that file. That is probably intended, but the property is that the **storage** is never corrupted along the way.

## Oracle
- `ALWAYS(after the failure the storage opens in streaming mode)`. Checked by the workload: run `list`, then a fetch with the checksum restored. While NONE-era files remain on the source, the fetch is expected to fail again (and purging them on the source would lose their GTIDs, giving error 1236). So progress is **not** asserted, only that every failure leaves a storage that opens and is unchanged except for whole transactions.
- `ALWAYS(every event stored in rewritten files carries a valid CRC32 footer)`. The workload parses the stored files and verifies the CRCs.
- `ALWAYS(stored added_gtids ⊆ GTIDs of CRC32 source files)`: no event originating from a NONE file reaches the storage.

## SUT-side instrumentation (missing)
- `REACHABLE("rewrite rejected checksum-less FDE mid-stream")` at `collector_context.cpp:318`, so we know the fault/workload combination is reached while the storage is non-empty.

## Open questions

- None. Both questions are resolved; see the Investigation Log.

## Answers and implications

- **Wedge is the documented current behaviour.** README line 593 says rewrite "can properly operate ... only when all binlog events received from the MySQL server have checksums", and the TODO at `collector_context.cpp:311-313` calls lifting this future work. The property therefore stays safety-only: no `SOMETIMES(rewrite resumes after checksum restored)` while NONE-era files remain on the source. A liveness check is reasonable only after the workload has purged the NONE files on the source, and that loses their GTIDs anyway (error 1236), so it is not added.
- **The reader in rewrite mode does not track the source's checksum setting** until the first generated FDE. `event_view` sizes the footer from `context.is_footer_expected()` (`event_view.cpp:60-63`). That flag starts at `verify_checksum` (`reader_context.cpp:47`) and changes only when an FDE goes through `process_event_view` (`:663`). Source FDEs are skipped before that (`collector_context.cpp:290-334`), and the generated FDE is processed only after `current_event_v` for the first transaction event has been built (`:153-162`). So the first GTID event of every connection is viewed with `footer_expected_ == verify_checksum`.
  - With `verify_checksum = true` (the only setting any rewrite MTR test uses) this matches a CRC32 source, and a NONE source is stopped by the FDE check before any of its events is viewed. The property's premise holds.
  - With `verify_checksum = false`, PBS sets `@source_binlog_checksum = 'NONE'` (`easymysql/connection.cpp:214-222`). The MySQL sender does not strip stored CRCs: `init_checksum_alg` only refuses an UNDEF replica, and events are sent as stored (`sql/rpl_binlog_sender.cc:999-1027, 1138-1145` in percona-xtrabackup f1aa0691f76, MySQL 9.7.1). So the first GTID_LOG event arrives with a CRC but is viewed without a footer. `rewriter::rewrite` uses `force_add_checksum` and appends a second CRC (`rewriter.cpp:80-101`, `:222-225`). The rewritten body then carries 4 stale bytes and fails `gtid_log_body` parsing ("extra bytes in the gtid_log event body", `gtid_log_body.cpp:126-128`), or, if an original_server_version field absorbs them, fails the transaction-length checks (`reader_context.cpp:1004-1015`). Traced from code, **not run**: rewrite + `verify_checksum=false` against a CRC32 source likely fails stop on the first transaction of every connection. This is a liveness gap, not corruption, because the exception comes before the transaction boundary and `~storage` flushes only up to the last boundary.
  - **Property change:** none for this property (keep `verify_checksum=true` in its workload config). **New candidate** for the orchestrator: `rewrite-without-checksum-verification-progresses`, `Sometimes(rewrite fetch with verify_checksum=false stores a transaction)`, low-to-medium priority. It is deterministic, so an MTR combination with `$binsrv_verify_checksum = FALSE` in `resume_streaming` / `gtid_renumbering` is the cheaper confirmation. The GTID_TAGGED_LOG path re-encodes the body (`rewriter.cpp:228-300`) and may survive; that is not traced.

### Investigation Log

#### Is a permanent wedge the intended operator experience, or should rewrite add/strip CRCs?

- Examined: README lines 592-595, `collector_context.cpp:300-321` (check and TODO), `mtr/binlog_streaming/t/gtid_rewrite_enforce_checksums.test` and `.combinations`.
- Found: README documents the restriction as current behaviour ("currently the utility can properly operate in 'rewrite' mode only when ..."). The TODO describes lifting it as a future "whole transaction rewrite". The MTR test asserts exit 1 plus the exact message, including the `gtid_on_checksum_off` combination where the server starts with `binlog-checksum=NONE`, so fail-stop is the tested contract.
- Not found: any plan for automatic recovery once checksums are restored.
- Conclusion: resolved. Fail-stop is the documented contract, so no liveness assertion is added. Adding or stripping CRCs is future work that would change this property if it lands.

#### Does reader_context in rewrite mode know the source's checksum setting?

- Examined: `reader_context.cpp:40-47, 640-670`, `event_view.cpp:44-150`, `collector_context.cpp:124-165, 274-425`, `rewriter.cpp:40-125, 140-300`, `gtid_log_body.cpp:78-130`, `easymysql/connection.cpp:214-222`, MySQL `sql/rpl_binlog_sender.cc` (percona-xtrabackup f1aa0691f76) `init_checksum_alg`, `fake_rotate_event` and FDE handling, and all MTR tests that set `$binsrv_rewrite_file_size` (each uses `verify_checksum = TRUE`).
- Found: see "Answers and implications" above. The footer assumption is used on that path, for the first event of every connection, and it is wrong exactly when `verify_checksum=false` meets a CRC32 source.
- Not found: an execution confirming the failure (no builds or tests run, per brief); whether the tagged-GTID path also fails.
- Conclusion: resolved for this property, whose premise holds with `verify_checksum=true`. The `verify_checksum=false` case is handed off as a new candidate property and a cheap MTR confirmation.
