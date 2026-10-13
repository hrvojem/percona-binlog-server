# stored-binlogs-replay-to-source-state — Replaying the stored binlogs reproduces the source's tables

## What led here

Evaluation gap G2 (wildcard W5 "restorability", plus W8 "event variety"). Every oracle in the catalog compares bytes, GTID sequences or served streams. None of them applies the backup. Yet the workflow PBS exists for is point-in-time recovery: restore a base backup, then `mysqlbinlog <stored binlogs> | mysql` (README:3, 711; ticket 21's impact text names "mysqlbinlog for point-in-time recovery"). An apply-based oracle checks the result the user cares about: table contents.

It matters most where the byte oracles are weak or don't apply:

- **Rewrite mode.** CRCs are recomputed, and the FDE, PREVIOUS_GTIDS, closing ROTATE, `last_committed`/`sequence_number` and `transaction_length` are regenerated (`rewriter.cpp`, `event_generation_helpers.cpp:48, 92, 117, 149`). Byte identity cannot hold, so the catalog checks rewrite storages by internal validity (`rewrite-binlog-chain-valid-across-crashes`) and by GTID sequence and payload equality (`rewrite-storage-transaction-sequence-matches-source`). Neither runs the files through the consumer.
- **Encrypted storages.** These are compared after decryption (offline decryptor, wildcard W2) or through pull. A decrypt-then-replay check is the end-to-end form of `encrypted-storage-decrypts-to-source`.
- **Boundary-sensitive event kinds.** Boundary detection is length-based (`reader_context.cpp:949-1019`). The kinds are compressed `TRANSACTION_PAYLOAD`, XA (an XA PREPARE group and a separate XA COMMIT group, each with its own GTID), `PARTIAL_UPDATE_ROWS` (PARTIAL_JSON), MINIMAL row image, and DDL (a GTID plus a Query event with no BEGIN/XID). `extra/qa/event_variety_test.py:18-40` exists because "the crash test only writes plain row-based transactions". A kill, rotation, checkpoint or rewrite `file_size` split that lands next to one of these is exactly the combination a byte oracle on plain storages never sees.
- **`stored-rotate-or-stop-is-last-event`.** Does a replay actually skip transactions after a terminal ROTATE/STOP? That property's open question 1 asks what downstream tools do with such files. Replay answers it empirically.

## Prior art in the repo

- `mtr/binlog_streaming/t/rs_tail_reuse.test:70-110`: dumps through pull with `mysqlbinlog --read-from-remote-server`, moves the original table aside, replays with `$MYSQL < dump`, and compares an order-independent row signature (`BIT_XOR(CRC32(CONCAT_WS('|', id, LENGTH(data), MD5(data)))`). That is this oracle at MTR scale, with one fixed scenario and no faults.
- `mtr/binlog_streaming/t/gtid_renumbering*.test`: runs `mysqlbinlog --base64-output=DECODE-ROWS` over rewritten files for textual inspection only. Nothing is applied.
- `extra/qa/event_variety_test.py`: the event-kind generators (`compression`, `minimal_row_image`, `partial_json`, `xa`, `tagged_gtid`, `:62-123`) can be reused directly as per-transaction choices in the load generator.

## Oracle design

The check runs at quiescence, with no writer or purge running. Run it in a `finally_` command, or in the `eventually_` command after convergence.

1. **Readable plaintext files.** Plain storage: copy the indexed binlogs `[0, metadata.size)`. Encrypted storage: decrypt offline with the independent Python decryptor (W2, planned in `encrypted-storage-decrypts-to-source`). Do not read through a fetch storage via pull, because that makes the oracle a writer (W2). S3: download through boto3 on `s3:9000`, bypassing the fault proxy.
2. **Candidate.** Start a scratch `mysqld` with the same `MYSQL_VERSION` as the source, freshly `--initialize-insecure`d, with `gtid_mode=ON`. Run `mysqlbinlog --verify-binlog-checksum <files in index order> | mysql`. The storage starts at the source's first binlog, because PBS fetches from the very beginning on an empty storage and the source never purges (`binlog_expire_logs_seconds=0`). So a full replay from an empty server rebuilds the workload schema. Record the `mysqlbinlog` and `mysql` exit codes and stderr.
3. **Reference.** There are two forms:
   - **Converged.** The stored GTID set (union of `added_gtids`) equals the source's `gtid_executed`. In position mode, the stored names and sizes equal `SHOW BINARY LOGS`. Then compare directly with `CHECKSUM TABLE` (or the `rs_tail_reuse` signature) for each workload table on the source.
   - **Prefix (not converged).** Build a second scratch server by replaying the source's own binlogs (`mysqlbinlog --read-from-remote-server --raw` from `mysql-source`, as the `anytime_storage_prefix.py` oracle already does), cut at the stored point: `--include-gtids=<stored set>` in GTID and rewrite mode, and the same file list plus `--stop-position=<tail metadata.size>` in position mode. Both sides then go through the identical `mysqlbinlog | mysql` path, so MySQL replay quirks cancel out and only PBS differences remain.
4. **Compare.** Compare the per-table checksum or signature for every workload table, plus the set of pending prepared XA transactions (`XA RECOVER`). A storage may legitimately end between XA PREPARE and XA COMMIT, and the reference then has the same pending xid.

Two replay variants, with distinct assertion messages:

- **Realistic (default):** `mysqlbinlog` emits `SET GTID_NEXT`, so the scratch server **auto-skips a GTID it has already executed**. A duplicated transaction in the storage (ticket 18, lead (a)) is therefore invisible in this variant, in GTID *and* position mode, because the harness source always runs `gtid_mode=ON`. This is deliberate: it measures what a real restore suffers.
- **Strict:** `mysqlbinlog --skip-gtids` drops `GTID_NEXT`, so every stored transaction is applied. A duplicate then shows up as a duplicate-key error or extra rows. This overlaps the exactly-once properties, but the result is in restore terms.

## Failure scenarios this catches that byte or sequence oracles may not

- A rewrite-mode file whose regenerated `transaction_length` or FDE/ROTATE makes `mysqlbinlog` mis-split or stop early. The transaction sequence matches, but the replay loses rows.
- A boundary misclassification for XA, a compressed payload or DDL: the storage holds a split or merged group that still parses. The replay applies it in the wrong session or transaction, or fails with "XAER_RMFAIL" or a statement error.
- Events after a terminal ROTATE/STOP (`stored-rotate-or-stop-is-last-event`). Whether `mysqlbinlog` in local-file mode ignores them or applies them decides whether this is silent loss, silent extra data or harmless. The replay shows which.
- Encrypted storages where decryption yields a valid-looking but wrong event stream (keystream reuse, lead (d), `ctr-keystream-not-reused-across-truncation`). The offset-based byte check could miss a case the decryptor reproduces "consistently".
- Triage value: ticket 14 (GTID resume drops a closing ROTATE) is expected to change files but **not** restored state. Replay separates "cosmetic file difference" from "restore-breaking" failures across the many expected-to-fail properties.

## Observations from code (not executed)

- PBS never alters event payloads in non-rewrite mode. It stores the source's bytes as received (`storage_core.cpp:299-322`). So in non-rewrite unencrypted mode, any replay mismatch implies a byte-oracle failure as well. The replay check's unique value there is the event-variety and triage angle. Its strongest unique value is rewrite and encrypted storages.
- `mysqlbinlog` replay does not use the logical clock (`last_committed`/`sequence_number`). Regenerated clock values matter only to multi-threaded replicas, which cannot use pull (ticket 20). So this oracle does **not** cover clock correctness. That stays with `rewrite-binlog-chain-valid-across-crashes`.
- Datetime-based selection (`--stop-datetime`) is deliberately not used. Lead (c) makes it fail in rewrite mode under lag, and `rewrite-pitr-by-datetime-matches-source` owns that.

## Harness needs (new)

- The `runtime-base` stage keeps only `bin/mysqlbinlog` from the generic tarball (deployment-topology §pbs). This property also needs `bin/mysqld` (and `bin/mysql`) from the same tarball, plus the time to start a scratch instance: `--initialize-insecure` takes several seconds, and the replay is proportional to the workload volume. Keep source load in a run modest, or sample the replay (for example in one timeline in k).
- A new command, `finally_replay_restore.py` in `pbs`, for the `fetch_fs`, `pull_fs` (after pull is stopped gracefully) and `s3` templates. Alternatively fold it into the single `eventually_` command after its convergence check. Because only one `eventually_` runs per timeline (topology open question), `finally_` is the safer home.
- Load generator: make event kind a per-transaction random dimension (compression per session, XA, PARTIAL_JSON with `JSON_SET`, MINIMAL image with UPDATE/DELETE, DDL including `CREATE/ALTER/DROP TABLE` on a side table, tagged GTIDs on 8.4 or later), reusing the `event_variety_test.py` generators. Tables need a primary key, so that the checksum comparison is deterministic.

## SUT instrumentation suggestions (all missing)

- None required. This is a workload oracle. Optional: `SOMETIMES("rewrite split a binlog inside an XA prepare/commit pair")` in `rewrite_and_process_binlog_event` as an exploration anchor for the riskiest event-kind combination.

## Open questions

- **Is the replay cost affordable per timeline?** If a scratch `mysqld` start plus replay takes tens of seconds, the property may need sampling, which lowers how often it sees rare combinations. A short local timing run would settle this. `(partial: cost is a scratch-instance initialize plus a replay linear in stored volume; no timing run was possible in this read-only pass)`
- **Is an encrypted storage expected to be restorable only through pull, or also offline?** If pull is the only supported read path, the realistic restore for encrypted storages is `mysqlbinlog --read-from-remote-server` against pull on a snapshot copy (W2), and the offline decryptor is an oracle convenience rather than the user path. `(needs human input)`

## Resolved during investigation (see Investigation Log)

- **`mysqlbinlog` local-file mode applies events after a ROTATE or STOP in the same file.** `dump_local_log_entries` reads until EOF and never stops on ROTATE/STOP (same in 8.0.35 and 9.7.1). Implication for `stored-rotate-or-stop-is-last-event`: trailing events are replayed. In the realistic variant, trailing transactions that also appear in the next file are auto-skipped by `GTID_NEXT` (harmless for restore); trailing events that are partial or foreign do reach the server. In the strict variant they are applied twice. So this property fires on that defect only when the trailing data is not a clean duplicate, or in the strict variant.
- **The prefix-form reference is sound for XA and compressed payloads.** `--include-gtids` filters per GTID group: the decision is taken at each GTID event and applies to every following event until the next GTID (`shall_skip_gtids`), so XA PREPARE and XA COMMIT (separate GTIDs) are cut independently and a `TRANSACTION_PAYLOAD` follows its GTID's decision. The reference keeps a pending XA exactly when the storage does.
- **Strict variant: log, do not assert.** Decision: record the strict (`--skip-gtids`) result in the details of the realistic assertion and in logs. Duplicates are owned by the exactly-once properties, and asserting the strict form would re-report every ticket 18 / lead (a) duplicate under a second message. **Property change:** only the realistic `Always` is asserted.

### Investigation Log

#### What does `mysqlbinlog` (local-file mode) do with events that follow a ROTATE or STOP inside the same file?

- Examined: `client/mysqlbinlog.cc` in `/Users/hrvoje/work/percona-xtrabackup` at 9.7.1 (`dump_local_log_entries` `:3052-3125`, `process_event` `:1380-1800`, `shall_skip_gtids` `:1125-1210`) and at `origin/8.0` (8.0.35, `dump_local_log_entries` `:3043-3080`, `case binary_log::STOP_EVENT/ROTATE_EVENT` `:1177-1179`).
- Found: the local loop is `for (;;) { ev = read_event_object(); if (!ev) return READ_EOF ? OK_CONTINUE : ERROR_STOP; process_event(...) }`. `process_event` returns `OK_STOP` only for `--stop-position`/`--stop-datetime`. ROTATE and STOP are printed as comments and are never filtered (`shall_skip_gtids` lists them as `filtered = false`). Events after them are decoded and emitted as usual.
- Conclusion: resolved. Trailing events are replayed; see "Resolved during investigation" for what that means for both properties.

#### Is the prefix-form reference (`--include-gtids` / `--stop-position` cut) equivalent to "source state at that point"?

- Examined: `mysqlbinlog.cc` 9.7.1 `shall_skip_gtids` (`:1125-1210`), `args_post_process` (`--include-gtids` parsing; `--raw` incompatible with `--include-gtids`, so the reference must be built from raw files fetched first, then filtered in decode mode).
- Found: the GTID decision is set at GTID/GTID_TAGGED/ANONYMOUS_GTID events and carried to all following events (default case), reset at XID and at group-ending Query events. XA PREPARE and XA COMMIT are separate GTID groups, compressed payloads sit inside one group. `--stop-position` in position mode stops at the first event at or past the position, and `metadata.size` is a transaction boundary.
- Not found: a run; MySQL replay quirks are assumed to cancel because both sides use the same `mysqlbinlog | mysql` path.
- Conclusion: resolved. Note for the harness: fetch the source's binlogs with `--raw` first, then apply `--include-gtids` in a second, non-raw `mysqlbinlog` pass.

#### Is the replay cost affordable per timeline?

- Examined: `deployment-topology.md` (runtime image keeps only `bin/mysqlbinlog`), `rs_tail_reuse.test:70-110` (MTR-scale replay).
- Found: the cost is a fresh `--initialize-insecure` plus server start, then a replay linear in the stored volume. No measurement was possible (no builds or runs in this pass).
- Conclusion: `(partial: cost model known; needs a local timing run)`.

#### Should the strict (`--skip-gtids`) variant be asserted, or only logged?

- Examined: the exactly-once property set and ticket 18 / lead (a) ownership in the catalog.
- Found: every duplicate the strict variant would catch is already asserted elsewhere.
- Conclusion: resolved as an analysis decision: logged and attached as details, not asserted.

#### Is an encrypted storage expected to be restorable only through pull, or also offline?

- Examined: README modes list (`:150-180`, no decrypt mode), `<storage.encryption>` section (`:673-679`), `src/operations/` (no decrypt operation), `mtr/binlog_streaming/t/encryption.test:160-241`.
- Found: the MTR test decrypts offline with the OpenSSL CLI (unwrap the file key with the KEK, then `openssl enc -<cipher> -d -K <key> -iv <iv>`) and diffs against the source binlog. So offline decryption is mechanically possible and the format is exercised in tests, which grounds the oracle's offline decryptor. Nothing in the README offers it to users.
- Not found: whether maintainers consider offline decryption a supported restore path.
- Conclusion: `(needs human input)`. The oracle can use the offline decryptor either way (MTR shows the procedure); the answer only decides which path is the "realistic" restore.
