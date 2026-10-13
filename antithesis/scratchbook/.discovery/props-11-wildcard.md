# Property discovery: Focus 11, Wildcard

This pass started where the ten focused lenses stop. I read the 87 evidence files already in `properties/` and skipped anything they cover. In particular I did **not** re-propose:
- `served-bytes-stable-across-restart` and `pull-chain-prefix-consistency` (served-bytes retraction);
- `query-sees-binlogs-flushed-before-start` (tail/prefix drop);
- `zombie-writer-takeover-fails-closed` (same server_id on the same storage);
- `metadata-timestamps-match-stored-events`;
- `rewrite-binlog-chain-valid-across-crashes`.

The six properties below are cross-cuts. Each one either finds a new outcome for a known mechanism, or applies a new oracle to an area that has none.

## Consumer-facing silent failures

### query-success-never-omits-middle-binlog — A successful query never silently omits a binlog from the middle of the index

| | |
|---|---|
| **Type** | Safety |
| **Property** | A `list` / `search_by_timestamp` / `search_by_gtid_set` response with `status: success` never contains a gap: its files are a contiguous run of the binlog index as it stood when the query started. |
| **Invariant** | Workload `Always` per query mode (three distinct messages): the returned names are contiguous in the pre-query `binlog.index` snapshot. It is `Always` because every successful response must satisfy it. SUT-side `Sometimes(dropped record is neither first nor last)` in the `querying_only` catch at `storage_core.cpp:943-949`, as an exploration anchor for the dangerous state. |
| **Antithesis Angle** | `querying_only` erases any record whose `.json` load throws (`storage_core.cpp:937-950`), sequentially and without logging (`null_logger`). A source↔S3 partition that outlasts the CRT GET retries for key k and heals before key m produces a hole without any shim. A concurrent `purge_binlogs` interleaving (deletes in the order m1, b1, m2, b2…, `storage_core.cpp:400-406`) produces `{1,3,…}`. Antithesis' partition timing during a tight query loop is the explorer. |
| **Why It Matters** | `search_by_timestamp` returns the files before and after the hole as `success` (`search_helpers.cpp:64-85` has no contiguity check), and a PITR replay silently skips a whole binlog. This is the W6 "silent" class, reached from a single transient fault. |

**Priority:** P1 · **Confidence:** high (the drop and the absence of a warning are confirmed in code; how likely a hole is depends on retry timing) · **Fault requirements:** network faults to S3 (native), or a per-key GET 5xx shim; optionally a concurrent purge.

**Open Questions:**

- Is silently dropping an unreadable middle record intended (the "forensics" comment), or should the status become `warning`/`error`?
- What are the S3 CRT default GET retry count and timeout, and how long must a partition last to cause a drop?

### rewrite-pitr-by-datetime-matches-source — PITR by datetime over a rewritten storage selects the same transactions as over the source

| | |
|---|---|
| **Type** | Safety |
| **Property** | For any T, `mysqlbinlog --stop-datetime=T` (and `--start-datetime`) over a rewrite-mode storage outputs the same GTID set as over the source's binlogs. |
| **Invariant** | Workload `Always(candidate GTID set == reference GTID set)`, evaluated at quiescence for sampled T values, including T inside windows where PBS lagged. SUT-side `Sometimes(generated preamble timestamp > first source event timestamp in the file)` in `rewrite_and_process_binlog_event`. |
| **Antithesis Angle** | Generated FDE, PREVIOUS_GTIDS and closing ROTATE carry PBS `now()` (`event_generation_helpers.cpp:48, 92, 117, 149`) and are stored. Any lag (partition, kill/restart, S3 stall) makes every file created during catch-up start with "future" events, and `--stop-datetime` stops at them. Lag alone is enough; forward clock jumps make it worse. |
| **Why It Matters** | Datetime PITR is the main restore workflow, and it silently loses transactions committed before T. `search_by_timestamp` itself is conservative-correct (checked), so the loss happens in the downstream tool the files exist for. This is W5/N11, made concrete. |

**Priority:** P1 · **Confidence:** medium (the PBS side is confirmed; the claim that mysqlbinlog exempts only FDE is from memory) · **Fault requirements:** network partition or process kill (native); clock jitter optional.

**Open Questions:**

- Does `mysqlbinlog --stop-datetime` exempt PREVIOUS_GTIDS_LOG/ROTATE as well as FDE, in 8.0/8.4/9.x?
- Is the wall-clock stamping of generated events intentional?

## Fault-amplified outcomes of known bugs

### position-resume-after-duplicate-flush-never-skips-source — A duplicated flush never turns into silently skipped source bytes on position-mode resume

| | |
|---|---|
| **Type** | Safety |
| **Property** | In position mode, after any failed flush and restart, every byte the source wrote to binlog N up to the resume point is stored exactly once; resume never jumps past source bytes. |
| **Invariant** | Workload `Always(stored N is a byte prefix of source N)`, plus a source-free `Always(walking stored N from offset 4, every event's next_event_position == offset + size)`. The duplicated copy's headers fail this at S+n. SUT-side `Unreachable("write_event_block re-appends a block whose data append already succeeded")`. SUT-side startup `Always(tail's last event header position == metadata size)`, the cheap fix for "data never parsed on open" (`storage_core.cpp:785`). Workload `Sometimes(resume after failed flush lands on a source event boundary)`. |
| **Antithesis Angle** | Ticket 18 leaves `[S) ++ B ++ B` with size S+2n. Siblings assume the position-mode resume (N, S+2n) gets 1236 forever. With **uniform-size transactions**, S+2n is a valid source boundary, so the source streams from there and every continuity check passes (`reader_context.cpp:547-556, 905-927`), while source bytes `[S+n, S+2n)` are never stored. If S+2n is past the source's EOF, retries succeed once the source catches up. Triggers: an S3 partition between the data PUT and the metadata PUT, ENOSPC on `*.json.tmp`, or N2 (a concurrent purge deleting `.json.tmp`). |
| **Why It Matters** | Duplicate plus loss, with every length, boundary and position invariant intact. It corrects the "detectable wedge" assumption in `failed-flush-never-reappends-data`. GTID mode fails loudly instead. |

**Priority:** P1 · **Confidence:** medium (the PBS mechanism is confirmed in code; MySQL's behaviour for dump starts past EOF or inside an event is unverified) · **Fault requirements:** S3 network faults (native), or `fsfault` ENOSPC on `.json.tmp`, or a concurrent purge; process restart. The workload must generate fixed-width transactions some of the time.

**Open Questions:**

- What does MySQL 8.0/8.4/9.x return for a position dump start (a) past EOF and (b) inside an event?
- Does the QA "binlog as sent" oracle compare whole bytes or only GTID sets?

### distinct-storages-shared-server-id-stay-consistent — Copy-pasted server_id across independent PBS instances never corrupts either storage

| | |
|---|---|
| **Type** | Safety (plus a liveness companion) |
| **Property** | Two PBS writers on **different** storages that share `replication.server_id` (every README example uses 42) each keep a storage that is a byte prefix of the source and that opens on the next run. |
| **Invariant** | Workload `Always(each storage is a byte prefix of the source)`. Workload `Sometimes(both instances advanced in the same window)` for progress. SUT-side `Sometimes(mid-stream fetch error != 2013)` at `connection.cpp:546-551`, labelled with the errno. |
| **Antithesis Angle** | PBS sets no replica UUID, so MySQL evicts the other dump thread with the same server_id. The pair becomes a continuous, natural mid-stream disconnect injector: every eviction runs discard, flush and resume (tickets 14 and 18 surfaces). CPU modulation and pauses decide who evicts whom. If eviction surfaces as a non-2013 error, pull fail-stops (N7). |
| **Why It Matters** | A realistic misconfiguration (HA pair or cron fetch plus pull from one config template) silently becomes either a crash loop or a resume-path torture test. `zombie-writer-takeover-fails-closed` covers only the same-storage case. |

**Priority:** P2 · **Confidence:** low-medium (eviction by server_id is standard MySQL behaviour; the client errno is unverified) · **Fault requirements:** none beyond the topology; timing faults help.

**Open Questions:**

- Which client error does `mysql_binlog_fetch` get when MySQL kills a same-server_id zombie dump thread?
- Should PBS detect or refuse a server_id collision (for example by registering a replica UUID)?

## Oracles for areas that have none

### chained-fetch-from-pull-reproduces-upstream — A PBS `fetch` pointed at a PBS `pull` reproduces the upstream storage byte for byte

| | |
|---|---|
| **Type** | Safety |
| **Property** | A downstream position-mode PBS `fetch` against an upstream PBS `pull` listener exits 0 with each downstream binlog equal to the upstream's flushed (decrypted) bytes, and never rejects the served stream. |
| **Invariant** | Workload `Always(downstream file N == upstream N[0:upstream size at fetch end], with only the tail allowed to be shorter)`. Workload `Always(downstream fetch never exits with a reader_context logic_error)`. Workload `Sometimes(downstream resumed at an upstream tail offset while the upstream next binlog was magic-only)`. |
| **Antithesis Angle** | It reuses PBS's strict `reader_context` (FSM, position continuity, artificial-ROTATE position, FDE checks) as an oracle on the least-settled code, `sender_context`. Downstream kills exercise start-from-offset at arbitrary offsets. Upstream kills, pauses and S3 faults overlap serving with rotation, checkpoints and recovery. It catches N5 (missing artificial ROTATE at a magic-only next file), ticket 26, regressions of 21/22, and decryption faults. Handshake compatibility was checked in code: version `9.7.0-pbs`, the SET-checksum query in `known_queries`, empty-name dump resolution, and NON_BLOCK EOF. |
| **Why It Matters** | Pull is the only restore path for encrypted storage. Lenient clients (`mysqlbinlog`) hide framing and file-switch errors that a strict reader exposes. It also tests cascaded backup as a topology. |

**Priority:** P1 · **Confidence:** medium (compatibility was traced in code, not executed) · **Fault requirements:** process kill and pause, network faults between containers (native).

**Open Questions:**

- Is PBS → PBS chaining a supported topology or only a test device?
- Does the strict reader accept every legitimate file-termination variant the sender may serve, including a binlog with no ROTATE/STOP after an improper source shutdown?

### rewrite-storage-transaction-sequence-matches-source — A rewritten storage holds exactly the source's transactions, in order, with identical payloads

| | |
|---|---|
| **Type** | Safety |
| **Property** | After any sequence of kills, partitions and source rotations, the transaction sequence (GTID plus event bodies, excluding headers, footers and the logical-clock and length fields) parsed from a rewrite-mode storage equals the source's sequence after the storage's initial purged set. |
| **Invariant** | Workload `Always(rewritten transaction sequence == source sequence)` and `Always(rewritten logical clock valid: 0 <= last_committed < sequence_number, sequence_number strictly increasing per file)`. SUT-side `Always(rewritten event size == source event size)` in `rewrite_and_process_binlog_event`, which the `transaction_length` logic depends on. SUT-side `Sometimes(rewrite resumed into a non-empty rewritten file)`. |
| **Antithesis Angle** | Rewrite has no byte oracle and has never been crash-tested. GTID-only resume does not verify that what the source skipped equals what was stored. Small `file_size`, frequent source `FLUSH BINARY LOGS`, kills across preamble generation and checkpoints, tagged GTIDs, and large transactions. |
| **Why It Matters** | Existing rewrite properties check internal validity and duplicates only. A storage can be internally valid and still be missing or altering transactions, and the recomputed CRCs (W6) hide payload corruption. |

**Priority:** P1 · **Confidence:** medium · **Fault requirements:** process kill, network partition (native).

**Open Questions:**

- After a GTID reconnect, does every tested MySQL version re-send a partially received transaction from its GTID event?
- Is "a source transaction is never split across rewritten files" the intended contract?

## Areas examined

- `querying_only` construction and the three query operations (`storage_core.cpp:150-200, 760-790, 930-1004`; `search_helpers.cpp`; `list_operation.cpp`; `search_by_*_operation.cpp`).
- Purge deletion order (`storage_core.cpp:331-433`).
- The write/flush/destructor path (`storage.cpp:68-77, 119-200, 276-302`; `storage_core.cpp:299-322, 700-715`) and position-mode resume (`collector_context.cpp:136-272`).
- `reader_context` boundary and position checks (`reader_context.hpp:40-130`, `.cpp:540-665, 900-1019`).
- The rewrite pipeline and event generation (`collector_context.cpp:274-429`, `event_generation_helpers.cpp`, `rewriter.cpp:130-220`).
- The listener: known queries, dump handling, session exception handling, handshake version and auth (`network_service.cpp:225-334, 480-685`; `connection_context.*`).
- The pull thread structure (`pull_operation.cpp:118-180`), easymysql flags and the checksum SET (`connection.cpp:185-223, 546`), and `replication_config.hpp`.
- The 87 existing evidence files, to avoid overlap.

**Negative results (not turned into properties):**
- `search_by_timestamp` under non-monotone source timestamps (clock steps, long transactions) is still a superset of what `mysqlbinlog --stop-datetime` needs, because it stops only at a file whose *minimum* exceeds T. No property is needed.
- Encryption-offset misalignment after recovery: in every path I traced, the counter offset equals the physical offset except two-writer and N1 truncation, which siblings (`cipher-offset-equals-physical-offset`) cover.
- The rewrite logical-clock translation looked sound across source rotations (agrees with focus 12).

**Odd things worth a human look (intuitions, not validated):**
- Toggling `replication.verify_checksum` between runs on the same tail file: PBS sends `@source_binlog_checksum='NONE'` when it is false. If MySQL then strips CRCs, the resumed tail mixes CRC and non-CRC events under a single stored FDE. I could not settle MySQL's NONE behaviour from code here.
- A source that loses its binlog tail on crash (`sync_binlog != 1`) leaves PBS *ahead* of the source. Position-mode resume past the source's EOF waits for the source to grow, then lands wherever the new bytes put a boundary. This needs page-cache loss on the source container, so it may be unreachable in Antithesis.
- No focus lens owns **consumer-tool semantics**: raw object copies, `mysqlbinlog` PITR, and scripts parsing `search_*` JSON. Two properties above live there. On the filesystem a raw copy of the tail can contain bytes beyond metadata size (pre-recovery, or the ticket-18 duplicate) that only the next PBS start removes.
- The source-free header walk (`next_event_position == offset + size` from offset 4 to metadata size) is a universal, cheap oracle for plain, encrypted (via pull or keyring) and rewrite storages. I recommend it as a shared workload helper rather than a property of its own.

## Cross-property assumptions

- The workload has direct read access to the storage (a filesystem volume or S3 credentials) and to the source's binlogs (via `mysqlbinlog --read-from-remote-server` or `binlog_as_sent()` from `extra/qa`).
- `mysqlbinlog` (matching the source's major version) is installed in the workload container.
- The SUT is a debug or ASan build, so assert-only checks stay live.
- The S3 stand-in is strongly consistent (MinIO or Versity), and the S3 client is left at SDK default retries.
- Fixed-width transaction generation is part of the workload's input mix, alongside variable-size transactions.
