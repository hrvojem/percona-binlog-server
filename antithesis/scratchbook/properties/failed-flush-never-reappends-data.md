# failed-flush-never-reappends-data — A failed flush is never re-appended (ticket 18)

Found by: focus 9 (idempotency/replay), 3 (failure recovery), 5 (resource), 11 (wildcard).
Merged from: failed-flush-never-reappends-data (focus 9), failed-flush-retry-never-duplicates (focus 3), enospc-write-fails-closed-without-duplicates (focus 5), position-resume-after-duplicate-flush-never-skips-source (focus 11).

## Synthesis notes

- **Type and priority:** Safety, P0. Every contributor rated it P0 except the wildcard, which rated its position-mode outcome P1.
- **Primary assertion:** a SUT-side `UNREACHABLE` that fires the moment a buffer is appended again after its data append already succeeded. The flag is set when `save_binlog_metadata` throws after `size +=` (`storage_core.cpp:307-321`). It is checked in `storage::flush_event_buffer_internal` / `write_event_block`.
- **Window anchor:** a twin `SOMETIMES(metadata save failed after a successful data append)`.
- **Workload `Always` (two checks):**
  - no transaction appears twice in the stored bytes (GTID scan);
  - a source-free header walk holds: `next_event_position == offset + size` from offset 4 up to the metadata size.
- **Conflict resolved:** focus 9 assumed that position mode always wedges with CR_1236 after the duplicate. The wildcard showed otherwise: with uniform-size transactions, S+2n can be a valid source boundary. Position-mode resume then silently skips source bytes [S+n, S+2n), and every continuity check passes. So the catalog does not treat "detectable wedge" as the position-mode outcome. There are three outcomes:
  - wedge;
  - silent duplicate (GTID or rewrite mode; exit 0 is possible);
  - silent duplicate plus skip (position mode).

  This is why the root-cause `UNREACHABLE` is primary, not an end-of-run oracle.
- **ENOSPC (focus 5) is a trigger variant, not a separate property.** It takes the same code path: the data fits, the `.json.tmp` write fails, and space is freed before the `~storage` retry.
- **Workload requirement:** some of the time, the workload must generate fixed-width transactions.

## What led to it

- **Ticket 18** (open; validated in the sut-analysis "Bug history" and re-read by focus 9 and 3):
  - `size +=` happens before the metadata save (`storage_core.cpp:310, 321`).
  - The buffer is erased only on success (`storage.cpp:286-296`).
  - The destructor re-flushes (`storage.cpp:68-77`).

  In short, the facade erases buffered bytes only after `storage_core::write_event_block` returns, and `write_event_block` bumps the in-memory `size` before it saves metadata. Any exception after the append leaves the bytes in the buffer, and the next flush (normally `~storage`) appends them a second time.
- **Focus 3 lens:** this is a retry that is not idempotent. The retry is implicit: after any exception in the collector, stack unwinding destroys `storage`, and `~storage` calls `flush_event_buffer()` again on the same buffer.
- **Focus 5 lens:** disk-full during a write. Both the filesystem storage volume and the S3 local buffer directory are finite, so exhaustion is a natural fault. The workload's own data fills a size-limited volume, and no injection shim is needed. The leak `s3-crash-leftovers-bounded` makes local ENOSPC on S3 a likely real-world outcome.
- **Focus 11 lens:** the siblings treated the permanent duplicate as "a duplicate" and assumed that position mode always wedges with CR 1236. That is wrong for a common workload shape (see trigger D / the position-mode outcome below).

## Code paths

- **Duplicate creation:**
  - `src/binsrv/storage_core.cpp:299-322` (also cited as `:305-321` and `:307-321`) `write_event_block`, in order:
    1. `write_data_to_stream(data, …, size)` appends n bytes;
    2. `size += n` (`:310`);
    3. GTIDs and timestamps are added;
    4. `save_binlog_metadata` / `put_object(<name>.json)` (`:321`).

    There is no rollback. All of this runs under the unique lock.
  - `src/binsrv/storage.cpp:276-302` (also cited as `:276-297`) `flush_event_buffer_internal`: calls `core_->write_event_block(...)` (`:286`), then `event_buffer_.erase(...)` (`:296`). If the call throws, the erase never happens.
  - `src/binsrv/storage.cpp:68-77` `~storage`: in streaming mode it calls `flush_event_buffer()` inside `try { } catch (...) {}`.
- **Exception sources that reach the destructor:**
  - a checkpoint-flush exception inside `write_event` (`storage.cpp:175-200`);
  - the timeout-disconnect flush (`collector_context.cpp:200`);
  - `close_binlog`'s flush.

  Each one propagates to `pull_operation.cpp:154` / `fetch_operation.cpp:100`. Then:
  - `fetch_operation.cpp:98-110` captures the exception, destroys the collector with the try block, and rethrows.
  - `pull_operation.cpp:133-166` follows the same pattern. Here `storage` is a `shared_ptr` that `network_service` also holds.
- **Filesystem backend:**
  - `filesystem_storage_backend.cpp:216-231`: `ofs_.write` + `flush`, with no fsync. On ENOSPC, part of the block may already be in the file when failbit is set. The failbit is sticky, so a failed data write makes the retry fail too (no duplicate).
  - `filesystem_storage_backend.cpp:127-160` `put_object`: `write_file_content(<name>.tmp)` (`util/file_operations_helpers.cpp:98-121`). On ENOSPC a partial `.tmp` is left behind, and the next streaming or purging start deletes it (`storage_core.cpp:597-630`).
  - A failed `.json` save leaves the data stream healthy, so the retry appends again. Causes: ENOSPC/EIO on `.json.tmp`, or N2 (a concurrent purge or second instance deletes the in-flight `.json.tmp` via `storage_core.cpp:597-630`, so the rename fails with ENOENT).
- **S3 backend:**
  - `s3_storage_backend.cpp:850-863` (also cited as `:850-862`) appends to the local temp file (which can hit ENOSPC on the buffer volume) before the upload. `size +=` runs only on success.
  - The whole object is then re-uploaded (`upload_tmp_stream_internal`, `:933-938`).
  - A failed data upload also leaves a duplicate in the object, but metadata stays at old+n, so startup truncation removes it. Only a metadata-PUT failure is permanent (ticket text and sut-analysis "Resolved contradiction").
  - `do_resize_object` downloads into the buffer directory (`s3_storage_backend.cpp:775-787`).
- **Startup recovery:**
  - on the tail, actual > metadata ⇒ truncate (`storage_core.cpp:979`);
  - metadata > actual ⇒ fatal (`:974-977`);
  - nothing parses data at open (`storage_core.cpp:785` TODO).
- **Resume:**
  - position dump at the stored name + size: `collector_context.cpp:253-261` (`:256-260`), `storage_core.hpp:313-337`;
  - GTID-mode info-only FDE/PREVIOUS_GTIDS: `reader_context.cpp:579-582`;
  - position validation that trips only on a new transaction in N: `reader_context.cpp:916-920`;
  - "cannot open an existing binlog that is not the latest one": `storage_core.cpp:276-279`.
- **Continuity checks that do not catch the position-mode skip:** `reader_context.cpp:547-556` (artificial ROTATE position), `reader_context.cpp:905-927` / `:908-921` (`validate_position`).

## Failure scenarios

### Core sequence (all triggers)

1. Binlog N has metadata size S. The collector flushes n bytes (B = n bytes of whole transactions). The data append succeeds, `size = S+n`, and the metadata save fails.
2. The exception ends fetch/pull. `~storage` re-flushes: it appends the same n bytes again, sets `size = S+2n` and saves the metadata, which now succeeds.
3. The storage now holds `[0,S) + B + B` with metadata S+2n. Recovery sees no mismatch, because metadata agrees with the data.

### Triggers

- **A. S3 metadata PUT fails, then the network heals before the destructor runs (focus 3 scenario 1, focus 9).**
  1. The upload of old+n succeeds and the in-memory size is set to old+n. The metadata PUT throws (a 5xx beyond the SDK retries, or a partition).
  2. The destructor appends locally (old+2n), uploads old+2n, sets the size to old+2n and PUTs metadata old+2n.

  The duplicate is permanent. Antithesis reaches this natively: a PBS↔S3 partition that starts between the data PUT and the metadata PUT and ends before `~storage`. The CRT SDK retries must be exhausted first.
- **B. Ambiguous success of the metadata PUT (focus 3 scenario 2).** The server applied the PUT but the response was lost, so S3 metadata already says old+n. If the network is back, the destructor's flush produces old+2n in both data and metadata. The outcome is the same as A.
- **C. S3 data PUT fails or is ambiguous; self-healing control case (focus 3 scenario 3).** `size +=` is not reached. The destructor appends again: the local file and the upload are old+2n, while size and metadata stay at old+n. The restart truncates to old+n. This is safe, as the sut-analysis already resolved.
- **D. Filesystem `.json` write fails (focus 3 scenario 4, focus 9, focus 11).** Causes: ENOSPC/EIO on `.json.tmp`, or ENOENT on rename because a concurrent purge or second instance deleted the `.tmp` (N2). The N2 cause is out of contract: README.md:473 says purge alongside a writer on the same storage "may cause data corruption and should be avoided". Keep N2 out of this property's runs. `ofs_` stays healthy, so the destructor's append succeeds. If the metadata PUT then succeeds, the duplicate is permanent.
- **E. ENOSPC, then space freed (focus 5 scenario 1).** The block exactly fills the volume and the `.json.tmp` write fails. Memory holds `size = old + n`, and the exception unwinds to `~storage`, which retries.
  - Usually the volume is still full, so the second data write fails and recovery truncates to old. This case self-heals.
  - If space is freed between the two attempts (another process deletes files; a concurrent purge on the same storage is out of contract per README.md:473), the retry appends the same n bytes and saves `size = old + 2n`. The duplicate is permanent.
- **F. Partial data block on ENOSPC (focus 5 scenario 2).** Garbage beyond the metadata size is truncated at the next start. Expected to be safe; the property confirms it.
- **G. ENOSPC during startup recovery (focus 5 scenario 3).** `resize_object` on the filesystem needs no space. On S3, `do_resize_object` downloads into the buffer directory, so a full buffer volume blocks every startup (a wedge that overlaps LG5).
- **H. ENOSPC on rotation (focus 5 scenario 4).** Magic → `.json` → index: this is the ticket 15 window, reached through ENOSPC instead of a kill.

### Outcomes after a permanent duplicate (by mode)

- **Position mode, wedge:** the next fetch asks the source for (N, S+2n). The MySQL sender only checks `start_pos > file length` at dump start (fatal "position > file size", CR_1236; `rpl_binlog_sender.cc` in the PXC 9.6 / PXB 9.7 trees, and ticket 18's 8.4.8 reproduction). It has no event-boundary check: a start inside an event seeks there and reads garbage, which ends in a sender read error ("bogus data in log event", "exceeded max_allowed_packet", truncated event) or in events that fail PBS's `validate_position` (`reader_context.cpp:907-921`). Either way the failure is loud. If the source's file N ends before S+2n for good (it rotated), the wedge is permanent.
- **Position mode, silent duplicate plus skip (focus 11):** the source's file N at `[S, S+n)` is B. If the source's `[S+n, S+2n)` is also a whole number of events, then S+2n is a valid source boundary. This happens whenever transactions have uniform size: fixed-width rows, sysbench-style loads, or n a multiple of a constant transaction size. The source then streams from S+2n:
  - The artificial ROTATE carries position S+2n, which matches the stored position, so `reader_context.cpp:547-556` passes.
  - The next event's `next_event_position == S+2n+size`, so `validate_position` passes.
  - The source bytes `[S+n, S+2n)` are never stored.

  The net result is n bytes duplicated plus n bytes silently skipped, with metadata, boundaries and position continuity all consistent. If S+2n is past the source EOF, MySQL returns 1236 and pull/fetch retries. Once the source writes past S+2n, the resume succeeds, so the "wedge" is only temporary and ends in the silent skip whenever the boundary lines up.
- **GTID mode (focus 9's analysis, which extends the ticket; confirmed by code, see Investigation Log):** the source starts from the newest file whose previous_gtids ⊆ the requested set (`find_first_log_not_in_gtid_set`, documented in `mtr/binlog_streaming/t/gtid_resume_after_rotation.test:7-11`). Skipped groups produce only heartbeats, which PBS asks libmysqlclient to drop (`MYSQL_RPL_SKIP_HEARTBEAT`, `connection.cpp:205`).
  - If the source has no new transactions in N, the FDE and PREVIOUS_GTIDS arrive info-only and no position check runs. **Fetch exits 0 with the duplicate stored**, which breaks "exit 0 ⇒ exact copy". This is the same path that `resume_streaming.test` step 3a (gtid combinations) requires to exit 0.
  - If the source has rotated past N, the collector closes N and opens a new file, and the duplicate is permanent and silent.
  - Only a new transaction in N trips `reader_context.cpp:916-920`. Focus 11 notes the fail-closed variant: the first unsent transaction carries a `next_event_position` based on S+n, and the reader at S+2n raises "unexpected next event position", a loud wedge.
- **Rewrite mode:** the reader validates positions of PBS's own rewritten events, so the duplicate is never detected.
- **pull readers** served the second copy receive every transaction in it twice.

## Key observations

- The retry runs in the destructor of the same process, so a single fault without a kill reproduces it.
- Antithesis needs a fault on the metadata write specifically:
  - an S3 stand-in or proxy that can fail `*.json` PUTs (or a PBS↔S3 partition timed between the data and metadata PUTs);
  - `fsfault` ENOSPC on `*.json.tmp` writes only;
  - a size-limited volume;
  - (the N2 race, a concurrent purge deleting `.json.tmp`, is out of contract per README.md:473).

  The fault must heal before process exit.
- The window between the data append and the metadata save also exists for SIGKILL. A kill there does not duplicate (no destructor runs), and recovery truncates. That case is only the "lose progress" path.
- In GTID mode, `added_gtids` cannot reveal the duplicate: the GTID is added twice to a set, so the set looks fine while the bytes contain the transaction twice. Only a byte-level or event-level check catches it.
- Source-free self-oracle (focus 11): the duplicate copy at `[S+n, S+2n)` carries event headers whose `next_event_position` values belong to `[S, S+n)`. Walking the stored file from offset 4 and checking `next_event_position == offset + event_size` fails exactly at S+n. This works for plain storages, for encrypted storages (through pull or the keyring; see `encrypted-storage-decrypts-to-source`) and for rewrite storages, whose recomputed positions are self-consistent.

## Oracle / workload checks

- **Workload shape:**
  - Some of the time, produce uniform-size transactions, for example a fixed-width `INSERT INTO t(id BIGINT, pad CHAR(100))` stream with a monotonically increasing id of constant encoded width.
  - Use a small `checkpoint_size` so that n is a small multiple of the transaction size.
  - Mixed-size workloads exercise the 1236 branch, and uniform ones exercise the silent-skip branch.
- **ENOSPC setup (focus 5):** mount the storage directory (or the S3 `fs_buffer_directory`) on a small size-limited volume (tmpfs `size=` or a loop device). A workload "filler" task randomly allocates and frees ballast files there while fetch and pull run. After every PBS exit, free the ballast and run fetch to EOF.
- **After every writer exit (non-zero or otherwise):** restart, converge, then check the following (each with a distinct message):
  - `Always("no transaction appears twice in stored binlog bytes")`: GTID scan, or `parse_events` / `transaction_boundaries` from `extra/qa`. In position mode, use (file, next_pos) uniqueness. Decrypt through pull for encrypted storages.
  - `Always("stored binlog header walk is continuous from offset 4 to metadata size")`: the source-free walk `next_event_position == offset + size`.
  - `Always("stored binlog is a byte prefix of the source binlog")`: QA `storage_holds_prefixes`.
  - `Always("fetch to EOF after a write fault exits 0 with a complete copy")`: `check_complete_copy()` (`extra/qa/fetch_crash_test.py:636-697`).
  - `Always("binlog added_gtids matches GTIDs parsed from stored bytes")`: metadata size equals an event boundary that equals the source offset.
  - `Always("PBS run that hit ENOSPC never exits 0 outside the final-flush path")`: an ENOSPC run is identified by the "cannot write data" or "cannot write data to … tmp" log line. The final-flush path is covered by `graceful-exit-leaves-flushed-storage` / `graceful-exit-leaves-flushed-storage`.
- `Sometimes("resume position after a failed flush lands on a source event boundary")`.
- `Sometimes("ENOSPC hit between data append and metadata save")`: from the SUT-side marker.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion below is **missing**.

| Status | Assertion | Site | Notes |
|---|---|---|---|
| missing (primary) | `UNREACHABLE("write_event_block re-appends a block whose data append already succeeded")` | entry of `storage_core::write_event_block`, or top of `storage::flush_event_buffer_internal` | Uses a flag (`write_failed_` / `last_write_left_size_ahead_of_metadata_`) that is set when `save_binlog_metadata` throws after `size +=` (`storage_core.cpp:307-321`). It fires before the bytes land. Merges focus 9's `ALWAYS(!write_failed_, "storage never re-flushes a buffer after a failed write_event_block")` and focus 3's and 11's UNREACHABLEs, which are the same check at the same site. Once fixed, the flag becomes the fix's own state. |
| missing | `SOMETIMES("metadata save failed after a successful data append")` | `write_event_block`, in the catch around `save_binlog_metadata` after a successful data write | Window anchor (proposed by focus 9 and 5). |
| missing | `REACHABLE("storage destructor swallowed a flush exception")` | `catch (...)` at `storage.cpp:74` | Exploration anchor (focus 3). Owned by `recovery-paths-exercised`; reference it, do not duplicate it. |
| missing | `ALWAYS("record size equals backend object size after write_event_block")` | after `write_event_block` returns; filesystem backend, debug builds | Focus 5. |
| missing | `ALWAYS("tail binlog ends on an event whose header position matches its offset")` | streaming-mode construction: parse the last event header of the tail (`storage_core.cpp:785` TODO) | Focus 11. A cheap check for any "never parsed on open" divergence. |
| missing | `SOMETIMES("position resume after duplicate hit CR_1236")` and `SOMETIMES("position resume after duplicate landed on a source boundary")` | collector resume path | Shows which position-mode branch was explored (focus 11). |

## Fault requirements

- **S3:** a PBS↔S3 partition or a proxy (`s3_fault_proxy.py`) that fails or delays `*.json` PUTs for longer than the CRT retries, then heals before `~storage`. Also a lost-response (ambiguous) PUT.
- **Filesystem:**
  - a size-limited volume with a ballast filler (natural ENOSPC);
  - or `extra/qa/fsfault.c` (LD_PRELOAD) ENOSPC/EIO on `*.json.tmp`;
  - not a concurrent `purge_binlogs` (N2): out of contract per README.md:473. A second writer instance is a separate contract question (see `stored-binlog-byte-prefix-of-source`).
- No kills or node termination are needed. A restart follows each writer exit.

## Answers from investigation (2026-10-13)

- **ENOSPC variant = ticket 18.** Ticket 18 itself names ENOSPC/EIO on any step of the `.json.tmp` save as a trigger ("A full disk is enough to trigger it"). A duplicate after "ENOSPC, then space freed" is the same known failure, not a new finding.
- **QA oracles compare bytes.** `storage_holds_prefixes()` (`extra/qa/fetch_crash_test.py:637-669`) compares the whole stored file with the prefix of `binlog_as_sent()` and checks the metadata size is a transaction boundary. `check_complete_copy()` (`:671-697`) requires byte equality. Both catch the position-mode duplicate-plus-skip, so the reused oracles are sufficient (no GTID-set-only blind spot).
- **GTID resume with nothing new: exit 0 with the duplicate is reachable** (see GTID-mode outcome above). The "fail-stop via `storage_core.cpp:276-279`" alternative does not happen, because the source never starts before the newest file whose previous_gtids ⊆ the requested set, and that is N or later.
- **Odd dump positions:** past EOF is a hard error at dump start, retried successfully once the source file grows past S+2n; mid-event is not rejected by the sender but always ends loudly. So the silent skip needs exact boundary alignment, as the catalog already says. Checked in 9.x sender source only; 8.0 is not available locally (8.4.8 past-EOF behaviour is confirmed by ticket 18).
- **Short writes (trigger F) are moot for the property.** Whether part of a failed data block stays on disk, the in-memory size is not raised (the throw is before `size +=`), the stream's badbit is sticky (an `ostream::write` sentry fails on a stream that is not `good()`), and the next start truncates anything beyond the metadata size (`storage_core.cpp:979`). Trigger F stays as a check-only control case. `fsfault` fails whole calls and never produces a short write, so F is reachable only with a natural ENOSPC.
- **Property changes:** none to the invariant, assertion types or priority. Scope: N2 and "concurrent purge frees space" are excluded as out of contract (README.md:473). The `stored-transactions-exactly-once-across-resumes` investigation added a block-level GTID-overlap check in `write_event_block` (block GTIDs ∩ previous ∪ added GTIDs = ∅). It also catches ticket 18 in GTID and rewrite modes, because `added_gtids +=` runs before the metadata save (`storage_core.cpp:311-316`). That check is owned by that property; the `UNREACHABLE` here stays primary because it covers position mode too.

## Open questions

- **S3 CRT retry policy.** What are the default retry count, backoff and request timeouts in SDK 1.11.774, and do retries exhaust on a transient stall or partition, or only on persistent 5xx? `(partial: PBS uses a default Aws::S3Crt::ClientConfiguration with no retry or timeout overrides; QA sweeps show 1 failing PUT is retried transparently and 6 consecutive 500/503/reset exhaust retries; exact count, backoff and timeouts are in the SDK/CRT, not in this repo)`
  - Why it matters: it decides whether an Antithesis PBS↔S3 partition alone reaches trigger A, or whether `s3_fault_proxy.py` is required.
- **Intended fix direction.** "All-or-nothing `write_event_block`" vs "no re-flush after failure" vs "re-read the stored size" (ticket 18 lists all three). `(needs human input)`
  - The property statement does not change; only the site of the SUT-side flag does.
- **Size-limited volumes in Antithesis.** Is compose `tmpfs: size=` (or a loop volume) honoured in the PBS container? `(needs human input)` — same as the catalog-wide `[needs-check]` in `deployment-topology.md`; `libfsfault.so` is the fallback.

### Investigation Log

#### S3 CRT retry policy

- Examined: `src/binsrv/s3_storage_backend.cpp:186-237` (aws_context construction), `CMakeLists.txt:85`, `extra/qa/s3_fault_sweep.py:17-31, 45-48`, `extra/qa/s3_fault_proxy.py` header, ticket 15/18 text in `.tickets-snapshot.txt:810, 986-1016`.
- Found: `configuration_{}` is default-constructed; only `region`, `scheme`, `useVirtualAddressing` and `endpointOverride` are set. No `retryStrategy`, `connectTimeoutMs` or `requestTimeoutMs`. The QA sweep runs each fault once and 6 times and its docstring states 6 "outlasts the AWS SDK's retries"; ticket 18 says single failures are all retried transparently.
- Not found: SDK 1.11.774 / aws-c-s3 sources or headers locally; the effect of a 3 s `stall` (sweep results are not in the repo); behaviour on a silent packet drop.
- Conclusion: `(partial: ...)`. Plan for `s3_fault_proxy.py` rules targeted at `.json` PUTs (count ≥ 6) as the reliable trigger; treat natural partitions as a bonus.

#### GTID resume when the requested set already covers everything

- Examined: `reader_context.cpp:547-600, 664-745, 905-927`; `connection.cpp:185-205`; `mtr/binlog_streaming/t/resume_streaming.test` + `.combinations`; `gtid_resume_after_rotation.test:1-14`; PXC 9.6 `sql/rpl_binlog_sender.cc:940-978, 615-704`.
- Found: the sender picks the file via `find_first_log_not_in_gtid_set` (newest file whose previous_gtids ⊆ the requested set), so with everything covered it starts at N (or a newer file if the source rotated). Excluded groups are skipped and only heartbeats are sent (`:627-666, :699-704`); PBS sets `MYSQL_RPL_SKIP_HEARTBEAT`. In GTID mode the artificial ROTATE always has position 4 and the FDE/PREVIOUS_GTIDS are info-only when `position_ > 4` (`reader_context.cpp:579-582`), so no `validate_position` runs. `resume_streaming.test` step 3a runs fetch in gtid combinations on a tail with no new events and requires exit 0.
- Conclusion: resolved. "Exit 0 with the duplicate stored" is reachable; the fail-stop alternative is not.

#### MySQL behaviour on odd dump positions

- Examined: PXC 9.6 and PXB 9.7.1 `sql/rpl_binlog_sender.cc` (`check_start_file` `:993-1011`, `send_binlog` `:497-515`, `log_read_error_msg` `:1222-1240`); ticket 18 reproduction (8.4.8).
- Found: (a) past EOF → fatal "Client requested source to start replication from position > file size" at dump start (ticket 18 shows CR_1236 on 8.4.8). The check is against the file length at that moment, so a later retry succeeds once the source file grows past S+2n. (b) inside an event → no boundary check; the sender seeks to `start_pos` and reads, so the result is a read error (BOGUS / EVENT_TOO_LARGE / TRUNC_EVENT) or garbage events that PBS's `validate_position` rejects.
- Not found: 8.0/8.4 sender source locally (only 9.x trees).
- Conclusion: resolved for the property. Silent skip needs boundary alignment; every misaligned resume is loud.

#### What the QA "binlog as sent" oracle compares

- Examined: `extra/qa/fetch_crash_test.py:637-697` (via the `stored-binlog-byte-prefix-of-source` and `stored-transactions-exactly-once-across-resumes` investigations).
- Found: whole-file byte comparison against `binlog_as_sent()` (prefix, then equality), plus a transaction-boundary check of the metadata size.
- Conclusion: resolved. The oracle catches the duplicate-plus-skip.

#### Intended fix direction

- Examined: ticket 18 "Suggested fix" (`.tickets-snapshot.txt:1006-1010`).
- Found: three alternatives listed; no maintainer decision recorded in the repo.
- Conclusion: `(needs human input)`. Does not affect the property.

#### ENOSPC variant classification

- Examined: ticket 18 impact and tests sections (`.tickets-snapshot.txt:1000, 1016`); `storage_core.cpp:299-322`; `storage.cpp:68-77, 280-300`.
- Found: the ticket covers ENOSPC/EIO on open, write, fsync or rename of `binlog.NNNNNN.json.tmp` and says a full disk is enough. The code path is identical: `size +=` (`:310`) before `save_binlog_metadata` (`:321`), erase only after return, destructor re-flush.
- Conclusion: resolved. Same bug as ticket 18; report as a known failure.

#### libstdc++ short writes

- Examined: `filesystem_storage_backend.cpp:216-231`; `CMakeLists.txt:39-52` (libc++ only by default on Darwin, so Linux builds use libstdc++); `extra/qa/fsfault/fsfault.c:24-34, 307-329`.
- Found: on a failed data write `write_event_block` throws before `size +=`, the stream state stays bad so the destructor's retry also fails, and startup truncates anything beyond the metadata size. `fsfault` fails the whole call (no short writes).
- Not found: whether libstdc++'s filebuf leaves the bytes of a short `write()` on disk (not checked; outside the repo).
- Conclusion: resolved as moot. The outcome is safe either way, so the answer cannot change the property.

#### Size-limited volumes in Antithesis

- Examined: `deployment-topology.md:246, 269`; `snouty` docs CLI (not installed).
- Found: already a catalog-wide `[needs-check]`; `libfsfault.so` via `LD_PRELOAD` is planned as the fallback.
- Conclusion: `(needs human input)` — Antithesis platform behaviour.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- With the `s3_fault_proxy` 5xx rule on `*.json` active, ticket 18 reproduces in almost every `s3` timeline that runs it (as `s3_fault_sweep.py` already shows). Until ticket 18 is fixed, the rule is applied at low probability per timeline so the natively triggered and combined variants (partition after the data PUT, ambiguous metadata PUT, ENOSPC freed before the destructor retry, fixed-width silent skip) get exploration budget and the `s3` template is not masked (antithesis-fit F9).
- Runs in `s3` and `fetch_fs` (fsfault). Pull on the filesystem gets no fsfault today (the supervisor starts pull without `LD_PRELOAD`); see "Uncataloged tickets and leads" in the catalog.
