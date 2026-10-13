# sender-file-switch-emits-artificial-rotate — Every binlog switch served by pull carries an artificial ROTATE and the new file's FDE (N5)

Found by: focus 2 (concurrency), focus 10 (version compat).
Merged from: sender-file-switch-emits-artificial-rotate (focus 2), pull-fde-switch-across-format-change (focus 10).

## Synthesis notes

Safety, P0 (focus 2 rated P0, focus 10 rated P1). Resolved to P0 for three reasons. The mechanism is traced end to end: `fetch_event_block` advances to N+1 on a zero-length read (storage_core.cpp:523-541), and `populate_event_block` returns EOF before `handle_binlog_file_switch` runs (sender_context.cpp:487-491). It needs only thread pausing / CPU modulation from the default fault set. When consecutive files differ in checksum or FDE version, the same skip produces misparsed events, not a cosmetic gap.

Workload `Always` messages:
- "every served file change is preceded by an artificial ROTATE naming the new file". This is the primary N5 detector.
- "first non-artificial event of each served file is its FDE". This is a sanity check only. It holds even under N5, because the stored FDE of N+1 is served as an ordinary event.
- "every served artificial ROTATE footer matches the checksum setting of the preceding FDE". This detects the checksum consequence of N5, which appears one switch **later**.

SUT-side:
- `ALWAYS(sender never changes binlog name on an empty fetch)` in `populate_event_block`
- `ALWAYS(no event of a binlog is sent before its FDE)`
- `SOMETIMES(sender at EOF saw a magic-only next binlog)`
- `SOMETIMES(sender crossed a checksum-format boundary)`

The existing concurrent-reader Hegel test strips artificial ROTATEs (storage_property_test.cpp:1356), so it cannot see this. chained-fetch-from-pull-reproduces-upstream (focus 11) also catches N5 through a strict downstream reader.

## What led here

- **Concurrency (focus 2).** The collector jthread and the asio sender sessions share one `storage_core`. This is SUT-analysis lead N5, re-traced for this pass. The sender builds its view of storage from several `fetch_event_block` calls, each under its own lock. The collector's rotation is also several steps under separate lock acquisitions: the `close_binlog` flush, then `open_binlog` of N+1, which creates a magic-only record. A sender polling at EOF of N can see the intermediate state "N+1 exists with size 4".
- **Version compat (focus 10).** `sender_context.cpp:81-140` states the serving contract. At every file switch the sender emits an artificial ROTATE, whose checksum follows the *previous* FDE's `checksum_algorithm`, and then file N+1's own FDE. Clients (mysqlbinlog, replicas) decide whether events carry a 4-byte CRC footer, and which post-header table to use, **only** from the most recent FDE. `sender_context.cpp:552-567` updates `current_binlog_checksum_` from the new file's FDE inside `handle_binlog_file_switch`. The storage can legitimately hold consecutive files with different formats:
  - the source toggles `SET GLOBAL binlog_checksum`, so MySQL rotates and the new file has a new FDE with `checksum_algorithm` off/crc32;
  - the source is upgraded 8.0→8.4 (42-entry vs 43-entry post-header table, different server version).
- If the files share a format, a client might survive the N5 skip. If N and N+1 differ in checksum setting, the client parses every event of N+1 with the wrong footer assumption. The result is a CRC mismatch, or 4 bytes of event data treated as CRC. Depending on the client, that corruption is silent or loud.
- No MTR or QA test serves a mixed-checksum or mixed-version storage through pull. MTR combinations run with checksum ON or OFF globally. `gtid_rewrite_enforce_checksums.test` toggles the setting only to check that rewrite refuses it.

## Code paths

- `storage_core::fetch_event_block` (`src/binsrv/storage_core.cpp` ~:436-565 at 99de682; :520-561; :523-541, the read_offset == size branch). When `read_offset == record_it->size`, it advances to the next record, sets `read_offset = 4`, and computes `read_length = min(len, size - 4)`. That length is **0** while N+1 is magic-only. On both backends, `get_object` with an empty range returns `{}` (`util/file_operations_helpers.cpp:40-42`; the first lines of `get_object_into_byte_buffer` in `s3_storage_backend.cpp`). It then sets `binlog_name = N+1` and returns true.
- `sender_context::populate_event_block` (`src/operations/sender_context.cpp` :484-491, :487-491) runs `if (std::empty(fetched_bytes)) { finalize_fetch_eof(...); return true; }` **before** `handle_binlog_file_switch(saved_binlog_name, buffer)`. `finalize_fetch_eof` keeps `binlog_name_`, which is now N+1.
- On the next poll, `saved_binlog_name == binlog_name_ == N+1`. `handle_binlog_file_switch` returns early and never sets `fsm_state_` to `start_from_beginning`. No artificial ROTATE(N+1, 4) is generated, and `current_binlog_checksum_`/FDE state is not refreshed from N+1's FDE (sender_context.cpp:552-567).
- Collector side: `storage::close_binlog` (`storage.cpp`) flushes N. Then the artificial ROTATE from the source makes `storage_core::open_binlog` → `open_new_binlog_file_internal` (`storage_core.cpp` ~:680-703) write magic and push a record of size 4. N+1 gets data only on the next flush. Without checkpointing, that is the next rotation, disconnect or shutdown (LG3).
- `sender_context.cpp:276-303` `extract_fields_from_fde` uses a fake reader_context with version 0. This is safe for FDEs because `event_view.cpp:72-96` sizes the FDE post-header from the event size.
- `network_service.cpp:487-550`: exact-string SET queries set the session checksum. The artificial ROTATE checksum uses it only for the first ROTATE.
- `network_service.cpp` `blocking_poll_interval` (500 ms).

## Window size

- Default config (no checkpointing): the window is the whole lifetime of N+1. Any blocking client that is caught up at rotation time will almost surely hit it.
- With `checkpoint_interval=1s` (as in `rs_blocking_dump.test`), the window is about 1 s of the next event, against a 500 ms poll.

## Why existing tests miss it

- `tests/storage_property_test.cpp:1356` (the concurrent reader) calls `strip_artificial_rotates(...)` before comparing, so a missing ROTATE at a switch is invisible.
- `sender_context_property_test` (PROPERTY_TESTS.md:213) specifies the ROTATE at a switch, but focus 2 found no generator state where the next binlog is magic-only while the reader sits at EOF.

## Failure scenarios

**What the client actually receives under N5 (corrected by investigation).** After the empty fetch, the next poll reads N+1 from offset 4 (`storage_core.cpp:523-541`). The first stored event there is N+1's own FDE, so the client **does** receive the new file's FDE as an ordinary event. What is missing:
- the artificial ROTATE(N+1, 4);
- the sender's state refresh from N+1's FDE. `current_binlog_checksum_` and the server id for generated events are updated only in `handle_start_states` (`sender_context.cpp:551-567`). The artificial ROTATE at the **next** switch (N+1 to N+2) is therefore generated with N's checksum setting instead of N+1's.

**A. File ends with a real ROTATE (ordinary rotation; focus 2 trigger: thread pausing / CPU modulation).** A `mysqlbinlog --read-from-remote-server --raw --stop-never` client is at EOF of binlog.000003 when the source rotates, and its sender skips the artificial ROTATE for 000004. mysqlbinlog in raw mode takes its output file name from **any** ROTATE, real or artificial, and opens a new output file at each FDE (PXC 9.6 tree `client/mysqlbinlog.cc:2779-2845`). The real ROTATE at the end of 000003 already names 000004, and 000004's FDE arrives in-stream, so the raw output files come out correct. The defect is visible only on the raw event stream.

**A2. File ends with STOP or with no terminator (source clean restart, or crash; sender cases 4b and 4c).**
- The collector writes STOP and closes N. On reconnect it creates N+1 (magic only). N+1 stays magic-only until its first transaction is flushed.
- A blocking raw client at EOF of N hits N5. No ROTATE names N+1, so mysqlbinlog's output name stays N.
- N+1's FDE then makes mysqlbinlog reopen **N** with `my_fopen(O_WRONLY)`, which is mode `"w"` and truncates (`mysys/my_fopen.cc:77-78`). It then writes N+1's bytes into it.
- **The client's copy of N is silently overwritten with N+1's content.** The byte-identity oracle catches this, and the in-plan source restart/kill faults reach it.

**B. Checksum-format change across the switch (focus 10 trigger: `SET GLOBAL binlog_checksum` toggle or source upgrade).**
1. Binlog N is CRC32. `SET GLOBAL binlog_checksum=NONE` makes the source rotate to N+1 (NONE).
2. A blocking client is at EOF of N while N+1 is magic-only, and its sender skips per N5. The client still gets N+1's FDE in-stream, so it parses N+1's events correctly.
3. At the next rotation (N+1 to N+2), the sender handles the switch normally. Its artificial ROTATE(N+2) carries a CRC footer, because `current_binlog_checksum_` was never refreshed from N+1's FDE. The client parses it under N+1's FDE (NONE).
4. With verification off, the client treats the 4 CRC bytes as part of the file name. mysqlbinlog `--raw` names its next output file `binlog.00000X` followed by 4 garbage bytes; with the formats reversed, the name loses 4 bytes. This is silent. With `--verify-binlog-checksum` (off by default), mysqlbinlog fails with "Could not construct log event object" (loud).

The same shape applies to an 8.0 to 8.4 source upgrade between files. That changes only the server id and version used for generated events, because the artificial ROTATE's layout does not depend on the post-header table. So it is cosmetic, unless the checksum also changes.

## Oracle / workload checks

- Run one client per blocking dump session that re-parses the received stream:
  - `Always("every served file change is preceded by an artificial ROTATE naming the new file")`. Each change of binlog file seen in the event stream (the real ROTATE target) must be followed by an artificial ROTATE (flags & LOG_EVENT_ARTIFICIAL_F, timestamp 0) naming that file, before any non-artificial event of that file.
  - `Always("first non-artificial event of each served file is its FDE")`.
  - `Always("every served event parses under the most recent FDE received")`. Use the footer rule of the most recent FDE received, and verify the CRC when it is on.
- Byte-identity alternative: with `mysqlbinlog --raw`, each output file must equal the stored (decrypted) file byte for byte (a prefix for the tail).
- Compare against the stored bytes: the stream with artificial events stripped must equal the concatenated stored files (the SG9 oracle), split at the received ROTATEs.
- The workload should sometimes toggle `SET GLOBAL binlog_checksum` (and, where the image allows, upgrade the source) between rotations while blocking clients sit at EOF.

## SUT-side instrumentation

None of this exists yet; the repo has no Antithesis SDK.

| Assertion | Site | Status |
|---|---|---|
| `ALWAYS(!(fetched_bytes.empty() && binlog_name_ != saved_binlog_name), "sender never changes binlog name on an empty fetch")` | `sender_context::populate_event_block` (sender_context.cpp:487-491) | missing |
| `ALWAYS(current_binlog_checksum_ derived from the FDE of the binlog being left, "sender artificial ROTATE checksum follows the previous file's FDE")` | `sender_context::handle_start_states` (sender_context.cpp:557-567), before `generate_rotate_event_ex` | missing. It replaces the earlier "no event of a binlog is sent before its FDE" suggestion, which N5 does not violate. |
| `SOMETIMES(next record size == magic offset when switching, "sender at EOF saw a magic-only next binlog")` | `storage_core::fetch_event_block` read_offset == size branch (storage_core.cpp:523-541) | missing |
| `SOMETIMES(fde_checksum_enabled != previous current_binlog_checksum_, "sender crossed a checksum-format boundary")` | `sender_context::handle_binlog_file_switch` (sender_context.cpp:552-567) | missing |

## Key observations

- The skip needs no process or node fault, only scheduling. With default config (no checkpointing), the window spans the whole life of N+1.
- The effect grows from a cosmetic gap (same format) to misparsed events (different checksum or FDE version).
- Both existing test layers miss it: the Hegel concurrent reader strips artificial ROTATEs, and the sender_context generator never builds the magic-only-next state.

## Fault requirements

- Default fault set: thread pausing / CPU modulation to widen the collector rotation vs sender poll interleaving.
- Workload-driven source actions: rotations (FLUSH BINARY LOGS, max_binlog_size), `SET GLOBAL binlog_checksum` toggles, and optionally a source version upgrade between files.
- Blocking dump clients that are caught up at rotation time.

## Property changes from investigation

- The property claim is narrowed. Under N5, the client still receives the new file's FDE (it is the first stored event). What is lost is the artificial ROTATE and the sender's checksum refresh. The title clause "and the new file's FDE" is satisfied anyway, so the invariant to enforce is "every served file change is preceded by an artificial ROTATE naming the new file". The checksum check moves to the artificial ROTATE at the following switch.
- The scenario A impact is downgraded. With a real ROTATE at the end of N, mysqlbinlog `--raw` output is unaffected. Scenario A2 is new: with STOP or no terminator, the client's copy of N is overwritten. It is the client-visible case and needs source restarts/kills in the workload.
- Workload addition: `Sometimes("dump client at EOF of a STOP-terminated binlog while the next binlog is magic-only")`.
- Assertion type and priority are unchanged: `Always`, P0.

## Open questions

- Do mysqlbinlog 8.0 and 8.4 behave as the 9.6 source does in raw mode? That is: output name from any ROTATE, a new (truncating) output file at each FDE, and no checksum verification unless `-c` is given. `(partial: confirmed in the PXC 9.6 tree client/mysqlbinlog.cc:2755-2845 and the option default at :2046-2049; 8.0/8.4 sources not read)`
- Is the intended fix "do not advance the name on an empty next file" or "emit ROTATE whenever the name changes"? The invariant is the same either way, but the answer decides where an SUT-side `ALWAYS` belongs. `(needs human input)`

### Investigation Log

#### Does `mysqlbinlog --raw` switch output files only on the artificial ROTATE?

- Examined: `~/work/percona-xtradb-cluster/client/mysqlbinlog.cc:2716-2830` (a MySQL 9.6.0-based tree, the only MySQL source available locally), `mysys/my_fopen.cc:70-85`, PBS `storage_core.cpp:523-541` and `sender_context.cpp:466-523`.
- Found: in raw mode, only ROTATE and FDE events are deserialized. **Every** ROTATE (real or fake) sets `log_file_name` from `new_log_ident` (`:2787-2794`). A fake ROTATE with `--to-last-log`/`--stop-never` is written as 0 bytes (`:2814-2817`). **Every** FDE closes the current output file and opens `log_file_name` with `O_WRONLY`, which maps to fopen mode `"w"` and truncates (`:2819-2845`, `my_fopen.cc:77-78`). On the PBS side, after the N5 empty fetch the next poll reads N+1 from offset 4, so N+1's stored FDE is served as an ordinary event.
- Conclusion: no. With a real ROTATE at the end of N, the raw output is correct despite N5. With STOP or no terminator, N's output file is truncated and overwritten with N+1. The property must be checked on the raw event stream; the byte-identity oracle covers only scenario A2. Resolved for 9.6. Whether 8.0/8.4 match stays open (partial).

#### Is the intended fix "do not advance the name on an empty next file" or "emit ROTATE whenever the name changes"?

- Examined: `sender_context.cpp:466-523` comments, `storage_core.cpp:520-541` contract comments, PROPERTY_TESTS.md entry for `sender_context_property_test`, ticket snapshot (no ticket covers N5).
- Found: the `fetch_event_block` contract documents advancing the name with a 0-length read. `finalize_fetch_eof` documents "leaving binlog_name_ as is". Neither comment says which layer owns the fix.
- Not found: a ticket or maintainer statement.
- Conclusion: `(needs human input)`. Until it is answered, the workload `Always` is the authoritative check.

#### Does a real MySQL source send the artificial ROTATE for N+1 with N's checksum algorithm?

- Examined: PXC 9.6 tree `sql/rpl_binlog_sender.cc:401-416` (loop: `fake_rotate_event(log_file, ...)` before opening each file), `:1048-1080` (`fake_rotate_event` appends a CRC iff `event_checksum_on()`, that is `m_event_checksum_alg`), `:1117-1155` (`send_format_description_event` sets `m_event_checksum_alg` from each file's FDE), `:1040-1045`.
- Found: yes. The fake ROTATE for N+1 is generated before N+1's FDE is read, so it uses the algorithm of N's FDE. The first fake ROTATE of a session uses the replica's announced algorithm. This matches the PBS sender comment (`sender_context.cpp:110-114`).
- Conclusion: resolved. The oracle expects the artificial ROTATE footer to follow the FDE of the file being left.

#### How do mysqlbinlog 8.0/8.4/9.7 react when the footer assumption is wrong: a hard error or silent garbage?

- Examined: PXC 9.6 `client/mysqlbinlog.cc:2755-2762` (raw mode deserializes ROTATE/FDE with `opt_verify_binlog_checksum`), `:2046-2049` (option default 0), `sql/binlog_reader.cc:125-185` (`binlog_event_deserialize` takes the algorithm from the client's current FDE and verifies only when asked).
- Found: by default nothing is verified, so a ROTATE with a mismatched footer is parsed with a name that is 4 bytes too long or too short, and raw mode names its next output file after it (silent). With `--verify-binlog-checksum` the deserialize returns `CHECKSUM_FAILURE`, and mysqlbinlog prints "Could not construct log event object" and stops (loud).
- Conclusion: resolved for 9.6. The workload must re-parse the stream itself and cannot rely on exit codes. The 8.0/8.4 confirmation is folded into the remaining partial question.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- In the default config (no checkpointing) N5 is effectively deterministic for any caught-up blocking client at a rotation; that case goes to an MTR reproduction. Antithesis weights `checkpoint_size`/`checkpoint_interval`-on configs (short window driven by the 500 ms poll and thread pausing) plus the checksum-change and A2 variants, and every assertion reports the checkpoint config in its details (antithesis-fit F10).
- Added vacuity marker `Sometimes("sender crossed a rewrite-generated file switch")` (coverage-balance F9).
- P0 status depends on bias B1 (is pull serving the release target).
- Runs in `pull_fs`.
