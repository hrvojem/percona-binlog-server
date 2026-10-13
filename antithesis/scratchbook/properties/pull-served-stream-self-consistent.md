# pull-served-stream-self-consistent — Every event pull serves carries a log_pos chain and CRC that agree with where it was served

## What led to it

Protocol-contract focus (MySQL replication protocol as served by pull). In a real MySQL binlog, every non-artificial event's common-header `next_event_position` (log_pos) equals the byte offset at which the event ends in its file. When the governing FORMAT_DESCRIPTION says `checksum_algorithm = CRC32`, every event also carries a CRC32 footer over its bytes. Replicas and `mysqlbinlog` rely on both: log_pos is how a client computes its resume coordinates (`pull-chain-prefix-consistency` depends on it), and CRCs are how it detects corruption.

PBS enforces the log_pos chain on **ingress** (`reader_context.cpp:904-920` `validate_position`, a `logic_error` on mismatch). On **egress** nothing re-checks it: `indexed_event_block` splits the buffer by the `event_size` header field alone (`indexed_event_block.cpp:29-55`, with the comment `// TODO: consider performing checksum validation here`), and `handle_fetch_event_from_storage_state` hands out each slice verbatim (`sender_context.cpp:590-597`). So whatever bytes reach storage, or whatever bytes the sender assembles from storage, are served as they are.

This is a cheap, source-independent, per-event oracle. It works on encrypted storages (the served stream is decrypted) and in rewrite mode (positions and CRCs are recomputed on ingress, `collector_context.cpp:274-429`), where byte comparison with the source is impossible (sut-analysis "Assumptions").

## Code paths

- Sender assembly: `sender_context::populate_event_block` (`sender_context.cpp:438-524`). It concatenates `carry_buffer_` (the unparsed tail of the previous 1 MiB block, `snapshot_block_tail_to_carry` :331-345) with the next `fetch_event_block` result and re-indexes. A wrong carry splice (off-by-N, a dropped or doubled tail) shows up as a broken chain or a CRC failure.
- Decryption on read: `storage_core::fetch_event_block` (`storage_core.cpp:543-557`), with the CTR context created at `read_offset`. A wrong offset produces garbage that may still have plausible sizes, and only the CRC catches it.
- Read clamping: `storage_core.cpp:513-538`. A short or stale read (S3 without read-after-write consistency, A10) either throws or returns old bytes.
- What can put a broken chain *into* storage despite the ingress check:
  - ticket 18: a failed metadata save, then `~storage` re-appends the same bytes (`storage.cpp:68-77`, `storage_core.cpp:310, 321`). The duplicate's log_pos is that of the first copy, so the chain breaks exactly at the duplicate.
  - N1: a purge constructor truncates a live writer's tail (`storage_core.cpp:974-983`); the writer's `app` stream then appends at the new EOF.
  - ticket 24: two writers append to one file.
  - ticket 19: lost page cache (only if simulated).

## Failure scenario

Pull runs with `checkpoint_size` small, two or three blocking dump clients tail the storage, and a fault makes a metadata PUT fail (S3 5xx via proxy, or a partition to MinIO). Pull exits, the destructor re-appends, and the restarted pull serves a file in which one transaction appears twice. A client resuming at the stored size receives events whose log_pos points backwards. A MySQL replica would record wrong coordinates and could apply the transaction twice in position mode. `mysqlbinlog` prints it twice.

## Oracle

The workload dump client (a Python raw COM_BINLOG_DUMP client, or `mysqlbinlog --raw --stop-never` whose output files are parsed afterwards) tracks, per session:
- the current file name (from the artificial ROTATE) and the current offset (requested position, or 4);
- for each non-artificial event: `assert next_event_position == offset + event_size`, then `offset = next_event_position`; a real ROTATE or STOP ends the file;
- the current checksum mode (from the last FDE received); when CRC32, verify the footer of every event, including the FDE itself and the artificial FDE (whose CRC is recomputed, `sender_context.cpp:305-329`).

Assertions (workload-side, Python SDK):
- `Always(served log_pos chain contiguous)`: message "pull: served event next_event_position equals file offset plus size".
- `Always(served CRC valid)`: message "pull: served event CRC32 verifies under the governing FDE".
- `Sometimes(session crossed a block boundary with carry)` and `Sometimes(session served an encrypted file)`, so a pass is not vacuous.

## SUT-side instrumentation (all missing, per existing-assertions.md)

- `ALWAYS` in `sender_context::handle_fetch_event_from_storage_state`: track the expected offset of the next event (reset on file switch and start states) and check `next_event_position == expected + size` for non-artificial events. Message: "sender: next_event_position matches served offset".
- `ALWAYS` in the same place when the current FDE says CRC32: the footer verifies. This is the TODO in `indexed_event_block.cpp`. Message: "sender: served event CRC32 valid".
- `SOMETIMES(!carry_buffer_.empty())` in `populate_event_block` to confirm the carry path is explored.

## Antithesis angle

Most violations need a write-side fault in a narrow window (metadata-save failure, purge constructor between data append and metadata save, a second writer) or a read-side race (S3 overwrite consistency). Antithesis explores those interleavings; the oracle detects them without any knowledge of the source. CPU throttling of the pull container plus small `checkpoint_size` makes the carry and the tail-growing reads frequent.

## Fault requirements

Process kill of pull, network faults between pull and S3 (or the repo's `s3_fault_proxy.py`/`fsfault.c` shims for 5xx/EIO on `.json` writes), CPU throttling. No clock or node-termination faults needed.

## Resolved answers (investigation 2026-10-13)

- **File end without ROTATE/STOP (contract 4c).** Code confirms the oracle must accept it. On reaching EOF of a non-last file, `storage_core::fetch_event_block` silently moves to offset 4 of the next record (`storage_core.cpp:520-534`); `handle_binlog_file_switch` (`sender_context.cpp:401-430`) only rejects a non-empty carry (partial event at EOF), then enters `start_from_beginning`, and `handle_start_states` always emits an artificial ROTATE (`sender_context.cpp:551-573`) whatever the previous file's last event was. MySQL's own sender does the same (fake rotate before every file, "even if a rotate log event exists", PXC 9.6 `sql/rpl_binlog_sender.cc:401-412`). Oracle rule: the chain resets on every artificial ROTATE; never require a real ROTATE/STOP before a switch. Consequence for the oracle: a partial-event-at-EOF switch ends the session with an error rather than serving a broken chain, so it shows up as a session error, not an `Always` failure.
- **Checksum of the artificial ROTATE when `binlog_checksum` toggles.** PBS: the first artificial ROTATE uses the session `@source_binlog_checksum` value (`network_service.cpp:487-545`, `sender_context.cpp:57-60`); each later one uses the `checksum_algorithm` of the previous file's FDE, because `current_binlog_checksum_` is updated only after the ROTATE is generated (`sender_context.cpp:557-567`). MySQL matches: `init_checksum_alg` seeds `m_event_checksum_alg` from the session variable (`rpl_binlog_sender.cc:1018-1045`), `fake_rotate_event` uses it (`:1048-1078`), and it is overwritten only when the next file's FDE is read (`:1149-1150`), after the fake rotate (`:412`). The contract stands. Oracle rule: the artificial ROTATE's footer is governed by the last FDE the client received in this session, or by the session variable if none was received yet. "Last FDE received" in the oracle therefore already implements the rule; no change to the property. (Source checked is PXC 9.6.0's copy of the MySQL Binlog_sender; 8.0/8.4 not separately checked, but the code path is long-standing.)
- **Instrumentation status vs existing-assertions.md:** unchanged; all three SUT-side suggestions remain **missing** (no Antithesis SDK in the codebase).

## Open questions

- Is the per-event CRC check worth enabling SUT-side in production builds, or only behind an Antithesis/CMake option? `(needs human input)`
  - Why it matters: one CRC per served event; decides whether the SUT-side `ALWAYS("sender: served event CRC32 valid")` is Antithesis-only or a product feature. It does not affect the property itself: the workload-side `Always` is the primary oracle.
  - Partial finding: an ingress precedent exists, the runtime flag `replication.verify_checksum` (`collector_context.cpp:129,143`; README:589), and the egress TODO at `indexed_event_block.cpp` is unresolved. The only CMake options today are `WITH_STDLIB_LIBCXX`, `WITH_ASAN`, `WITH_PROPERTY_TESTS` (`CMakeLists.txt:45,62,78`).

### Investigation Log

#### Should the oracle accept a file end with no real ROTATE/STOP (contract step 4c)?

- Examined: `sender_context.cpp:80-195` (contract comment and FSM), `:401-430` `handle_binlog_file_switch`, `:438-524` `populate_event_block`, `:527-575` `handle_start_states`; `storage_core.cpp:486-557` `fetch_event_block`; PXC 9.6 `sql/rpl_binlog_sender.cc:395-461`.
- Found: the file switch is driven only by reaching the stored size of a non-last record; the artificial ROTATE is emitted unconditionally on every switch. A non-empty carry at the switch is a hard error (session ends). MySQL sends a fake rotate before every file too.
- Not found: nothing that requires a real ROTATE before a switch.
- Conclusion: resolved. The oracle resets its chain on each artificial ROTATE and does not require ROTATE/STOP. Property unchanged.

#### Which FDE governs the artificial ROTATE's checksum when `binlog_checksum` toggles mid-run?

- Examined: `sender_context.cpp:57-64, 275-302, 551-567`; `network_service.cpp:487-545`; PXC 9.6 `rpl_binlog_sender.cc:412, 1018-1078, 1140-1160`.
- Found: PBS and MySQL both use the session variable for the first fake rotate and the previous file's FDE algorithm for subsequent ones (the variable is updated after the rotate is sent).
- Not found: separate verification against MySQL 8.0/8.4 sources (only the 9.6-based PXC tree is available locally).
- Conclusion: resolved. The contract matches MySQL; the oracle's "last FDE received, else session variable" rule is correct. `sender-file-switch-emits-artificial-rotate` does not need revisiting on this point.

#### Is the CRC check worth enabling SUT-side in production builds?

- Examined: `indexed_event_block.cpp:29-55` (TODO), `collector_context.cpp:129-143`, `replication_config.hpp:35`, README:589, `CMakeLists.txt` options.
- Found: ingress CRC checking is a runtime config flag (`verify_checksum`); no egress equivalent and no Antithesis build option exists.
- Not found: any maintainer statement on egress verification cost or intent.
- Conclusion: `(needs human input)` — a product decision for maintainers. Does not block the property (workload-side oracle is primary).
