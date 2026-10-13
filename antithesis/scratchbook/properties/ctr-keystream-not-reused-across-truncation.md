# ctr-keystream-not-reused-across-truncation — the same file key, IV and offset never encrypt two different plaintexts

## What led here

Encryption is AES-CTR with one random file key and IV per binlog. The keystream for byte offset o is a pure function of (key, IV, o) (`storage_core.cpp:1135-1145`, `create_with_offset(offset, …)`). Crash recovery truncates the tail to the metadata size (`storage_core.cpp:960-985`), and the resumed writer then encrypts new bytes at the *same offsets with the same envelope*. If the re-written plaintext differs from what was there before, two ciphertexts under one keystream exist: the classic CTR nonce-reuse leak (C1 ⊕ C2 = P1 ⊕ P2). The old ciphertext survives in S3 object versions, backups, filesystem snapshots, and PBS's own leaked S3 local buffer copies (N10: `/tmp/<uuid>` is not removed on SIGKILL, `s3_storage_backend.cpp:563-571, 640-651`).

In non-rewrite mode, a resumed stream from the same source normally re-sends byte-identical data (SG2), so the reuse is benign: same plaintext, same ciphertext. The plaintext differs in these cases:
- **Rewrite mode.** The generated non-artificial ROTATE at the end of a file carries `now()` (`collector_context.cpp:376-385`; `event_generation_helpers.cpp`, `current_timestamp=true` → `ctime_timestamp::now()`). A kill after the ROTATE was appended but before its metadata save leaves it in the truncated region. On resume (`context.is_fresh()` → same binlog name, `:367-370`) the next bytes at that offset are a new ROTATE with a different timestamp, or a transaction.
- **Source identity change** (ticket 25: RESET BINARY LOGS, failover to a server with the same binlog names). After truncation, different transaction bytes land at the same offset of the same file.
- **Ticket 14** GTID-mode resume across a source rotation (reuses the tail name in some branches).

## Code paths

- `storage_core.cpp:1125-1160` `write_data_to_stream`: keystream offset = caller's `offset` argument.
- `:299-322` `write_event_block`: `offset = record.size` (the metadata/in-memory size, i.e. the truncation point after restart).
- `:960-985` tail truncation via `resize_object`; the envelope is reused (`:715`, `:308`).
- `s3_storage_backend.cpp:765-808` `resize_object` on S3: download, truncate, re-upload. If bucket versioning is on, the old object version keeps the old ciphertext.

## Failure scenario

Encrypted storage, rewrite mode, small `file_size`. The collector appends the generated ROTATE (timestamp T1) at offset X; Antithesis kills PBS before `save_binlog_metadata`. On restart, recovery truncates to X. After reconnect, PBS writes a new event at X with timestamp T2 ≠ T1 under the same keystream. An attacker holding the pre-crash ciphertext (leaked `/tmp` copy, S3 version, backup) XORs the two and learns P1 ⊕ P2. With a transaction instead of a second ROTATE, that is row data XOR known event structure.

## Oracle

- Workload: after every quiet period, snapshot each encrypted binlog's raw bytes keyed by `(name, iv_hex from .json)`. Also snapshot leaked `/tmp/<uuid>` copies and, if MinIO versioning is enabled, old versions.
- For every pair of snapshots with equal `(name, iv_hex)`, over the overlapping byte range: `Always(bytes equal)`. A mismatch at the same offset under the same IV means two different plaintexts were encrypted with the same keystream. This is the right type because any single occurrence is a confidentiality violation.
- `Sometimes(tail truncated by recovery on an encrypted storage)` (SUT-side marker), to know the precondition was reached.
- SUT-side (missing): `SOMETIMES("recovery truncated an encrypted tail")` at `storage_core.cpp:979` when `record.encryption` is set.

## Antithesis angle

Kills inside `write_event_block` between the data append and the metadata save; rotation timing in rewrite mode; source RESET or failover driven by the workload. The snapshot cadence must catch the pre-truncation bytes. On the filesystem backend that needs the leaked bytes to be observed before restart. The S3 local buffer leak is the natural "old copy" in this SUT.

## Instrumentation

- missing: SUT-side SOMETIMES above.

## Open questions

- Is "the old ciphertext is unavailable to an attacker" part of the threat model (no bucket versioning, no backups or snapshots, no local-disk access)? The code confirms reuse in rewrite mode, so this decides whether a firing assertion is a bug or accepted behaviour. `(needs human input)`

## Investigation results (2026-10-13)

**Rewrite-mode reuse is confirmed by code reading, and two shapes are reachable.** On every new connection, after a restart or after an in-process reconnect, `receive_binlog_events` builds a new `reader_context` (`collector_context.cpp:142-146`), so `is_fresh()` is true. The first non-preamble source event then generates a preamble for the **current** file (`:352-370`). The artificial ROTATE names the stored file at position > 4, so `expect_info_only_preamble_events_` is set (`reader_context.cpp:579-582`), and the generated FDE and PREVIOUS_GTIDS are not written. The rotation check is skipped on the fresh path (`:352`, `is_fresh() ||`), so **the first transaction after any reconnect always lands in the reused file at `storage_->get_current_position()`**, which is the truncation point after a crash (`rewriter::rewrite(..., get_current_position())`, `:422-428`). The rotation ROTATE is flushed by `close_binlog` → `flush_event_buffer` → `write_event_block`, which writes the data and then saves the metadata (`storage.cpp:204-214`, `storage_core.cpp:298-322`). A kill between those two steps leaves the ROTATE beyond the metadata size.

- **Shape A (substantial overlap).** If the last checkpoint flush landed exactly at the transaction boundary before the ROTATE, the metadata size is X, the ROTATE's own offset. That is the normal case when `checkpoint_size` is at most one transaction, because `storage::write_event` flushes at every qualifying boundary (`storage.cpp:180-200`). After truncation to X, the resumed connection writes the **next transaction's GTID event** at X under the same key and IV. ROTATE(T1) and GTID(T_k+1) overlap for the ROTATE's length (19 + 8 + name + 4, about 50 bytes). The pre-crash ROTATE plaintext is almost fully predictable (the name and position are known, T1 is close to the file mtime), so those bytes of the GTID event are recovered. They hold the source UUID, GNO and commit timestamps: metadata, not row data.
- **Shape B (minimal overlap).** If the metadata size is below X, the truncated region holds buffered transactions plus the ROTATE. They are re-sent byte-identically: the rewriter copies the source bytes, sets `sequence_number` from the persisted `last_sequence_number` (`storage_core.cpp:317-321`), and derives `last_committed` deterministically (`rewriter.cpp:149-174`). The rotation then re-fires at the same offset X with ROTATE(T2). Only the 4-byte timestamp and the 4-byte CRC differ.
- **Source identity change** (ticket 25) is still the case where full transaction bodies, including row data, overlap. Non-rewrite mode with an unchanged source re-sends identical bytes and stays benign.

**Harness: there is no bucket versioning.** The `s3` container runs Versity S3 Gateway 1.8.0 as `versitygw ... posix /s3data` with no versioning option (deployment-topology.md:78-79). Old ciphertext is observable through these channels:
1. **S3 template**: the leaked local buffer copy under `/data/s3buf` (deployment-topology.md:105). After a SIGKILL the `<uuid>` temp file still holds the pre-crash ciphertext, including the bytes beyond the metadata size (`s3_storage_backend.cpp:640-652`, `:850-863`).
2. **Filesystem templates**: the driver that sent the SIGKILL (`parallel_driver_fetch.py`) snapshots the binlog bytes beyond `.json` `size` before the next `fetch` starts and truncates them.

Enabling versioning would require a Versity versioning option that this research did not verify. It is not needed.

**Property changes.** The invariant and the assertion type stay the same (`Always(bytes equal for equal (name, iv_hex) over the overlapping range)`). The property is now **expected to fire in rewrite mode by design** (shape A, and shape B on the timestamp and CRC bytes), so whether a firing is a bug depends on the threat-model question above. Add these triggers: rewrite mode with `checkpoint_size` small (≤ 1 transaction) for shape A, and a larger or absent checkpoint for shape B. Split the workload message so that triage separates the shapes: "keystream reused for different plaintext (rewrite rotate overlap)" vs. "... (transaction body overlap)". The first is metadata-only; the second (source swap) leaks row data.

### Investigation Log

#### Is the rewrite-mode resume really "same file, same offset", and what lands at the truncated offset?

- Examined: `collector_context.cpp:120-170` (a `reader_context` per connection), `:274-429` (rewrite path, fresh vs. rotation branch), `:430-482` (`process_binlog_event`); `reader_context.cpp:520-600` (info-only preamble); `storage.cpp:143-214` (checkpoint and `close_binlog` flush); `storage_core.cpp:298-322` (data, then metadata), `:955-985` (truncation), `:1125-1160` (keystream offset = record size); `rewriter.cpp:130-226` (deterministic field rewrites).
- Found: a resume always reuses the current file, skips rotation for the first transaction, and writes it at the truncated offset. With per-transaction checkpoints, a kill inside the rotation flush leaves only the ROTATE beyond the metadata size, so the next transaction's GTID event is encrypted at the ROTATE's offset (shape A). Otherwise the re-sent transactions are byte-identical and only the regenerated ROTATE's timestamp and CRC differ (shape B).
- Not found: nothing executed. The conclusion comes from code reading only.
- Conclusion: resolved. Reuse with different plaintext is reachable in rewrite mode, and the overlap is about 50 bytes of GTID-event metadata at most. Row data overlaps only on a source identity change.

#### Is "the old ciphertext is unavailable to an attacker" part of the threat model?

- Examined: README encryption, keyring and storage sections (:609-707), README "Graceful termination" (:717-728), the ticket snapshot, and code comments in `storage_core.cpp` and `s3_storage_backend.cpp`.
- Found: nothing about attacker capabilities, bucket versioning, backups or local temp files. README:618 says only that an explicit `fs_buffer_directory` is never deleted automatically, which means old copies are expected to persist there.
- Not found: any threat-model statement.
- Conclusion: `(needs human input)`.

#### Does Versity/MinIO in the harness have versioning enabled?

- Examined: deployment-topology.md:76-83 (`s3` container), :105 (`/data/s3buf`), :174-207 (drivers).
- Found: Versity 1.8.0 posix backend started without any versioning option, so versioning is off. MinIO is only the fallback.
- Not found: Versity's versioning flag (not needed).
- Conclusion: resolved. The workload observes old ciphertext through leaked `/data/s3buf` copies (S3) and a pre-restart snapshot taken by the killing driver (filesystem), not through object versions.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Filesystem snapshot timing: the bytes beyond the `.json` size must be captured before the next opener truncates them. In `fetch_fs` the writer's own driver snapshots right after its child exits and before releasing the `flock`; in `pull_fs` the supervisor snapshots before restarting pull (implementability P6).
- S3 variant: old ciphertext is observed only through leaked `/data/s3buf` copies, which is the N10 leak. Once N10 is fixed the S3 variant goes blind unless the workload GETs the object before the restart.
- Runs in `fetch_fs`, `s3` (rewrite + encryption).
