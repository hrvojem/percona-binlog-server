# encrypted-storage-decrypts-to-source — Encrypted storage decrypts to the source's bytes after any crash

Found by: focus 1, 6 (data integrity, security)
Merged from: encrypted-storage-decrypts-to-source (focus 1), encrypted-binlog-decryptable-after-crash (focus 6), cipher-offset-equals-physical-offset (focus 6)

## Synthesis notes

Safety, **P1**. Focus 1 rated it P1. Focus 6 rated its two contributions P1 and P2.

Distinct workload `Always` messages:
- "pull plaintext of each encrypted binlog equals the source binlog as sent". The workload takes per-file non-blocking dumps from position 4 and strips the artificial events.
- "independent decryption yields magic plus a valid event chain and CRCs up to metadata size". This needs a Python implementation of the envelope format and access to the KEK.

SUT-side assertions:
- `ALWAYS(encryption offset == backend physical stream size before write)` in `storage_core::write_data_to_stream` (`:1125-1160`). It needs a stream-size accessor. Focus 1 and focus 6 both proposed it, so it is kept once.
- `ALWAYS(decrypted block at offset 0 starts with binlog magic)` in `fetch_event_block` (`:544-557`), with its own message.
- `ALWAYS(decrypted block at offset 4 starts with a valid FDE header)` in `fetch_event_block` (`:544-557`), with its own message.

Triggers:
- A kill mid-append, followed by truncation at a non-block-aligned offset and a CTR re-append.
- A kill inside rotation. Until the `<name>.json` PUT completes, the envelope exists only in memory (`:680-703`).
- ~~The magic rewrite when a binlog is reopened at offset 0 (`:715`).~~ Dropped after investigation: that branch is unreachable from crashes or in-contract faults (see Investigation Log).
- On S3, a failed upload followed by the destructor retry (ticket 18).
- S3 `resize_object`.
- The divergence paths from a concurrent purge truncation (N1) and from two writers (ticket 24).

## What led to it

- **SG6 (README:673) and SG9.** For an encrypted storage, the only in-product way to read the data back as plaintext is pull's replication source (`storage_core.cpp:544-557`). An undecryptable file therefore means data loss for an encrypted backup, and nothing reports it until restore time.
- **Encryption meets every crash path:**
  - Appends reuse the file's own envelope and must encrypt at CTR offset = current size (`storage_core.cpp:307-309`, `:715`).
  - Recovery truncates ciphertext (`:979-983`). On S3 this is a download, truncate and re-upload (`s3_storage_backend.cpp:765-808`).
  - After a failed S3 upload, the retry briefly leaves ciphertext encrypted for one offset sitting at another (sut-analysis "Failed flush").
- **No crash testing.** SUT analysis surface 8 says that "encryption and rewrite under crash ... have never been crash-tested". The Hegel model test "crashes" only between operations (`storage_property_test.cpp:1845-1856`). The encryption code is "quiet because well tested" in-process, so the Antithesis value is in crash and fault interleavings.
- **Crash-surviving state.** Encryption adds state that must survive crashes alongside the data:
  - the per-file envelope (wrapped file key plus data IV) in `<name>.json`;
  - the CTR keystream position, which is derived from the logical offset.
- **Unchecked offset.** `write_data_to_stream` derives the CTR keystream from a *logical* offset passed by the caller (`record.size`). The backend stream is opened in **append** mode: the filesystem backend uses an `ofstream` opened `app|ate`, and the S3 backend appends to a local temp file and then uploads the whole object. Nothing checks that the two agree.
  - When they diverge, the bytes are not lost, but they decrypt to garbage at read time, because the sender decrypts with the physical read offset (`storage_core.cpp:550-553`).
  - On an unencrypted storage the same divergence produces misplaced bytes, which byte-prefix oracles catch.
  - On an encrypted storage only decryption reveals it, so it is silent until restore.

## Code paths

- `generate_binlog_encryption_record` (`storage_core.cpp:1008-1090`): random file key wrapped with the KEK. The IV is random per file.
- New binlog (`storage_core.cpp:680-703`), in order:
  1. generate the envelope;
  2. encrypt and write the magic at offset 0 (`:686`);
  3. push the record;
  4. `put_object(<name>.json)` with the envelope;
  5. update the index.

  Until the `.json` PUT completes, the file key exists only in memory.
- Checkpoint (`:299-322`):
  1. the data append is encrypted at `offset = record.size` (`:307-309` / `:307-310`);
  2. `size +=` runs, only after the backend write succeeds;
  3. the `.json` is re-saved.

  `to_model` (`:69-90`) re-serialises the same envelope every time.
- `open_existing_binlog_file_internal` (`:704-719` / `:705-720`): append mode reuses the record's envelope from `.json`. When it reopens with a stream offset of 0, it re-writes the magic at offset 0 using the record's envelope (`:715`). **Investigation: this branch is unreachable in practice.** `open_binlog` raises when the flushed position (the record size) is 0 (`storage_core.cpp:280-283`); every saved record has size ≥ 4 (`open_new_binlog_file_internal` creates it with `magic_binlog_offset`, `:695-699`, and `:717` is the only other assignment); and construction makes the actual object size equal the metadata size or fails (`:957-983`). Reaching `:713` needs the object to shrink below 4 bytes between construction and `open_binlog` in the same process, which no in-contract path (and not even N1, which truncates to a metadata size ≥ 4) does.
- `write_data_to_stream(data, encryption, offset)` (`:1125-1160`): the keystream comes from the `offset` argument.
- Recovery (`:960-985`, truncation at `:974-983` / `:979-983`) truncates the tail to the `.json` size. CTR is seekable, so truncation alone keeps the prefix decryptable.
- Load validation (`:877-889` / `:884-904`): an envelope requires an initialised keyring that contains its KEK. Otherwise the storage fails to open (fail-closed). `validate_binlog_metadata` rejects a missing KEK.
- `fetch_event_block` (`:513-557`, decryption at `:544-557`, `:550-553`): decrypts with `create_with_offset(read_offset, …)`.
- S3: `s3_storage_backend.cpp:765-808` handles recovery truncation as download, truncate and re-upload. `:850-863` appends locally, then uploads.
- `basic_storage_backend.hpp`: the backend interface has no "current stream size" getter today.
- Rewrite-mode generated events carry PBS wall-clock timestamps: `event_generation_helpers.cpp:48`, `:117`, `:149`.
- Ticket 8 (validated, low): the CTR counter does not carry into the IV high half (`cipher_context.cpp:457-461`).
  - It matters only for random IVs whose low 64 bits overflow within a file. That is deterministic and rare.
  - Hegel covers it, so it is out of scope here and not prioritised.

## Failure scenarios

1. **Append after recovery truncation at a non-16-byte-aligned offset.** Trigger: kill mid-append, then restart. The offset-based CTR context must produce the same keystream position. A bug shows up as garbage after the resume point.
2. **S3 failed data upload, then destructor re-flush (ticket 18).** Trigger: S3 5xx on the data PUT.
   - The local temp file ends up with old+n bytes encrypted at offset old, then bytes old+n..old+2n encrypted *again* at offset old instead of old+n. The logical `size` stays at old.
   - After the retry, the metadata ends at old+n, and restart truncation drops the second copy.
   - **Investigated: no reader can see the mis-encrypted bytes.** `fetch_event_block` clamps every read to `record.size` (`storage_core.cpp:514-516`, `:537-538`), and the retry runs in `~storage`, which in pull runs only after `network_service` (and so every sender) is destroyed (`pull_operation.cpp:108-118`, objects destroyed in reverse order). Other processes read through their own metadata, which says old+n. The mis-encrypted range `[old+n, old+2n)` lies wholly past the logical size and is truncated by the next streaming or purging construction (`:974-983`).
   - The physical/keystream mismatch exists transiently. It becomes permanent only if anything prevents that truncation (e.g. a future change that adopts the physical size).
3. **Lost `.json` of a new encrypted file (ticket 15 window).** Trigger: a crash or failed `.json` PUT after the magic was written. The file exists, but its key is unrecoverable.
   - Today ticket 15 makes the storage unopenable anyway, which `kill-anywhere-storage-reopens` covers (formerly `storage-reopens-after-writer-crash`).
   - A future fix for ticket 15 that "adopts" the orphan file without its envelope would turn this into silent garbage.
4. **Envelope re-generated for an existing file.** Trigger: a future change that calls `generate_binlog_encryption_record` on reopen. This does not happen today; the property is a regression guard.
5. **Two writers (ticket 24 / A1).** Trigger: overlapping writers on one storage. Each has its own in-memory `size`, and both append at the real EOF, so bytes are encrypted for the wrong offset.
6. **Concurrent purge truncation (N1).** Trigger: thread pausing between pull and a cron `purge_binlogs`.
   - Setup: an encrypted filesystem storage with pull running. Purge fires between pull's data append and its metadata save.
   - Purge's constructor truncates the tail to the metadata size (`:974-983`), while pull's in-memory `size` stays larger.
   - Pull's next checkpoint appends at physical offset P while encrypting for offset P + k. Every later byte in that binlog decrypts to garbage.
   - The storage still opens, until someone notices a size mismatch on restart.
   - Pull clients get garbage events, and the event parser in mysqlbinlog or a replica fails. PBS's own `indexed_event_block` may loop on a garbage `event_size` (ticket 7).
7. **Keystream reuse in rewrite mode.** Trigger: truncation, then a re-send.
   - After truncation, the re-sent bytes are re-encrypted at the same offsets with the same key and IV.
   - In non-rewrite mode the plaintext is identical (a byte prefix), so the ciphertext is identical too.
   - In rewrite mode, the regenerated preamble and ROTATE carry PBS wall-clock timestamps (`event_generation_helpers.cpp:48, 117, 149`). Different plaintext may then be encrypted with the same keystream at the same offset.
   - The earlier ciphertext was truncated, so this is a confidentiality concern only if old object versions persist (S3 versioning). Recorded at low priority.

## Oracle / workload checks

- **Pull comparison.** Point `mysqlbinlog --read-from-remote-server --raw` (non-blocking) at pull. Take per-file dumps from position 4 and strip the artificial ROTATE/FDE that PBS synthesises. Compare each file with the source's binlog as sent, or for rewrite mode run the rewrite chain checks.
  - `Always("pull plaintext of each encrypted binlog equals the source binlog as sent")`.
- **Independent decryption.** The workload holds the keyring. For each indexed binlog, without going through PBS, it unwraps the file key with the KEK and AES-CTR-decrypts from offset 0. It then checks that:
  - the first 4 bytes are the binlog magic;
  - the event chain parses (`next_event_position == position + size`);
  - every event's CRC32 verifies.

  Reuse `parse_events` from `extra/qa/fetch_crash_test.py`.
  - `Always("independent decryption yields magic plus a valid event chain and CRCs up to metadata size")`, checked after every PBS restart that opens the storage. This is also the black-box fallback for the offset check.
- **Cross-check.** The non-blocking pull dump must equal the workload's own decryption.
- **Direct storage check.** For files whose `.json` has an envelope, `data[0:4]` ≠ `\xfebin`, meaning the data is ciphertext.
- `Sometimes(physical/logical divergence observed and later healed by recovery)` shows the transient ticket-18 path was exercised.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion below is missing.

- `ALWAYS(encryption offset == backend physical stream size before write)` in `storage_core::write_data_to_stream` (`:1125-1160`), or in the backend. **Missing.** This is the primary oracle: it fires the moment divergence happens, whatever the cause.
  - The filesystem backend can use `ofs_.tellp()`, and the S3 backend can use `tmp_fstream_.tellp()` (both streams are opened `app|ate`, `filesystem_storage_backend.cpp:201-213`, `s3_storage_backend.cpp:822-847`).
  - A small accessor in `basic_storage_backend.hpp` is needed.
  - `stored-binlog-byte-prefix-of-source` has the same append-offset assertion for plaintext. On encrypted storages a mismatch is silent corruption rather than a duplicate.
  - **Placement (resolved):** evaluate it only while no write on the current stream has thrown since `open_stream`. Otherwise it fires on the benign, self-healing S3 data-upload-failure retry (offset old, physical old+n; the mis-encrypted range is past the logical size and never readable, see failure scenario 2). On the filesystem backend, a failed write leaves `ofs_` with a sticky failbit, so `tellp()` returns -1 on the retry; the same guard covers that. Pair it with `SOMETIMES("encrypted append retried after a failed write with physical size ahead of logical size")` at the exempted site, so the transient path is still known to be exercised.
- `UNREACHABLE("encrypted binlog reopened with an empty data object")` at `storage_core.cpp:713` (the offset-0 branch of `open_existing_binlog_file_internal`). **Missing.** Optional regression guard: investigation shows the branch is unreachable today (see Investigation Log).
- `ALWAYS(decrypted block at offset 0 starts with binlog magic)` in `fetch_event_block`, after decryption when `read_offset == 0` (`:544-557`). **Missing.**
- `ALWAYS(decrypted block at offset 4 starts with a valid FDE header)` in `fetch_event_block` when `read_offset == 4`. **Missing.** A cheap integrity tripwire on the read path.
- Hegel `storage_property_test` on an encrypted storage. **Partial**, non-Antithesis: it crashes only between operations.

## Key observations

- **Encryption is quiet in-process.** The Antithesis value is in crash, fault and concurrency interleavings, not in the cipher code itself.
- **Fail-closed on a missing KEK.** If the workload rotates KEKs, it must keep the old ones.
- **Silent until read.** Corruption from an offset mismatch shows up only on decryption: through pull, or at restore.

## Fault requirements

- Kills at arbitrary instants in rotation, checkpoint, recovery and the destructor re-flush on an encrypted storage.
- S3 5xx or ENOSPC on data and `.json` PUTs, via `s3_fault_proxy` or `fsfault`. This includes the ticket-18 destructor re-flush path on encrypted data.
- Thread pausing and process scheduling between pull and a concurrent purge or second fetch process. This applies to the misuse variant only.
- No node termination needed.

## Open questions

- Are two concurrent fetch/pull writers on one storage (ticket 24) in contract, or misuse? `(needs human input)`

Resolved by investigation (see Investigation Log): the per-file pull comparison method and its exposure to N5; KEK rotation; independent decryption; the offset-0 reopen path; concurrent purge being out of contract; the S3 transient mismatch.

### Changes to the property from investigation

- **Invariant and assertion type unchanged** (`Always`, safety, P1).
- **Trigger dropped:** the magic rewrite at offset 0 (`:715`) is unreachable; it becomes an optional `UNREACHABLE` regression guard instead of a trigger.
- **Scope narrowed:** the N1 (concurrent purge truncation) variant is out of contract per README:473 ("may cause data corruption and should be avoided"). It runs only in a misuse phase, read as "fails closed / detectable", not as an `Always` violation.
- **SUT assertion placement fixed:** the offset `ALWAYS` exempts writes after a failed write on the same stream; a twin `SOMETIMES` anchors the exempted path.
- **Workload oracle made concrete:** both the pull comparison and independent decryption are kept (details in the Investigation Log); the keyring uses AES KEKs only and may add KEKs between restarts, never removing old ones.

### Investigation Log

#### Is pull's output, after stripping artificial events, byte-equal to the stored plaintext (SG9)?

- Examined: `src/operations/sender_context.cpp:80-140` (serving contract), `storage_core.cpp:513-561` (`fetch_event_block`), sut-analysis N5 (line 240).
- Found: a request for (N, 4) sends an artificial ROTATE, then file N's own FDE and events from storage as is (steps 1-4), then an artificial ROTATE for N+1 (step 5). N5 only affects the switch *into* a magic-only N+1 (the artificial ROTATE and FDE for N+1 are skipped); it does not alter or drop any bytes of file N. In non-blocking mode, the dump ends at EOF.
- Conclusion: resolved for this property's purposes. The workload takes a fresh non-blocking dump from (N, 4) per file, drops the leading artificial ROTATE (flag `artificial`, `next_event_position == 0`), and keeps bytes until either the stored metadata size of N is reached or the next artificial-flagged event. The concatenation must equal decrypted `data[4:size]`. With this cut, N5 cannot produce false positives here. Whether SG9 holds across file switches is owned by `pull-served-stream-self-consistent`. Prefer the workload's own protocol client over `mysqlbinlog --raw`, because `--raw` splits output files on ROTATE and a skipped ROTATE (N5) would merge files.

#### Is the keyring rotated during tests (new KEK, old kept)?

- Examined: `storage_core.cpp:630-660` (active KEK set from config, must exist in keyring), `:1008-1090` (`generate_binlog_encryption_record` stamps `kek_id = active_kek_id_` into each envelope), `:544-553` and `decrypt_file_key` (`:1091-1123`, looks up the envelope's own `kek_id`), `:864-905` (`validate_binlog_metadata`), README:609-611, :673-704.
- Found: each file's envelope names its own KEK, and decryption looks up that ID, not the active one. Changing `storage.encryption.kek_id` between runs is supported: new files use the new KEK, while the reopened tail keeps its old envelope (`open_existing_binlog_file_internal` reuses the record). The keyring is read once at construction, so rotation can only happen between restarts. Removing a KEK that any envelope references makes construction fail closed.
- Conclusion: resolved. It is a workload choice, and the code supports it. The workload should sometimes switch the active KEK between restarts (append-only keyring) to cover mixed-KEK storages, the tail reopened under a different active KEK, and the "old KEK still needed" path. Removing a referenced KEK is a separate fail-closed check, not part of this `Always`.

#### Should the workload hold the KEK and decrypt independently, or rely only on pull?

- Examined: `storage_core.cpp:69-90` (`to_model`: `file_key_envelope{kek_id, data_hex, iv_hex?, tag_hex?}`, `file_data_envelope{cipher, iv_hex}`), `src/opensslpp/cipher_context.cpp:170-172` (padding disabled), `:429-480` (`create_with_offset`), README:673-704, ticket 8.
- Found: the envelope is plain JSON in `<name>.json`. File-key unwrap is a single AES operation (ECB/CBC with padding disabled, CTR, or GCM with the tag), and data is AES-CTR with the file IV. All of these are in Python `cryptography`. One subtlety: offset 0 uses OpenSSL's native 128-bit counter, while offset > 0 uses PBS's 64-bit low-half counter without carry (ticket 8). A standard CTR implementation disagrees only if the low 64 bits overflow within a file, which is negligible for random IVs and file sizes in tests.
- Conclusion: resolved. Do both. Independent decryption is cheap (about 30 lines), does not trust PBS's decryption, and works when pull is not running. Restrict the test keyring to AES KEKs (CAMELLIA/ARIA are not universally available in Python), and implement the decryption with a 128-bit counter while noting ticket 8 as the only known benign disagreement source. The pull comparison stays as the in-product readback check.

#### Can a crash leave a binlog with a `.json` but zero-length data (the `open_existing` offset-0 path, `:715`)?

- Examined: `storage_core.cpp:258-296` (`open_binlog`), `:680-703` (`open_new_binlog_file_internal`), `:704-719`, `:957-983` (load and recovery), `storage_core.hpp:335-337` (`get_flushed_position_unsafe`), `filesystem_storage_backend.cpp:127-231`, `s3_storage_backend.cpp:822-863`.
- Found: rotation writes the encrypted magic (and on S3 uploads it, `do_write_data_to_stream` calls `upload_tmp_stream_internal`) before the `.json` PUT, so a `.json` always follows a data object of ≥ 4 bytes under kills. Every saved record has size ≥ 4, and `open_binlog` raises on a flushed position of 0 (`:280-283`). Construction forces the actual size to equal the metadata size (truncate) or fails (`:974-977`). So `open_stream` on an existing binlog returns ≥ 4, and the `:713-717` branch is dead under kills and in-contract faults. The filesystem data stream is not fsync'd (`filesystem_storage_backend.cpp:229`) while `.json` is (`:134-171`), so an OS crash could leave `.json` ahead of the data. That ends in the fatal "metadata bigger than actual" branch (`:974-977`), not at offset 0, and node termination is not in this property's fault set anyway.
- Conclusion: resolved. The path is unreachable; the trigger is dropped and an optional `UNREACHABLE` guard is suggested.

#### Is a concurrent purge or second writer in contract?

- Examined: README:473, tickets 9 and 24 in `.tickets-snapshot.txt`, README sections on configuration and resuming, `storage_core` (no cross-process lock).
- Found: README:473 states that running `purge_binlogs` on a storage "while another instance is fetching / pulling data to the same storage may cause data corruption and should be avoided". So the N1 variant is out of contract. README says nothing about two fetch/pull instances on one storage; ticket 24 (open, filed as a bug) treats that as a defect (no cross-process lock), but there is no maintainer statement of intent.
- Not found: any doc or code comment that defines whether two writers are a supported configuration, or that PBS is expected to reject the second one.
- Conclusion: the purge half is resolved (misuse phase only). The two-writer half is tagged `(needs human input)`: maintainers must say whether ticket 24 is meant to be fixed by a lock (then the property should demand fail-closed) or whether the two-writer setup is unsupported misuse.

#### Should the S3 backend's transient mismatch (ticket 18 data-upload failure) count as a violation?

- Examined: `storage_core.cpp:299-322`, `:513-561`, `:974-983`, `s3_storage_backend.cpp:765-863`, `storage.cpp:68-77`, `pull_operation.cpp:108-166`.
- Found: on a failed data upload, `size +=` is not reached; the destructor retry encrypts at offset old but appends at physical old+n, then uploads, sets the size to old+n and saves metadata old+n. The mis-encrypted bytes are wholly past the logical size; all reads are clamped to `record.size`; pull's senders are gone before `~storage` runs; the next construction truncates them away. The bytes in `[0, old+n)` were encrypted at their correct offsets by the first attempt.
- Conclusion: resolved. This is not a violation of "decrypts to source" (no readable byte is wrong). It does violate the raw offset-equality check, so the SUT `ALWAYS` is guarded to writes with no prior failure on the stream, with a twin `SOMETIMES` for the exempted path. The property does not split. The permanent ticket-18 duplicate (metadata-PUT failure) is still caught here only through the byte-prefix comparison and is owned by `failed-flush-never-reappends-data`.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- The independent decryption is now the primary check: "independent decryption yields magic plus a valid event chain and CRCs up to metadata size" and "independently decrypted binlog equals the source binlog as sent". the independent Python decryptor is the canonical encrypted reader. Pull is not used to read a storage it does not own: pull always builds a streaming storage and runs the collector (`pull_operation.cpp:108-153`), so on a `fetch_fs` or `s3` storage it would run recovery on, and append to, the storage under test. A pull readback there runs only at quiescent points on a snapshot copy with `connection.host` pointing at an unreachable endpoint (lenses: implementability P1, wildcard W2).
- The pull comparison stays as the SG9 serving check, in `pull_fs` (pull is the writer) or on a snapshot copy as above.
- The SUT offset `ALWAYS` is reachable from `contention` timelines (N1, ticket 24), so its message carries the harness timeline tag. Antithesis tallies an assertion by its message across every timeline of a run, so an SUT message cannot be scoped to a template (wildcard W1).
- Runs in `fetch_fs`, `pull_fs`, `s3`.
