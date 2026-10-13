# mixed-encryption-storage-served-correctly — Pull serves the exact source stream across restarts that change encryption settings

Found by: focus 10, 8 (version compat, lifecycle)
Merged from: mixed-encryption-storage-served-correctly (focus 10), pull-stream-survives-encryption-config-changes (focus 8)

## Synthesis notes

Safety, **P1**. Focus 8 rated it P1 and focus 10 rated it P2. It is P1 because pull is the only in-product decryption path, and no test combines encryption config changes with kills or reads the result through pull.

Workload checks:
- `Always`: "pull stream minus artificial events equals source bytes across encryption config changes".
- `Always`: "file starts with plaintext magic iff its .json has no encryption envelope".
- `Sometimes(one session crossed an encrypted<->plaintext file boundary)`.
- `Sometimes(an encrypted tail was appended to under a different active KEK)`.

SUT-side:
- `SOMETIMES(per-file envelope presence differs from metadata.json encryption presence)` in `open_binlog`.
- The decrypted-magic and offset-4 FDE checks in `fetch_event_block` are the same site as `encrypted-storage-decrypts-to-source`'s assertions, so this file references them instead of duplicating them.

Triggers:
- Restarts that switch between no encryption, KEK A + cipher X, and KEK B + cipher Y. All KEKs stay in the keyring.
- Kills inside `open_new_binlog_file_internal`.
- Truncation recovery on an encrypted tail.
- Metadata-PUT failures.
- Concurrent clients reading across 1 MiB block boundaries.

## What led to it

- **Encryption is per file, fixed at creation.**
  - `generate_binlog_encryption_record()` (`storage_core.cpp:683`, `:1007-…`) uses the *current* config's active KEK and cipher. When the process has no active KEK, it returns `nullopt`.
  - Appends reuse the file's own envelope (`:307-309` / `:308`, `:715`).
  - `metadata.json.encryption` is advisory. Once set, it is sticky (`:174-178` / `:175-178`).
  - Validation rejects only a *different* format, not the absence of one (`:810-818`; `validate_metadata` `:808-826`).
- **One storage can legitimately mix settings.** Across restarts with changing configs, a single storage can contain:
  - plaintext files written before encryption was enabled;
  - a **plaintext tail that keeps growing after encryption is enabled** (N9 in sut-analysis: the tail's record has `encryption = nullopt`);
  - files encrypted under KEK A, then files under KEK B after a `kek_id` rotation, with the tail staying under A until the next rotation;
  - plaintext files created after encryption was removed from the config, while `metadata.json` still says `generic`.
- **Per-file mixing looks intended.** sut-analysis N9 calls both plaintext cases weak spots. However, `mtr/binlog_streaming/t/encryption.test` deliberately produces both: its last step disables encryption and downloads a plaintext fourth file into a `generic` storage. This property therefore does **not** assert "never plaintext". It asserts that mixing never breaks reads. `encrypted-storage-no-plaintext-at-rest` owns the "never plaintext" question.
- **Pull is the only way back to plaintext.** The only in-product path that turns this mix back into plaintext is pull's sender (sut-analysis "Egress"; `storage_core.cpp:544-557` decrypts per record).
  - The S3 path decrypts after a ranged GET.
  - The AES-CTR counter offset must equal the file offset for every 1 MiB block, and blocks cross event boundaries mid-event.
  - Ticket 8 (CTR carry only in the low 64 bits) is a separate, deterministic defect and does not matter at realistic sizes.
- **Test gap.** `encryption.test` and `rs_roundtrip_encryption.test` use a single setting per storage.
  - `encryption.test` does change the config between runs (plaintext → KEK2/cipher2 → KEK3/cipher3 → plaintext). However, it always issues `FLUSH BINARY LOGS` before each change, never kills PBS, never serves the result through pull, and never runs on top of a tail that is still being appended.
  - No test kills PBS in the middle of a config change.
  - Encryption and rewrite have "never been crash-tested" (sut-analysis attack surface 8).

## Code paths (verified at 99de682)

- **Start-up:**
  - `initialize_storage_encryption` (`storage_core.cpp:632-664`) requires the configured KEK to be in the keyring.
  - `validate_metadata` (`:808-826`) rejects only a *different* non-empty format.
  - `:175-178` stamps `metadata.json` when encryption is first configured.
  - `validate_binlog_metadata` (`:893-904`) requires every record's KEK to be in the keyring, and fails closed at `:899-903` otherwise.
- **Append to an existing tail:** `open_binlog` takes the append path, and data writes use `get_current_binlog_record_unsafe().encryption` (`:307-309`). That is the **tail's original envelope**, regardless of the new config's KEK and cipher. A plaintext tail stays plaintext, and an encrypted tail keeps its old KEK and cipher.
- **New binlog:** `open_new_binlog_file_internal` (`:680-703`) generates the envelope before the `.json` is written. It calls `generate_binlog_encryption_record()` (`:683`, `:1007-…`).
- **CTR offset for appends:** `write_data_to_stream(..., record.size)` passes the current size as the counter offset. Decryption uses `create_with_offset(read_offset, …)` (`:550-553`).
- **Reads:** the sender reads per record (`:544-557`). A file switch between an encrypted and a plaintext file goes through the same `fetch_event_block` path (`:523-535`).
- **Recovery:** tail truncation happens at `:979`.

## Failure scenarios

1. **Kill during the first encrypted binlog's creation** (trigger: kill inside `open_new_binlog_file_internal`). The kill comes in the first run after enabling encryption: the magic is encrypted and the `.json` with the envelope is saved, but the index is not. This is the ticket 15 shape, now with an envelope that the next run must reproduce or discard consistently.
2. **Restart with a rotated KEK while the tail is mid-file** (trigger: config switch from KEK A to KEK B). The tail continues under the old KEK, so the old KEK must stay in the keyring forever.
   - If a later run's keyring drops it, startup fails closed (`:899-903`). That is acceptable.
   - Pull must never serve garbage.
3. **Tail truncation recovery on an encrypted tail, followed by an append** (trigger: kill mid-append, then restart). After truncation the counter offset is `size` (`:979`). It must match the keystream position of the truncated bytes.
4. **Ticket 18 on an encrypted tail** (trigger: metadata-PUT or data-upload failure, then the destructor retry). Checked:
   - After a failed *metadata* save, `size` is already S+n. The destructor retry encrypts at counter offset S+n, which is where the copy lands, so it decrypts to a clean duplicate. `failed-flush-never-reappends-data` catches that, not this property.
   - After a failed S3 *data* upload, `size` is still S. The retry's second local copy is encrypted at offset S but sits at S+n, so it would decrypt to garbage. Startup truncation to the metadata size (S+n) removes it.
   - This is worth a `Sometimes` anchor, because a change to either ordering would turn it into served garbage.
5. **Plaintext ↔ encrypted file switch inside one pull session** (trigger: a config toggle between rotations, with a concurrent client). The sender must switch its decryption per record at the file boundary.
6. **Reads that cross 1 MiB block boundaries mid-event** (trigger: concurrent clients on S3). The CTR offset of each ranged GET block must equal the file offset.

## Oracle / workload checks

- **Workload.** Between PBS restarts (fetch loop or pull), pick a config at random from {no encryption, KEK A + cipher X, KEK B + cipher Y}. Keep every KEK in the keyring. Kill or SIGTERM PBS at random moments. Periodically run `mysqlbinlog --read-from-remote-server --raw` against pull from the beginning, with concurrent clients.
- `Always("pull stream minus artificial events equals source bytes across encryption config changes")`.
  - Strip the artificial ROTATE/FDE events, and compare against the source events as sent, as a prefix, for every file whatever its envelope (SG9 + SG2 combined).
  - Reuse `binlog_as_sent()` / `parse_events` from `extra/qa/fetch_crash_test.py`.
  - The served stream must decode as valid events with valid CRC32. Garbage from a wrong keystream fails the CRC immediately.
- `Always("file starts with plaintext magic iff its .json has no encryption envelope")`.
  - A file whose `.json` has `encryption == nullopt` is plaintext on disk and starts with `fe 62 69 6e`.
  - A file whose `.json` has an envelope does not start with the plaintext magic, and its ciphertext differs from the source bytes at the same offsets.
  - This catches a tail that gets mixed content.
- `Sometimes("one session crossed an encrypted<->plaintext file boundary")`: a single pull session crossed a file boundary between an encrypted and a plaintext binlog.
- `Sometimes("an encrypted tail was appended to under a different active KEK")`: after a restart with a different active KEK.

Both `Sometimes` checks keep the property from passing vacuously.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion below is missing.

- `SOMETIMES(encryption_record.has_value() != metadata encryption_format_.has_value(), "per-file envelope presence differs from metadata.json encryption presence")` in `open_binlog`. **Missing.** Shows that the storage reaches a mixed state.
- `SOMETIMES(record.encryption.has_value() != has_active_kek(), "appending to a tail whose encryption differs from current config")` on the `storage_core::open_binlog` append path. **Missing.** This condition is distinct from the one above: it also fires on enable/disable while `metadata.json` already agrees.
- `SOMETIMES("encrypted tail retry copy at a mismatched counter offset was removed by startup truncation")` at startup truncation (`:979`), for an encrypted tail whose data object exceeded its metadata size. **Missing.** This is the ticket 18 data-upload anchor from scenario 4.
- Decrypted-content sanity in `fetch_event_block`. **Missing.** The decrypted first 4 bytes of each file must equal the magic, and the decrypted block at offset 4 must start with a valid common header. Focus 10 proposed this as an `ALWAYS_OR_UNREACHABLE` and focus 8 as an `ALWAYS`. These are the same sites as `encrypted-storage-decrypts-to-source`'s `ALWAYS(decrypted block at offset 0 starts with binlog magic)` and `ALWAYS(decrypted block at offset 4 starts with a valid FDE header)`, so they are not duplicated here.

## Key observations

- **Envelope stickiness.** Appends always use the envelope persisted with the tail, never the current config. A config change takes effect only at the next rotation.
- **`metadata.json` lags behind the files.** Once it says encrypted it never reverts, so it is a weak indicator of what is on disk.

## Fault requirements

- Process kill and pause on PBS, driven by the workload.
- S3 faults on the metadata PUT.
- No node termination needed.

## Open questions

- Is per-file mixing the intended design (N9)? In particular, is a plaintext tail that keeps receiving appends after encryption is enabled intended, given README:674 says that with `storage.encryption` present "all the binlog data files will be encrypted"? `(needs human input)`

## Investigation results (2026-10-13)

- **Mixing is exercised on purpose by the existing test, but the README wording contradicts part of it.** `encryption.test:40-67` fetches `binlog.000001` unencrypted, runs `FLUSH BINARY LOGS`, enables encryption, and fetches again. That second run resumes the plaintext `binlog.000001` tail and appends the source ROTATE to it in plaintext before creating the encrypted `binlog.000002`. The decrypt loop then treats file 1 as plaintext (`encryption.test:185-197`, `kek_id == ""`). So "an encryption-enabled run appends plaintext bytes to a plaintext tail" is accepted test behaviour. README:674 ("all the binlog data files will be encrypted before written to the storage") does not describe this. Whether a tail that keeps growing in plaintext for a long time is intended is still owner intent. **Property unchanged**: it asserts that mixed storages are served correctly, not that they are never plaintext. `encrypted-storage-no-plaintext-at-rest` owns the doc-vs-code conflict, and the README:674 claim is its premise.
- **Old KEKs: the code answers the behaviour question, so the property does not depend on the answer.** `load_and_validate_binlog_metadata_set` (`storage_core.cpp:927-953`) calls `validate_binlog_metadata` for **every** indexed binlog, not just the tail. `validate_binlog_metadata` (`:893-904`) throws when a record's `kek_id` is missing from the keyring. The call sits outside the `querying_only` try/catch (`:953`), so `list` and `search` fail as well. Removing **any** KEK that any stored file references, not only the tail's, makes the whole storage unopenable in every mode. The README states only that the keyring "must be present if the storage has at least one encrypted binlog file" (README:610) and nowhere says that historical KEKs must be kept. This property's workload keeps every KEK in the keyring, so its invariant holds either way. Whether the fail-stop is supported, or is a wedge that needs its own property and a doc fix, is a catalog-wide question about operator recovery. It is recorded as such and is no longer a question for this property.

### Investigation Log

#### Is per-file mixing the intended design (N9)?

- Examined: `mtr/binlog_streaming/t/encryption.test` (whole file); README:593-679 (storage and encryption sections); `storage_core.cpp:299-322` (append uses the tail record's envelope); `git log --grep=encrypt` (PBS-39 parts 1-8, PBS-54); ticket snapshot (only ticket 8 concerns encryption).
- Found: in `encryption.test`, the run with encryption enabled resumes the plaintext `binlog.000001` and appends its ROTATE in plaintext (`:48-67`), and the decrypt loop accepts file 1 as plaintext (`:185-197`). The final run with encryption disabled creates plaintext files in a `generic` storage. README:674 claims that "all the binlog data files will be encrypted" when the section is present.
- Not found: any design note, commit message or comment that says whether a plaintext tail continuing under an encryption-enabled config is intended or only tolerated.
- Conclusion: `(needs human input)`. The test makes mixing deliberate at least at the rotation boundary, while the README contradicts it. The property does not change, because it tests reads of a mixed storage, not the absence of plaintext.

#### Must the old KEK remain in the keyring forever, and is that documented?

- Examined: `storage_core.cpp:632-664` (`initialize_storage_encryption`), `:864-904` (`validate_binlog_metadata`), `:927-953` (`load_and_validate_binlog_metadata_set`); README:609-611 and :673-707.
- Found: every indexed binlog's metadata is validated at open time, and a missing `kek_id` throws (`:899-903`) even in `querying_only` mode, because the validation call sits after the try/catch that skips invalid metadata. Every KEK that any stored file references must therefore stay in the keyring for the storage to open at all. The README does not state this.
- Not found: documentation or comments on KEK retirement or re-wrapping (there is no re-encrypt tool).
- Conclusion: resolved for this property. The behaviour is fail-closed for the whole storage, and the workload keeps all KEKs, so the invariant is unaffected. "Is KEK removal a supported fail-stop or a wedge?" moves to the catalog-wide open questions as owner intent.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Runs in `pull_fs`, where pull is the writer, so the through-pull read adds no second writer. Where a non-pull storage is read, the independent Python decryptor is the canonical encrypted reader. Pull is not used to read a storage it does not own: pull always builds a streaming storage and runs the collector (`pull_operation.cpp:108-153`), so on a `fetch_fs` or `s3` storage it would run recovery on, and append to, the storage under test. A pull readback there runs only at quiescent points on a snapshot copy with `connection.host` pointing at an unreachable endpoint (lenses: implementability P1, wildcard W2).
- Added vacuity marker `Sometimes("writer restarted after the encryption config changed")`: config is drawn once per timeline in the current topology, so the transitions exist only once the config-mutation driver (topology change) runs (coverage-balance F1, implementability C5).
