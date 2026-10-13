# encrypted-storage-no-plaintext-at-rest — Once a storage is encrypted, no new binlog content is written in plaintext

Found by: focus 6, 1 (security, data integrity)
Merged from: encrypted-storage-no-plaintext-at-rest (focus 6), encryption-envelope-on-every-new-binlog (focus 1)

## Synthesis notes

> **Updated 2026-10-13:** the reverse direction is accepted by design. The SUT assertion and the second workload assertion below are superseded; see "Property changes from investigation".

Safety, **P1**. Focus 6 rated it P1 and focus 1 rated it P2. It is P1 because README:673 claims files are encrypted before they are written, and the data exposure is silent.

Distinct workload `Always` messages:
- "no per-transaction marker committed while encryption was configured appears in plaintext at rest". The scan covers storage objects, the S3 local buffer and logs.
- "every binlog created after metadata.json records encryption has an envelope and ciphertext data".

SUT-side: a single `ALWAYS(!encryption_format_ || encryption_record)` with one message. Focus 6 proposed it in `write_data_to_stream`, and focus 1 proposed it in `open_new_binlog_file_internal`. It is kept once, at the `write_data_to_stream` site, which covers both the tail-append and the new-file case.

Mechanism (N9) has two directions:
- Enabling encryption keeps appending plaintext to the existing tail (`storage_core.cpp:172-178`).
- Dropping the encryption section is accepted by `validate_metadata` (`:808-829`). New files are then plaintext under an "encrypted" `metadata.json` (`:1008-1012`).

Whether this is a bug depends on intent: `encryption.test` and README:610 tolerate per-file mixing. That stays an open question. `mixed-encryption-storage-served-correctly` checks that the same transitions stay readable.

## What led to it

- **SG6 (README:673).** "data files are encrypted before they are written, and file keys are never stored in plaintext". `metadata.json` records an encryption format, and operators will read that as "this storage is encrypted".
- **Lead N9 (sut-analysis, focus 4).** The storage-level flag and the per-file reality can diverge. Focus 6 re-verified this.

## Code paths

- **`write_data_to_stream`** (`src/binsrv/storage_core.cpp:1125-1160`): encryption is decided **per binlog record** (`encryption_record.has_value()`).
  - With no record, bytes go to the backend as they are (`:1129-1132`).
  - The storage-level `encryption_format_` is never consulted on the write path.
- **`write_event_block`** (`:299-322`): appends with `get_current_binlog_record_unsafe().encryption`. That is the tail record's own envelope, loaded from its `.json`.
- **Constructor** (`:166-178`, update at `:172-178`, `save_metadata` at `:177`), verified:
  - Suppose `metadata.json` has no encryption format and the config has one. The constructor updates `metadata.json` to say encrypted.
  - The existing tail record still has `encryption = nullopt`, so **every further append to that tail is plaintext** until the next rotation.
  - Newly created files are encrypted.
- **Reverse direction**, verified. Suppose the config has no `encryption` but the storage's `metadata.json` says `generic`.
  - `validate_metadata` (`:808-826` / `:808-829`) errors only if both formats are present and differ, so this case passes.
  - `has_active_kek()` is false, so `generate_binlog_encryption_record` (`:1008-1010` / `:1008-1012`) returns `nullopt`.
  - Result: **new binlogs are created in plaintext** while `metadata.json` still says encrypted.
- **New binlog** (`:680-703`), verified:
  - The envelope is generated and the magic is encrypted before anything is written.
  - Right after, the envelope is persisted in `<name>.json` (`:907-925`, `to_model` `:69-90`).
  - KEKs stay only in the keyring file. File keys are stored only wrapped: only `file_key_encrypted_with_kek` is persisted (`:1079-1087`).
- **S3 backend local buffer** (`s3_storage_backend.cpp:555-580`): keeps a full local copy under `fs_buffer_directory`, or under `/tmp/<uuid>` created with default permissions.
  - The copy is leaked on SIGKILL (`:640-651`, N10).
  - It receives the same bytes the backend receives, so it is ciphertext exactly when the record is encrypted.
- **Logs**: event bodies are not printed. `unknown_body`'s `operator<<` prints nothing, and QUERY/ROWS bodies fall to `unknown_body`, so debug logs do not leak row data. Checked; no property needed.

## Failure scenarios

1. **Enabling encryption on a live storage (N9 forward).** Trigger: a restart that adds `storage.encryption` to the config. The workload runs pull on an unencrypted storage, kills or stops it, and restarts it with encryption configured.
   - This is a plausible operator action, and README:610 implies a storage can mix encrypted and non-encrypted files.
   - The current tail keeps growing in plaintext. Transactions committed *after* encryption was turned on are stored in cleartext on disk or S3 and in the S3 local buffer, while `metadata.json` says the storage is encrypted.
   - The window lasts until the source rotates, which can take hours.
2. **Dropping encryption (N9 reverse).** Trigger: a restart whose config has no `encryption`, against an encrypted storage. This can be an operator error or a mis-templated restart.
   - Every new binlog is stored in plaintext.
   - The outcome is deterministic and driven by config. Antithesis adds little here beyond a workload action that randomly restarts with config variants.
3. **Crash between the `metadata.json` update and anything else.** Harmless on its own. A kill inside the constructor's `save_metadata` (`:177`) is still worth timing, because it moves the enablement boundary.

## Oracle / workload checks

- **Markers.** The workload inserts rows that carry a random 16-byte hex marker per transaction. For each one, it records the commit time and whether encryption was "on" in the PBS config at that time.
- **Scan.** After each quiet period, scan every at-rest location:
  - storage objects (the filesystem directory, or the S3 bucket via the MinIO client);
  - `fs_buffer_directory`;
  - PBS's `/tmp/*` directories;
  - PBS log files.
- `Always("no per-transaction marker committed while encryption was configured appears in plaintext at rest")`.
- `Always("every binlog created after metadata.json records encryption has an envelope and ciphertext data")`. It holds when, for every binlog created after `metadata.json` gained an `encryption` format:
  - its `.json` has a `file_key_envelope`;
  - its data object does not start with `\xfebin`.

  Binlogs created before encryption was first enabled are exempt. The workload tracks this "created after" boundary, because it knows when it first enabled encryption.
- **Key check.** No `.json` contains a plaintext file key field.

## SUT-side instrumentation suggestions

The codebase has no Antithesis SDK, so every SDK assertion below is missing.

- `ALWAYS(!encryption_format_.has_value() || encryption_record.has_value(), "encrypted storage writes only enveloped data")` in `write_data_to_stream` (`storage_core.cpp:1125-1160`). **Missing.** It fires directly for N9 in both directions.
  - Focus 1 proposed the same condition in `open_new_binlog_file_internal`, with the message "new binlog in encrypted storage has an envelope". That site covers only the reverse direction, so it is folded into this one.
  - If plaintext-tail-after-enabling is intended, the assertion must exempt pre-enablement records.
- `tests/storage_property_test.cpp` runs on an encrypted storage, per ticket 8's notes. **Partial**, non-Antithesis: it does not toggle encryption across reopenings.

## Key observations

- **The plaintext window.** It depends on when rotation happens relative to the toggle.
- **More at-rest locations.** Crash recovery (`resize_object`) and the S3 local buffer leak (N10) multiply the places where data sits at rest.
- **Wrapped keys.** File keys are never persisted unwrapped. This part of SG6 holds.

## Fault requirements

- Workload-driven restarts with config toggles (none ↔ encryption) at arbitrary points: mid-transaction, and just before or after rotation.
- Kills in the constructor's `save_metadata` (`:177`).
- SIGKILL to leak the S3 local buffer (N10).
- No network or node faults needed.

## Open questions

- Is the plaintext tail after enabling encryption intended? Transactions that pull or fetch append to a binlog created before encryption was configured are stored in cleartext. `(partial: per-file envelope design and maintainer MTR encryption.test imply per-file semantics (it only enables encryption at a rotation boundary); README:674 says "all the binlog data files will be encrypted"; the maintainer position on a live tail is unknown)`
  - If intended: the marker check exempts transactions written to a binlog created before encryption was enabled.
  - If not intended: it is a confidentiality bug that the first mid-binlog toggle hits deterministically.

## Property changes from investigation (2026-10-13) — scope narrowed

- **Reverse direction (disabling encryption) is accepted by design and is no longer a violation.**
  - The maintainers' MTR test `mtr/binlog_streaming/t/encryption.test:100-110` (PBS-39, 3e812f2) re-generates the config "with encryption disabled" and expects the fourth binlog to be **unencrypted**. The verification loop handles `kek_id == ""` as a plaintext file (`:166-180`).
  - `metadata.json`'s `encryption` is used only for format-compatibility validation (`storage_core.cpp:808-829`) and for description (`:589-594`). It is a "format in use" marker, not a promise that every file is encrypted.
  - **Invalid as written:** the SUT `ALWAYS(!encryption_format_.has_value() || encryption_record.has_value())` in `write_data_to_stream` fires on the maintainers' own tested flow.
- **Replacement SUT assertion:** `ALWAYS(!has_active_kek() || encryption_record.has_value(), "new binlog created while a KEK is configured has an envelope")` at `open_new_binlog_file_internal` (`storage_core.cpp:679-702`, after `generate_binlog_encryption_record`). Missing. This is focus 1's site, now with a config-based condition.
  - Add the tail-append check (`write_event_block`, `storage_core.cpp:299-322`) only if the open question resolves to "not intended", as `ALWAYS(!has_active_kek() || get_current_binlog_record_unsafe().encryption.has_value(), "append while a KEK is configured goes to an enveloped binlog")`.
- **Workload assertions change.**
  - Second assertion: `Always("every binlog created while encryption was configured has an envelope and ciphertext data")`. Before, it covered every binlog created after `metadata.json` recorded encryption. The workload records the config in effect at each binlog's creation.
  - First assertion: `Always("no per-transaction marker committed while encryption was configured appears in plaintext at rest")` now exempts markers appended to a binlog created before enablement. Remove that exemption if the tail question resolves to "not intended".
  - Keep the wrapped-key check, which is unchanged.
- **Config toggles are in scope.** The maintainers' test toggles encryption on and off and rotates KEKs and ciphers across runs. The workload should vary the encryption config across restarts at arbitrary points, including mid-binlog, so the tail case is exercised.
- Priority: P1 still stands for the forward and new-file case (README:674). The reverse direction no longer contributes.

### Investigation Log

#### Is the plaintext tail after enabling encryption intended?

- Examined:
  - `storage_core.cpp:166-178` (`metadata.json` is updated to encrypted; the tail record keeps `encryption = nullopt`), `:299-322`, `:1125-1160` (the write path consults only the record);
  - `mtr/binlog_streaming/t/encryption.test:35-90` (the first fetch downloads binlog 1 unencrypted, then `FLUSH BINARY LOGS`, then encryption is enabled; the next fetch appends only binlog 1's closing ROTATE in plaintext and creates binlog 2 encrypted);
  - README:610 and :673-674; the `git log` of the test (3e812f2, PBS-39 part 8).
- Found:
  - The design is per-file: a file's envelope is fixed at creation.
  - The maintainers' test enables encryption only at a rotation boundary, so it never exercises transactions appended to a pre-existing tail.
  - README:674's wording ("all the binlog data files will be encrypted before written") is broader than the code.
- Not found: a maintainer statement on a live tail.
- Conclusion: `(partial: ...)`. The workload exemption is parametrised on the answer.

#### Is disabling encryption on an encrypted storage meant to be accepted?

- Examined: `mtr/binlog_streaming/t/encryption.test:100-110` and its `.result`; `storage_core.cpp:589-594`, `:805-831` and `:1008-1012`.
- Found: the maintainer test disables encryption and expects a plaintext fourth binlog. `metadata.json.encryption` is never consulted on the write or read path except for format validation.
- Conclusion: resolved. Accepted by design, and the property is narrowed (see "Property changes"). Whether `metadata.json` should be updated when encryption is disabled is a documentation nicety, not a property question.

#### Should the workload toggle or vary the encryption config across restarts at all?

- Examined: `encryption.test` (toggles on, off and KEK/cipher across runs); README:610 (a mixed storage needs the keyring).
- Found: toggling is a maintainer-exercised operator flow.
- Conclusion: resolved. Yes: toggle at arbitrary points, including mid-binlog and just before or after rotation.

## Evaluation refinements (2026-10-13)

Applied by the evaluation synthesis (`../evaluation/synthesis.md`). Where this section and earlier text in this file disagree, this section wins; the catalog entry has been updated to match.

- Added vacuity marker `Sometimes("writer restarted after encryption was enabled on an existing storage")`; the trigger needs the config-mutation driver (topology change; coverage-balance F1, implementability C5).
- The deterministic N9 half (plaintext tail after enabling encryption) is also MTR territory; whether to move it is part of bias B4.
- Runs in `fetch_fs`, `pull_fs`.
