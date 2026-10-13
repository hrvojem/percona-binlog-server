# s3-crash-leftovers-bounded — Crashes and failed uploads do not accumulate S3 leftovers locally or in the bucket

Found by: focus 5 (resource; two findings)
Merged from: s3-local-buffer-bounded-across-crashes (focus 5), s3-no-orphaned-multipart-uploads (focus 5)

## Synthesis notes

Safety, P1 (local-buffer variant P1, multipart variant P2).

Distinct workload `Always` messages:
- "S3 local buffer stays within C x largest binlog after kill/restart cycles"
- "incomplete multipart uploads older than T stay within k"
- "an interrupted upload leaves the old or the new full object content"

Workload `Sometimes("kill happened while a temp file existed")`, `Sometimes("an incomplete multipart upload observed")`.

Mechanisms: each SIGKILL skips `~s3_storage_backend` (`s3_storage_backend.cpp:640-652`) and leaves a full binlog copy in `fs_buffer_directory` (never cleaned; the README sample sets it); a partial-download failure leaks without a kill (`:828-832`); whole-object re-uploads (`:369-386`, `:933-938`) are possibly split into multipart by CRT (unverified). Leaks fill the local disk and wedge every run; plaintext/ciphertext in leaked copies also matters for the encryption properties.

## What led here

- (local buffer) The S3 backend keeps a full local copy of the active binlog and removes it only on a clean shutdown. Attention focus 5 (resource boundaries) and sut-analysis lead N10 (focus 2, 8, 9, 12). README:618 says the auto-created buffer directory "is automatically removed when the server exits". That is true only for a clean exit.
- (multipart) Every checkpoint re-uploads the whole active binlog (`s3_storage_backend.cpp:933-938`) through the **CRT** client (`Aws::S3Crt::S3CrtClient`, `:190-191`). The CRT meta-request for PutObject usually splits large bodies into multipart uploads (part size and threshold are SDK defaults, because PBS sets neither, `:193-196`). A process killed during an upload cannot abort it. The parts stay in the bucket, invisible to `ListObjectsV2`, billed and using space. SUT-analysis lists "an aborted CRT multipart upload never replaces the object" as an unverified assumption.

## Code paths (commit 99de682, `src/binsrv/s3_storage_backend.cpp`)

- `:190-191` CRT client; `:193-196` no part size / threshold configured.
- `:369-386` `put_object_from_stream`: a single `client_->PutObject` with no multipart configuration.
- `:559-572`: with no `fs_buffer_directory`, the constructor creates `/tmp/<uuid>` and sets `owns_tmp_file_directory_ = true`. With an explicit `fs_buffer_directory` (the README sample config uses `/tmp/binsrv`, `main_config.json`), the directory is shared and never cleaned.
- `:574-590`: the constructor only creates the buffer directory or checks that it is one; nothing scans `fs_buffer_directory` for stale files at startup.
- `:640-652` `~s3_storage_backend`: the only place that removes the per-process directory (`remove_all`) or the current temp file (`close_stream_internal`). SIGKILL, OOM-kill and abort() skip it.
- `:765-808` `do_resize_object` (startup tail recovery): downloads the object into another UUID temp file, then one more whole-object PUT. If `resize_file` or the PUT throws, that file is not removed (no RAII); it is cleaned only if the directory is owned and the destructor runs. `:768-774`: the only mention of `multipart` (a TODO comment); PBS never lists or aborts multipart uploads.
- `:821-848` `do_open_stream`: every open generates a new UUID temp path (`generate_tmp_file_path`, `:928-931`). In append mode it first downloads the **whole** object into that path (`:828-832`). If the download fails partway, the partial file stays on disk, because `tmp_fstream_` is not open and neither `close_stream_internal` nor the destructor removes `current_tmp_file_path_`.
- `:850-863` `do_write_data_to_stream`: the local file grows to the full binlog size; also the path that re-uploads on every checkpoint, rotation flush and `~storage` flush, with object size growing up to `max_binlog_size`.
- `:933-938` whole-object re-upload per checkpoint.

## Failure scenarios

- **Kill trigger, local buffer** — run fetch (cron) or pull on S3 with `max_binlog_size` M. Each SIGKILL during a run leaks one file of up to M bytes, plus a possible resize copy at the next startup. After k crashes the buffer filesystem holds about k·M of garbage. When it fills, `do_write_data_to_stream` fails ("cannot write data to the temporary file"), and so do `get_object_into_file` on append-open and `do_resize_object` at startup. Every run then fails at startup or at the first checkpoint. This turns a leak into a wedge (LG5), and it feeds the ENOSPC paths in failed-flush-never-reappends-data (ENOSPC variant).
- **No-kill trigger, local buffer** — a partial append-open download failure (`:828-832`) or a failed `do_resize_object` leaks a temp file without any process fault.
- **Kill / upload-failure trigger, bucket** — PBS on S3 is SIGKILLed repeatedly (by the workload), or its PUTs fail with partitions or MinIO faults, while the active binlog is above the multipart threshold. Each event can leave an incomplete multipart upload holding up to the full object size. Bucket usage grows by up to one binlog per crash, and so does the operator's bill on AWS (unless a lifecycle rule aborts incomplete uploads).
- **Encryption angle** — leaked local copies may hold plaintext or ciphertext; relevant to encrypted-storage-no-plaintext-at-rest.

## Oracle / workload checks

- The workload runs fetch on S3 (MinIO) as a subprocess and SIGKILLs it at random moments. Pull can be killed the same way if the workload owns the process. Node termination is **not** required.
- Local buffer: after each cycle, measure the total bytes of regular files in the buffer root (the `fs_buffer_directory` if set, otherwise the `/tmp/*` UUID directories), via a shared volume or a helper in the PBS container. `Always(bytes ≤ C · largest stored binlog size + slack, "S3 local buffer stays within C x largest binlog after kill/restart cycles")`, C small (for example 3: the live stream copy, a resize copy and an append download). Run for both the explicit and the default directory configuration.
- `Sometimes("kill happened while a temp file existed")` (target k ≥ 3 such kills), so the leak window is known to be exercised.
- Bucket: after each kill/restart cycle, call `ListMultipartUploads` on the bucket (MinIO supports it). `Always(count of incomplete uploads older than T under the storage prefix ≤ k, "incomplete multipart uploads older than T stay within k")`, k about 1 for an in-flight upload.
- `Sometimes("an incomplete multipart upload observed")`, to confirm CRT actually uses multipart at these sizes. If this never fires, the multipart check is vacuous: lower the object size threshold or drop it.
- Atomic replace: after an upload is interrupted, `GetObject` of the binlog returns either the old or the new full content: `Always("an interrupted upload leaves the old or the new full object content")` (supports SG4 on S3).

## SUT-side instrumentation suggestions

- `SOMETIMES(owns_tmp_file_directory_ == false && stale files found at startup, "startup found stale files in a shared S3 buffer directory")` — requires adding a scan at construction — **missing**.
- `REACHABLE("append-open download failed and left a partial temp file")` in the `do_open_stream` append branch after `get_object_into_file` throws — **missing**.
- Bucket side: none at the SUT level; leftover uploads are visible only from the bucket. Nothing to add in code beyond logging the upload size.
- No Antithesis SDK in the repo (see existing-assertions.md).

## Fault requirements

- Workload-driven SIGKILL of fetch/pull on S3 (no node termination); network partitions / MinIO faults on PBS<->S3; binlogs large enough to cross the CRT multipart threshold; both default and explicit `fs_buffer_directory` configs.

## Open questions

- Is leaking a full binlog copy per crash accepted, given that the README promises cleanup only "when the server exits" (README:618) and says only that kill -9 "may result in losing some progress" (README:728)? If yes, the local-buffer check becomes "a startup with stale files does not fail" plus documented operator cleanup. `(needs human input)`
- Is leaving incomplete multipart uploads acceptable, with operators expected to configure an `AbortIncompleteMultipartUpload` lifecycle rule? If yes, downgrade the multipart check to documentation and keep only the atomic-replace check. `(needs human input)`

## Investigation results (2026-10-13)

- **CRT does use multipart at realistic sizes.** Ticket 17's measurements were taken against Versity 1.8.0 with SDK 1.11.774, the same pins as the harness. They show 10 PUTs for one 64 MiB binlog with no checkpointing, and 297 PUTs for 1 MiB checkpoints against an estimate of 288 = Σ⌈k/8⌉. That count fits UploadPart requests of 8 MiB plus the `.json` and index PUTs; a single-PUT upload would give about 3. The `aws-c-s3` header (`s3_client.h:420-450`, `:725-750`, read from the locally available 0.7.1, which is older than the version bundled with 1.11.774) documents an 8 MiB default part size and a default threshold of max(part_size, 5 MiB), with "uploads smaller or equal to this will use a single HTTP request". PBS sets neither value (`s3_storage_backend.cpp:180-260`, no `partSize` or threshold). **Implication:** the multipart check is not vacuous, but only for binlog objects above 8 MiB. The workload must grow at least one binlog past 8 MiB, preferably past 16 MiB so that a kill lands mid-upload with several parts. With the small binlogs used elsewhere in the harness, `Sometimes("an incomplete multipart upload observed")` would never fire. The header also lists `AWS_S3_REQUEST_TYPE_ABORT_MULTIPART_UPLOAD`, and the library binary contains `AbortMultipartUpload`, so an in-process upload *failure* probably triggers a CRT abort. Leftovers are then expected mainly from SIGKILL, or from a failed abort under a partition (likely, not verified from source).
- **CRT does not buffer to local disk on the PUT path.** The `aws-c-s3` client API buffers parts in memory (`memory_limit_in_bytes`, the "limit the amount of data buffered in memory" window). Disk is involved only through the explicit `send_filepath` and `recv_filepath` options, and PBS passes an iostream body instead (`put_object_from_stream`, `:369-386`). No extra slack is needed in the local bound.
- **A shared `fs_buffer_directory` is harmless today.** Only two paths create temp files: `do_open_stream` (`:821-848`, writer streaming) and `do_resize_object` (`:765-808`, startup recovery). Recovery runs only when `construction_mode_ != querying_only` (`storage_core.cpp:956`). Query-mode processes (`list`, `search_*`) therefore create no temp files in the directory. Every name is a fresh UUID (`:928-931`), and nothing scans or deletes other files (`:574-590`, `:640-652`). The bound needs to count only writer processes, plus one live temp file per running writer. A future startup cleanup must spare files of live processes (cf. `startup-tmp-cleanup-spares-live-writer`), but that is a design constraint for a fix, not a question about this property.
- **Property changes:** the scope is narrowed. The multipart sub-check requires binlogs > 8 MiB (a new workload requirement), and the bound's C counts writer processes only.

### Investigation Log

#### Is leaking a full binlog copy per crash accepted?

- Examined: README:618 (`fs_buffer_directory`), README:717-728 (graceful termination, kill -9), `s3_storage_backend.cpp:559-590`, `:640-652`; the ticket snapshot (ticket 17 mentions only the resize download/upload).
- Found: the README promises removal of the auto-created directory on exit, says that an explicit directory is never deleted, and says that kill -9 may lose progress. Nothing addresses accumulated temp files.
- Not found: any statement of intent about leftovers after a crash.
- Conclusion: `(needs human input)`.

#### Is `fs_buffer_directory` meant to be shared between a writer and query-mode processes?

- Examined: all `generate_tmp_file_path` callers (`s3_storage_backend.cpp:775`, `:826`), `storage_core.cpp:955-985` (recovery guarded by construction mode), `:600-625` (the `.tmp` object cleanup is for storage objects, not the local buffer).
- Found: only writers create local temp files, all names are UUIDs, and nothing deletes foreign files. Sharing is harmless in current code.
- Not found: documentation about sharing (none needed for the property).
- Conclusion: resolved. Count only writer processes in the bound.

#### Does the AWS CRT client buffer anything else on local disk?

- Examined: `/nix/store/...-aws-c-s3-0.7.1/include/aws/s3/s3_client.h` (part size, threshold, memory limit, send/recv file options); `s3_storage_backend.cpp:369-386`.
- Found: part buffering is in memory. File I/O happens only through `send_filepath` and `recv_filepath`, which PBS does not use.
- Not found: the exact `aws-c-s3` source bundled with SDK 1.11.774. The header read is 0.7.1.
- Conclusion: resolved, with a version caveat. No extra slack is needed.

#### Does `S3CrtClient::PutObject` in SDK 1.11.774 use multipart for an iostream body, and above what size?

- Examined: ticket 17 measurements (`.tickets-snapshot.txt:918-936`); `s3_client.h:420-450`, `:725-750`; the `AbortMultipartUpload` string in `libaws-c-s3`.
- Found: the measured PUT counts match 8 MiB parts against Versity 1.8.0 with SDK 1.11.774. The documented default threshold is max(8 MiB part size, 5 MiB).
- Not found: whether a 1-part upload (≤ 8 MiB) is a single PUT or a 1-part MPU on the wire. The proxy counts only PUTs, so the two look the same, and the header says single request.
- Conclusion: resolved. Multipart applies above 8 MiB, so the workload must produce larger binlogs.

#### Is leaving incomplete uploads acceptable (lifecycle rule expected)?

- Examined: README (no lifecycle or multipart mention), `s3_storage_backend.cpp:768-774` (a TODO about multipart for resize only).
- Found: PBS never lists or aborts multipart uploads, and the docs are silent.
- Not found: the owners' intent.
- Conclusion: `(needs human input)`.
