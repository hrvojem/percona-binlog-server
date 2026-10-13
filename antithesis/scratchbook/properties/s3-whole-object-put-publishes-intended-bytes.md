# s3-whole-object-put-publishes-intended-bytes — Every S3 whole-object round trip (startup truncation, checkpoint re-upload) publishes exactly the intended bytes, including after a failed transfer

## What led here

Evaluation gap G6 (coverage-balance F7). `s3_storage_backend.cpp` is a churn hotspot (16 commits) with mechanics that no other backend has. "Append" is a local temp-file append followed by a whole-object PUT. Startup truncation is GET, then a local `resize_file`, then a whole-object PUT. One `std::fstream` member, `tmp_fstream_`, is the body of every checkpoint upload for the open binlog. Only `s3-crash-leftovers-bounded` is S3-specific. Two items were named as uncovered:

1. `resize_object` during startup recovery: the truncation round trip on S3.
2. The "sticky failbit" hypothesis: after one failed `PutObject` that read `tmp_fstream_` as its body, eof or fail bits stay set and every later write in the process fails (focus 8, sut-analysis:277, `[needs-repro]` at :316).

## Code paths

- `src/binsrv/storage_core.cpp:956-985`: at streaming/purge construction, if the **tail** binlog's listed object size (from `ListObjectsV2`, `object_names.at`) differs from its `.json` size and is larger, it calls `backend_->resize_object(name, metadata.size)` and logs "recovered binlog file '{}' by truncating it to the size from the metadata". Larger metadata than object size fails ("cannot perform recovery"). A mismatch on a non-tail binlog fails.
- `src/binsrv/s3_storage_backend.cpp:765-813` `do_resize_object`:
  1. `get_object_into_file` downloads the **whole** object into a new UUID temp path (`:779-780`; the stream factory opens it with `trunc`, `:330-341`);
  2. `std::filesystem::resize_file(tmp, new_size)` (`:782-787`). **If the download is shorter than `new_size`, this zero-extends**;
  3. a fresh `std::fstream` on the temp file is the body of `put_object_from_stream` (`:789-801`);
  4. the temp file is removed (`:802-808`). A failure there throws *after* a successful PUT, so the open fails although the truncation landed.
- `:342-366` the GET stream handler: checks `content_length == range length` only for ranged GETs. For whole-object GETs it checks that the bytes written to the file equal the response `content_length`, then `assert(file_size == response_content_length)` (debug only). Nothing checks the downloaded length against the **listed** size or against `new_size`.
- `:821-848` `do_open_stream(append)`: downloads the whole object again into another temp file and opens `tmp_fstream_` with `in|out|binary|app|ate`.
- `:850-863` `do_write_data_to_stream`: `tmp_fstream_.write` then `flush`. A false stream state raises "cannot write data / cannot flush the temporary file". Then `upload_tmp_stream_internal()` (`:933-938`) passes `tmp_fstream_` itself as the PutObject body (`:369-386`, wrapped in a non-owning `shared_ptr`). PBS never calls `clear()`, `seekg` or `seekp` on `tmp_fstream_`. Repositioning to offset 0 before upload, and clearing state after it, is left entirely to the AWS SDK / CRT body-stream wrapper.
- `src/binsrv/storage.cpp:68-77`: after a failed `write_event_block`, the only later write on the same `tmp_fstream_` in the same process is the `~storage` retry (fetch exits on the exception; pull ends on a storage exception, N7). So "poisoning" can affect exactly one write: that retry.

## Failure scenarios

1. **Short download during recovery gives zero-padded tail (silent).** The GET returns fewer bytes than the listed size, for example a truncated body that the SDK reports as complete, or a stale or short read on a non-Versity store (A10). If the downloaded length is still at least `new_size`, nothing is wrong. If it is below `new_size`, `resize_file` zero-extends, the PUT publishes an object of exactly `metadata.size` whose tail is zeros, and every later check that compares object size with metadata passes. Only content oracles see it: the byte prefix for plain storages, `stored-state-never-regresses`' prefix hash. On **encrypted** storages the zeros decrypt to garbage, and a source-free check must catch it. Likelihood with Versity and the SDK's own length check is low. A SUT guard makes it impossible to miss.
2. **Failed or ambiguous resize PUT.** A 5xx exhausts retries: the constructor throws, the next start redoes the truncation (idempotent). With `reset_after` the PUT applied but the client does not know: the next start sees sizes equal and continues (fine). A stale resize PUT that lands *after* the next process appended regresses the object. That is the replay variant of `stored-state-never-regresses` and is referenced, not duplicated.
3. **Poisoned or mispositioned upload stream after a failed PUT.** After a failed `PutObject`, the `~storage` retry appends the same block (ticket 18) and uploads again. Three outcomes are possible:
   - (a) the stream is clean and the SDK reads it from offset 0: the object is the whole local file. This is the observed behaviour for 5xx, see the Investigation Log;
   - (b) a sticky failbit makes the retry's `write` fail: no upload and no duplicate. That is benign, and it actually avoids ticket 18's data-upload variant;
   - (c) the SDK reads the body from the stream's *current* position, or from a position left mid-file by an aborted transfer: the PUT publishes a suffix of the local file as the whole object. That is a corrupted tail whose length may still be at least `metadata.size`, so startup truncation "repairs" it into wrong bytes.

   (c) is the dangerous one. The `s3_fault_proxy` cannot produce it, because the proxy reads the whole request body before acting on any rule, so the SDK always finishes sending. Only a transport failure **mid-body** produces it: an Antithesis partition or reset on pbs↔s3 during an upload, or CRT's low-throughput monitor aborting a stalled transfer (catalog Open Q 14: 1 B/s over at least 3 s). That makes it an Antithesis-unique angle.

## Oracle

- **SUT-side (all missing):**
  - `ALWAYS("S3 resize download covers the requested size")` in `do_resize_object` before `resize_file`: `file_size(resize_tmp) >= new_size`. This turns scenario 1 into a hard failure at its root.
  - `REACHABLE("S3 tail truncated by resize_object during startup recovery")` after the resize PUT succeeds. The site is inside the S3 backend, so it does not collide with `recovery-paths-exercised`'s generic tail-truncation marker at `storage_core.cpp:979`. It is the vacuity guard for this property and for S3 runs of `stored-state-never-regresses`.
  - `SOMETIMES("S3 upload succeeded on a stream whose previous upload failed")` in `upload_tmp_stream_internal`, driven by a member flag set when a `put_object_from_stream` on `tmp_fstream_` throws and cleared on open. It shows that the post-failure path (scenario 3) was exercised.
  - `ALWAYS("S3 checkpoint upload published the whole local buffer")`: after a successful `put_object_from_stream` in `upload_tmp_stream_internal`, a `HeadObject` in the Antithesis build only, with `ContentLength == file_size(current_tmp_file_path_)`. This catches (c). It costs one HEAD per checkpoint, so gate it behind the Antithesis CMake option. A length check catches the suffix case. The workload's content check below covers same-length corruption.
- **Workload (`s3` template):**
  - `Always("S3 tail object equals its metadata size after startup recovery")`, evaluated when a writer logged the "recovered binlog file … by truncating" line and the tail `.json` size is still the pre-start value (no append yet, for example while the source link is partitioned): `HEAD(tail).ContentLength == json.size`. When an append has already happened, the prefix-stability and length checks of `stored-state-never-regresses` apply instead. This one is not repeated.
  - `Sometimes("S3 startup truncation observed in a writer log")`: a log-line fallback for the `REACHABLE`, in case SUT macros are not accepted (catalog Open Q 7).
  - Content: for plain storages, `stored-binlog-byte-prefix-of-source` already judges the bytes. For **encrypted and rewrite** S3 storages, the source-free prefix hash in `stored-state-never-regresses` plus decryption through the offline decryptor (`encrypted-storage-decrypts-to-source`) are the judges. This property names the triggers they must see.
- **Triggers to schedule:** a writer kill between the data PUT and the `.json` PUT (object longer than metadata, which drives the resize at the next start); `s3_fault_proxy` 5xx on six or more consecutive **binlog** PUTs (ticket 18's data-upload variant: the destructor retry uploads old+2n with metadata old+n, and the next start must truncate it back, `.tickets-snapshot.txt:978-990`); Antithesis partitions and stalls on pbs↔s3 timed inside uploads of a large tail (a big `max_binlog_size` with a small `checkpoint_size`, so bodies are large and slow); `truncate_body` on GETs during startup (download for resize and for `open_stream(append)`).

## Open questions

- **Should `do_resize_object` verify the downloaded length against the listed size and refuse, instead of trusting `resize_file`?** This is a design question. If maintainers add the check, the SUT `ALWAYS` becomes a regression guard. If they consider the SDK length check sufficient, it stays as a tripwire. `(needs human input)`
- **Is the remove-after-successful-PUT failure (`:802-808`, the open fails although the truncation landed) acceptable?** It leaves a correct storage plus a leaked temp file and a failed start, and the next start succeeds. It is benign for content, but it is a spurious failure for `fetch-converges-after-faults-stop` if the buffer volume is full or read-only. `(needs human input)`

## Resolved during investigation (see Investigation Log)

- **Scenario 3(c) (suffix published after a mid-body abort) is not reachable through SDK 1.11.774's request path.** PBS sets no Content-Length (`s3_storage_backend.cpp:368-385`), so `AWSClient::AddContentLengthToRequest` computes it with `seekg(0, end)` / `tellg()` and then `seekg(0, beg)` on every request build. Every upload therefore starts at offset 0 and declares the whole local file's length, wherever an earlier aborted transfer left the read position. `tmp_fstream_` is opened with `app`, so PBS's next `write` appends at the end regardless of the get pointer. If an abort left `failbit` set, PBS's own `write` fails first ("cannot write data to the temporary file"), and `S3CrtClient::PutObjectAsync` also refuses a body whose `fail()` is true ("Input stream in bad state"). That is outcome (b), not (c). Inside one PutObject, aws-c-s3 retries reuse the buffered part body and do not re-read the stream.
- **Multipart:** S3 CRT clamps `part_size` to at least 8 MiB and passes `multipartUploadThreshold` through unchanged (PBS sets neither). Bodies above the threshold go multipart. Parts are read from the stream once, in order, and part retries reuse the in-memory part buffer, so multipart does not create a non-sequential-read path to (c).
- **Property change:** the scenario-3 SUT `ALWAYS("S3 checkpoint upload published the whole local buffer")` (HEAD after PUT) is downgraded from the primary catch for a predicted defect to an optional regression tripwire (keep it only if the per-checkpoint HEAD cost is acceptable). The `SOMETIMES("S3 upload succeeded on a stream whose previous upload failed")` anchor stays. The property's main value is now scenario 1 (resize zero-extension) and scenario 2 (ambiguous resize PUT). The invariant and assertion types are unchanged.

### Investigation Log

#### Does one failed PutObject poison `tmp_fstream_` for later writes in the same process (the sticky-failbit hypothesis)?

- Examined: `s3_storage_backend.cpp:369-386, 821-863, 933-946` (no `clear()`, `seekg` or `seekp` on `tmp_fstream_` anywhere in PBS); `storage.cpp:68-77` and `storage_core.cpp:299-322` (the only later write on the same stream is the `~storage` retry); ticket 18's S3 reproduction (`.tickets-snapshot.txt:978-992`); `extra/qa/s3_fault_proxy.py:18-45` (actions; the proxy reads the full request before any rule fires).
- Found: in ticket 18's repro, six consecutive 500 responses to the **binlog** PUT exhaust the SDK retries. The destructor retry then writes the same block again and uploads successfully. The stored `binlog.000002` is 308,063 bytes against the source's 154,699 = 1,335 (old size) + 2 × 153,364 (the block). So after a failed `PutObject`, the next `write` on `tmp_fstream_` succeeded, and the next upload sent the **whole** local file from offset 0. Normal multi-checkpoint runs also show that the SDK repositions and clears the stream between uploads, since `tmp_fstream_` is always at EOF after a write and an upload. For HTTP-level failures the hypothesis is refuted.
- Not found: behaviour after a failure **mid-body** (connection reset or partition while the SDK is still reading the stream, or the CRT throughput monitor aborting). The proxy cannot produce this, and neither the AWS SDK nor the aws-crt-cpp sources are available locally to read how `StdIOStreamInputStream` / the S3 CRT meta-request reposition the body before a new request.
- Conclusion: `(partial: HTTP-error failures leave the stream usable and the next upload is whole (ticket 18 repro numbers); mid-body transport aborts unverified, AWS SDK/CRT stream handling not read)`.

(Second pass, this investigation.)
- Examined: `s3_storage_backend.cpp:368-385` (`put_object_from_stream`: `SetBody` on a non-owning `shared_ptr`, no `SetContentLength`), `:821-863` (`tmp_fstream_` opened `in|out|binary|app|ate`; write then flush, throw on a false state), CMakeLists.txt:85 (`AWSSDK 1.11.774 EXACT`). AWS SDK C++ 1.11.774 on GitHub: `src/aws-cpp-sdk-core/source/client/AWSClient.cpp` (`BuildHttpRequest` → `AddContentBodyToRequest(httpRequest, request.GetBody(), ...)` → `AddContentLengthToRequest`: `body->seekg(0, body->end); auto streamSize = body->tellg(); body->seekg(0, body->beg);`), `generated/src/aws-cpp-sdk-s3-crt/source/S3CrtClient.cpp` (`PutObjectAsync` returns `INVALID_PARAMETER_VALUE` "Input stream in bad state" when `GetContentBody()->fail()`; `part_size` clamped to `DEFAULT_PART_SIZE` 8 MiB; `multipart_upload_threshold` copied from config). aws-crt-cpp v0.29.4 `source/io/Stream.cpp` (`StdIOStreamInputStream::SeekImpl` calls `clear()` before seeking; `ReadImpl` does not clear). aws-c-s3 v0.7.1 `source/s3_auto_ranged_put.c` (on re-preparation of a part, "we can skip over the async steps that read the body stream"; the body is read once per part via `aws_s3_meta_request_read_body`).
- Found: each PBS upload re-seeks the body to offset 0 and recomputes the length during request construction, so a position left mid-file by an aborted transfer cannot shift the published bytes. A sticky `failbit` causes a PBS-side write error or an SDK-side "bad state" refusal (no upload), which is outcome (b). CRT-internal retries reuse buffered bytes.
- Not found: the exact aws-crt-cpp / aws-c-s3 versions bundled as submodules of SDK 1.11.774 (the CRT and aws-c-s3 files read are from the versions in the local nix store, 0.29.4 and 0.7.1, which may differ). No run with a mid-body partition.
- Conclusion: resolved. Scenario 3(c) is not reachable by the SDK request path; property changed as described in "Resolved during investigation". A mid-body partition trigger stays in the schedule because it still exercises the (a)/(b) outcomes and the ticket 18 duplicate.

#### Does CRT upload PBS's un-sized stream body as multipart for bodies over 8 MiB, and if so, does a failed multipart leave the stream mid-file?

- Examined: as above (S3CrtClient init, aws-c-s3 `s3_auto_ranged_put.c`).
- Found: the body is not actually un-sized at the CRT level: the SDK computes Content-Length from the stream before handing it over. With a known length, aws-c-s3 sizes every part at `part_size` (≥ 8 MiB) and checks each part read is complete (`AWS_ERROR_S3_INCORRECT_CONTENT_LENGTH` otherwise). A failed multipart may leave the stream mid-file, but the next PBS upload re-seeks to 0 (see above), so it does not matter for (c).
- Not found: the effective `multipartUploadThreshold` default in `Aws::S3Crt::ClientConfiguration` for 1.11.774 (PBS does not set it), so the exact body size at which multipart starts is unconfirmed.
- Conclusion: resolved for the property (multipart cannot produce (c)). The threshold value only affects which trigger sizes exercise multipart; the driver should include tails above 8 MiB.

#### Should `do_resize_object` verify the downloaded length against the listed size?

- Examined: `s3_storage_backend.cpp:336-365` (GET stream handler: checks the file length equals the response `Content-Length` for whole-object GETs, and the range length for ranged GETs), `:765-813`.
- Found: a truncated transfer is caught (bytes on disk must equal the response `Content-Length`), so zero-extension needs a *complete* response whose `Content-Length` is below `new_size` (a stale or wrong object version). Nothing compares against the listed size or `new_size`.
- Conclusion: `(needs human input)` — a design choice. The SUT `ALWAYS` stays as a tripwire either way.

#### Is the remove-after-successful-PUT failure acceptable?

- Examined: `s3_storage_backend.cpp:802-808` (throws "cannot remove temporary file after S3 object resizing" after the PUT), `storage_core.cpp:956-985` (the next start sees object size == metadata size and skips the resize).
- Found: confirmed: the start fails although the object is already correct, and the next start succeeds unless the temp directory is still unwritable. Compare `close_stream_internal` (`:940-946`), which deliberately tolerates a failed remove.
- Not found: maintainer intent for the inconsistency between the two remove paths.
- Conclusion: `(needs human input)`. For `fetch-converges-after-faults-stop`, a run whose only failure is this message should be bucketed separately.
