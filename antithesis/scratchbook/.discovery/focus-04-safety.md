# Focus 4: Safety guarantees (claimed and implicit)

Commit 99de682. Read-only pass. Each item says what the system **claims** (with source) or what is **implicit**. The code checks listed for tickets confirm the mechanism; a ticket alone is not treated as evidence.

## Findings

### S1. Resume continues exactly where the previous run stopped (claimed)
- Claim: README.md:3 says the tool can "resume operation from the point when it was previously stopped / terminated"; README.md:711 says "Running the utility for the second time (in any mode) results in resuming streaming from the position at which the previous run finished."
- Mechanism: the resume point is taken from the tail record's metadata. In position mode that is name + `size` (collector_context.cpp `open_connection_and_switch_to_replication`). In GTID mode it is `previous_gtids ∪ added_gtids` of the tail (storage_core.hpp:313-331). The reader checks itself: in position mode the artificial ROTATE position must equal the stored position (reader_context.cpp:547-556, collector_context.cpp `process_artificial_rotate_event`), and every event's `next_event_position` must equal `position + event_size` (reader_context.cpp:915-920). These are built-in assertions that Antithesis can catch as non-zero exits.
- Property: across any number of fetch/pull runs, reconnects and SIGTERMs, concatenating the stored events gives no duplicated and no missing transaction compared with the source.
- Known violation, confirmed in code (ticket 14): in GTID mode, when the artificial ROTATE names a different binlog, `process_artificial_rotate_event` just calls `storage_->close_binlog()` and opens the new one (collector_context.cpp, "if names do not match"). No closing ROTATE is written, and binlogs skipped in between are never created. The GTID-mode branch has no position check (reader_context.cpp:566-573).

### S2. Each stored binlog is a byte-identical prefix of the source binlog (implicit; tested by MTR)
- This is not stated in the README. It is implied by "enhanced mysqlbinlog --read-from-remote-server" (README.md:3), and the MTR suite asserts it with `include/diff_with_storage_object.inc` (used in binsrv.test, pull_mode.test, recover.test) and `diff_files` in gtid_resume_after_rotation.test and storage_interrupted_binlog_creation.test.
- Exceptions: it does **not** hold in rewrite mode, which renumbers GTIDs and regenerates ROTATE/FDE/PREVIOUS_GTIDS (collector_context.cpp `rewrite_and_process_binlog_event`), so properties must branch on mode. Closed files should end with the source's ROTATE/STOP event (storage.cpp:207 comment: the close flush "guarantees the file-final ROTATE/STOP event lands on the backend").
- Known violations: ticket 14 (above); ticket 25: storage_metadata only holds `mode` and `encryption` (storage_core.cpp `load_metadata`/`save_metadata`), so nothing ties a storage to a source server, and a different or reset source is not detected.

### S3. A graceful stop (SIGINT/SIGTERM) or a fetch/pull error leaves storage in a consistent state (claimed)
- Claims: README.md:373 ("Any error ... results in immediate termination of the program making sure that storage is left in consistent state") and README.md:715 (signals). README.md:721 limits the claim: `kill -9` "may result in losing some progress". It says progress may be lost, not that the storage may be corrupted.
- Mechanism: only whole transactions are flushed. `flush_event_buffer_internal` writes up to `last_transaction_boundary_position_in_event_buffer_` (storage.cpp:268-298). On a timeout the incomplete transaction is discarded before the flush (collector_context.cpp case (2)), and `~storage` flushes complete transactions (storage.cpp:66-75).
- Properties: (a) after any termination, the next run opens the storage without error; (b) the tail size in metadata is always at a transaction boundary.
- Known violation, confirmed (ticket 18): `storage_core::write_event_block` writes data, increments `size`, then `save_binlog_metadata` (storage_core.cpp:~300-320). If that throws, the event buffer is not erased, and `~storage` → `flush_event_buffer` writes the same bytes again and adds to `size` a second time. On S3, `do_write_data_to_stream` appends to the local tmp file before uploading (s3_storage_backend.cpp:850-863), so a failed upload also leads to a duplicate append.

### S4. Crash recovery: only the tail binlog may be torn and it is recovered by truncation (claimed in code)
- Claim: storage_core.cpp:960-985. A size mismatch is tolerated only in the most recent binlog and only when the metadata size is ≤ the actual size ("recovered binlog file ... by truncating it"). Any other mismatch is a hard error.
- Claim: put_object is atomic. basic_storage_backend.hpp:49-51 says "a concurrent / post-crash reader either sees the previous bytes in full or the new bytes in full". The filesystem backend writes tmp, fsyncs, renames, then fsyncs the directory (filesystem_storage_backend.cpp:129-158). Leftover `*.tmp` files are removed at start (storage_core `remove_temporary_objects`).
- Property: kill -9 at any instant (Antithesis process kill) leaves the storage openable, and the next fetch makes it byte-identical to the source again.
- Known violations, confirmed in code:
  - Ticket 15. `open_new_binlog_file_internal` writes the magic, then the metadata, then the index (storage_core.cpp:~680-700). A crash in between leaves an unindexed object. `validate_binlog_index` then rejects it with "storage contains an object that is not referenced in the binlog index" (~storage_core.cpp:780).
  - Ticket 19. The stream write only calls `ofs_.flush()`, with "TODO ... use fsync()" (filesystem_storage_backend.cpp:~222-224), while metadata is fsynced. After a power loss metadata.size > file size, which hits "cannot perform recovery - size from the binlog metadata is bigger". This needs a node/host crash fault, not a process kill.
  - Ticket 13. An empty `binlog.index` with no binlogs passes validation (0 == 0), then `binlog_records_.front()` runs on an empty vector (storage_core.cpp:1001 and the assert at :220), which is UB.

### S5. purge_binlogs drops only a strict prefix and never the tail (claimed)
- Claim: README.md:374-376 ("remove every binlog file with sequence number less than or equal to ...", "Specifying binlog name which is currently being written to ... is not allowed"). Error text: "at least one binlog file must remain in the storage to preserve the resume position" (storage_core.cpp:356-361, README.md:441).
- Mechanism: `select_purge_victim_count` returns `distance(begin, target)+1` and refuses the last record (storage_core.cpp:331-366). The index rewrite is the commit point. File removal after it is best-effort and reported as status `warning` (storage_core.cpp:387-430; README.md:443).
- Properties: (a) the surviving index is a suffix of the old index; (b) the resume position and GTID set are unchanged by a purge; (c) a `warning` never means the index still lists a deleted file. The comment admits that leftover files "will trip the constructor's validators on next startup" (storage_core.cpp:391-396), so a partial-delete crash makes the storage unopenable. That is a documented weakness and a candidate property.
- Known violation, confirmed (ticket 9 and ticket 24): there is no cross-process lock (no flock/lockf/O_EXCL anywhere in src). A running fetch/pull keeps `binlog_records_` in memory and rewrites the whole index from it at the next rotation (`save_binlog_index`, storage_core.cpp:~790). That brings back purged names that no longer exist, and the next start then fails with "binlog index contains a reference to a non-existing object". Two fetch processes on one storage race the same way.

### S6. Encryption: data files are never written in plaintext when encryption is configured (claimed)
- Claim: README.md:673 says "all the binlog data files will be encrypted before written to the storage". Only CTR is allowed for data (encryption_config.cpp:35-38). File keys are never stored in plaintext (storage_core.cpp `decrypt_file_key` comment).
- Mechanism: encryption happens in `storage_core::write_data_to_stream` before the backend sees the bytes, so S3 tmp files under `fs_buffer_directory` also hold ciphertext. Appends use `create_with_offset` (cipher_context.cpp:429-).
- Weak spots, found by reading the code (no ticket):
  - (a) When a storage was created unencrypted and encryption is added later, `validate_metadata` accepts it and updates the metadata (storage_core.cpp:172-178). The **existing tail binlog keeps `encryption = nullopt` and keeps growing in plaintext**, because `write_event_block` uses the record's own encryption.
  - (b) When encryption is removed from the config of an encrypted storage, `validate_metadata` only errors if both sides have a value (storage_core.cpp `validate_metadata`). New binlogs are then created in plaintext (`generate_binlog_encryption_record` returns nullopt without an active KEK), while storage metadata still says `generic`.
  - Whether (a) and (b) are intended is an open question.
- Ticket 8 (CTR counter does not carry into the upper half of the IV) is consistent with `create_with_offset` loading only the low 64 bits of the IV as the counter (cipher_context.cpp:457-461). Property: decrypt(stored file) == the source bytes, for any checkpoint/append split.

### S7. GTID bookkeeping is consistent (implicit; partly asserted)
- Mechanism: new binlog `previous_gtids = get_gtids_unsafe()` (storage_core.cpp:691). `added_gtids += block_gtids` happens only for flushed, complete transactions (storage.cpp:160-166, storage_core `write_event_block`). The code asserts `purged_gtids_ == front().added_gtids` after load (storage_core.cpp:219-220).
- Properties: for consecutive records, `prev[i+1] == prev[i] ∪ added[i]` and the `added[i]` sets are pairwise disjoint. `added[i]` must equal the GTIDs actually present in the file bytes. `search_by_gtid_set` must return only files whose added sets cover the target (search_helpers.cpp:22-52). `list` metadata (size, timestamps, GTIDs) must match the bytes.
- Oddity to examine: on load, `purged_gtids_` is set to the first record's **added** GTIDs, not its `previous_gtids` (storage_core.cpp:999-1004). It only matters while the storage is empty, but the name suggests a semantic mismatch.

### S8. Checkpointing makes data durable at size/interval boundaries (claimed)
- Claim: README.md:619/626 says `checkpoint_size` / `checkpoint_interval` "flush its internal buffers and write received binlog data permanently". This is implemented in storage.cpp:175-200 (ticket 2 fixed). Ticket 17 (the S3 backend re-uploads the full object at every checkpoint) is documented behaviour (README.md:670-671, `upload_tmp_stream_internal`) and is not a safety violation.
- Property: after a kill -9, the stored tail is never behind the last completed checkpoint by more than one checkpoint's worth of whole transactions. On the filesystem backend this holds for process kill but not for host crash (S4/ticket 19).

### S9. Replication source (pull) streams stored bytes faithfully (claimed in code and tests)
- Claim: sender_context.cpp:81-140 gives the event-sequence contract (artificial ROTATE, FDE byte-for-byte or artificial FDE from a position, then stored events). tests/PROPERTY_TESTS.md:213 says "every stored byte is fetched from storage exactly once" and that a partial last event is held back and reported as EOF.
- Property: a dump from pull, with artificial events stripped, is byte-identical to the stored files and therefore (by S2) to the source.
- Known violations: tickets 10 (events of 16 MB or more), 20 (MySQL replicas cannot replicate), 26/PBS-96 (start from position on empty storage); ticket 14 propagates missing ROTATEs to clients.

## Areas examined
README.md (modes, purge, encryption, resume, termination); src/binsrv/storage.cpp, storage_core.cpp/.hpp, filesystem_storage_backend.cpp, s3_storage_backend.cpp (stream/put/resize), basic_storage_backend.hpp; src/operations/collector_context.cpp, fetch_operation.cpp, pull_operation.cpp, search_helpers.cpp, sender_context.cpp (contract comments); src/binsrv/events/reader_context.cpp (preamble and position validation); opensslpp/cipher_context.cpp (offset); encryption_config.cpp; tests/PROPERTY_TESTS.md; the MTR test list and diff_with_storage_object.inc; ticket snapshot overview plus the full text of tickets 14/15.

## Assumptions
- "Consistent state" in README.md:373/715 means "openable by the next run and resumable without loss or duplication".
- The byte-identity oracle applies only to non-rewrite modes.
- Tickets 9, 13, 18, 19, 24, 25 were confirmed only by reading the code path, not by running them.

## Open questions
- [needs-human] Is plaintext growth of a pre-existing tail after enabling encryption (S6a), and plaintext new files after removing the encryption config (S6b), intended?
- [needs-human] Is `purged_gtids_ = front().added_gtids` (storage_core.cpp:1003) intended, or should it be `previous_gtids`?
- [needs-human] Is "byte-identical to the source" an official guarantee, or only an MTR convention? It decides whether ticket 14 is a safety violation or a fidelity gap.
- [unvalidated] The leftover-objects-after-partial-purge path (S5c) has no ticket. It needs a fault during `remove_objects` to confirm.
