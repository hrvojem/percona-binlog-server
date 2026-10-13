# collector-memory-bounded-by-checkpoint — collector buffer memory is bounded by the checkpoint size plus the open transaction

## What led here

Resource focus: memory growth under sustained load. `storage` buffers received events in RAM and writes them only at checkpoints, on close, on a timeout disconnect, or in `~storage` (sut-analysis, Architecture step 5).

## Code paths

- `src/binsrv/storage.cpp:143-202` `write_event`: every event is appended to `event_buffer_` (`:150-151`). A flush happens only when a size or interval checkpoint is due (`:187-200`) **and** a transaction boundary exists in the buffer (`has_event_data_to_flush`).
- `:276-302` `flush_event_buffer_internal`: writes up to the last boundary and erases that prefix from the vector (a memmove of the remainder each time).
- `:204-215` `close_binlog`: the only other periodic flush, at rotation (followed by `shrink_to_fit`).
- With **no** `checkpoint_size`/`checkpoint_interval` (the default; README:619-626 says "if not set … disabled"), `event_buffer_` holds the entire active binlog until the source rotates: up to `max_binlog_size` (1 GiB by default on MySQL), plus the overflow of the last transaction.
- With checkpointing on, the buffer is bounded by about `checkpoint_size + size of the open transaction`, because the interval trigger fires only on the next event. A single huge transaction (bulk load, `LOAD DATA`, large `DELETE`) is held completely.
- If `write_event_block` throws, the buffer is not erased (`:286-296`), but the collector exits, so this does not accumulate.

## Failure scenario

PBS runs in a memory-limited container. A transaction larger than the remaining memory, or a default-config binlog that grows towards 1 GiB, gets PBS OOM-killed. On restart, resume asks the source for the same data, the transaction arrives again, and PBS is killed again: a crash loop with no progress (an LG5-style wedge caused by memory). A kill also loses everything in the buffer. With the default config that is up to a whole binlog, which is more than README:721's "lose some progress" may suggest.

## Oracle

- SUT-side `ALWAYS(std::size(event_buffer_) <= checkpoint_size_bytes_ + bytes since last boundary + one event)` after each `write_event` when size checkpointing is enabled. This is cheap and catches any regression in the checkpoint logic (ticket 2 was "checkpoint settings ignored").
- Workload: run PBS with a container memory limit (for example 512 MiB), using a mix of configs with and without checkpointing, while the source runs transactions of varying size (up to about 100 MiB). `Always`: if the source's largest transaction is below a documented bound X, PBS is never OOM-killed. `Sometimes(PBS reached EOF after a ≥ 64 MiB transaction)`.
- Without checkpointing: `Sometimes(pull served data from the active binlog before rotation)`. This is expected to fail today (LG3) and documents the behaviour.

## Instrumentation suggestions (missing)

- The `ALWAYS` above in `storage::write_event`.
- `SOMETIMES(event_buffer_ size > 64 MiB)` as an exploration hint for large transactions.
- `SOMETIMES(event_buffer_.capacity() > 2 * checkpoint_size_bytes_ after a flush)` — **missing**; shows the retained-capacity state (memory not returned until rotation) was reached.

## Investigation outcome (2026-10-13)

- Ticket 2 ("checkpoint settings ignored") is fixed in this tree: the `storage` constructor loads `checkpoint_size` / `checkpoint_interval` (`src/binsrv/storage.cpp:49-65`). The checkpoint-driven bound is therefore live code.
- **Resident memory is larger than `size(event_buffer_)`.** `event_buffer_` is a `std::vector<std::byte>` (`storage.hpp:146-147`). `flush_event_buffer_internal` erases the flushed prefix (`storage.cpp:291-296`), but `erase` keeps the capacity. `shrink_to_fit` runs only in `close_binlog` (`storage.cpp:204-215`). After one large transaction, the peak capacity (up to about 2x the peak size, because of geometric growth) stays allocated until the source rotates. The SUT-side `ALWAYS` on `size()` stays as written, because it catches checkpoint-logic regressions. The workload's OOM bound must allow for about 2x the largest buffered amount, plus libmysqlclient's packet buffer for the largest single event.
- A transaction with a declared length above 4 GiB throws (`reader_context.cpp:951-956`), so the open-transaction term is effectively capped below that. At that point PBS exits instead of growing further, which is a separate fail-stop.
- S3 does not add a RAM copy: backend writes go through a temp file in `fs_buffer_directory` (`s3_storage_backend.cpp:330-340,556-590`). The RAM bound is the same for both backends.
- README documents that checkpointing is off when unset (README:619-626) and describes the S3 cost trade-off (README:670). It says nothing about memory. README:721 ("may result in losing some progress") does not quantify the loss.
- Property unchanged. The memory-limit question is narrowed to a harness parameter that depends on a platform check.

## Open questions

- Is the whole-binlog RAM buffering of the default config intended, or should checkpointing default to on? (needs human input) If it is intended, the default-config memory bound is `max_binlog_size` (times about 2 for vector capacity) and needs documenting. If not, the `ALWAYS` should apply in all configs.
- What memory limit will the Antithesis PBS container have? `(partial: harness parameter; the useful range is set by checkpoint_size + largest test transaction, times about 2 for vector capacity; blocked on the catalog-wide check whether Antithesis honours compose mem_limit)`

### Investigation Log

#### Is the whole-binlog RAM buffering of the default config intended?

- Examined: `storage.cpp:49-65,143-224,276-302`, `storage.hpp:146-172`, README:619-626, :670, :721, `.tickets-snapshot.txt` ticket 2 (its "user impact" describes the whole-binlog buffering as the *bug's* effect when the settings were ignored).
- Found: with neither setting, `write_event` never flushes, and only `close_binlog` / a timeout disconnect / `~storage` do. The default is documented as "disabled", but the memory consequence is not. Ticket 2 treats whole-binlog buffering as harmful when it happened by accident, which suggests (but does not settle) that the owners do not intend it as a default.
- Not found: a stated default policy or a memory-sizing guideline.
- Conclusion: `(needs human input)`.

#### What memory limit will the Antithesis PBS container have?

- Examined: `deployment-topology.md:120` (a `mem_limit` variant compose is planned) and `:269` ([needs-check] whether Antithesis honours `mem_limit`), plus the buffer/capacity analysis above.
- Found: the limit is a harness design choice. The meaningful value is about 2 x (checkpoint_size + largest generated transaction) plus a fixed overhead, so the workload's transaction sizes and the limit must be chosen together.
- Not found: confirmation that the platform enforces `mem_limit`.
- Conclusion: `(partial: ...)`. It resolves once the topology-level `mem_limit` check is answered.

## Evaluation refinements (2026-10-13)

- **G3 split.** The durability half moved to `checkpoint-bounds-unpersisted-data`. That property owns the checkpoint/durability SUT check (`ALWAYS("unflushed complete-transaction bytes stay below checkpoint_size")`, the backlog form) and the ticket 2 regression role. This property keeps only the memory checks: the buffer-size `ALWAYS` (now named "collector event buffer stays within checkpoint_size plus the open transaction plus one event", distinct from the backlog message at the same site), the capacity `SOMETIMES` and the OOM check.
- **R27 applied.** When the platform does not honour compose `mem_limit`, the safety half is checked by the SUT `ALWAYS` plus workload VmRSS sampling of the PBS process against ≈ 2 × (checkpoint_size + largest transaction). An OOM SIGKILL is told apart from a harness SIGKILL using the kill driver's log of every pid it signalled (topology change T15).
- Open questions unchanged.
