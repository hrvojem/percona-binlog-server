---
sut_path: /Users/hrvoje/work/percona-binlog-server
commit: 99de68207c84f83e0d477ebe63d2f531facbb954
updated: 2026-10-13
external_references:
  - path: https://claude.ai/artifact/9om6fiPd64QzVXbEWEtDT1
    why: QA bug-ticket list for binlog_server (local snapshot antithesis/scratchbook/.tickets-snapshot.txt); open tickets used as leads, each validated against the code before use
---

# Evaluation lens 1: Antithesis Fit

Scope: all 60 properties in `property-catalog.md`, read against `sut-analysis.md`, `deployment-topology.md` (templates `fetch_fs`, `pull_fs`, `s3`, `contention`; `failover` and `upgrade` deferred) and the evidence files where depth was needed. The question for each property is whether it needs a state space that deterministic tests (MTR, Boost unit, Hegel, the `extra/qa` Python harnesses) cannot reach, and whether its assertion type matches how Antithesis will actually exercise it.

Overall: the catalog's centre of gravity (crash and fault consistency of multi-object storage updates, resume across kills and reconnects, pull serving during rotation and checkpoints) is squarely in Antithesis's sweet spot. The problems are at the edges: a cluster of known, deterministically triggered failures that will dominate the first runs, a set of bounded-liveness `Always` checks that ungated faults will trip, one liveness property whose main assertion is a `Sometimes`, several `Sometimes` anchors that no planned template can satisfy, and roughly eight properties that are unit/MTR territory.

## Catalog-wide findings

### F1. Known, deterministically triggered failures will dominate early runs and blind later checks in the same timeline

**Properties:** `kill-anywhere-storage-reopens`, `gtid-resume-preserves-source-rotations`, `inprocess-reconnect-resumes-without-dup-or-gap`, `resume-continuity-guard-never-trips`, `sender-file-switch-emits-artificial-rotate`, `blocking-dump-ahead-of-flushed-position-eventually-served`, `rewrite-pitr-by-datetime-matches-source`, `ctr-keystream-not-reused-across-truncation`, `large-event-served-or-rejected-cleanly`, `interrupted-purge-leaves-openable-storage` (warning/retry), `stored-rotate-or-stop-is-last-event`, `source-crash-restart-resume-is-faithful`, plus four not labelled as failing today: `dump-session-ends-with-eof-or-err-packet`, `query-modes-never-write-storage`, `encrypted-storage-no-plaintext-at-rest`, `rewrite-toggle-between-runs-keeps-storage-resumable`.

**Concern.** About a third of the catalog fires on today's code. For most of these the trigger is an ordinary, frequent workload action rather than a rare interleaving:

- Ticket 14 fires "whenever a caught-up PBS reconnects after a rotation" (catalog, `gtid-resume-preserves-source-rotations`). That happens in every GTID-mode timeline with `FLUSH BINARY LOGS` during a disconnect.
- Lead (a) fires on every graceful source `SHUTDOWN` while pull holds unflushed complete transactions. Without checkpointing, pull nearly always holds some. The restart wrapper issues `SHUTDOWN`, so `pull_fs` hits it routinely (`inprocess-reconnect…`, `resume-continuity-guard…`, the case (3) `UNREACHABLE` in `pull-chain-converges-after-heal`).
- N5 (`sender-file-switch…`): without checkpointing the window "spans the whole life of N+1" (catalog), so any blocking client caught up at a rotation hits it. In the default config this is close to deterministic.
- `dump-session-ends-with-eof-or-err-packet`: the planned `UNREACHABLE` sits at `network_service.cpp:312`. I checked the code: every `sender_ctx.get_event()` false return `co_return`s there without an ERR packet. That includes the "position past flushed size" branch (`storage_core.cpp:513-515`, N12), which `parallel_driver_pull_reader.py` requests on purpose ("including ahead-of-flushed and empty-storage cases"). The property fires today on its own listed cases, but the catalog does not mark it as failing today.
- Lead (c) (`rewrite-pitr…`) fires on any catch-up lag of 1 s or more. Lead (d) (`ctr-keystream…`) fires after any rewrite-mode kill with small checkpoints. Ticket 10 fires for any event of 16,777,211 bytes or more. N9 (`encrypted-storage-no-plaintext-at-rest`) and lead (e) (`query-modes-never-write-storage`) fire every time the encryption-enabled config variant meets an unencrypted storage.

Some of these are **absorbing** states. Once a timeline hits ticket 15 (unindexed object), the rewrite-toggle wedge, the `rewrite-checksum-none-fail-stop` crash loop, or N3, every later writer start in that timeline fails. The convergence oracles (`stored-transactions-exactly-once-across-resumes`, `eventually_converge.py`) can then never be evaluated on that branch. Antithesis keeps exploring the branch, but every new finding on it is masked.

**Why this matters for fit.** Re-finding a deterministic defect costs exploration budget and adds nothing, and the masking hides the interleaving bugs the catalog was built to find. Several of these already have deterministic reproductions: an MTR test for ticket 15 (`storage_interrupted_binlog_creation.test`), QA repros for tickets 14, 18, 24 and 25, and `s3_fault_sweep.py`.

**Suggested action.**
1. Give each known defect a workload knob that avoids its trigger, chosen per timeline from Antithesis randomness and defaulting to "avoid" until a fix lands. Examples: no `SHUTDOWN` or `KILL QUERY` while pull is mid-buffer (lead (a)); checkpointing always on in `pull_fs` (narrows N5 to the interleaving window); no requests ahead of the flushed position (N12 and ticket 26); never select the encryption-added query config (lead (e)).
2. Run the triggers in a separate `known_defects` template, or behind a flag, so their firings don't swamp the in-contract templates. This is the same reasoning the topology already applies to `contention`.
3. After a validator rejection, let the harness reset to a fresh storage. Record the reset with `Sometimes("storage reset after known wedge")` so the timeline keeps producing signal.
4. Mark `dump-session-ends-with-eof-or-err-packet` as expected to fail today.

### F2. Bounded-liveness `Always` checks are not gated against the faults that legitimately stretch the bound

**Properties:** `sigterm-honoured-within-bound`, `stale-sessions-reclaimed-within-bound` (both pre-auth and idle-dump halves); the trailing `Always` in `pull-chain-converges-after-heal` evidence ("SIGTERM at the end of the quiet period completes within read_timeout + longest S3 PUT + delta"). `stalled-dump-client-isolated` gets this right: it gates its `Always` on fault-free storage and source links.

**Concern.** These properties use a time-bounded `Always`, which is the right technique, but they evaluate it while faults are active. All sessions, timers and the signal handler share one io thread with synchronous storage I/O. The time it takes to close a session or exit after SIGTERM therefore grows with any `pbs` node hang, node throttling, CPU modulation, or S3 latency. The catalog's own "Antithesis Angle" for `sigterm-honoured-within-bound` lists S3 latency, node throttling and pause as the exploration tools. Those are exactly the faults that push the latency past `read_timeout + 1 + slack`. The evidence file excludes only timelines where SIGTERM was escalated to SIGKILL (`sigterm-honoured-within-bound.md:117`).

`stale-sessions-reclaimed-within-bound` measures "close time" at the attacker client, by when it sees EOF or RST. Antithesis network faults on `pbs ↔ workload` (packet hold, drop) delay or swallow that RST, so the client-observed close can exceed B even when PBS closed on time. The SUT-side `ALWAYS(pre-auth session duration ≤ B + slack)` has the same problem under a `pbs` node hang, because wall time advances while the container is frozen.

**Result.** These checks either fire on injected faults (noise) or need slack so large that they stop testing anything. Antithesis can't tell a real io-thread starvation from a node hang it injected itself.

**Suggested action.** Evaluate each bounded `Always` only inside an `ANTITHESIS_STOP_FAULTS` window. Start the measured action (SIGTERM, the attacker's connect, the flush that should wake stale sessions) after the quiet period begins, and keep the window longer than the bound. To study S3 latency, have the workload inject it deterministically through `s3_fault_proxy` stall rules during the quiet period, so the slack can include the known stall. Keep the unbounded "under arbitrary faults" variant as a `Sometimes`/`Reachable` exploration marker, not an `Always`. For `stale-sessions…`, measure close time on the `pbs` side (`ss` or `/proc/<pid>/fd` from a `pbs` helper), not from the client.

### F3. Liveness checked with `Sometimes` cannot catch non-convergence

**Property:** `pull-chain-converges-after-heal`; to a lesser degree `gtid-failover-resume-progresses`.

**Concern.** The main assertion is `Sometimes(converged within T after a heal)`. The catalog's rationale is that "convergence is eventual progress, so it is `Sometimes`". In Antithesis, a `Sometimes` passes as soon as one timeline converges once. The failures this property exists to catch go unreported whenever any other timeline converged: the hypothesised stall → drop → bigger re-upload spiral, a client that never catches up, and a reconnect that wedges. The evidence file already describes a bounded form ("checked after ANTITHESIS_STOP_FAULTS mid-run"; line 86 has a once-per-quiet-period `Always`), but only the narrow N7 "pull did not exit" sub-check got promoted to `Always`. Client catch-up stayed `Sometimes`.

**Suggested action.** Make the main check `Always("converged within T after a heal")`, evaluated once per quiet period with T as defined in the catalog (read_timeout + idle_time + 500 ms + write time + margin). Keep `Sometimes(quiet period reached with ≥ 1 live blocking client)` as the vacuity guard. If T can't be stated because of the open question "Is any upper bound on catch-up time acceptable?", use a generous parametric T and report it in the details. Apply the same treatment to the clean-failover half of `gtid-failover-resume-progresses` once its template exists: rewrite mode is expected to progress, so that half can be an `Always` after the quiet period.

### F4. Several `Sometimes` anchors depend on scenarios that no planned template schedules, so they will report as failing

**Concern.** An unsatisfied `Sometimes` shows up as a failed property. These anchors need scenarios the topology does not run:

| Property | Anchor | Why no template reaches it |
|---|---|---|
| `stale-index-commit-never-wedges-storage` | `Sometimes(two overlapping purges both reported success or warning)` | The property's in-contract value now "rests on the concurrent-purges variant". `fetch_fs` runs purge only as `serial_driver_purge.py` under the writer lock, so purges never overlap. Only `contention` runs `parallel_driver_purge.py`, and there purges also race writers. No template runs purge+purge without a writer. |
| `dump-preamble-matches-request` | `Sometimes` for an empty-name request "within one second of a pull restart that followed a purge (pull stopped during the purge)" | `pull_fs` has no purge command (topology table). |
| `dump-session-ends-with-eof-or-err-packet` | Fault requirement "a purge task" | Same: there is no purge in `pull_fs`. |
| `distinct-storages-shared-server-id-stay-consistent` | `Sometimes(same-server_id eviction observed)` | Needs two PBS writers on **different** storages against one source at the same time. `fetch_fs` serialises writers under one lock on one storage, `pull_fs` runs one pull (the chained fetch dumps from `pbs:3307`, not from the source), and `contention` uses one storage. |
| `pull-client-version-matrix` | `Sometimes(...)` per client version V ∈ {8.0, 8.4, 9.7} | The image ships only the `mysqlbinlog` that matches `MYSQL_VERSION` (topology, `runtime-base`). Each launch can satisfy only one V; anchors for the other versions never fire. |

**Suggested action.** For each anchor, either add the scenario to a template (a purge+purge step with no writer, a stop-pull/purge/restart step in `pull_fs`, a second-writer-on-another-storage driver in `contention`) or scope the anchor to the template that can satisfy it. Make the `pull-client-version-matrix` anchor per launch (only the shipped V), or ship all three client tarballs.

### F5. Properties that are really unit, Hegel or MTR territory

Each of these is reproducible from fixed inputs, and Antithesis adds little beyond what a deterministic test gives. They consume workload actions and triage attention.

| Property | Deterministic core | Remaining Antithesis value | Suggested disposition |
|---|---|---|---|
| `pull-client-version-matrix` | Handshake allowlist; the evidence file calls it "Low by itself (deterministic)" | Smoke check that the pull properties are not starved | Workload setup assertion (`setup_complete` precondition), not a catalog property |
| `storage-format-version-mismatch-handled` | Legacy fixture plus binary choice; the catalog says "Mostly deterministic" | S3 GET faults during `list` on a legacy storage (already owned by `query-success-never-omits-middle-binlog`) | Move to "Candidates not cataloged" (MTR upgrade test) |
| `rewrite-toggle-between-runs-keeps-storage-resumable` | One config edit produces a wedge on every restart; "none for the base case" | Toggle combined with a kill just after file creation | MTR for the wedge; keep at most a `Sometimes` exploration marker |
| `query-modes-never-write-storage` | Encryption-added query config always rewrites `metadata.json` (`storage_core.cpp:174-178`, verified: no construction-mode check) | Collision with a live writer on `metadata.json.tmp` | MTR or unit test for the write; fold the collision into `other-process-startup-spares-live-writer` |
| `encrypted-storage-no-plaintext-at-rest` (N9 half) | Enabling encryption on an existing storage keeps the plaintext tail; "no network faults needed" | Kill timing around `save_metadata` (`:177`) and rotation | MTR for N9; keep only the "new file has an envelope" `ALWAYS` in Antithesis |
| `large-event-served-or-rejected-cleanly` | Frame-limit boundary at exactly 16,777,211 bytes (ticket 10) | N×event memory under concurrent re-fetch, but only with an unconfirmed `mem_limit` | MTR for the boundary sizes. Large events are also costly in simulation (S3 whole-object re-upload per checkpoint), so keep only a low-probability "occasional big event" knob in the load generator. |
| `tagged-gtid-resume-exact` | Encode/decode round-trip (`AlwaysOrUnreachable(decode(encode(g)) == g)`), already covered by Hegel and `event_variety_test.py` | Set-shape variety comes from the load generator, not timing | Fold into `stored-transactions-exactly-once-across-resumes` as a load-generator dimension (tagged GTIDs on ≥ 8.4) |
| `source-session-tls-when-configured` | `MYSQL*` rebuilt from config on every attempt | Low; the reconnect count doesn't matter | Unit or MTR test of the reconnect path; at most a cheap `Always` in an existing driver |
| `collector-memory-bounded-by-checkpoint` | Buffer bound is a function of the event sequence; ticket 2 is fixed | OOM crash loop, but only with an unconfirmed `mem_limit` | Hegel model of the storage facade; drop or park until `mem_limit` is confirmed |
| `search-by-timestamp-includes-all-early-files` | Already flagged by the catalog (`SET TIMESTAMP` variant is MTR) | Clock-jitter variant | Agree with the catalog; keep at P2 or move out |
| `misaligned-dump-position-rejected-cleanly` (direct case) | A hostile request at a random offset is input fuzzing (`listener_fuzz_test.py` already exists); in debug builds the `indexed_event_block.cpp:43` assert aborts the **whole** pull | Misaligned positions arising naturally from earlier faults | Keep the "naturally arising" variant. Move the hostile-request driver out of `pull_fs`: each firing kills the collector and every other session, which starves every other pull property in that timeline (F1 masking). Use a dedicated template or the release-build ticket-7 launch. |
| `rs-unauthenticated-session-never-served` | Linear per-session handshake; the catalog has "high that it holds" | Interleaving on the single io thread | Cheap `Always` inside `parallel_driver_listener_abuse.py`; no exploration budget needed |

## Property-specific findings

### F6. `source-swap-never-silently-accepted` is P0 but has weak Antithesis fit

The defect is a design gap: no source identity is recorded (`storage_metadata.hpp:35-37`), and ticket 25 has a QA reproduction (`source_change_test.py`). Whether a swap "fails closed" or is "silently accepted" depends only on whether the new server's file length or event boundary happens to line up with the stored offset. That is a property of the data, not of the interleaving. The independent-server swap needs the deferred `failover` template (VIP plus a second `mysqld`). The RESET variant in `fetch_fs` "makes ticket 25 fire constantly, which may swamp other signal" (topology open question). Antithesis will mostly re-confirm a known gap, so P0 overstates how much exploration it deserves. **Suggested action:** keep the property, but treat it as a known-gap tracker. Run it only in the deferred `failover` template, or in a `known_defects` template (F1), and weight its exploration below the P0 crash/consistency group.

### F7. Underestimated: `source-crash-restart-resume-is-faithful` (P1)

Source SIGKILL at a random instant, through the restart wrapper with `sync_binlog=1`, is a natively random, high-fit fault. It reaches the unterminated-binlog branch (`collector_context.cpp:514-553`), which has no byte-level test, and lead (b) (the ticket-6 path with a debug abort at `storage.cpp:138`) using only planned faults. It needs no custom shim and no deferred template, unlike `source-swap-never-silently-accepted` (P0). **Suggested action:** consider P0, or at least rank it above F6 when allocating budget.

### F8. Underestimated: the stale-PUT replay variant of `stored-state-never-regresses`

The catalog keeps this variant at P2 behind a `SOMETIMES(S3 PutObject succeeded after an SDK retry)` guard. Antithesis's partition and bad-node faults can hold packets "until the fault expires then deliver at once" (faults reference). That is the one fault shape that makes a delayed earlier PutObject land after a later one, and neither `s3_fault_proxy` nor any QA harness can produce it. Because the proxy runs in the `s3` container, packets held on the `pbs ↔ s3` link reach the proxy late and are forwarded to Versity late, so the delivery path exists. Reachability still depends on two open items: whether CRT retries a held request, and whether Versity commits a late-arriving body. **Suggested action:** keep the SDK-retry `SOMETIMES` as the vacuity guard, and make sure the `s3` template enables packet-hold partitions on `pbs ↔ s3`. This variant is unique to Antithesis, so it should not be the first one cut.

### F9. `failed-flush-never-reappends-data`: the dominant trigger is a deterministic proxy rule

The catalog notes that roughly six consecutive failures exhaust CRT retries, so the `s3_fault_proxy` `.json` 5xx rule is "likely needed". With that rule active, ticket 18 reproduces in almost every `s3` timeline that runs it, which `s3_fault_sweep.py` already does. The interleaving value lies in the natively triggered and combined variants: a partition that starts after the data PUT, an ambiguous metadata PUT, ENOSPC freed by ballast before the destructor, and the fixed-width position-mode silent skip. **Suggested action:** until ticket 18 is fixed, apply the proxy `.json` rule at low probability, so the native and combined variants get budget and the rest of the `s3` template is not masked (F1).

### F10. `sender-file-switch-emits-artificial-rotate`: split default-config and checkpointed variants

In the default config (no checkpointing) N5 is effectively deterministic for any caught-up blocking client at a rotation, so MTR could reproduce it. Antithesis earns its keep in the checkpointed configuration, where the window between N+1's creation and its first flush is short and depends on the 500 ms poll and thread pausing, and in the checksum-change and A2 (STOP / no terminator) variants. **Suggested action:** add an MTR reproduction for the default-config case. In Antithesis, weight the `checkpoint_size`/`checkpoint_interval`-on configurations for this property and report the configuration in the assertion details.

## Passes (fit confirmed)

- **Storage-integrity core:** `stored-binlog-byte-prefix-of-source`, `metadata-size-at-transaction-boundary`, `kill-anywhere-storage-reopens`, `graceful-exit-leaves-flushed-storage`, `binlog-metadata-matches-stored-bytes`, `interrupted-purge-leaves-openable-storage`, `stored-rotate-or-stop-is-last-event`. Their violations need kills or faults inside multi-object update windows. Existing tests crash only between operations (the Hegel model) or with hand-made states (MTR). This is the strongest Antithesis fit in the catalog. The `Always` and `Unreachable` types match.
- **Resume:** `stored-transactions-exactly-once-across-resumes` (an `Always` after a final fault-free fetch is the right shape), `repeat-fetch-on-caught-up-storage-is-noop` (kill during reopen; correctly kept at P2 as a regression guard), `rewrite-binlog-chain-valid-across-crashes`, `rewrite-storage-transaction-sequence-matches-source`.
- **Pull serving:** `pull-served-stream-self-consistent`, `pull-chain-prefix-consistency`, `chained-fetch-from-pull-reproduces-upstream`, `blocking-dump-before-first-flush-eventually-served` (a `Sometimes` per state, paired with an `Always(no PBS-initiated close)`, is correct for unbounded-latency delivery), `dump-preamble-matches-request` apart from the F4 anchor.
- **Multi-process:** `second-writer-on-storage-fails-closed`, `other-process-startup-spares-live-writer`, `cli-json-envelope-and-exit-code-agree` (N4 is a genuine readdir/stat race), `query-success-never-omits-middle-binlog` (a partition outlasting retries on one key is native-fault reachable).
- **Encryption:** `encrypted-storage-decrypts-to-source` and `mixed-encryption-storage-served-correctly`, both never crash-tested.
- **Liveness and resource:** `stalled-dump-client-isolated` (correctly gated on fault-free links), `s3-crash-leftovers-bounded`, and `listener-survives-fd-exhaustion` (the EMFILE race between the collector and `accept()` is real, though it depends on `ulimits` being honoured).
- **Reachability:** `recovery-paths-exercised` is the right complement. It makes vacuous passes visible, and its conditional rows are omitted rather than left always-failing, which is correct.
- **Other source-side:** `source-auth-survives-auth-cache-flush` (cache-warming interleavings are timing-dependent), `gtid-failover-no-lost-transaction-merge` (asymmetric partition creating "PBS ahead of promoted replica"; deferred but high fit), `source-upgrade-resume-continuity`, `distinct-storages-shared-server-id-stay-consistent` (good amplifier of resume paths once F4 is fixed), `rewrite-checksum-none-fail-stop` (medium fit; its crash loop is absorbing, see F1).
- No property needs node termination. Workload-driven SIGKILL is an adequate substitute because the page cache survives either way. Ticket 19 is correctly excluded.

## Uncertainties

- **CRT retry and timeout behaviour** (SDK 1.11.774) is unverified. It decides whether native network faults alone reach ticket 18, N3, N7 and the stale-PUT replay (F8, F9), and therefore whether those properties are native-fault or proxy-driven. A short local run with Toxiproxy on the S3 link would settle it.
- **Frequency of N5 under checkpointing:** I could not estimate it without a run. It decides whether F10's checkpointed variant is reachable without thread pausing.
- **Compose `mem_limit`, `ulimits` and `tmpfs: size=`:** whether Antithesis honours them decides whether `collector-memory-bounded-by-checkpoint`, the resource half of `large-event…`, and `listener-survives-fd-exhaustion` have any Antithesis value.
- **Whether `workload` is itself subject to node hang and network faults:** this affects F2 (client-observed close times) and `stalled-dump-client-isolated`.
- **`source-swap-never-silently-accepted`, RESET variant:** I did not verify how MySQL treats a GTID dump whose set contains the server's **own** UUID's GTIDs after `RESET BINARY LOGS AND GTIDS`. MySQL's "replica has more GTIDs than source" check is per own UUID, so the RESET variant may fail closed deterministically in GTID mode. If it does, that strengthens F6.
