# Implementation plan — realtime-data-compression (v1)

**Status:** draft • 2026-05-11
**Feature:** opt-in, transparent, server-side compression of eligible STRING values using trained ZSTD dictionaries
**Design of record:** [`design/detailed-design.md`](../design/detailed-design.md), [`idea-honing.md`](../idea-honing.md)
**Walkthrough audit trail:** [`DESIGN_TODO.md`](../DESIGN_TODO.md) — 31/31 review threads addressed
**Target release:** Valkey 9.0 (tentative)

---

## 1. Goals of this plan

1. Sequence the work so **two engineers can implement in parallel** with minimal merge friction.
2. Define **subsystem interface contracts** up front — nothing downstream starts before Phase 0 lands the skeleton.
3. Map every design requirement (R2.*) to a concrete subsystem + owner so nothing falls through the cracks.
4. Name the merge-gate deliverables (COW audit, perf numbers, §7.5 benchmark runs) explicitly rather than "we'll test it later".

## 2. Team & ownership

| Engineer | Role | Subsystems |
|---|---|---|
| **@ikolomi** | Lead engineer | S2, S5 (concurrency-critical, COW audit owner) |
| **@GilboaAWS** | Co-owner | S1, S3, S4, S6, S7 |

Code reviews: each engineer reviews the other's PRs. Design divergences from `detailed-design.md` require agreement from both.

## 3. Subsystems

Each subsystem is owned end-to-end — code + unit tests ship in the same PR. Design-requirement cross-references in the **R-refs** column.

| # | Subsystem | Files (from §4.1) | R-refs | Owner | Effort |
|---|---|---|---|---|---|
| **S1** | Dictionary lifecycle (registry, training, promotion, retirement, drift-retrain, main-thread iteration + sample copy, bio train job) | `src/compression_registry.c`, `src/compression_train.c` | R2.3.1–R2.3.12, R2.11.4 | @GilboaAWS | L |
| **S2** | Compression hot path (eligibility predicate, worker pool, encoder, decoder, write/read hooks, sweep pacing) | `src/compression.c`/`.h`, `src/compression_workers.c`, `src/compression_header.c` + `db.c` hook points | R2.1, R2.2, R2.4, R2.5, R2.11 | @ikolomi | L |
| **S3** | Persistence — RDB format extension + AOF behavior + full-sync RDB uncompressed | `src/rdb.c`, `src/aof.c`, `src/replication.c` (full-sync flag) | R2.6.1–R2.6.9, R2.7 | @GilboaAWS | M |
| **S4** | Observability & admin (`INFO compression`, `COMPRESSION` command family, latency monitor, telemetry) | `src/compression.c` (`infoCompression`), `src/commands/compression-*.json` | R2.8, R2.9, R2.10 | @GilboaAWS | S |
| **S5** | Benchmark suite + perf validation (extend `valkey-benchmark`, canonical scenarios, perf dashboard, regression harness) | `src/valkey-benchmark.c` extensions, `tests/perf/` | §7.3, §7.5 | @ikolomi | L |
| **S6** | Integration tests — COW invariant merge-blocker, end-to-end, replication, RDB round-trip, soak | `tests/unit/type/compression.tcl`, `tests/unit/compression-cow-invariant.tcl`, `tests/integration/compression/` | §7.2 | @GilboaAWS | M |
| **S7** | Dev infra — build flags (`USE_ZSTD`), CI jobs, unit test scaffolding (gtest), `deps/zstd/` vendored bump if needed | `src/Makefile`, `.github/workflows/*`, `deps/zstd/` | §7.1 | @GilboaAWS | S |

**Effort legend:** S ≤ 1 week · M ≈ 2–3 weeks · L ≈ 4+ weeks of focused work.

## 4. Interface contracts (Phase 0 deliverable)

All inter-subsystem interactions go through these contracts. Phase 0 lands **compilable skeleton headers** with stubs returning sensible defaults (NULL dicts, pass-through encoders). No subsystem builds against another's internal state.

### 4.1 `compression.h` — public API surface (owned: S2)

From §4.3 — the following symbols are shared with S3, S4, S6:

```c
/* lifecycle */
void compressionInit(void);
void compressionCron(void);
int  compressionToggle(int enabled, sds *err);   /* R2.1 master switch */

/* hot path (main thread only) */
int    compressionIsEligible(const robj *o, const sds key);        /* R2.2 */
robj  *objectGetUncompressedView(const robj *o);                   /* R2.5 */
void   compressionEnqueueCandidate(const sds key, robj *o);        /* R2.4 */

/* cross-thread */
void   compressionAfterSleep(void);  /* drain worker outbox on main thread */

/* introspection (S4 consumer) */
void   infoCompression(sds *info);                                 /* R2.10 */
```

### 4.2 `compression_registry.h` — S1 → S2, S1 → S3 contract

```c
typedef struct compressionDictPair {
    uint32_t       dict_id;       /* monotonic, never reused; 0 = no-dict */
    ZSTD_CDict    *cdict;
    ZSTD_DDict    *ddict;
    size_t         dict_bytes;
    mstime_t       trained_at;
    atomic_int     refcount;
    /* ... */
} compressionDictPair;

/* main-thread only */
compressionDictPair *compressionRegistryActive(void);               /* S2 read hot */
compressionDictPair *compressionRegistryLookup(uint32_t dict_id);   /* S3 RDB load */
uint32_t             compressionRegistryAdd(compressionDictPair *p);
int                  compressionRegistryRetire(uint32_t dict_id);
void                 compressionRegistryIncRef(uint32_t dict_id);
void                 compressionRegistryDecRef(uint32_t dict_id);

/* iter for INFO and RDB aux write */
void compressionRegistryForEach(void (*cb)(const compressionDictPair *, void *), void *ctx);
```

Contract: S2 and S3 never touch `compressionDictPair` internals; only these functions. Atomic pointer swap on promote/retire is an internal detail of S1.

### 4.3 RDB format contract — S2 ↔ S3

From §5.3:

```
RDB per-value encoding for compressed STRING:
  byte 0:    RDB_ENC_COMPRESSED       (new; chosen value reserved in Phase 0)
  varint:    alg_magic                (algorithm tag, e.g. ASCII "ZSTD")
  varint:    alg_meta                 (per-alg: ZSTD uses dict_id)
  varint:    uncompressed_size
  varint:    compressed_size
  bytes:     compressed payload

RDB AUX for dictionary bytes (ZSTD algorithm only — one entry per dict alive at save):
  aux key:   "compression-dict-<dict_id>"
  aux value: raw dict bytes (max compression-dict-size; default 100 KB)
```

S2 owns **header encode/decode** inside `src/compression_header.c` — callable from S3's `rdb.c`. S3 owns **RDB stream format** (varints, AUX entries, save/load orchestration). On load, S3 must reject frames whose `alg_magic` is unknown, and for ZSTD-magic frames must call `compressionRegistryLookup(alg_meta)` and reject frames referencing an unknown dict with a clear error (R2.6.5).

### 4.4 Queue primitives contract — S1 ↔ S2

From §4.6. Internal to S2 but referenced by S1's train-completion callback:

```c
/* bio train completion: called by bio thread, re-dispatched to main via event fd */
void compressionTrainCompleteFromBio(compressionDictPair *new_pair,
                                     sds /* takes ownership */ err_or_null);
```

### 4.5 `COMPRESSION` command contract — S2 ↔ S4

S4 owns the command tree (from §4.5):

```
COMPRESSION STATUS                  # human-readable; from infoCompression()
COMPRESSION DICT LIST               # iterate registry
COMPRESSION DICT DROP <dict_id>     # force-drop; R2.3 / Q5 third-state
COMPRESSION SWEEP [ASYNC]           # operator-triggered sweep; Q5
COMPRESSION DEBUG ...               # R2.13 debug surface
```

Every subcommand calls into S2 public API; S4 owns reply schema, command JSON, and ACL categorization.

### 4.6 Build/CI contract — S7

- `make BUILD_ZSTD=yes` (default) — feature compiled in. Can be disabled for constrained builds via `make BUILD_ZSTD=no`.
- CI matrix: `{ BUILD_ZSTD=yes, BUILD_ZSTD=no } × { ubuntu-latest, macos-latest }`
- New long-running job `test-compression-soak` (nightly only): runs §7.3 stress harness for 30 min.

## 5. Phased plan

### Phase 0 — Skeleton + contracts (1 week, both engineers, joint PR)

**Exit criteria:** feature compiles with `BUILD_ZSTD=yes`, new config knobs parse, `COMPRESSION STATUS` returns "disabled", zero behavior change (feature defaults off).

- [x] S7: add `deps/zstd/` vendored build + `BUILD_ZSTD` flag, wire into Makefile + CMake. Add CI matrix entry. *(PR #3 — `phase-0-skeleton`, merged 2026-05-17)*
- [x] S2: create `src/compression.{c,h}` + `src/compression_registry.{c,h}` + `src/compression_header.{c,h}` + `src/compression_workers.{c,h}` + `src/compression_train.{c,h}` stubs. All public APIs return "feature-disabled" defaults. *(PR #3)*
- [x] S4: register 16 config knobs (5 primary + 11 advanced) per §2.12, stubbed to no-op. Add `COMPRESSION` command tree with `STATUS`-only implementation. *(PR #3)*
- [x] S3: reserve `RDB_ENC_COMPRESSED` enum value; document it in `src/rdb.h`. *(PR #3)*
- [x] S6: add empty test fixture `tests/unit/type/compression.tcl` that verifies feature-off is identical to current behavior. *(PR #3)*
- [x] Joint sign-off on interface contracts §4.1–4.6 of this doc. *(PR #3 merged — contracts accepted)*

### Phase 1 — Core subsystems offline (4 weeks, parallel)

#### @ikolomi track (S2/S5)

- [x] **S2.1 — Header encode/decode** (`compression_header.c`): round-trip tests, malformed-header rejection (R2.5.3). Allocation helpers for `OBJ_ENCODING_COMPRESSED` robjs.
- [x] **S2.2 — Eligibility predicate** (`compressionIsEligible`): implements R2.2 consolidated predicate — size bounds, encoding filter (EMBSTR excluded), policy-aware hot-key skip (`lru_idle_secs` >= `compression-min-idle-seconds` in LRU/noeviction, `lfu_freq` < `compression-lfu-threshold` in LFU).
- [ ] **S2.3 — ~~Incompressible-keys hashtable~~ (REMOVED).** Originally planned as a dict-ID-scoped side hashtable per Thread #20; dropped during S2.3 implementation review (PR #10 design discussion). Per-key rejection state is functionless under a fixed dict (ZSTD is deterministic), and the dict-change retry signal is better expressed at the system level via the rejection-rate drift trigger added to S1.4. No code change tracked under S2.3 anymore — kept here for traceability.
- [x] **S2.4 — Worker pool** (`compression_workers.c`):
  - thread startup/shutdown per `compression-threads`; mutexQueue inbox + mpsc outbox; runtime resize via Stop+Start
  - QSBR worker contract: workers atomically load the active dict, never touch `robj` or registry; report quiescent generation per job (R2.11.4 + §4.4)
  - mutexqueue infrastructure (`src/mutexqueue.{c,h}`): added `mutexQueueWakeAll` for QSBR grace barriers; added new `mutexQueuePopWakable` variant for wake-aware consumers. The original `mutexQueuePop`/`PopAll` keep their "never NULL on blocking" contract; bio is unchanged.
  - sentinel-based race-free shutdown: `kShutdownSentinel` (data-in-queue) is distinct from grace-barrier wake-all. Wake-all races are benign for barriers but would deadlock `pthread_join` on shutdown.
  - resize-aware `canFree`: `compressionDictPair.retire_n_workers` field bounds the gen check to `min(snapshot, current)`. Handles resize-up/down correctly without violating QSBR purity.
  - `compressionWorkersGetThreadCount()` test/introspection accessor.
  - design doc §4.4 + §4.6 updated to match. (PR #13)
- [x] **S2.5 — Encoder path**: replaced placeholder worker body with `ZSTD_compress_usingCDict` + per-worker reusable `ZSTD_CCtx`; replaced placeholder drain handler with the post-compression net-savings guard (R2.4.3); added `testOnly*` accessors so unit tests can verify the worker output before the production drain frees it; new gtest cases — RealCompressionRoundTrip, NetSavingsGuardRejectsIncompressible, NoActiveDictMarksJobNotCompressed, CompressionFromMultipleWorkersIsConsistent. The full install path (createCompressedObject + dbOverwrite + dict-frame-ref + caller-pin decRef) lands with the write-path hook in **S2.7** when there's a real production caller.
- [x] **S2.6 — Decoder path**: `objectGetUncompressedView(robj *o, sds *scratch, robj *view_out)` on main thread, sync decompression (R2.5.1) via file-static `ZSTD_DCtx` (lazy-init, freed in `compressionShutdown`). Handles dict-ID lookup + dict-not-found error (R2.6.5 parallel). Three-arg signature with caller-provided stack `view_out` (vs the original two-arg form that would heap-allocate per call) — keeps every read off the allocator hot path. R2.5.2 + Appendix §4.3 updated. 6 new gtest cases (DecoderRoundTripsEncoder, DecoderPassthroughOnUncompressed, DecoderRejectsBadAlgMagic, DecoderRejectsMissingDict, DecoderReusesScratchAcrossCalls, DecoderAllocatesScratchOnFirstCall). The decoder is NOT yet wired into any read path — that's S2.8.
- [x] **S2.7 — Write-path hook**: two seams in `db.c` (end of `dbAddInternal` and end of `dbSetValue`) call `compressionEnqueueCandidate(key, value, db->id)`. Producer-side guards: master switch + R2.2 eligibility + active-dict check (R2.1.5) + `incrRefCount` to pin (R2.4.4 immutable-snapshot invariant + ABA safety for the drain pointer-equality stale check). `compressionWorkersEnqueue` signature changed to `(robj *value, int dbid)` — the worker reads `objectGetVal(value)` once at enqueue (captured into `job->src`), drain re-resolves the kvstore slot via `objectGetKey(value)` and compares `*slot == job->value`. Drain install path: `createCompressedObject` + `dbReplaceValue` (which routes through `dbSetValue(..., overwrite=0, ...)` — does NOT call `signalModifiedKey` / `moduleNotifyKeyUnlink` per R2.9.2) + `compressionRegistryIncRef`. Pin released on every drain completion path (success, stale-discard, error). Existing tests migrated to a new test-only `testOnlyCompressionWorkersEnqueueRaw(sds src, int dbid)` (job->value=NULL sentinel; production drain skips install — tests extract via `testOnlyCompressionWorkersDrainOutbox` first).
- [ ] **S2.8 — Read-path hook (transient-view model, R2.5.7 + Appendix E)**: introduce a `LOOKUP_READ_BYTES` flag on `lookupKey*`. When set on a compressed value, the lookup helper decompresses into a temp uncompressed sds, registers the robj in a per-server transient-view side-map (compressed buffer preserved), pins via `incrRefCount`, flips encoding to RAW, and returns the robj. At the next `beforeSleep` boundary, `compressionRestoreTouchedKeys()` iterates the side-map and either restores the compressed buffer via pointer swap (if the kvstore slot still points to our pinned robj) or discards (if mutated/overwritten/expired — detected via the same pointer-equality stale check used by the write-path drain in PR #19).
  Files touched (per Appendix E codebase sweep): `db.c` (flag plumbing + side-map + ~30 call sites annotated with `LOOKUP_READ_BYTES`), `compression.c`/`compression.h` (side-map data structure + `compressionRestoreTouchedKeys()`), `server.c` (wire into `beforeSleep`), explicit `objectGetUncompressedView` calls in `aof.c` (AOF rewrite child) and `rdb.c` (RDB save for replication full-sync — R2.6.8). Replication feed (`feedReplicationBufferWithObject`) does NOT need wiring (operates on argv, not kvstore). The transparency Tcl harness (§7.1) validates end-to-end once this lands.
- [ ] **S2.9 — Master switch + sweep**: `COMPRESSION SWEEP [ASYNC]` triggers; runtime toggle drains safely (pattern from Thread #11).
- [ ] **S2.10 — Cron integration**: `compressionCron` called from `serverCron`; sweep pacing; dict drift-ratio evaluation.
- [ ] **S2.11 — Bounded inbox + per-caller back-pressure counters** (`compression_workers.c`, `compression.c`): bounded inbox capacity (sized as `max(256, 128 * compression-threads)` per design §4.6), drop policy per §2.10 R2.10.4, `INFO` counters `compression_candidates_dropped_total`, `compression_sweep_backpressure_total`, `compression_outbox_backpressure_total`, `compression_sweep_pacing_sleeps_total`. Production safety net for the write path: lands close-on-the-heels of S2.7 (or S2.10) once a real producer exists to validate the back-pressure path. Not a hard prerequisite for S2.7 — the unbounded `mutexQueue` inbox is correct for any single-burst load size; S2.11 makes the steady-state behavior under sustained over-saturation observable and bounded.
- [ ] **S5.1 — `valkey-benchmark` extensions**: `--value-size-distribution`, `--value-data`, `--key-distribution` flags per §7.5.
- [ ] **S5.2 — Canonical scenarios harness**: the six scenarios in §7.5 (uniform large, skewed JSON, time-series, etc.) as reproducible runs.
- [ ] **S5.3 — Perf dashboard**: extend Valkey performance dashboard or add a feature-scoped one showing baseline-vs-compressed per scenario. Publish to `perf-dashboard.valkey.io` infrastructure.

#### @GilboaAWS track (S1/S3/S4/S6/S7)

- [x] **S1.1 — Dictionary registry** (`compression_registry.c`): add/lookup/promote/retire, refcounting, cap enforcement. Unit tests covering every branch of R2.3.9 promotion + R2.3.10 retirement. Owns `compression-max-dict-cap` behavior. **Merged in PR #12.** Follow-up in PR #13 (S2.4) extended `compressionDictPair` with `retire_n_workers` for resize-aware QSBR and switched the registry to use `COMPRESSION_WORKERS_MAX` from `compression_workers.h`.
- [ ] **S1.2 — Training sampler (main thread)**: kvstore shard iteration + contiguous-buffer sample copy, spliced across `serverCron` ticks. Implements R2.3.6 corrected flow (per Thread #29). `LOOKUP_NOTOUCH` semantics.
- [ ] **S1.3 — Bio train job (`BIO_COMPRESSION_TRAIN`)**: accepts `(buffer, sizes[], count)`, calls `ZDICT_trainFromBuffer`, signals completion via event fd. Never touches `robj`/`kvstore`/refcounts.
- [ ] **S1.4 — Train completion + promotion on main thread**: creates `ZSTD_CDict`/`ZSTD_DDict`, inserts into registry, atomic promotion. Implements R2.3.5 drift-retrain trigger detection: `compression_live_ratio_10m > post_training_ratio / drift_ratio` where the rolling ratio includes both successful compressions AND rejections (each rejection contributes its actual measured ratio, typically in `[0.9, 1.05]` since the net-savings guard rejected it for being too close to 1.0). The single combined signal captures both workload-content drift among compressible values and dict-fit drift (rejection rate climbing); see PR #10 design discussion for the rationale.
- [ ] **S4.1 — `INFO compression` section**: all fields per R2.10 + §5.6. Unit tests for schema stability.
- [ ] **S4.2 — `COMPRESSION` command tree full implementation**: `STATUS`, `DICT LIST`, `DICT DROP`, `SWEEP`, `DEBUG`. Reply-schema tests per §7.4.
- [ ] **S4.3 — Latency monitor events** per R2.10 (train start/finish, worker stall, etc.)
- [ ] **S7.2 — CI — long-running perf regression**: nightly job running §7.3 under AddressSanitizer + ThreadSanitizer.
- [ ] **S3.1 — RDB encode path**: new `RDB_ENC_COMPRESSED` marker, varint-encoded alg_magic/alg_meta/sizes, AUX entries for dictionaries (ZSTD). Implements R2.6.1–R2.6.4.
- [ ] **S3.2 — RDB decode path**: lookup dict by id, rehydrate `OBJ_ENCODING_COMPRESSED` robj. Unknown-dict error path per R2.6.5.
- [ ] **S3.3 — Full-sync RDB uncompressed flag**: implements R2.6.8 (new this walkthrough) — full-sync RDB always emits uncompressed regardless of `compression-enabled` state. Disk RDB still compressed.
- [ ] **S3.4 — AOF**: compressed values serialized as their uncompressed command forms (`SET key value`). R2.7.

**End of Phase 1 gate:**
- Feature works end-to-end on a single replica set with `compression-enabled yes` + manual dict training.
- All 14 unit tests (per §7.1/§7.2) pass.
- No SANITIZE-detected races.
- Preliminary perf numbers for at least 2 §7.5 scenarios.

### Phase 2 — Integration + production hardening (4 weeks, parallel)

#### @ikolomi track

- [ ] **S2.11 — Edge cases**: OOM during compress, worker crash recovery (R2.7 § 6.7), sweep-during-shutdown, dict-cap reached.
- [ ] **S2.12 — Module API DMA on compressed keys**: coordinate with S6.1 test module.
- [ ] **S2.13 — COW audit pass 1**: walk every mutating code path (t_string.c, bitops.c, module.c, debug.c) per R2.4.5 audit checklist. File CR as merge-blocker review.
- [ ] **S5.4 — Full §7.5 benchmark run**: all 6 scenarios × baseline/enabled-sync/enabled-off. Publish results.

#### @GilboaAWS track

- [ ] **S1.5 — Drift-retrain end-to-end**: `compression-dict-drift-ratio` trigger → new train run → promotion → old dict retirement. Integration test covers full lifecycle.
- [ ] **S3.5 — `MEMORY USAGE` + `maxmemory` accounting** for compressed robjs per R2.8.
- [ ] **S6.1 — COW invariant Tcl test** (`compression-cow-invariant.tcl`): runs every mutating command against a live compression job in parallel; asserts no corruption. **Merge blocker.** (§7.2)
- [ ] **S6.2 — Replication integration tests**: full-sync with R2.6.8 uncompressed RDB, PSYNC correctness, mixed-version primary/replica.
- [ ] **S6.3 — RDB round-trip stress**: save with many dicts, corrupt various fields, verify error detection (R2.6.5).
- [ ] **S6.4 — `CLUSTER SLOTS` / `MIGRATE` under compression**: ensures no hidden read path bypasses `objectGetUncompressedView`.

**End of Phase 2 gate:**
- All integration tests green.
- COW audit CR approved by both engineers.
- Full §7.5 benchmark run complete, p99 regression ≤ budget from §7.3.

### Phase 3 — Stabilization + release prep (2 weeks, both)

- [ ] Docs: `topics/compression.md`, update `valkey.conf.example` with new section, update `redis.conf`/`sentinel.conf` references.
- [ ] Release notes draft.
- [ ] Module API docs if any public changes.
- [ ] 30-min soak test under sanitizers at nightly CI.
- [ ] Triage and fix any perf regressions surfaced by §7.5.
- [ ] @ikolomi — final COW audit pass 2 before merge.
- [ ] Reviewer rollcall: @GilboaAWS + one additional maintainer sign-off on main PR.

**End of Phase 3 gate:**
- Release-ready. PR opened against `unstable`.

## 6. Milestones & target dates

| Milestone | Gate | Target |
|---|---|---|
| M0 — Skeleton landed, contracts signed | Phase 0 exit | T + 1 wk |
| M1 — Offline encoder/decoder + registry + bio training | S1+S2+S3+S4 Phase 1 work done | T + 5 wk |
| M2 — First end-to-end compression of a running instance | Phase 1 gate | T + 5 wk |
| M3 — COW audit approved, replication correctness verified | Phase 2 gate | T + 9 wk |
| M4 — Full §7.5 perf run meets budgets | Phase 2 gate | T + 10 wk |
| M5 — Ready for merge | Phase 3 gate | T + 11 wk |

Total calendar: ~11 weeks assuming no significant delays. Series work was estimated at ~17 weeks; parallelism buys ~6 weeks.

## 7. Risks & mitigations

| Risk | Severity | Mitigation | Owner |
|---|---|---|---|
| COW invariant violation discovered late in the audit | High | Phase 2 schedules audit before perf validation; if violation found, fall back to memcpy-at-enqueue per Appendix D for affected code paths. | @ikolomi |
| ZSTD version pin causes build breakage on distros | Med | Vendor ZSTD in `deps/zstd/`, pin version; verify on CI matrix including musl. | @GilboaAWS (S7) |
| §7.5 reveals unacceptable p99 regression | Med | Phase 2 gates on this; Appendix A rejected alternatives (LZ4) are a fallback. Worst case: narrow eligibility window via config defaults. | @ikolomi (S5) |
| Training on bio starves bio for other tasks | Low | Training is infrequent; enforce single-inflight train per §2.3. Perf test (S5) includes bio-load-during-train scenario. | @ikolomi (S1) |
| Module authors not aware of DMA transparency requirements | Med | Explicit R2.9 DMA test in `tests/modules/compression.c` (S2.12 + S6.1). Docs call out in migration guide. | both |
| Dict-cap exhaustion in pathological drift scenarios | Low | R2.3.11 enforces cap; `COMPRESSION DICT LIST` + `DICT DROP` give operator recovery. | @ikolomi (S1) |
| Gilboa or ikolomi blocked / out for extended period | Med | Phase 0 contracts mean either can pick up the other's in-flight work from stable interfaces. Shared code reviews keep context. | both |

## 8. What's explicitly out of scope

From §1.4 (non-goals) and the walkthrough decisions — do **not** implement in this plan:

- Wire-level compression negotiation (`REPLCONF compression yes`) — deferred to v2 (Threads #2, #31)
- IO threads for decompression — rejected in v1 per Appendix §C.7 (Thread #25)
- Async decompression path for long reads — v2 opt-in (Thread #26 context)
- LIST/HASH/ZSET per-element compression — v1 is STRING-only (§1.4)
- Preshared/imported dictionaries — v2 (Q2)
- Hardware compression — deferred (Appendix A.4)
- Adaptive kill-switch — rejected (Q4)

## 9. Open questions for v2 planning (not blocking v1)

- Wire-level replication compression negotiation protocol design
- Async read path: dedicated decompress workers? reuse io-threads?
- Dict-export/import API surface for cross-instance reuse
- Hardware accelerator integration (AVX-512, QAT)
- Per-element compression for LIST/HASH/ZSET

## 10. Cross-references

- Design: [`design/detailed-design.md`](../design/detailed-design.md)
- Requirements: [`idea-honing.md`](../idea-honing.md)
- Review walkthrough: [`DESIGN_TODO.md`](../DESIGN_TODO.md) — 31 resolved threads
- GitHub PR: https://github.com/ikolomi/valkey/pull/1
- Research notes: [`research/`](../research/)

---

_This plan is a living document. Amend via CR to this file; re-confirm interface contracts before modifying them in code._
