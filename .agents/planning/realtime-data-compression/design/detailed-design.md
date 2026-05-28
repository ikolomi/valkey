# Detailed Design — Real-time In-memory Value Compression for Valkey

_Status: proposed, v1_
_Scope: `OBJ_STRING` values only_
_Source issue: [valkey-io/valkey #3423](https://github.com/valkey-io/valkey/issues/3423)_

---

## Table of contents

1. [Overview](#1-overview)
2. [Detailed requirements](#2-detailed-requirements)
3. [Architecture overview](#3-architecture-overview)
4. [Components and interfaces](#4-components-and-interfaces)
5. [Data models](#5-data-models)
6. [Error handling](#6-error-handling)
7. [Testing strategy](#7-testing-strategy)
8. [Appendix A — Technology choices](#appendix-a--technology-choices)
9. [Appendix B — Research findings summary](#appendix-b--research-findings-summary)
10. [Appendix C — Alternative approaches considered](#appendix-c--alternative-approaches-considered)
11. [Appendix D — Explicit v1 non-goals and v2 roadmap](#appendix-d--explicit-v1-non-goals-and-v2-roadmap)

---

## 1. Overview

### 1.1 What this feature does

Valkey keeps values in memory as SDS strings. For workloads dominated by `OBJ_STRING` values of moderate size (256 B – 1 MiB) with repetitive content (JSON blobs, URL-keyed DTOs, HTML fragments, serialized protocol buffers, etc.), a large fraction of the memory footprint is compressible. Internal fleet analysis on AWS ElastiCache shows ≥50% memory savings are achievable on 92% of memory-bound production snapshots using ZSTD with a trained dictionary.

This feature adds **opt-in, transparent, server-side, in-memory compression** for `OBJ_STRING` values. Compressed values are automatically decompressed on every **client-facing read path** (client commands, scripts, transactions, the replication feed, AOF writer, module API), so no client or operational tooling changes are required. The feature is disabled by default; enabled, it is fully observable via `INFO`, controllable via `CONFIG SET compression-*` and a new `COMPRESSION` subcommand container, and bounded in both CPU and memory overhead.

Note: the **on-disk RDB file may contain compressed values** (to preserve the memory win across restarts and shrink snapshot size/save time). This is the one intentional exception to "decompressed at every boundary" — it is an internal persistence format, not a client-facing read path. See §2.6 for the full persistence-boundary specification; full-sync replication RDB streams are always uncompressed per R2.6.8.

### 1.2 Headline scope statement

> v1 ships compression for `OBJ_STRING` values only, with synchronous decompression on the main thread, one active dictionary per server, self-trained by a keyspace-scan job on `bio`, with explicit operator controls (`COMPRESSION` subcommands + `compression-*` configs). All other value types, async decompression, adaptive behaviors, and cluster-level dictionary coordination are explicit v2 scope.

### 1.3 Goals

- **Reduce memory** by ≥30% on target workloads (POC baseline) at production-safe CPU cost (<20% TPS degradation).
- **Transparent** to clients, scripts, transactions, replication, AOF, and modules — no wire or semantic changes.
- **Opt-in**, with a single master switch (`compression-enabled`) and zero fixed cost when disabled.
- **Observable** — memory saved, compression ratio, training events, dict lifecycle, and errors exposed via `INFO`, logs, and latency monitor.
- **Bounded blast radius** — the feature is encapsulated in a small number of files; existing code paths are touched only through well-defined helpers.
- **Extensible** — the encoding-tag design structurally supports future value types and async decompression without breaking changes.

### 1.4 Non-goals (v1)

See [Appendix D](#appendix-d--explicit-v1-non-goals-and-v2-roadmap). Key exclusions: non-string data types; async decompression; decompressed-view cache; adaptive kill-switch; compressed-in-place MIGRATE; cluster-wide dictionary gossip; advanced trainer tuning; module-provided compression backends.

---

## 2. Detailed requirements

Requirements are consolidated from `idea-honing.md`. Each bullet is traceable to a Q1–Q16 discussion.

### 2.1 Master switch and operator surface

- **R2.1.1** The feature is gated by a master switch `compression-enabled` (bool, default `no`, `MODIFIABLE_CONFIG`). When `no`, no compression CPU is spent and no worker threads run. (Q5)
- **R2.1.2** Two surfaces toggle the switch: `CONFIG SET compression-enabled yes|no` (primary) and `COMPRESSION ENABLE`/`COMPRESSION DISABLE` (convenience alias, writes a `LL_NOTICE` log entry for audit trails). (Q5)
- **R2.1.3** `no → yes` transition: background sweeper starts on the next cron tick; values are compressed opportunistically by new writes and by the sweeper. `COMPRESSION SWEEP` triggers immediate sweep. **Operators who want to enable without auto-sweeping existing data** can achieve this without any new config: set `compression-threads 0` before the toggle (worker pool disabled, candidates queue but no work happens), flip the master switch, then raise `compression-threads` to 1+ when ready to drain the queue. This is the symmetric counterpart to the `yes → no` default in R2.1.4. (Q5)
- **R2.1.4** `yes → no` transition: new writes stop being compressed; existing compressed values continue to be decompressed **on read only** (cold/untouched keys remain compressed indefinitely); the dictionary registry stays alive. No automatic full-keyspace decompress. Operator explicitly runs `COMPRESSION SWEEP direction=decompress` to decompress all keys in the background — this **guarantees eventual full coverage** regardless of read activity, and eventually drains the compressed-frame population so the dictionary registry can retire. Peak memory during an explicit sweep grows proportionally to the uncompressed dataset size (factor `1/compression_ratio` vs. the compressed baseline — e.g. ~2–3× for typical ratios of 0.3–0.5), but growth is bounded by `compression-sweep-max-cpu-pct` pacing — never a synchronous spike. (Q5)
- **R2.1.5** A third state — "`compression-enabled yes` but no active dictionary for new writes" — behaves identically to disabled for writes. Decompression of any existing compressed frames continues to work: **refcount-based dictionary retirement (R2.3.4) and the safety check in `COMPRESSION DICT DROP` (§4.5) together guarantee that a dict cannot be freed while any frame references it.** A "retiring" dict stays in the registry and services decompressions until its last referencing frame is rewritten, overwritten, expired, or explicitly decompressed. The state *"no dicts in registry AND compressed frames exist"* is by-construction unreachable. Documented as expected behavior. (Q5)

### 2.2 Value eligibility

The compression sweeper considers a value eligible iff the predicate holds:

```
eligible(obj) ⇔
    obj->type == OBJ_STRING
 && obj->encoding == OBJ_ENCODING_RAW
 && obj->refcount != OBJ_SHARED_REFCOUNT
 && sdslen(val) >= compression-min-value-size
 && (compression-max-value-size == 0 || sdslen(val) <= compression-max-value-size)
 && hot_key_check(obj)                                        // see below — policy-aware

where hot_key_check(obj) is:
    if lfu_mode:                                              // robj->lru encodes a freq counter
        lfu_freq(obj) < compression-lfu-threshold
    else:                                                     // LRU/noeviction: robj->lru is seconds-based
        lru_idle_secs(obj) >= compression-min-idle-seconds
```
(Q6, Q7)

The eligibility surface has a single time-based knob in LRU/noeviction modes (`compression-min-idle-seconds`) and a single freq-based knob in LFU mode (`compression-lfu-threshold`):

- **LRU and noeviction modes:** `robj->lru` is touched on every read and every write, so `lru_idle_secs(obj)` reflects time-since-last-touch regardless of source. v1 cannot distinguish read-recency from write-recency from this single signal, so a single threshold gates eligibility on the "value has been quiet long enough to be worth compressing" property.
- **LFU mode:** `robj->lru` encodes a 16-bit minutes counter + 8-bit freq counter. There is no per-second access timestamp, so the time-based threshold cannot be applied meaningfully. The freq counter IS the access-recency signal — high freq = recently or repeatedly accessed — and `compression-lfu-threshold` filters on it directly.

Earlier drafts of this design also exposed `compression-settle-seconds` as a "recent-write protection" knob alongside `compression-min-idle-seconds`. Both compared to the same metric (`lru_idle_secs(obj)`), so the effective behavior was always `idle >= max(settle, min_idle)` — the second knob added no expressive power v1 could deliver, only a footgun (operators tuning them differently expecting different effects). The dual surface was justified as forward-compat for a hypothetical v2 with per-object write-time tracking; per the YAGNI principle and Valkey's preference for minimal operator surfaces (see PR #1 Thread #3, the 5+11 → 5+10 split), v1 ships only the single knob. v2 can reintroduce a second knob non-breakingly when the underlying signal genuinely supports it.

**Post-compression net-savings guard** runs on the main thread after the worker returns:

```
compressed_size + header_size >= uncompressed_size * (1 - compression-min-savings-ratio)
  → discard compressed form, leave value uncompressed, increment
    compression_skipped_incompressible.
```
(Q6)

v1 does **not** track per-key rejection state. Under a fixed dict and a fixed workload, the probability that any given write at key K produces compressible bytes is constant — time-based throttling between retries doesn't shift this probability, it only spaces attempts. CPU bounding is already provided at the sweep level (`compression-sweep-max-cpu-pct`); per-key throttling would be redundant with that, while adding correctness burden (stale entries when a key is overwritten via `dbOverwrite` or in-place-mutated via APPEND/SETRANGE/BITOP/BITFIELD/module DMA writes — every mutation path would need a `Clear` integration point).

The legitimate trigger for "the dict has stopped fitting the workload" is **dict change**, which is handled at the system level. Rejected attempts contribute their actual measured ratio to `compression_live_ratio_10m` (R2.10.1) — typically in `[0.9, 1.05]` since the net-savings guard rejected them for being too close to 1.0 — so a sustained high rejection rate naturally drives the metric toward "no savings" and trips the drift threshold, firing retraining (R2.3.5). After dict promotion, the next sweep tick re-attempts under the new dict naturally — no per-key state required.

This is a deliberate simplification over an earlier design (PR #10) which proposed a per-key `incompressibleKeys` side hashtable scoped by `(key, failed_dict_id)` with a `compression-retry-interval` time fallback. That approach added a module + a config knob + integration points in every mutating code path, but the underlying assumption (waiting between retries improves the per-attempt hit rate) is incorrect for fixed-distribution workloads under a fixed dict. The simpler model — retry on every sweep tick, let the drift signal handle systemic dict-fit issues — is correct and operationally cleaner.

### 2.3 Dictionary lifecycle

- **R2.3.1** The server maintains a **dictionary registry**: a small set of `{dictID, raw_bytes, ZSTD_CDict*, ZSTD_DDict*, refcount, state}` entries. State ∈ `{active, retiring, retired}`. (Q1)
- **R2.3.2** At most **one** dictionary is `active` at any time (the one used to compress new values). Zero-or-more are `retiring` (decompress-only for older frames). (Q1)
- **R2.3.3** The registry is capped at `compression-dict-max-versions` entries (int, default `4`, min `2`, `MODIFIABLE_CONFIG`). When full, retraining/promotion is blocked, a `LL_WARNING` log entry is emitted, and `compression_dict_cap_reached` is set to `1` in `INFO`. Operator unblocks by running `COMPRESSION SWEEP` to retire the oldest dict, or by raising the cap. (Q1)
- **R2.3.4** A dict's refcount tracks the number of compressed frames that reference its dictID. When refcount hits zero, the dict transitions to `retired` and its CDict/DDict/raw_bytes are freed. (Q1)
- **R2.3.5** **Training triggers** (Q9):
  - **First training**: fires when the eligible-keys write counter reaches `compression-dict-first-training-keys-count` (default `10000`).
  - **Drift-based steady-state retraining**: fires when `compression_live_ratio_10m > post_training_ratio / compression-dict-drift-ratio` (default drift ratio `70%`).

    The ratio convention is `compressed/uncompressed` (lower is better; see R2.10.1). `compression_live_ratio_10m` is computed over compression *attempts* (successful + rejected) in the rolling 10 min window, weighted by uncompressed bytes. Each successful compression contributes its actual `compressed/uncompressed` ratio. Each rejection (post-compression net-savings guard, R2.4 / §6.6) contributes its actual measured ratio (typically in `[0.9, 1.05]` since the net-savings guard rejected it for being too close to 1.0). Worker errors are excluded from the metric — they're tracked separately via `compression_errors_total`. With drift_ratio = 0.7, drift fires when the current ratio exceeds post-training by a factor of `1/0.7 ≈ 1.43` — i.e., compression efficiency has degraded by about 43%. Both forms of degradation feed the metric uniformly:
      - workload-content drift among compressible values (post-training values were highly compressible; live values compress worse), and
      - workload composition drift (a growing fraction of values compress poorly enough to fail the net-savings guard).
  - **Optional time-based retraining**: `compression-dict-refresh-interval` (default `0` = disabled).
  - **Manual**: `COMPRESSION TRAIN` forces an immediate training job.
- **R2.3.6** **Training sampling — main-thread iteration, bio-thread training, no sustained reservoir.** The main thread walks `kvstore` shards in random order (reusing the active-expiry / defrag incremental-iteration pattern, spliced across `serverCron` ticks), selects eligible samples, and **copies sample bytes into a pre-allocated contiguous training buffer** with a parallel `sizes[]` array. When `compression-dict-first-training-keys-count` samples are collected, the main thread submits `(buffer, sizes[], count)` as a `BIO_COMPRESSION_TRAIN` job. Iteration and any `kvstore` / `refcount` manipulation stay on the main thread; bio never touches `robj`, `kvstore`, or refcounts (consistent with R2.11.4). Scan uses `LOOKUP_NOTOUCH` semantics — training reads do not update LRU/LFU. The training buffer is transient (~10–16 MiB for default settings, freed once bio returns), not a long-lived reservoir. Iteration window is bounded to ~1 s for default settings (hz=10, ~1000 keys/tick) — ≪ the dict's post-training active lifetime, so spread-in-time sampling does not meaningfully affect dictionary quality; each collected sample is an immutable snapshot of real bytes at copy time, and drift-retraining (R2.3.5) is the backstop for post-training workload shifts. Low-keyspace edge case: if fewer than `first-training-keys-count` eligible values exist, training aborts with a logged warning and retries on the next trigger. (Q9)
- **R2.3.7** **Training location**: a new `bio` job type `BIO_COMPRESSION_TRAIN`. Fits the `bio` model (long-running, infrequent, one-at-a-time). Does not occupy a compression worker. (Q9)
- **R2.3.8** **Training algorithm**: `ZDICT_trainFromBuffer` with target size `compression-dict-size` (default `102400` bytes). Advanced tuning is v2. (Q9)
- **R2.3.9** **Promotion**: after training, the main thread creates `ZSTD_CDict` and `ZSTD_DDict` from the new bytes, inserts into the registry with a fresh dictID, atomically swaps the `active` pointer, and transitions the previous active to `retiring`. (Q1, Q9)
- **R2.3.10** **Preshared dictionary**: `COMPRESSION DICT IMPORT <base64-bytes>` installs as a new registry entry, going through the same promotion path as a trained dictionary. `COMPRESSION DICT EXPORT <dictID>` returns raw dictionary bytes (base64). Both require `@admin` ACL. (Q2)

### 2.4 Compression path

- **R2.4.1** Eligible values are enqueued on a **candidate queue** (SPMC) by write paths (`dbAdd`, `dbOverwrite`) when the eligibility predicate holds at write time, and by the **background sweeper** during its cron tick. (Q6, Q8)
- **R2.4.2** Compression workers (separate pool from `io-threads`) dequeue from the candidate queue, call `ZSTD_compress_usingCDict` on the flat value bytes, and post the result to an outbox (MPSC). Workers **never touch `robj`**. (Q8)
- **R2.4.3** The main thread polls the outbox on `afterSleep`/cron, runs the post-compression net-savings guard (R2.2 second block), and if accepted:
  - Allocates the compressed buffer (header + ZSTD frame).
  - `zfree`s the old uncompressed sds; `zmalloc`s the new buffer. `used_memory` accounting is correct automatically (Q14).
  - Mutates `robj->val_ptr` to the new buffer, sets `encoding = OBJ_ENCODING_COMPRESSED`.
  - Increments the dictID refcount.
  - Does **not** call `signalModifiedKey`. Background compression is a storage change, not a logical value change. (Q11)
- **R2.4.4** **Immutable-snapshot invariant.** A compression worker reads `src` bytes concurrently with the main thread. Correctness requires that those bytes are **not mutated in place** while the worker runs. v1 relies on Valkey's existing copy-on-write discipline: every command that mutates an existing `OBJ_STRING` value MUST funnel through `dbUnshareStringValue()` (or an equivalent refcount check) **before** modifying `val->ptr`. When a compression job is queued, the main thread holds `incrRefCount(val)`, forcing `refcount >= 2`; the next mutating command sees the bumped refcount and creates a COW copy via `dbUnshareStringValue`, leaving the original bytes immutable for the worker. The rule: **if refcount > 1, thou shalt not mutate in place** — this is a pre-existing Valkey invariant; compression only depends on it, does not introduce it.
- **R2.4.5** **Invariant-enforcement audit (v1 sign-off checklist).** The `dbUnshareStringValue`-before-mutate discipline is code-discipline, not enforced by the type system, so the design commits to auditing every mutating string code path before v1 merges. Required sites:
  - `src/t_string.c` — `appendCommand`, `setrangeCommand`, `getsetCommand`, `getdelCommand`, `setCommand` (overwrite path), and any command that writes to an existing value's sds.
  - `src/bitops.c` — `setbitCommand`, `bitopCommand` (write targets), `bitfieldCommand` (write-intent operations), `bitcountCommand` is read-only so OK.
  - `src/module.c` — `VM_StringDMA` with `REDISMODULE_WRITE` intent path; `VM_StringAppendBuffer`; `VM_StringTruncate`. All MUST call `dbUnshareStringValue` (or perform the equivalent refcount-guarded COW) on the underlying `robj` before returning the mutable buffer.
  - `src/debug.c` — any `DEBUG` subcommand that mutates string values.
  - **New code paths introduced by this feature** — ensure none bypass the discipline.
  - The audit is tracked as a merge-blocker test (see §7.2 `tests/unit/compression-cow-invariant.tcl`). Any newly discovered violator either calls `dbUnshareStringValue` or is documented in `src/t_string.c` header as a reason to skip compression for its key (add to the eligibility filter). (Q4-review feedback)
- **R2.4.6** **Fallback path if the invariant fails.** If the post-compression net-savings guard passes but the worker's output doesn't round-trip to the *current* value (main thread decompresses the result and memcmps against live bytes as a cheap sanity check in debug builds), the result is discarded, `compression_errors_total` increments, and a `LL_WARNING` log entry identifies the command that ran during the window. This is a belt-and-suspenders guard; in a correct implementation it fires zero times. The check is gated behind `#ifdef DEBUG_COMPRESSION_SNAPSHOT` so it has no prod cost.

### 2.5 Decompression path

- **R2.5.1** All decompression is **synchronous on the main thread** in v1. No worker-side decompression path exists. (Q7)
- **R2.5.2** A single helper function `robj *objectGetUncompressedView(robj *o, sds *scratch)` is the **only** entry point for getting uncompressed bytes from a value:
  - If `o->encoding != OBJ_ENCODING_COMPRESSED`: returns `o` unchanged. Zero cost.
  - Otherwise: looks up the dictID in the registry, calls `ZSTD_decompress_usingDDict` into the caller-provided scratch sds, and returns a temporary view `robj` (or equivalent) pointing at the scratch buffer.
- **R2.5.3** The helper **must not** call `signalModifiedKey`. (Q11)
- **R2.5.4** `compression-max-value-size` (default `131072` bytes = 128 KiB, `0` = no bound) excludes values large enough that their main-thread decompression cost is not worth the memory win, keeping per-read decompression latency within the event-loop budget. (Q7)
- **R2.5.5** **Per-read decompression cost is proportional to value size** (~1 µs/KB at ZSTD level 3 with dictionary). **Multi-key commands pay the sum of per-value costs**, with no per-command cap — by design: a per-command cap would either break transparency (error mid-command) or defeat its own purpose (block). v1 is tuned for the small/moderate-value sweet spot (256 B – 8 KB). Workloads that routinely read many compressed values per command (wide `MGET`, long `SORT`, scripts touching many keys, heavy pipelines over large values) should benchmark before enabling; their remedies are (a) raise `compression-min-value-size` or lower `compression-max-value-size` to exclude the large values, or (b) wait for v2 async decompression. (Q7)

### 2.6 Persistence

- **R2.6.1** **RDB on-disk format** (Q12, research `persistence-and-replication.md`):
  - New string encoding marker `RDB_ENC_COMPRESSED` (value `4`). The marker is generic — it says "a compressed payload follows" without naming the algorithm, so future backends (LZ4, snappy, hardware) can reuse the same encoding byte.
  - Layout per compressed value: `[RDB_ENCVAL | alg_magic (len-encoded) | alg_meta (len-encoded) | uncompressed_len (len-encoded) | compressed_len (len-encoded) | compressed frame bytes]`. `alg_magic` is the four-byte algorithm tag (ASCII `ZSTD`, reserved `LZ4 ` etc.); `alg_meta` is interpreted per algorithm — for ZSTD it is the `dict_id` of the referenced dictionary, for a hypothetical LZ4 backend it would be `0` or mode bits. Unknown `alg_magic` → reject as corrupt.
  - Dictionary bytes (ZSTD-specific) are written as `RDB_OPCODE_AUX` entries (key `"compression-dict-<dict_id>"`, value = raw bytes) **before** any compressed value that references them. Non-dict-based algorithms may omit AUX.
  - `RDB_VERSION` is bumped (`80 → 81`). Pre-feature loaders will refuse the file cleanly.
- **R2.6.2** **RDB load when `compression-enabled yes`**: loader reads the dictionary bytes from AUX entries and uses them to construct `ZSTD_CDict`/`ZSTD_DDict` handles (via `ZSTD_createCDict`/`ZSTD_createDDict`; no retraining), inserts them into the registry, decompresses values on the fly only if needed (frames reference their dictID so they stay compressed in memory). (Q3, Q12)
- **R2.6.3** **RDB load when `compression-enabled no`**: loader reads the dictionary bytes from AUX entries and constructs only the `ZSTD_DDict` handles needed (no retraining), decompresses every `RDB_ENC_COMPRESSED`-marked value inline, stores uncompressed, discards DDicts after load. (Q3)
- **R2.6.4** **Missing dictionary**: if a compressed value references a dictID for which no AUX entry was emitted, the RDB is rejected as corrupt regardless of the `compression-enabled` setting. (Q3)
- **R2.6.5** **AOF**: always uncompressed RESP. Writer routes every `robj` through `objectGetUncompressedView` before emitting. (Q12)
- **R2.6.6** **Replication feed**: always uncompressed RESP. `feedReplicationBufferWithObject` routes through `objectGetUncompressedView`. Cross-version replication unaffected. (Q12)
- **R2.6.7** **`DUMP` / `RESTORE` / `MIGRATE`**: v1 decompresses before emitting the RDB chunk. Compressed-in-place migration is v2. (Q12)
- **R2.6.8** **Full-sync replication RDB** (primary → replica during `SYNC`/`PSYNC` full resync): emitted **uncompressed** regardless of `compression-enabled` state on the primary. When the RDB writer is invoked with a replication sink, every compressed value is routed through `objectGetUncompressedView` before serialization — same helper used by `feedReplicationBufferWithObject`. Disk RDB (local save / `BGSAVE` target) continues to use the `RDB_ENC_COMPRESSED` path from R2.6.1. This keeps cross-version replication working without replica-side awareness and preserves the "wire stays uncompressed RESP/RDB, disk may be compressed" property. Opt-in compressed full-sync (via `REPLCONF` negotiation) is a v2 extension point. (Q12)

### 2.7 Introspection surfaces

- **R2.7.1** **`OBJECT ENCODING key`** returns `compressed` for compressed values. New encoding name documented. (Q13)
- **R2.7.2** **`DEBUG OBJECT key`** extends its debug line with `dictID:<N> compressedlength:<N> uncompressedlength:<N>` when the value is compressed. (Q13)
- **R2.7.3** **`MEMORY USAGE key`** returns compressed footprint (frame + header + `robj` overhead). Matches what eviction and `maxmemory` actually see. (Q13, Q14)
- **R2.7.4** **`TYPE`**, **`OBJECT FREQ`**, **`OBJECT IDLETIME`**: unchanged. (Q13)
- **R2.7.5** **Module API** (`ValkeyModule_StringDMA` read intent, `ValkeyModule_OpenKey`): decompresses transparently via `objectGetUncompressedView`. Existing modules need no changes. (Q13)
- **R2.7.6** **Module API** (`ValkeyModule_StringDMA` write intent on a compressed value): decompresses in place first (mutates `robj` to `raw`, frees the compressed frame), then returns the sds pointer. The value becomes eligible for re-compression on the next sweep tick. (Q13)

### 2.8 Memory accounting

- **R2.8.1** Eviction sampler, `MEMORY USAGE`, and `INFO memory` use the compressed footprint for compressed values, via standard `zmalloc_size` / `used_memory` accounting. No custom accounting layer is introduced. (Q14)
- **R2.8.2** Fixed overhead (CCtx/DCtx, digested dicts in the registry, candidate queue) is allocated via `zmalloc` → counted in `used_memory`. `INFO compression` additionally reports it under `compression_net_saved_bytes`. (Q14)
- **R2.8.3** `MEMORY STATS` sub-aggregate for compression is v2. (Q14)

### 2.9 Scripting and transactions

- **R2.9.1** **Rule 1 — sync-mandatory**: decompression MUST run synchronously on the main thread inside `EVAL`/`EVALSHA`/`FCALL`/`FCALL_RO`, inside queued `MULTI`/`EXEC` transactions, on the replication feed, and on the AOF writer. v1's always-sync model satisfies this automatically. Rule remains as a forward-compatibility constraint for v2. (Q11)
- **R2.9.2** **Rule 2 — no `signalModifiedKey`**: background compression and in-place decompression **must not** call `signalModifiedKey`. This is enforced via the single-entry-point helper (R2.5.2) plus code comments plus tests. (Q11)

### 2.10 Observability

- **R2.10.1** New `INFO compression` section with the following fields (see §5.6):
  `compression_enabled`, `compression_state`, `compression_active_dict_id`, `compression_dict_age_seconds`, `compression_known_dicts`, `compression_dict_cap_reached`, `compression_compressed_objects`, `compression_total_uncompressed_bytes`, `compression_total_compressed_bytes`, `compression_ratio`, `compression_live_ratio_10m`, `compression_net_saved_bytes`, `compression_candidates_pending`, `compression_candidates_dropped_total`, `compression_sweep_backpressure_total`, `compression_sweep_pacing_sleeps_total`, `compression_outbox_backpressure_total`, `compression_compressions_per_sec`, `compression_decompressions_per_sec`, `compression_skipped_incompressible`, `compression_training_last_duration_ms`, `compression_training_last_sample_count`, `compression_errors_total`. (Q10)
- **R2.10.2** Latency-monitor events `compress-sync`, `decompress-sync`, `compression-train` use the existing `latency-monitor-threshold` floor. No new config. (Q10)
- **R2.10.3** **No keyspace notifications** for compression events. Operator audit trail is provided by server log entries at `LL_NOTICE` (normal transitions) and `LL_WARNING` (training failures, cap reached). (Q10)
- **R2.10.4** **Queue back-pressure observability contract.** The worker pool is fed by a single shared bounded queue (§4.6), so "compression is not keeping up" has several distinct root causes and each one has a different remediation. The `INFO compression` section exposes four counters plus one gauge so operators can disambiguate without reading logs:

  | Metric | Increments when | Remedy if climbing |
  |---|---|---|
  | `compression_candidates_pending` *(gauge)* | sampled at INFO time — current inbox depth | — (baseline signal) |
  | `compression_candidates_dropped_total` | write-path enqueue (`dbAdd` / `dbOverwrite` / `dbSetValue`) or multi-key fan-out enqueue finds the inbox full and drops the candidate | raise `compression-threads`; the sweeper will catch dropped keys on its next tick, but while this counter rises some keys are temporarily uncompressed |
  | `compression_sweep_backpressure_total` | the background or manual sweep pauses its iteration cursor because the inbox is full (distinct from CPU-pacing) | raise `compression-threads`; the sweep is ready to generate more work but workers can't absorb it |
  | `compression_sweep_pacing_sleeps_total` | the sweep sleeps because of `compression-sweep-max-cpu-pct` (normal operation) | raise `compression-sweep-max-cpu-pct` if faster keyspace coverage is desired |
  | `compression_outbox_backpressure_total` | a worker retries posting a result because the outbox is full (main thread hasn't drained fast enough) | raise the `compressionAfterSleep` drain budget; generally indicates a bigger main-loop problem |

  **Why `sweep_backpressure_total` and `sweep_pacing_sleeps_total` are distinct:** the sweep has two reasons to sleep — workers can't keep up vs. operator-configured pacing — and they have different remedies (more workers vs. looser pacing). Folding them into one counter would prevent operators from telling which knob to turn.

  **Why write-path drops and future multi-key fan-out drops share one counter:** both are "caller dropped a candidate because the inbox was full" and both are resolved by the same remediation. Distinguishing them does not drive a different action; splitting later is non-breaking if operational experience argues for it.

### 2.11 CPU and concurrency

- **R2.11.1** Dedicated compression worker pool, sized by `compression-threads` (int, default `1`, range `0..16`, `MODIFIABLE_CONFIG`). `0` = disabled (feature becomes no-op). Separate from `io-threads`. (Q8)
- **R2.11.2** Sweep pacing: `compression-sweep-max-cpu-pct` (int, default `25`, range `1..100`, `MODIFIABLE_CONFIG`). Applied only to background sweep batches; training and multi-key compression are naturally arrival-bounded. (Q8)
- **R2.11.3** CPU pinning: `compression_cpulist` (string, default empty). Follows the existing `bio_cpulist` / `aof_rewrite_cpulist` precedent. (Q8)
- **R2.11.4** Compression workers never touch `robj`, never mutate the dictionary registry, and never manage frame refcounts. They atomically load the active dict pointer, use the immutable `CDict*` for compression, produce a flat output buffer, and report a quiescent state. The main thread owns all `robj` mutation, registry mutation, and frame-ref accounting. Dictionary lifetime safety is guaranteed by the QSBR grace-period model (§4.4), not by per-job refcounting. (Q8)

### 2.12 Configuration summary

All configs below are `MODIFIABLE_CONFIG` and persist via `CONFIG REWRITE`. Configs are split into two tiers:

- **Primary** — every operator enabling the feature should review and set these consciously. Documented prominently in `valkey.conf` under a `# Compression` section.
- **Advanced** — defaults are tuned for the common workload (see §2.5 and §2.9 rationales). Touch only for specific tuning; `valkey.conf` groups these below a clearly-labeled `# Compression — advanced tuning` divider so operators aren't required to reason about them.

All configs remain in code and in `CONFIG GET *` / `CONFIG SET`; the split is documentation-only, not a hiding mechanism.

#### Primary knobs (5)

| Name | Type | Default | Scope |
|---|---|---|---|
| `compression-enabled` | bool | `no` | master switch |
| `compression-threads` | int | `1` | worker pool size (0..16; 0 = disabled) |
| `compression-min-value-size` | bytes | `256` | lower size bound for eligibility |
| `compression-max-value-size` | bytes | `131072` | upper size bound (0 = unbounded; default 128 KiB bounds worst-case sync decompression latency) |
| `compression-dict-size` | bytes | `102400` | zstd trainer target dict size |

#### Advanced knobs (10)

| Name | Type | Default | Scope |
|---|---|---|---|
| `compression-sweep-max-cpu-pct` | int | `25` | sweep pacing (1..100) |
| `compression_cpulist` | string | `""` | CPU pinning |
| `compression-min-savings-ratio` | percent | `10` | post-compression net-savings guard |
| `compression-lfu-threshold` | int | `5` | LFU skip-hot-key guard (only active in LFU eviction mode) |
| `compression-min-idle-seconds` | seconds | `60` | LRU/noeviction time-based skip; applies to `lru_idle_secs(obj)`. Inactive in LFU mode (LFU branch uses `compression-lfu-threshold` instead). |
| `compression-dict-first-training-keys-count` | int | `10000` | first-training trigger |
| `compression-dict-drift-ratio` | percent | `70` | retrain drift trigger; fires when `compression_live_ratio_10m > post_training_ratio / drift_ratio` (R2.3.5). Lower values mean less tolerance for degradation (drift fires sooner). |
| `compression-dict-refresh-interval` | seconds | `0` | optional periodic retrain (0 = disabled) |
| `compression-dict-max-versions` | int | `4` | registry cap (min 2) |

---

## 3. Architecture overview

### 3.1 High-level view

```mermaid
graph TB
    subgraph Clients
        C1[Client]
    end

    subgraph MainThread[Main thread]
        AE[Event loop ae.c]
        CMD[processCommand]
        LK[lookupKey* - db.c]
        GV[objectGetUncompressedView]
        DB[db.c / kvstore]
        PROP[Replication / AOF feed]
        SWEEP[Sweep cron tick]
        POLL[Outbox poll afterSleep]
        REG[Dictionary registry]
    end

    subgraph Workers[Compression worker pool]
        W1[worker 1]
        Wn[worker N]
    end

    subgraph Bio[bio thread pool]
        TR[BIO_COMPRESSION_TRAIN]
    end

    subgraph Queues
        INBOX[SPMC candidate inbox]
        OUTBOX[MPSC compressed outbox]
    end

    C1 --> AE
    AE --> CMD
    CMD --> LK
    LK --> GV
    GV -. decompress sync .-> REG
    GV --> DB
    CMD --> PROP
    PROP --> GV

    SWEEP --> INBOX
    DB --> INBOX
    INBOX --> W1
    INBOX --> Wn
    W1 -. ZSTD_compress_usingCDict .-> OUTBOX
    Wn -. ZSTD_compress_usingCDict .-> OUTBOX
    OUTBOX --> POLL
    POLL --> DB
    POLL --> REG

    TR -. ZDICT_trainFromBuffer .-> REG
```

### 3.2 Hot-path seams

All feature integration happens at a small, well-defined set of seams:

- **Read path** (`lookupKey*` in `src/db.c`): every type-command handler already funnels through this. Handlers that read value bytes (`getCommand`, `appendCommand`, etc.) call `objectGetUncompressedView` to get a decompressed view.
- **Write path** (`dbAddInternal`, `dbSetValue`, `dbOverwrite` in `src/db.c`): on insert/overwrite, check eligibility and enqueue on the candidate inbox. Compression itself happens later, off-thread.
- **Replication feed** (`feedReplicationBufferWithObject` in `src/replication.c`): routes through `objectGetUncompressedView`.
- **AOF writer**: routes through `objectGetUncompressedView`.
- **RDB writer/reader** (`src/rdb.c`): new encoding marker `RDB_ENC_COMPRESSED` + AUX entries for dicts.
- **Cron** (`serverCron` in `src/server.c`): sweep tick enqueues candidates; dict age / drift is evaluated.
- **`afterSleep` hook**: polls the MPSC outbox and installs compressed frames.

### 3.3 Threading model

```mermaid
graph LR
    Main[Main thread<br/>- command dispatch<br/>- robj mutation<br/>- registry install/retire<br/>- sync decompress<br/>- outbox poll]
    Pool[Compression workers<br/>ZSTD_compress_usingCDict<br/>flat buffers only]
    Bio[bio worker<br/>BIO_COMPRESSION_TRAIN<br/>keyspace scan + ZDICT_trainFromBuffer]
    IO[io_threads<br/>socket r/w - unchanged]

    Main -->|SPMC inbox| Pool
    Pool -->|MPSC outbox| Main
    Main -->|bio queue| Bio
    Bio -->|main-thread install| Main
```

Separation invariants:
- The worker pool is independent of `io-threads`. They are sized and scheduled separately.
- Workers never touch `robj` or mutate the registry — see §2.11 R2.11.4 for the full worker contract and §4.4 for the QSBR lifetime model.
- `bio` is reused for training (one-at-a-time, long-running); not for per-value compression.
- Synchronous decompression runs on the main thread directly; no offload.

---

## 4. Components and interfaces

All new source files live under `src/`. Every `.c` is registered in **both** `src/Makefile` (in the appropriate `ENGINE_*_OBJ` list) and `src/CMakeLists.txt` per the repo convention.

### 4.1 New source files

| File | Role |
|---|---|
| `src/compression.c` / `compression.h` | Public entry points: `compressionInit`, `compressionCron`, `compressionToggle`, `objectGetUncompressedView`, `compressionIsEligible`, `compressionEnqueueCandidate`, `compressionAfterSleep`, `infoCompression`. |
| `src/compression_registry.c` | Dictionary registry: add / lookup-by-dictID / promote / retire, refcounting, cap enforcement. |
| `src/compression_workers.c` | Worker pool: thread startup/shutdown, SPMC inbox, MPSC outbox, sweep pacing. |
| `src/compression_train.c` | Training: keyspace-scan sample collector, `ZDICT_trainFromBuffer` invocation (called by `bio`). |
| `src/compression_header.c` | Per-value header encode/decode, `OBJ_ENCODING_COMPRESSED` allocation / free helpers. |
| `src/commands/compression-*.json` | Subcommand JSON metadata for `COMPRESSION *` (see §4.5). |

### 4.2 Touched existing files

| File | Change |
|---|---|
| `src/server.h` | New encoding constant `OBJ_ENCODING_COMPRESSED`; global `compressionState` struct; forward declarations. |
| `src/server.c` | Call `compressionInit` at startup; wire `compressionCron` into `serverCron`; wire `compressionAfterSleep` into the event-loop `afterSleep` hook; add `infoCompression` to `genValkeyInfoString`. |
| `src/object.c` | `createCompressedObject`; `freeStringObject` frees compressed buffers correctly; `OBJECT ENCODING` returns `"compressed"`. |
| `src/config.c` | Register `compression-*` configs; register `compression_cpulist`. |
| `src/db.c` | `lookupKey*` does not call the decompression helper itself — that is done by the type-command handlers. `dbAddInternal`, `dbSetValue`, `dbOverwrite` call `compressionEnqueueCandidate(obj)` after the new value is installed. |
| `src/t_string.c` | `getCommand`, `appendCommand`, `strlenCommand`, `getrangeCommand`, `setrangeCommand` etc. route through `objectGetUncompressedView` for reads and decompress-in-place for writes on compressed values. |
| `src/replication.c` | `feedReplicationBufferWithObject` routes through `objectGetUncompressedView`. |
| `src/aof.c` | `feedAppendOnlyFile` path routes through `objectGetUncompressedView`. |
| `src/rdb.c` | `RDB_ENC_COMPRESSED` encode/decode; AUX emission/load for dictionary bytes; bump `RDB_VERSION` to `81`. |
| `src/rdb.h` | `#define RDB_ENC_COMPRESSED 4`; bump `RDB_VERSION`. |
| `src/debug.c` | `DEBUG OBJECT` prints `dictID`, `compressedlength`, `uncompressedlength` for compressed values. |
| `src/evict.c` | No change — `zmalloc_size`-based accounting handles compressed robjs automatically. |
| `src/module.c` | `RM_StringDMA` (read): transparently returns decompressed view. `RM_StringDMA` (write) on compressed value: decompress in place first. |
| `src/bio.h`, `src/bio.c` | New job type `BIO_COMPRESSION_TRAIN`, new worker slot in `bio_job_to_worker`. |

### 4.3 Public internal API — `compression.h`

```c
/* Lifecycle */
void compressionInit(void);
void compressionCron(void);             /* called from serverCron */
void compressionAfterSleep(void);       /* called from event-loop afterSleep */

/* Toggle */
void compressionToggle(int enabled);    /* config hook */

/* Read path (hot) */
robj *objectGetUncompressedView(robj *o, sds *scratch);

/* Write path (main thread) */
int  compressionIsEligible(robj *o);
void compressionEnqueueCandidate(robj *o, robj *key, int dbid);

/* Operator surface (COMPRESSION subcommands call these) */
int  compressionForceTrain(client *c);
int  compressionSweep(client *c, int direction /* compress|decompress */);
int  compressionDictList(client *c);
int  compressionDictExport(client *c, uint32_t dictID);
int  compressionDictImport(client *c, const unsigned char *bytes, size_t len);
int  compressionDictDrop(client *c, uint32_t dictID);
int  compressionStatus(client *c);

/* INFO */
void infoCompression(sds info);
```

### 4.4 Dictionary registry — `compression_registry.c`

```c
typedef enum { DICT_STATE_ACTIVE, DICT_STATE_RETIRING, DICT_STATE_RETIRED } compressionDictState;

typedef struct compressionDict {
    uint32_t        dictID;
    unsigned char  *bytes;          /* raw training output or imported bytes */
    size_t          bytes_len;
    ZSTD_CDict     *cdict;          /* immutable after publication; NULL only after free */
    ZSTD_DDict     *ddict;
    size_t          frame_refs;     /* number of installed compressed frames referencing
                                       this dictID (main-thread only) */
    compressionDictState state;
    mstime_t        promoted_at_ms;
    uint64_t        retire_worker_gen[COMPRESSION_WORKERS_MAX];
                                    /* per-worker quiescent-gen snapshot taken at
                                       retirement time (see QSBR model below) */
    int             retire_n_workers;
                                    /* number of live workers at retirement time
                                       (server.compression_threads captured during
                                       startRetirement). canFree iterates only
                                       min(retire_n_workers, current n_threads).
                                       Handles resize cleanly: a slot occupied by
                                       a worker spawned AFTER retire couldn't have
                                       observed this dict; a slot vacated by a
                                       worker joined via resize-down can no longer
                                       hold a pointer. */
} compressionDict;

typedef struct compressionRegistry {
    compressionDict *dicts[COMPRESSION_DICT_MAX];  /* sized by compression-dict-max-versions */
    int              n_dicts;
    _Atomic(compressionDict *) active;  /* published via atomic store; workers load atomically */
    /* counters, error totals, etc. */
} compressionRegistry;
```

#### Dictionary lifetime model — QSBR (Quiescent-State-Based Reclamation)

The registry uses a grace-period reclamation model inspired by the Linux kernel's RCU (Read-Copy-Update) pattern. The motivation is **decoupling**: the main thread owns the dictionary registry exclusively (writes, retirement, GC, frame-ref accounting), while compression workers operate against an immutable, self-managed view of the active dictionary. Workers never call into the registry on the per-job hot path. The main thread retires dicts safely without having to track in-flight jobs — it observes worker generation counters directly.

**Ownership split:**

| Owner | Responsibilities |
|---|---|
| Main thread | Dictionary creation, promotion (atomic publish), retirement, frame-ref accounting, GC, final free |
| Workers | Load active pointer (atomic read), use immutable `CDict*` for compression, report quiescent state after each job |

**How it works:**

1. **Promotion:** Main thread creates a new `compressionDict`, atomically stores it as `registry->active`. The previous active dict is retired via `compressionDictStartRetirement()` (state set to RETIRING, worker generations snapshotted).

2. **Worker usage:** A worker atomically loads `registry->active` to get the current dict pointer. The `compressionDict` and its `CDict*` are immutable after publication — safe to read without locks. The worker uses the `CDict*` for compression, then reports a quiescent state.

3. **Quiescent reporting:** After finishing a job (including all `CDict` usage), the worker advances its per-worker `quiescent_gen` counter. This signals: "I no longer hold any dict pointer obtained before this point."

4. **Retirement:** When the main thread retires a dict, it snapshots every worker's current `quiescent_gen` into `dict->retire_worker_gen[]`. The dict cannot be freed until every worker has advanced past its snapshotted value.

5. **GC:** Periodically (from `compressionCron`, after result drain, etc.), the main thread calls `compressionDictTryGc()` which checks each retiring dict: if `frame_refs == 0` AND all workers have crossed the grace period, the dict is freed.

6. **Grace barriers (wake-all via cond_broadcast):** If a worker is idle (blocked on the SPMC inbox cond var waiting for work), it may never advance its generation. The main thread forces progress by issuing a wake-all on the inbox (see §4.6 "wake-all primitive"). Every blocked consumer wakes simultaneously via `pthread_cond_broadcast`, advances its generation if a barrier signal is set, then either resumes consuming or re-blocks. Enqueueing barrier jobs into the SPMC inbox is *not* sufficient under work-stealing semantics — a single worker could drain all barriers while siblings stay asleep on the cond var.

7. **Bounding retiring dicts (cap interaction with R2.3.3):** Retiring dicts remain in `dicts[]`, which is capped at `compression-dict-max-versions` (R2.3.3, default 4). Each retiring dict occupies a slot until step 5 reclaims it. Under normal load the grace-barrier mechanism (step 6) keeps reclamation latency bounded and the cap is not hit. If draining cannot keep up — e.g. workers are starved, or `frame_refs` stays > 0 on retiring dicts because old frames are not being rewritten/expired — the cap is reached and **both training and promotion are refused** per R2.3.3: a `LL_WARNING` log entry is emitted, `compression_dict_cap_reached` is set to `1` in `INFO`, and the operator must intervene (raise the cap, or run `COMPRESSION SWEEP` to force-rewrite frames referencing the oldest retiring dict so it can drain). No separate retiring list is maintained — GC scans `dicts[]` directly (max 16 entries).

**Why QSBR over per-job refcounting:**

Two approaches were considered:

| | Per-job refcount | QSBR grace-period |
|---|---|---|
| Worker contract | Receives `CDict*` in job struct; main thread manages inc/dec per job | Loads active pointer when ready; reports quiescent after use |
| Pointer lifetime | Caller must decRef on every completion path (success, error, discard) | Structural guarantee — dict outlives all workers that observed it |
| Encapsulation | Job carries a cross-thread pointer whose lifetime depends on external discipline | Worker self-serves; registry internally guarantees safety |
| Failure mode of a bug | Memory leak (missed decRef) or use-after-free (early decRef) | Delayed reclamation (missed quiescent report) — safe direction |
| API surface | incRef, decRef, pass pointer in job | loadActive, reportQuiescent, barrier |

The QSBR approach was chosen because:
- It avoids passing lifetime-managed pointers across thread boundaries: workers never call into the registry on the per-job hot path, and the registry retires dicts by observing worker generation counters directly rather than tracking in-flight jobs.
- The worker contract is minimal and hard to misuse: load, use, report done. No refcount management on any control path.
- Failure modes are safe-directional: a missed quiescent report delays reclamation but cannot cause use-after-free.
- The complexity is concentrated in the registry (written once, tested thoroughly) rather than distributed across every job completion path.

### 4.5 `COMPRESSION` subcommand container

| Subcommand | Summary | ACL |
|---|---|---|
| `COMPRESSION ENABLE` | Set `compression-enabled yes`; log `LL_NOTICE`. | `@admin` |
| `COMPRESSION DISABLE` | Set `compression-enabled no`; log `LL_NOTICE`. | `@admin` |
| `COMPRESSION TRAIN` | Submit an immediate `BIO_COMPRESSION_TRAIN` job. | `@admin` |
| `COMPRESSION SWEEP [direction=compress|decompress]` | Trigger a full-keyspace sweep. | `@admin` |
| `COMPRESSION STATUS` | Returns the `INFO compression` section as a flat structured reply. | `@read` |
| `COMPRESSION DICT LIST` | Returns an array per registry entry: `{dictID, state, age_ms, refcount, bytes_len}`. | `@admin` |
| `COMPRESSION DICT EXPORT <dictID>` | Returns base64-encoded dictionary bytes. | `@admin` |
| `COMPRESSION DICT IMPORT <base64-bytes>` | Installs as a new dict; atomic promotion. | `@admin` |
| `COMPRESSION DICT DROP <dictID>` | Force-retires a dict. Fails if `refcount > 0`. | `@admin` |
| `COMPRESSION HELP` | Subcommand listing. | `@read` |

Each subcommand has a JSON file under `src/commands/compression-*.json` with arity, flags, reply schema. `utils/generate-command-code.py` regenerates `src/commands.def`.

### 4.6 Queue primitives

Reuse the existing queue primitives from `src/queues.h`:
- **`spmcQueue`** — main thread (producer) enqueues candidates; N workers dequeue. Matches `io_threads` shared inbox shape.
- **`mpscQueue`** — N workers produce compressed results; main thread consumes. Matches `io_threads` outbox shape.

**Shared queues, not per-worker.** The feature uses a single shared SPMC inbox fed by the main thread and drained by the worker pool, plus a single shared MPSC outbox producing back to the main thread. No per-worker queues. Rationale: (a) work-stealing falls out by construction — idle workers race to dequeue, so if one worker stalls (cold CDict cache, a large value, kernel throttling) the others keep draining without any scheduler decision; (b) enqueue is a single ring-buffer CAS regardless of pool size — per-worker queues would need "pick a worker" logic (round-robin, shortest-queue, work-stealing) that cost more on the main thread and balance worse when jobs vary in cost; (c) same shape as `io_threads.c`, minimizing review surface and reusing the queue primitives directly.

**Sizing.** Inbox capacity = `max(256, 128 * compression-threads)`; outbox capacity equals inbox capacity (never more results in flight than jobs). For `compression-threads=16` that's 2048 slots × ~48 B per `compressionJob` ≈ 100 KB — immaterial for a memory-saving feature. The floor of 256 ensures a single-thread pool still absorbs burstiness. No new config knob in v1; the formula is computed at pool-start from `compression-threads`. If operational experience shows the formula is wrong, exposing `compression-candidate-queue-size` later is a non-breaking addition.

**Back-pressure policy.** Every caller must treat "inbox full" as a recoverable condition; the specific handling differs by caller so operators can diagnose the cause:

| Caller | Policy on inbox full | Counter |
|---|---|---|
| Write-path hook (`dbAdd`/`dbOverwrite`/`dbSetValue`) | Drop the candidate; the sweeper will re-discover the key on its next tick. | `compression_candidates_dropped_total` |
| Background sweeper | Pause iteration at the current shard cursor and return from the tick; resume from the same cursor next tick. Distinct from CPU-pacing sleeps. | `compression_sweep_backpressure_total` |
| Manual `COMPRESSION SWEEP` | Same as background sweeper — pause at cursor, resume when inbox has room. An explicit operator command should eventually make progress, not silently drop. | `compression_sweep_backpressure_total` |
| Multi-key compression fan-out (future) | Drop; the main compression path will re-enqueue the affected keys on subsequent writes or sweep ticks. | `compression_candidates_dropped_total` |

The outbox side has its own back-pressure: if a worker has a result to post but the MPSC outbox is full (main thread has not drained `compressionAfterSleep` often enough), the worker retries rather than drops — discarding a completed compression would waste CPU work already done. Retries are observed via `compression_outbox_backpressure_total`. Steady-state outbox saturation indicates a main-loop problem rather than a compression-feature problem, but surfacing the counter keeps the diagnostic honest.

Both back-pressure events are categorized separately from the normal operating signals (`compression_candidates_pending` gauge, `compression_sweep_pacing_sleeps_total`) so operators can identify the exact root cause without reading logs. See §2.10 R2.10.4 for the remediation table.

**Wake-all primitive (used by QSBR grace barriers — §4.4 step 6).** The QSBR model needs a way to advance the generation counter of every worker, including idle workers blocked on the inbox cond var. The existing `mutexqueue.h` (which provides the pthread-cond-var-based blocking semantics underneath the SPMC inbox) is extended with **a wake-aware pop variant plus an explicit broadcast primitive** that lets new callers opt in to wake-all semantics without changing the contract for existing callers (e.g. `bio.c`):

- `mutexQueueWakeAll(q)` — calls `pthread_cond_broadcast` on the queue's cond var. Every consumer parked in `pthread_cond_wait` exits simultaneously.
- `mutexQueuePop(q, blocking=true)` — **unchanged contract**: never returns NULL when blocking. If the cond_wait wakes spuriously (POSIX-spec or because of a `mutexQueueWakeAll`), the loop re-parks until a real item arrives. Existing callers (`bio.c`) keep their original behavior.
- `mutexQueuePopWakable(q, blocking=true)` — **new**: returns NULL once on a spurious wake or wake-all. Callers use this when they want to be notified by `mutexQueueWakeAll` so they can perform a per-loop housekeeping step (e.g. advance a QSBR generation counter) before re-entering the wait.

The compression worker loop uses `mutexQueuePopWakable`; bio uses `mutexQueuePop`.

**Sentinel-based shutdown (used by pool teardown).** Wake-all alone is unsafe for shutdown: there is a small race window where a worker has read its `shutdown_requested` flag at the top of the loop and is about to enter `mutexQueuePopWakable`. If `compressionWorkersStop` fires its broadcast in that window, the broadcast hits no waiters and is lost; the worker then parks and deadlocks against `pthread_join`.

The pool teardown therefore uses **shutdown sentinels** — N pointers to a static address are pushed into the inbox via `mutexQueueAdd` (one per worker). Each parked consumer's `mutexQueuePopWakable` sees length>0 under the mutex and pops a sentinel without entering `cond_wait`. The race is impossible because the sentinels are data-in-queue, not a transient broadcast. Workers distinguish jobs from sentinels by pointer equality with the static address (zero memory cost).

Wake-all is reserved for grace barriers because **missed barrier wakes are benign** — the next event (real job, another retire) catches the worker. Shutdown is correctness-critical and cannot tolerate the race.

Enqueueing N "barrier jobs" into the SPMC inbox is **not** equivalent to a wake-all for grace barriers either: under work-stealing semantics the dequeue is not round-robin, so a single fast worker can drain all barrier jobs while siblings stay asleep on the cond var. The cond_broadcast path is the only mechanism that guarantees every worker wakes up.

Job structure:

```c
typedef struct compressionJob {
    robj         *key;         /* key name, used to resolve robj later */
    int           dbid;
    uint64_t      version;     /* robj version counter; detects concurrent rewrites */
    sds           src;         /* value sds at enqueue time (held via incrRefCount).
                                  The worker reads sdslen(src) to get the length — no
                                  separate src_len needed, and safe across threads
                                  because the immutable-snapshot invariant (R2.4.4)
                                  guarantees the sds metadata bytes are not mutated
                                  while the worker holds the reference. */
    /* filled by worker: */
    uint32_t      dict_id;     /* dict_id of the dict the worker used (loaded at
                                  compress time from the active pointer); carried into
                                  the compressed frame header for decompression. */
    void         *dst;         /* zmalloc'd buffer: compressedHeader + compressed frame.
                                  Ownership transfers to the main thread on outbox
                                  delivery; main thread hands it to
                                  createCompressedObject() which installs it into the
                                  robj without a memcpy (see compression_header.h for
                                  the zero-copy ownership contract). */
    size_t        dst_len;     /* total bytes in `dst` (header + frame) */
    int           err;         /* 0 = ok, else ZSTD error */
} compressionJob;
```

**Concurrency notes**:
- Enqueue holds `incrRefCount(val)` so the sds pointer stays valid for the worker **and** the object has `refcount >= 2`, which forces any subsequent mutating command to COW instead of mutating in place (Valkey's `dbUnshareStringValue` discipline — see R2.4.4 and R2.4.5 for the invariant and its enforcement).
- On the outbox side, the main thread re-fetches the current `robj` for the key; if it has changed (version counter moved), the compressed result is discarded.
- `decrRefCount(val)` is called after the outbox handler finishes. This drops the refcount back to 1 and restores in-place-mutate eligibility for future commands.
- The worker loads the active dict pointer atomically at compress time (not at enqueue time). The dict pointer is guaranteed valid by the QSBR grace-period model (§4.4) — the dict cannot be freed until all workers have reported a quiescent state after retirement.
- After compression (regardless of success or error), the worker calls `compressionWorkerReportQuiescent()` to advance its generation counter. This is the single synchronization obligation of the worker.

**Why the worker loads the active dict itself** (rather than receiving it in the job): the QSBR model (§4.4) structurally guarantees that any dict pointer loaded by a worker remains valid until the worker reports quiescent. This decouples worker code from registry lifetime concerns — the worker contract is minimal: load the active dict when ready, use it, report done. No registry mutation, no per-job refcount management, no pointer-in-queue lifetime concerns. The main thread, which owns the registry, observes worker generation counters to determine when retired dicts are safe to free; it does not have to track in-flight jobs.

**Why the worker produces a flat `dst` buffer and not an `robj`**: this is the §2.11 R2.11.4 invariant — compression workers never touch `robj`. Three reasons it stays this way: (1) `robj` manipulation in Valkey assumes single-threaded access (no atomics on refcount, shared-object singletons, LRU/LFU bit updates, encoding-tag swaps); moving `robj` work to workers would silently break those assumptions. (2) Failed compressions (net-savings guard in R2.4.3) are cheap to discard — `zfree(dst)` — rather than allocate-and-free an entire `robj` container. (3) The `robj` container still has to be allocated and installed on the main thread anyway, because it carries LRU/LFU bits inherited from the old object and has to be swapped into the kvstore. Worker-side flat buffer + main-thread `createCompressedObject(buffer, len)` splits the work along the invariant without a memcpy — the zero-copy ownership contract in `compression_header.h` makes this explicit.

---

## 5. Data models

### 5.1 `robj` encoding tag

A new value in the `OBJ_ENCODING_*` enum (4-bit field in `robj`, room available):

```c
#define OBJ_ENCODING_COMPRESSED 12   /* value = pointer to compressedBuffer */
```

Layout for a compressed `robj`:

```
robj { type=OBJ_STRING, encoding=OBJ_ENCODING_COMPRESSED, hasembval=0, val_ptr → compressedBuffer }
```

### 5.2 Per-value compressed buffer

```
+---------------------------+---------------------------+
| compressedHeader          | compressed frame bytes    |
|   (16 bytes total)        |                           |
+---------------------------+---------------------------+

compressedHeader {
    uint32_t alg_magic;      /* algorithm tag, doubles as magic:
                                'Z','S','T','D' (0x4454535A) = ZSTD + trained dict
                                reserved: 'L','Z','4',' ' (0x20345A4C) = LZ4
                                unknown → reject as corrupt */
    uint32_t alg_meta;       /* per-algorithm metadata:
                                ZSTD = dict_id of the registry entry
                                LZ4  = 0 (reserved for future flags) */
    uint32_t uncompressed_len;
    uint32_t compressed_len; /* frame bytes, excluding this header */
}
```

**Why a generic algorithm tag** (rather than a bare `dict_id`): the in-memory header and the matching on-disk RDB layout (§2.6 R2.6.1) are write-once formats. Reserving an explicit `alg_magic` in Phase 0 lets v2 add LZ4 / snappy / hardware-accelerated backends without another encoding-byte migration. The tag doubles as corruption magic (wrong pattern → reject), so there is no size cost for the generality. v1 only emits / accepts the ZSTD magic.

Total on-heap footprint per value = `sizeof(compressedHeader) + compressed_len + robj overhead`. `zmalloc_size` reports this automatically.

**Ownership contract for the buffer** (see `compression_header.h`): the buffer is allocated by the compression worker via `zmalloc`. The worker writes the header at offset 0 and compresses directly into the remaining space. `createCompressedObject(buffer, len)` takes ownership of `buffer` — no memcpy — and the `robj` it returns reclaims the buffer via `zfree` at free time. The Phase 1 installer MUST preserve the zero-copy contract; copying the buffer into a new allocation would silently shift the compressed payload's memcpy cost onto the main thread's hot path.

**Worker shrink decision** (affects the compressed-size-vs-allocated-size story): `ZSTD_compress_usingCDict` requires the worker to allocate up to `ZSTD_compressBound(src_len)` before compression because the actual output size is known only after the call returns. For typical 1 KB values this over-allocates by ~500 B. The worker shrinks the buffer to `sizeof(compressedHeader) + actual_compressed_len` via `zrealloc` before posting to the outbox. This may copy if the shrink crosses a jemalloc size-class boundary, but the copy is (a) of the compressed bytes only — at most a few hundred bytes to a few KB — and (b) paid on the worker thread, not on the main-thread hot path. The alternative of not shrinking would leak the bound's slack into `used_memory`, undermining the feature's memory-saving goal.

### 5.3 RDB encoding

`RDB_ENC_COMPRESSED = 4` (new). When the writer emits a compressed string it writes:

```
[ 0xC0 | RDB_ENC_COMPRESSED ]      // one byte (RDB_ENCVAL | encoding)
[ len-encoded alg_magic ]           // algorithm tag, e.g. ASCII "ZSTD"
[ len-encoded alg_meta ]            // per-alg: ZSTD uses dict_id
[ len-encoded uncompressed_len ]
[ len-encoded compressed_len ]
[ compressed_len bytes of compressed frame ]
```

For the ZSTD+trained-dict algorithm, dictionary bytes are emitted as `RDB_OPCODE_AUX` entries (key = `"compression-dict-<dict_id>"`, value = raw dictionary bytes). **AUX entries for a dict MUST be written before any compressed value referencing it.** Algorithms that do not use dictionaries (e.g., plain LZ4 in a future version) omit AUX.

`RDB_VERSION` bumps `80 → 81`. Older loaders refuse the file (`rdb.c` already rejects unknown string encoding markers); new loaders accept both old and new files. Unknown `alg_magic` in a new-version file is rejected as corruption.

### 5.4 Global state

```c
typedef struct compressionState {
    int                  enabled;
    int                  state;             /* idle | training | active | disabled */
    compressionRegistry *registry;
    spmcQueue           *inbox;             /* candidates */
    mpscQueue           *outbox;            /* compressed results */
    pthread_t           *worker_tids;
    int                  n_workers;
    /* counters (updated on main thread only; workers post deltas via outbox) */
    uint64_t             compressed_objects;
    uint64_t             total_uncompressed_bytes;
    uint64_t             total_compressed_bytes;
    uint64_t             skipped_incompressible;
    uint64_t             errors_total;
    /* rolling EMAs */
    double               live_ratio_10m;
    double               compressions_per_sec;
    double               decompressions_per_sec;
    /* training state */
    mstime_t             last_training_duration_ms;
    size_t               last_training_sample_count;
    /* sweep state */
    int                  sweep_in_progress;
    int                  sweep_direction;
    size_t               sweep_shard_cursor;
    /* write-path counter for first-training trigger */
    uint64_t             eligible_keys_written;
} compressionState;

extern compressionState server_compression;
```

### 5.5 Candidate queue entry

See §4.6 `compressionJob`.

### 5.6 `INFO compression` field specification

| Field | Source |
|---|---|
| `compression_enabled` | `server_compression.enabled` |
| `compression_state` | `server_compression.state` |
| `compression_active_dict_id` | `registry->active->dictID` or `0` |
| `compression_dict_age_seconds` | `(now - registry->active->promoted_at_ms) / 1000` |
| `compression_known_dicts` | `registry->n_dicts` |
| `compression_dict_cap_reached` | set when promotion was blocked; cleared on successful promotion or cap raise |
| `compression_compressed_objects` | `server_compression.compressed_objects` |
| `compression_total_uncompressed_bytes` | accumulator |
| `compression_total_compressed_bytes` | accumulator (includes headers; excludes fixed overhead) |
| `compression_ratio` | `total_compressed / total_uncompressed` |
| `compression_live_ratio_10m` | EMA, 10-minute window |
| `compression_net_saved_bytes` | `total_uncompressed - total_compressed - fixed_overhead_bytes` |
| `compression_candidates_pending` | queue depth (worker inbox) |
| `compression_candidates_dropped_total` | counter; incremented by write-path and multi-key fan-out enqueues when the inbox is full (see §2.10 R2.10.4, §4.6) |
| `compression_sweep_backpressure_total` | counter; incremented when the sweep pauses iteration because the inbox is full (distinct from CPU-pacing) |
| `compression_sweep_pacing_sleeps_total` | counter; incremented when the sweep sleeps because of `compression-sweep-max-cpu-pct` |
| `compression_outbox_backpressure_total` | counter; incremented when a worker retries posting a result because the outbox is full |
| `compression_compressions_per_sec` | EMA, 1-second window |
| `compression_decompressions_per_sec` | EMA, 1-second window |
| `compression_skipped_incompressible` | counter |
| `compression_training_last_duration_ms` | last value |
| `compression_training_last_sample_count` | last value |
| `compression_errors_total` | counter |

---

## 6. Error handling

### 6.1 Compression errors (worker path)

- `ZSTD_compress_usingCDict` returns an error size: worker posts the job with `err` set; main thread increments `compression_errors_total`, drops the result, leaves the value uncompressed, logs `LL_WARNING` at first error per minute (rate-limited).
- Worker allocation failure (`zmalloc` OOM for compressed buffer): same path as above.

### 6.2 Decompression errors (read path)

- `ZSTD_decompress_usingDDict` failure on read is a **data corruption event**. The server logs `LL_WARNING`, increments `compression_errors_total`, returns a `-ERR compressed value corrupt` reply to the client, and does not crash. Running `DEBUG DIGEST-VALUE` against the key post-facto aids diagnosis.
- Dictionary lookup miss (frame references a dictID not in the registry): treated as above. Should never happen in a healthy server because retirement only fires at refcount == 0.

### 6.3 Training errors

- Insufficient samples (fewer than `compression-dict-first-training-keys-count` eligible values in the keyspace): `BIO_COMPRESSION_TRAIN` aborts, logs `LL_WARNING`, schedules a retry on the next trigger.
- `ZDICT_trainFromBuffer` error: abort, log `LL_WARNING`, increment `compression_errors_total`, do not promote.

### 6.4 Dict cap reached

- Attempted promotion when `n_dicts == compression-dict-max-versions`: promotion refused, `compression_dict_cap_reached = 1`, log `LL_WARNING`. Continues serving existing compressed values; simply doesn't retrain. Cleared on next successful promotion.

### 6.5 RDB integrity

- `RDB_ENC_COMPRESSED` value with missing dictionary AUX → RDB rejected as corrupt. `valkey-check-rdb` reports which dictID is missing.
- Header magic mismatch on in-memory compressed value → logged `LL_WARNING`, treated as corruption (R6.2 path).

### 6.6 Net-savings guard failure

- Post-compression check determines the compressed form is not worth keeping: discard the compressed buffer, leave the value as-is, increment `compression_skipped_incompressible`. Not an error — normal operation for incompressible data. v1 does not track per-key rejection state; the next sweep tick will re-attempt compression of the same value (or whatever value is at that key by then). Under a fixed dict and a fixed input distribution, time-based throttling doesn't change the per-attempt hit probability — it only spaces attempts, which is what sweep pacing already does at the global level. Sustained high rejection rate inflates `compression_live_ratio_10m` (rejections contribute their actual measured ratio, typically in `[0.9, 1.05]`; see R2.10.1), trips the drift threshold (R2.3.5), and triggers retraining; after dict promotion the retry naturally succeeds (or fails again under the new dict, in which case the value is truly incompressible regardless of dict).

### 6.7 Worker thread crash

- A compression worker crashing takes the server down today (any thread crash in Valkey does). Workers execute a very small code surface (`ZSTD_compress_usingCDict` + memcpy); assertion failures would be in our code, not zstd's.

---

## 7. Testing strategy

Two tiers, per Q15.

### 7.1 Tier 1 — transparency mode

A new `--compression` flag on each Tcl test driver starts the server under test with an aggressive compression config that forces every eligible value through the compression path:

```
--compression-enabled yes
--compression-min-value-size 0
--compression-max-value-size 0            # no upper bound
--compression-lfu-threshold 255
--compression-min-idle-seconds 0
--compression-min-savings-ratio 0
--compression-dict-first-training-keys-count 10
--compression-threads 1
--compression-sweep-max-cpu-pct 100
```

Deliverables:
- `--compression` on `runtest`, `runtest-cluster`, `runtest-sentinel`, `runtest-moduleapi`.
- `make test-compression`, `make test-cluster-compression`, `make test-sentinel-compression`, `make test-moduleapi-compression`.
- New test tag `compression:skip` for the ≤~20 tests that legitimately assert on exact `OBJECT ENCODING`, exact `MEMORY USAGE`, or exact `DEBUG OBJECT` output.
- Test-author guidance added to `tests/README.md`.
- Helper `assert_string_encoding $key $expected_set` that accepts `compressed` alongside `raw`/`embstr`.
- CI: `.github/workflows/ci.yml` gains a `compression=on` matrix cell per suite.

### 7.2 Tier 2 — feature-specific tests

**Tcl tests** (under `tests/`):

- `tests/unit/compression.tcl` — core feature (toggle, sweep, eligibility, size bounds, post-compression guard, retry cooldown, per-key introspection).
- `tests/unit/compression-dict.tcl` — `COMPRESSION TRAIN`, `DICT LIST/EXPORT/IMPORT/DROP`, drift detection, dict-version cap.
- `tests/unit/compression-multi.tcl` — Q11 invariants (`WATCH` + background compression → `EXEC` does not abort; `CLIENT TRACKING` → no spurious invalidations; `EVAL` / `EXEC` semantics).
- `tests/unit/compression-cow-invariant.tcl` — **merge-blocker**: for every mutating string command (`APPEND`, `SETRANGE`, `SET` overwrite, `GETSET`, `GETDEL`, `SETBIT`, `BITOP` write, `BITFIELD` write) and for module write-DMA, verify that enqueueing a compression job then issuing the mutation leaves the original bytes intact for the worker and produces a correct compressed frame for the post-mutation state. Fails loud if any code path mutates in place while `refcount > 1`. Implements the R2.4.5 audit via a runtime test rather than a static audit, so it protects against future drift.
- `tests/unit/compression-persistence.tcl` — RDB save/load with active+retiring dicts; load with `compression-enabled no`; missing dict AUX rejection; AOF stays uncompressed; cross-version replication.
- `tests/integration/compression-replication.tcl` — primary compresses, replica does not; toggle on primary does not disrupt stream.
- `tests/unit/cluster/compression-migrate.tcl` — `MIGRATE` decompresses on source.
- `tests/unit/compression-transparency.tcl` — canary meta-test for a handful of commands.

**C++ gtest units** (under `src/unit/`):

- `src/unit/test_compression_registry.cpp` — add/lookup/promote/retire; refcount; cap enforcement.
- `src/unit/test_compression_eligibility.cpp` — every branch of the R2.2 predicate.
- `src/unit/test_compression_header.cpp` — encode/decode round-trips; malformed-header rejection.
- `src/unit/test_compression_sweep_pacing.cpp` — pacing math.

**Module API test** (under `tests/modules/`):

- `tests/modules/compression.c` — DMA (read/write) and `OpenKey` transparency on compressed keys.

### 7.3 Performance regression

One scenario added to the existing benchmark workflows:
- Workload: 80/20 GET/SET, 1 KiB values, warm cache.
- Compared: `compression-enabled no` vs. `yes` (default production config).
- Expected: ~30% lower `used_memory`; ≤20% TPS degradation.
- Status: **informational**, not a merge gate.

### 7.4 Reply-schema and formatting

- JSON reply schemas for all `COMPRESSION *` commands under `src/commands/`, validated by `.github/workflows/reply-schemas-linter.yml`.
- `clang-format-18` on all new sources.
- `.config/typos.toml` clean.

### 7.5 Compression-aware benchmark suite

The §7.3 primitive scenario (uniform keys, fixed 1 KiB values) validates a lower bound but does not exercise the feature's design assumptions — especially "skip hot items, compress cold items" (needs hotset skew) and "multi-key commands pay the sum of per-value costs" (needs mixed value sizes and wide-fanout commands). For v1 we therefore commit to extending `valkey-benchmark` with three general-purpose workload knobs and shipping a canonical scenario set.

**`valkey-benchmark` extensions** (each independently useful beyond this feature, which strengthens the upstream case):

| Flag | Semantics |
|---|---|
| `--key-distribution uniform\|zipf` with `--zipf-alpha ALPHA` | Default `uniform` (current behavior). `zipf` produces a skewed key-access distribution with Zipfian alpha (typical: 0.99 for GET-heavy caching). Gives us a real hotset to test the skip-hot-items policy. |
| `--value-size-distribution constant\|uniform:MIN:MAX\|lognormal:MEAN:SIGMA` | Default `constant` (current `-d SIZE` behavior). The mixed distributions exercise `compression-min-value-size`/`compression-max-value-size` boundaries and produce realistic MGET latency curves. |
| `--value-data random\|zero\|corpus:FILE` | Default `random` (current behavior — incompressible). `zero` = trivially compressible (upper bound on ratio). `corpus:FILE` = load a text corpus and sample substrings from it (produces realistic 50–70% compression ratios for JSON-like data). |

**Canonical scenario set** under `tests/compression/benchmarks/`:

| Scenario | Purpose |
|---|---|
| `baseline-uniform-1k` | 80/20 GET/SET, uniform keys, 1 KiB `random` values. Existing §7.3 scenario; regression floor. |
| `realistic-hotset` | GET-heavy, `zipf-0.99` keys, `lognormal:1024:2.0` sizes, `corpus:samples/json.txt`. Exercises skip-hot-items and realistic compression ratios. |
| `wide-mget` | `MGET` over 100 and 1000 random keys, `lognormal:1024:2.0` sizes, `corpus:samples/json.txt`. Validates the thread-1 concern (sum-of-per-value-costs for multi-key). |
| `sort-heavy` | `SORT` of a 1000-element list where every element is a compressed value. Stresses main-thread blocking in list/set operations. |
| `mixed-pipeline` | Pipelined GET/SET/APPEND with 20% writes on `zipf-0.99` keys. Exercises the COW invariant from R2.4.4 under realistic load. |

A `tests/compression/benchmarks/run.sh` driver runs each scenario under both `compression-enabled no` and `yes`, produces a comparison report (P50, P99, P999 latency per command type; `used_memory`; `compression_ratio` from `INFO compression`), and commits a reference JSON of accepted numbers into the repo. Future changes that regress beyond a per-metric threshold are flagged for reviewer attention but remain **informational**, consistent with §7.3 policy — not a merge gate.

CI runs only `baseline-uniform-1k` on every PR (cheap, seconds). The full suite runs nightly / on release candidates.

---

## Appendix A — Technology choices

### A.1 ZSTD with trained dictionary (chosen)

Reasons:
- Best-in-class compression ratio for small buffers (256 B – 8 KB) when a dictionary is available. POC confirmed ≥50% savings on 92% of fleet snapshots.
- Mature, widely deployed, stable API. Dictionary support is first-class (`ZSTD_CDict` / `ZSTD_DDict`, dictID in frame header, multi-DDict decoder).
- Thread-safe `CDict`/`DDict` (immutable after creation) simplifies our publish-once-read-many model.
- Linked as a vendored dependency, no new system library requirement.

### A.2 LZ4 (rejected)

- Pro: lower CPU.
- Con: poor compression ratio on small buffers without a dictionary. The fleet analysis specifically established that LZ4 cannot hit the ≥50% savings goal.

### A.3 LZF (rejected)

- Pro: already in Valkey for RDB.
- Con: same small-buffer ratio problem as LZ4; no dictionary support.

### A.4 Hardware compression (deferred)

- Graviton2/Nitro, QAT offer further CPU reductions.
- Deferred to v2+; feature is designed so the algorithm can be swapped behind the encoding-tag interface without wire format changes.

---

## Appendix B — Research findings summary

### B.1 Valkey internals (`research/valkey-internals.md`)

- `robj` is the universal value wrapper. Its 4-bit `encoding` field has room for a new `OBJ_ENCODING_COMPRESSED` tag.
- `lookupKey*` in `src/db.c` is the single seam every read path (commands, scripts, transactions) funnels through. Type-command handlers (and helpers) then read value bytes; this is where `objectGetUncompressedView` integrates.
- Writes arrive via `dbAddInternal` / `dbSetValue` / `dbOverwrite` — the natural seam for the "candidate for compression" enqueue.
- `kvstore` + `hashtable` is the current storage; shards are the right granularity for a background sweeper (matches active expiry / defrag).
- LRU/LFU via `lrulfu_getIdleness` / LFU-decayed counter are free to query during sweep ticks — exact signal for "skip hot keys".
- `zmalloc_size`-based accounting flows directly into eviction, `MEMORY USAGE`, and `INFO memory`; for compressed values we get correct memory accounting "for free" by using standard allocator calls.
- Interaction with replication / AOF / modules / `signalModifiedKey` / `CLIENT TRACKING` all flows through well-defined helpers that preserve invariants.

### B.2 ZSTD dictionaries (`research/zstd-dictionaries.md`)

- Single per-server dictionary (~100 KB raw + ~100 KB digested) is sufficient.
- dictID is frame-embedded and cheaply extractable; enables "active + retiring" coexistence via `ZSTD_d_refMultipleDDicts`.
- Training is heavy (hundreds of ms to seconds); always off-main-thread.
- `CDict`/`DDict` are immutable after creation → safe to publish once and share across threads.
- Drift detection must be synthesized from observability metrics; zstd has no built-in "is this dict still good?" API.

### B.3 Existing building blocks (`research/existing-building-blocks.md`)

- `bio` is a natural fit for training: long-running, infrequent, one-at-a-time. A new `BIO_COMPRESSION_TRAIN` job type follows the established pattern.
- `io_threads` is tempting to reuse for compression but has different semantics (I/O, not CPU) and different sizing needs. We ship a separate pool (`compression-threads`) with the same queue primitives (`spmcQueue`/`mpscQueue`) as `io_threads`.
- `lazyfree` handles compressed robjs correctly by going through standard `decrRefCount`; no changes needed.

### B.4 Persistence and replication (`research/persistence-and-replication.md`)

- RDB already has a precedent for "new encoding marker means special payload follows" (`RDB_ENC_LZF`). We add `RDB_ENC_COMPRESSED` as a generic tag so future algorithms reuse the same encoding byte.
- `RDB_VERSION` must bump because older loaders would reject the new encoding anyway — we bump so the rejection is a clean "too new" rather than "unknown encoding".
- Replication feed and AOF stay uncompressed (steady-state RESP is the wire contract; cross-version replication keeps working).
- `DUMP`/`RESTORE`/`MIGRATE` decompress-before-serialize in v1; compressed-in-place is v2.

### B.5 Scripting and transactions (`research/scripting-and-transactions.md`)

- Two hard rules emerged: sync-mandatory predicate (for v1 automatically satisfied since all decompression is sync); never call `signalModifiedKey` from compression paths (enforced by single-entry-point helper plus tests).
- Tests: `WATCH` + background compression → `EXEC` does not abort; `CLIENT TRACKING` → no spurious invalidations.

### B.6 Metrics and admin surface (`research/metrics-and-admin-surface.md`)

- Three surfaces available: `CONFIG`, `INFO`, subcommand container. We use all three: `CONFIG` for toggles/knobs, `INFO compression` for metrics, `COMPRESSION *` for actions.
- Latency monitor integration is free via the existing `latency-monitor-threshold` mechanism.
- Keyspace notifications would misrepresent semantics (compression is not a keyspace event). Server log is the right operator-audit channel.

### B.7 Prior art (`research/prior-art.md`)

- Redis/Valkey RDB LZF validates the encoding-tag pattern.
- KeyDB active-memory-compression validates the encoding-tag integration but ships without dictionaries and compresses inline on write — we improve on both.
- Application-level compression has no dictionary sharing and breaks debuggability — server-side fills the gap.
- Academic research (MDPI 2023, SIGPLAN ISMM 2023) confirms ZSTD-with-dict as state of the art, and validates "background compression + skip hot items + batching" as the winning design directions.

---

## Appendix C — Alternative approaches considered

### C.1 Inline-on-write compression (rejected)

KeyDB ships this. Rejected because:
- Adds worker-round-trip latency to every `SET` on the hot path.
- Wastes CPU on values that turn out to be rewritten/deleted before they'd matter.
- Doesn't benefit from the "skip hot keys" policy, which is central to production safety.

### C.2 Async decompression on read (v2)

The POC implemented "block the client, decompress off-thread." Rejected for v1 because:
- Round-trip cost (~10 µs) exceeds decompression cost (~1 µs/KB) for the common value size (256 B – 8 KB).
- Adds a client-yielding state machine for a win that applies only to large values.
- Makes the worker pool serve three customers (sweep, multi-key, decompress) requiring a priority queue.
- Must still ship a sync path for scripts/`EXEC`/replication, so async is additional code, not replacement.
- Deferrable: v2 can add `compression-async-decompress-threshold` (default `0` = disabled) non-breakingly.

### C.3 Reservoir sampling for training (rejected)

Proposed in research: sample values on the write path into a reservoir. Rejected because:
- 10000 samples × 1 KB = 10–16 MiB of extra memory dedicated to holding copies of values already in the keyspace. For a memory-saving feature, that overhead is unacceptable.
- Keyspace scan on the main thread + training on `bio` uses a transient contiguous training buffer (~10–16 MiB for default 10,000 × ~1 KB samples) allocated only during training. No long-lived reservoir; the buffer is freed once bio returns the trained dict.
- Bias toward long-lived keys is actually desirable: we want the dictionary to compress data that sticks around.

### C.4 Adaptive kill-switch (rejected for v1)

If `compression_net_saved_bytes` stays negative for a window, auto-disable compression. Rejected because:
- Auto-disabling a feature the operator explicitly enabled is a surprise, inconsistent with Valkey's operator culture.
- Observability (the metric is right there in `INFO`) is sufficient.
- Deferrable: v2 can add `compression-kill-switch yes/no` (default `no`) non-breakingly.

### C.5 Decompressed-view coexistence cache (v2)

POC's cyclic array of recently decompressed forms. Rejected for v1 because:
- Adds standalone complexity to a feature whose v1 scope is already substantial.
- Addresses only the hot-large-key case, which the `compression-max-value-size` cap can mitigate as well.
- Deferrable: v2 orthogonal extension.

### C.6 Keyspace notifications for compression events (rejected)

Initially proposed but removed. Rejected because:
- Keyspace notifications are designed for logical keyspace changes. Compression is infrastructure.
- The channel scheme (`__keyevent@<db>__:<event>`) is db-scoped; compression is server-global.
- Application clients have no legitimate reason to subscribe.
- Server log + `INFO` + latency monitor cover the operator needs.

### C.7 IO threads for decompression / read preparation (rejected for v1, deferred to v2 exploration)

IO threads already parse protocol input on a pool of worker threads and could, in theory, speculatively prepare decompressed values for read commands — amortizing decompression cost while the main thread is busy dispatching. Rejected for v1 because:

- **Boundary crossing.** Correct decompression requires key lookup, object-lifetime handling, command ordering, script/transaction semantics, and revalidation against earlier commands in the same event-loop batch. These are main-thread keyspace-execution concerns, not network-IO concerns. Blurring the boundary creates a broad test matrix and subtle race classes (e.g., a key queued for decompression on an IO thread may have been deleted or overwritten by an earlier queued command in the same batch).
- **Resource coupling.** Decompression CPU would scale with `io-threads` count — a knob sized for network parallelism, not compression workload. Operators would lose the ability to size IO bandwidth and compression CPU independently. A single workload-level bottleneck would then be ambiguous between "not enough IO capacity" and "not enough compression capacity".
- **Simpler alternative exists.** A dedicated compression worker pool (§2.11 R2.11.1) gives the same parallelism with clean separation of concerns: background compression runs on compression workers; synchronous decompression runs on the main thread; IO threads handle only their own domain.

Deferred to **v2** as a targeted optimization: speculative pre-decompression for simple read-only commands (plain `GET`, `STRLEN`, `GETRANGE`, etc.) with an object-version validation step on the main thread to detect stale speculations. This would reduce P99 latency for large-value reads without changing the sync-decompression-is-always-correct invariant enforced by §2.9 R2.9.1.

---

## Appendix D — Explicit v1 non-goals and v2 roadmap

| Area | Out of v1 | v2 plan |
|---|---|---|
| HASH / SET / ZSET / LIST / STREAM / module types | Yes | Planned — structurally enabled by the encoding-tag design. |
| Async decompression | Yes | Planned as opt-in `compression-async-decompress-threshold` (default `0` = disabled). |
| Decompressed-view coexistence cache | Yes | Planned as orthogonal extension. |
| Adaptive kill-switch | Yes | Planned as opt-in `compression-kill-switch` (default `no`). |
| Compressed-in-place cluster `MIGRATE` | Yes | Planned — ship dict prelude inside the RDB chunk. |
| Cluster-wide dictionary gossip | Yes | Conditional — only if operational pain surfaces. Preshared import (R2.3.10) covers the deterministic-fleet use case today. |
| Advanced trainer parameters (`fastCover` tuning) | Yes | Planned — expose via new configs if measurement justifies. |
| Per-key "force uncompressed" pinning | Yes | Conditional — `OBJECT` subcommand if demanded. |
| Module-provided compression backends | Yes | Conditional — only if a concrete use case emerges. |
| Compressed full-sync replication stream | Yes | Planned — negotiated via a new `REPLCONF compression yes` handshake. Replicas that advertise support receive the compressed RDB + dictionary AUX stream; older replicas continue to receive the v1 uncompressed stream (R2.6.8). Non-breaking. |
| `MEMORY STATS` compression sub-aggregate | Yes | Planned — small code cost. |
| Primary↔replica dictionary synchronization | Yes | Not planned — independent operation is the intended model. |

**Headline scope statement:** v1 ships compression for `OBJ_STRING` values only, with synchronous decompression on the main thread, one active dictionary per server, self-trained by a keyspace-scan job on `bio`, with explicit operator controls (`COMPRESSION` subcommands + `compression-*` configs). All other value types, async decompression, adaptive behaviors, and cluster-level dictionary coordination are explicit v2 scope.
