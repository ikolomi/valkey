# Realtime data compression — feature summary

**Status:** design complete, implementation planned
**Scope:** opt-in, transparent, server-side compression of STRING values using trained ZSTD dictionaries
**Target release:** Valkey 9.0 (tentative)
**Owners:** @ikolomi (lead), @GilboaAWS (co-owner)

---

## What the feature does

When enabled, Valkey keeps a small set of ZSTD compression dictionaries trained on live keyspace content and uses them to compress eligible STRING values in memory. Clients see uncompressed values on every read path (commands, scripts, replication, RDB load) — the feature is **transparent** to every client-facing API.

**The one-line pitch:** compress cold, moderately-sized STRING values in memory using a dictionary trained on your actual data; pay ~1 µs/KB on decompression reads; save ~2–4× on memory for typical JSON/text workloads.

## Design choices in one page

| Decision | What we chose | Why |
|---|---|---|
| Algorithm | ZSTD with trained dictionary | Best ratio-per-CPU for small values; the dictionary is the unique value add. See Appendix A. |
| Activation | Opt-in via `compression-master-switch` (default `off`, zero-cost when off; `compression` enables productive operation, `decompression` is the drain mode) | Safe rollout; operators pick when to turn it on. Three-state declarative model — operator declares the desired DB state, system maintains it. |
| Eligibility | STRING only; size 256 B – 128 KiB; not EMBSTR; policy-aware hot-key skip (`lru_idle_secs` in LRU/noeviction, `lfu_freq` in LFU); post-compression net-savings guard | Narrow window where the dictionary pays off and compression cost is amortized over reads. |
| Hot path (reads) | Sync decompression on main thread, ~1 µs/KB budget | Simple, predictable; async is v2. |
| Hot path (writes) | Async compression on dedicated worker pool; main thread never compresses | Zero client-visible write latency added. |
| Dictionary training | On `bio` thread using a contiguous sample buffer copied by main thread; main thread never blocks on training | Avoids robj/kvstore thread-safety concerns; satisfies `ZDICT_trainFromBuffer` API. |
| Dictionary lifecycle | Registry with monotonic IDs, refcounted, atomic promotion, max-cap enforced, drift-retrain | One-cell ABA-safe; supports multiple live dicts during rolling replacement. |
| Persistence | Disk RDB compressed; **full-sync RDB always uncompressed in v1**; AOF compressed as uncompressed commands | Replication correctness and mixed-version compatibility come first; v2 negotiates. |
| Concurrency invariant | Compression workers never touch `robj`; they consume flat byte buffers. Main thread owns all `robj` mutation. SDS immutability via existing `dbUnshareStringValue` COW discipline | Keeps concurrency reasoning tractable; leverages an invariant Valkey already enforces. |
| Operator surface | 5 primary knobs (enable, size bounds, idle threshold, worker count) + 11 advanced knobs | Most operators only need the primaries. |

## Requirement index

36 numbered requirements across 12 sub-sections of §2 in the design doc, covering:

- §2.1 Master switch + operator surface (R2.1.1–R2.1.6)
- §2.2 Value eligibility (R2.2.1–R2.2.7)
- §2.3 Dictionary lifecycle (R2.3.1–R2.3.12)
- §2.4 Compression path (R2.4.1–R2.4.6)
- §2.5 Decompression path (R2.5.1–R2.5.5)
- §2.6 Persistence (R2.6.1–R2.6.9)
- §2.7 AOF (R2.7.1–R2.7.3)
- §2.8 Memory accounting (R2.8.1–R2.8.4)
- §2.9 Scripting & transactions (R2.9.1–R2.9.3)
- §2.10 Observability (R2.10.1–R2.10.5)
- §2.11 CPU & concurrency (R2.11.1–R2.11.5)
- §2.12 Configuration summary (16 knobs: 5 primary + 11 advanced)

Full text: [`design/detailed-design.md`](design/detailed-design.md).

## What the walkthrough changed

The 2026-05-10 PR review walkthrough addressed all 31 review threads (22 self-review by @ikolomi, 9 by @GilboaAWS). Highlights of substantive design evolution:

- **Defaults recalibrated:** `compression-max-value-size` 1 MiB → 128 KiB
- **Replication reframed:** v1 full-sync RDB is uncompressed (new R2.6.8); wire negotiation deferred to v2
- **Operator surface cut in half** for the common case (5 primary knobs vs 16 shown by default)
- **SDS immutability made explicit:** rely on existing `dbUnshareStringValue` COW + merge-blocker audit + new `compression-cow-invariant.tcl` test (R2.4.4–R2.4.6, §7.2)
- **Benchmark suite formalized** as §7.5 — extend `valkey-benchmark` with `--value-size-distribution`, `--value-data`, `--key-distribution` + six canonical scenarios
- **EMBSTR excluded** from eligibility (6 places)
- **Hotness signals decoupled from `maxmemory-policy`:** universal `write_age` + `idle_seconds` gates in every mode; LFU-freq only when LFU is active; knob renamed `compression-lru-idle-seconds` → `compression-min-idle-seconds`
  - **Refined again during S2.2:** the time-based and freq-based checks are policy-conditional (the lru field encodes seconds in LRU/noeviction but freq in LFU; the time check can't be applied universally). `compression-settle-seconds` dropped — both knobs compared to the same metric in v1, so the dual surface added no expressive power.
- **Retry-guard scoped by dict ID** via new `incompressibleKeys{}` side hashtable
  - **Refined again during S2.3 implementation review:** the side hashtable was dropped entirely. Per-key rejection state is functionless under a fixed dict (ZSTD is deterministic — same input + same dict = same output, so retries can't change outcome) and the dict-change retry signal is better expressed at the system level. R2.3.5 drift trigger extended to fold rejected attempts into `compression_live_ratio_10m` (each rejection contributes its actual measured ratio, typically in `[0.9, 1.05]`); a sustained high rejection rate naturally trips the existing drift threshold. No new knob; one knob removed (`compression-retry-interval`). S2.3 sub-task removed from the plan; the drift-signal extension absorbed into S1.4.
- **Training flow corrected:** main thread iterates + copies samples into a contiguous buffer; bio runs `ZDICT_trainFromBuffer`. Bio never touches `robj`, `kvstore`, or refcounts. Dropped incorrect "zero copies" claim
- **Appendix §C.7 added:** io-threads explicitly rejected for decompression in v1

Per-thread rationale: [`DESIGN_TODO.md`](DESIGN_TODO.md). GitHub PR with posted resolutions: https://github.com/ikolomi/valkey/pull/1.

## What changed during implementation

Three follow-on design refinements landed after Phase 0 once implementation surfaced new information:

- **Training trigger + sampling rewritten** (PR #14, R2.3.5/R2.3.6): the original single `compression-dict-first-training-keys-count` knob (default 10000) was replaced by a three-knob model — `compression-dict-min-training-keys` (default 1000, both the trigger and a sample-count floor), `compression-dict-max-training-keys` (default 10000, sample-count cap), and `compression-training-buffer-size` (default 16 MiB, memory cap). The trigger semantic also changed from "eligible-keys counter incremented on the write path" to "DB total-keys count check" via cheap `kvstoreSize` polling. `idea-honing.md` Q9 retains the original walkthrough wording with a "superseded" annotation; the design doc reflects current behaviour.

- **Read-hot compressed-value gap explicit** (PR #15, R2.5.6): post-compression decisions are not reconsidered in v1. A key compressed when cold and later read-hot pays sustained sync decompression CPU. v1 mitigations are bounded per-read cost (R2.5.4: `compression-max-value-size` defaults to 128 KiB), latency-monitor observability (R2.10.2: `LATENCY HISTORY decompress-sync`), and operator-driven recovery (`CONFIG SET compression-master-switch decompression` + `compression-active-sweeper enabled`). Auto-demotion (sweeper-based, with hysteresis) is explicit v2 scope — see Appendix D.

- **Worker-pool QSBR plumbing** (PR #13, §4.4 + §4.6): the worker pool's correctness required additional infrastructure not anticipated in the original design — a dual mutexqueue API (`mutexQueuePop`/`PopAll` keep their original "never NULL on blocking" contract; new `mutexQueuePopWakable` for wake-aware consumers + `mutexQueueWakeAll` broadcast), sentinel-based race-free shutdown (`kShutdownSentinel`), and a `retire_n_workers` field on `compressionDictPair` for resize-aware `canFree`. Mostly internal to the implementation; design doc §4.4 + §4.6 capture the contract for future readers.

## Implementation plan summary

See [`implementation/plan.md`](implementation/plan.md) for the full sequenced plan. Top-level shape:

- **7 subsystems** with narrow interface contracts. @ikolomi owns the concurrency-critical pieces (S1 dict lifecycle, S2 hot path + COW audit). @GilboaAWS owns persistence (S3), observability (S4), benchmarks (S5), integration tests (S6), and dev infra (S7).
- **Phase 0** (week 1): land compilable skeleton with stubbed public APIs + interface contracts. Joint PR.
- **Phase 1** (weeks 2–5): parallel implementation of all subsystems; end-of-phase gate is a working single-instance demo.
- **Phase 2** (weeks 6–9): integration + COW audit + full §7.5 benchmark run.
- **Phase 3** (weeks 10–11): stabilize, doc, release prep.
- **Total:** ~11 weeks calendar with parallelism (vs ~17 serial).

Each subsystem's unit tests ship in the same PR as the code they cover. The §7.2 COW invariant test is a merge-blocker; the §7.5 perf run gates Phase 2.

## Explicit v1 non-goals (deferred to v2)

- Wire-level replication compression (`REPLCONF compression yes`)
- IO threads for decompression
- Async decompression path for long reads
- LIST/HASH/ZSET per-element compression
- Preshared/imported dictionaries
- Hardware accelerators (AVX-512, QAT)
- Adaptive kill-switch

## Artifacts produced by this planning cycle

| File | Role |
|---|---|
| [`idea-honing.md`](idea-honing.md) | Q1–Q16 requirements with walkthrough decisions folded in |
| [`design/detailed-design.md`](design/detailed-design.md) | §1–§7 detailed design + Appendices A–D |
| [`implementation/plan.md`](implementation/plan.md) | Phased parallel-ownership implementation plan |
| [`DESIGN_TODO.md`](DESIGN_TODO.md) | 31-thread walkthrough audit trail |
| [`pr-feedback.json`](pr-feedback.json) | Machine-readable sidecar to DESIGN_TODO |
| [`research/`](research/) | 7 research notes backing the design (ZSTD dicts, Valkey internals, existing building blocks, prior art, etc.) |
| [`tools/fetch-pr-comments.sh`](tools/fetch-pr-comments.sh) | GitHub REST fetcher (paginated, cached) |
| [`tools/normalize-pr-comments.py`](tools/normalize-pr-comments.py) | Thread roll-up + DESIGN_TODO regenerator (preserves operator fields) |
| [`tools/post-pr-replies.py`](tools/post-pr-replies.py) | Round-trip: post resolution replies + resolve threads via GraphQL |
| [`.pr-cache/`](.pr-cache/) | Raw GitHub API JSON (gitignored; regenerate via `tools/fetch-pr-comments.sh`) |

## What happens next

1. **Land the plan** — open a CR against `unstable` that ships the design docs + this plan under `.agents/planning/realtime-data-compression/`.
2. **Phase 0 kickoff** — @ikolomi and @GilboaAWS co-author the skeleton PR.
3. **Execute the phase plan** — track progress in the task checkboxes of `plan.md`.
4. **Revisit DESIGN_TODO during implementation** — if an implementation detail contradicts a walkthrough decision, amend the design and re-open the relevant thread for discussion before coding around it.
