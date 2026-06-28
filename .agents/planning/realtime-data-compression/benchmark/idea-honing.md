# Idea Honing — Inline-compression benchmarking subsystem

_Interactive Q&A to refine the rough idea into concrete requirements. One
question at a time; answers recorded (verbatim or summarized to the final
decision) as we go. Same method as `.agents/planning/realtime-data-compression/idea-honing.md`._

_Anchor: amz_redis-benchmark-orc. Principle: work backwards from what we render._

---

## Q1. The primary result — what do we render, and what question does it answer?

The core question is: *given (data model, target TPS, command mix, compression
config), how does latency behave?* Latency is the output; the rest are inputs.
Before any config schema or orchestration detail, we must fix the **primary
chart**, because it dictates what a "data point" is and therefore the whole data
model.

Candidate framings:

- **A — Latency-by-percentile bars, series = compression config, one fixed
  operating point.** Hold (data model, command mix, target TPS) fixed. X-axis =
  latency percentile {p50, p99, p999} as groups; series (bars) = compression
  config {off, compression, …}; plus a side panel for memory + ratio. Answers:
  *"at this workload and this TPS, what does each compression config cost at
  each percentile, and what memory does it save?"* One chart per
  (data-model × command-mix × TPS).

- **B — Latency-vs-load curves.** X = offered TPS (sweep), Y = latency (one
  chart per percentile, or p99 primary); series = compression config. Answers:
  *"as we push TPS, how does latency diverge between off and compression — where
  is the knee / max sustainable TPS under an SLA?"* Client-count is the internal
  knob used to reach each TPS point.

- **C — Both:** B as the headline (latency-vs-load), with A as the per-operating-
  point detail at a chosen TPS.

Each implies a different "data point":
- A → a data point is one (config) at one fixed TPS → bars.
- B → a data point is one (config, TPS) → a point on a curve; the run sweeps TPS.

**Recommendation (tentative):** **C**, with **B as primary** — latency-vs-offered-
TPS curves, off vs compression, because the feature's central risk is "decompress
on the main thread raises latency under load," which only a load curve exposes; A
is the readable per-point summary at a chosen TPS. Memory + compression_ratio are
reported alongside (they're load-independent, so a single value per config).

**Answer:** The framing above missed the point. The headline is a **tradeoff
question, not a latency question**:

> **For a given workload and compression config, how much memory do we save, and
> what latency degradation do we pay for it?** — across workloads × configs.

Multiple graph types are welcome ("a number of graph types if it makes it more
convenient"). The hierarchy:

- **Headline — the memory-vs-latency-degradation tradeoff.** Each point/bar is a
  (workload × compression config). Two numbers per point:
  - **memory saving** = % reduction in `used_memory` vs the `off` baseline
    (load-independent → one value per (workload, config)).
  - **latency degradation** = % increase in latency (p99 primary) vs `off`,
    measured at a defined operating point (load-DEPENDENT → see Q2).
  `off` is the baseline (0% saved, 0% degradation). Wins = much saved, little
  degradation. Likely rendered as a scatter (X = % memory saved, Y = % p99
  degradation, color = workload, marker = config) and/or grouped bars per
  workload.
- **Supporting — latency-vs-load curves (B)** to *show* how the degradation
  arises under load, and **memory / ratio bars (A-style)** for absolute values.

Key consequence: "latency degradation" is only well-defined at a chosen load
(compression's cost grows with TPS). So the headline depends on **at what
operating point degradation is read** — carried into Q2 (load model). Tentative:
report degradation at one or more fixed reference TPS values (clean,
interpretable) rather than at per-config saturation (where "max TPS" itself
differs between configs and muddies the comparison).

### Q1 resolution — adopt the rendering proposal as the target design

The full rendering design is captured in
[`research-rendering-proposal.md`](research-rendering-proposal.md) and adopted as
the **north-star report design**. Core principle: *present compression as a cost
curve, not "faster/slower" — "for this workload, every extra X% memory saving
costs Y µs at p99/p99.9 and Z% CPU."* Everything is **delta-from-baseline**
(workload is static per report).

Report sections (per workload + fixed TPS):
1. **Workload summary** (shown once: TPS, command mix, value-size + key
   distribution, dataset size, eligibility coverage).
2. **Headline — Pareto tradeoff:** X = % memory saved vs baseline, Y = latency
   penalty vs baseline (one per percentile, p99 / p99.9 primary); each point a
   compression config; best configs form the bottom-right frontier. Optional
   point-size = CPU/worker utilization.
3. **Latency-distribution impact:** per-percentile delta (p50/p90/p95/p99/p99.9)
   per config — shows whether compression shifts the whole distribution or only
   the tail. Prefer **absolute µs/ms delta** (％ exaggerates small baselines);
   show both.
4. **Memory breakdown:** stacked (payload / metadata / dictionary /
   fragmentation) — explains "why only 28% saved when ratio says 45%."
5. **Per-command heatmap:** GET/SET/MGET/… × p50/p95/p99/p99.9 deltas.
6. **Operational headroom:** main-thread CPU, worker CPU, queue depth,
   evictions, skipped/hot-skip.
7. **Summary table** (the screenshot artifact): per config — memory saved, per-
   percentile deltas, CPU delta, recommendation.

**Percentiles (answers the Q1 follow-up):** include the commonly-used set
**p50 / p90 / p95 / p99 / p99.9 (and p99.999)**. valkey-benchmark already builds
an HDR histogram, so all percentiles exist internally; stock `--csv` only prints
p50/p95/p99. **Exposing the rest is a tiny valkey-benchmark change** (extra
`hdr_value_at_percentile` calls + CSV columns) — and since the tail (p99.9/
p99.999) is the headline Y-axis and exactly where decompress cost shows up, this
is promoted to **the first L1 extension** (ahead of corpus-import).

**Data-availability tiers (drives phasing — not all sections are equally cheap):**
- _Available now_ (stock INFO + CSV p50/p95/p99): headline tradeoff (p99), memory
  absolute + ratio, summary table (partial), workload summary.
- _Tiny C change_ (expose HDR percentiles): full §3 percentile-delta + p99.9/
  p99.999 on the headline. **First L1 extension.**
- _Moderate orchestration_ (per-command CSV parse): §5 heatmap.
- _Partial / needs more data_: §4 memory breakdown (dict/metadata/fragmentation
  split not cleanly in INFO today — approximate or add INFO fields); §6 per-thread
  worker CPU (workers are threads → needs `/proc/<pid>/task` parsing; process-total
  CPU via mpstat is easy).

**Phasing (velocity):** first cut = §1 workload summary + §2 headline tradeoff +
§3 percentile-delta + §7 summary table, using the small HDR-percentile change +
stock INFO. §4/§5/§6 land as their data sources arrive.

**Load-model consequence → Q2:** the proposal fixes **one TPS per report**
("TPS: 100k"). Comparing configs at the *same offered TPS* requires **open-loop /
rate-limited** load (`--rps`, which already exists), not closed-loop — under
closed-loop each config reaches a different TPS, so latency isn't comparable.
This supersedes the earlier "closed-loop first" lean and is resolved in Q2.

_**Q1 status: CONFIRMED** (2026-06-23). Headline = Pareto memory-vs-latency
tradeoff, delta-from-baseline, full percentile set via a small HDR-exposure
change, open-loop fixed-TPS. Phasing per the data-availability tiers above._

---

## Q2. Load + workload model — how a run is defined and driven

Q1 fixed that a report page is **one (workload, target TPS)** with **compression
configs as the compared axis** (the Pareto points). Q2 defines that workload and
how we drive it. The proposal + open-loop consequence imply most of it; the open
decisions:

**Proposed model (amz-orc-flavored, for confirmation):**
- **Open-loop, rate-limited.** Each command type runs as its own
  `valkey-benchmark` process (the binary runs one command per process), capped
  via `--rps` to its share of the target TPS. Mixed ratios are achieved by
  running the per-command processes concurrently — no benchmark-core change.
- **Command mix in JSON:**
  ```json
  "workload": {
    "target_tps": 100000,
    "commands": [
      {"type": "get",  "ratio": 0.7},
      {"type": "set",  "ratio": 0.2},
      {"type": "mget", "ratio": 0.1, "keys": 10}
    ],
    "connections_total": 50, "pipeline": 1,
    "duration_seconds": 60, "warmup_seconds": 5, "iterations": 3
  }
  ```
  Ratios normalize to 1; each command process gets `--rps = ratio * target_tps`
  and a share of `connections_total` (proportional, min 1).
- **Saturation signal:** capture **achieved vs target TPS** per process; if a
  config can't sustain the offered load, achieved < target and latency balloons
  — that *is* the result (this config can't handle this load), surfaced in the
  report.

**Open sub-decisions:**

- **(Q2a) Data-point / iteration semantics under open-loop.** A correct tail
  percentile across N iterations needs the underlying **HDR histograms merged**
  — averaging per-iteration p99s is statistically wrong. valkey-benchmark
  doesn't dump its histogram today. Options:
  1. **One long duration-bounded run per (config) data point** (e.g. 60 s),
     single HDR histogram → correct percentiles, simplest. Optional small repeat
     count only for a variance band on memory/RPS.
  2. Accept median-of-per-iteration percentiles (approximate).
  3. Extend valkey-benchmark to **dump the histogram** so the orchestrator merges
     iterations (a second small C change, sibling to the percentile-exposure one).
  _Tentative: (1) now; (3) later if we want tight confidence intervals._

- **(Q2b) Connections / pipeline.** Open-loop needs enough connections×pipeline
  to *sustain* the target rps without self-queueing, but `--rps` caps the rate.
  Fixed `connections_total` split by ratio (proposed) vs per-command connection
  counts? Pipeline default 1 (compression latency is per-request; pipelining
  muddies per-op latency) — confirm.

- **(Q2c) Scope of one run.** One JSON = one (workload, target TPS) × list of
  configs (the compared axis) → one report page. Multiple TPS / workloads =
  multiple runs / report pages (matrix templates deferred, per locked decision).
  Confirm this is the unit.

**Answer:** Confirmed/resolved after studying amz_redis-benchmark-orc's source
(not just its README).

- **Q2c — CONFIRMED.** One JSON = one (workload, target TPS) × list of configs →
  one report page. Multiple TPS/workloads = multiple runs (matrix deferred).

- **Q2b — RESOLVED, porting amz-orc's exact math.** Add
  `max_clients_per_process` (single-threaded benchmark per process). Per command:
  `procs = max(1, ceil(conns / max_clients_per_process))`, connections
  distributed evenly (`ceil(conns/procs)`); each process's `--rps` =
  (command's TPS share) / procs. Also porting amz-orc's **FIFO barrier**
  (all processes block on a named pipe, released together → zero-skew start),
  **mpstat** CPU capture, and (later) isolated-dir / binary-checksum / NUMA rigor.
  Pipeline default 1.

- **Q2a — RESOLVED, and we do it *more correctly* than amz-orc.** amz-orc's
  source (`compute_iteration_summary`) **averages per-interval/per-process
  percentiles** (`mean(set_latencies["p99"])`) and scales RPS by process count
  (`mean(per-proc rps) × procs`). That is the statistically-loose shortcut — fine
  for its RPS-regression goal, **wrong for our tail-latency headline**. Instead:
  - Run each process to a **duration-bounded** single run (`--duration`); parse
    its **end-of-run "Latency by percentile distribution"** (stock
    valkey-benchmark already prints it — full tail + **cumulative counts per
    bucket**).
  - Reconstruct each process's per-bucket counts → **sum across all processes**
    (and iterations) → one merged distribution → compute the **true** merged
    percentile (p50…p99.999). No averaging of percentiles; no C change.
  - Throughput = **sum** of per-process `requests_finished / elapsed` (true
    concurrent total), plus **achieved-vs-target TPS** as the saturation signal.
  - A clean `--latency-dump` C flag stays a *nice-to-have* (verbose-text parsing
    is more fragile), **not** a first-cut prerequisite. The per-second interval
    CSV (amz fork feature) is needed only for **drift-over-time** (deferred).

**amz-orc learnings folded in:** FIFO-barrier zero-skew launch; connection→process
split math; mpstat CPU; isolated server dirs + checksums + NUMA (reproducibility,
later). **Divergence:** amz-orc sweeps `total_clients` (closed-loop) as its
x-axis; we fix TPS and compare **configs** (open-loop via `--rps`, Pareto axis).
**Correction over amz-orc:** merge histograms instead of averaging percentiles.

_**Q2 status: CONFIRMED** pending your nod._

_**Q2 status: CONFIRMED** (2026-06-23). Histogram-merge from stock end-of-run
distribution (no C change, better than amz-orc averaging); `--latency-dump` a
later nice-to-have; per-second interval / drift deferred; Q2b split math + FIFO
barrier ported; one run = (workload, TPS) × configs._

---

## Q3. The data model — content, value-size distribution, key distribution

This defines the dataset a run operates on, and surfaces the first real
valkey-benchmark extensions. The rendering proposal's workload summary already
names the pieces: *"Value-size distribution: p50 512B p99 64KB; Key
distribution: Zipfian θ=0.99; Compression candidates: 58% of keys, 82% of
bytes."* So value-size is a **distribution** (not a single `-d` size), key
selection is **skewed** (hotness), and eligibility coverage is **derived** from
the size distribution × the compression config.

**Proposed model (for confirmation):**
```json
"data_model": {
  "name": "json-mixed",
  "value_shape": "json",                          // kv | json | log | coordinates
  "value_size_distribution": "lognormal:512:1.6", // or constant:512 | uniform:256:8192
  "corpus_entries": 50000,                        // representative blobs, script-generated
  "key_count": 1000000,                           // dataset size (keys preloaded)
  "key_distribution": "zipf:0.99"                 // or uniform
}
```
- **Corpus = script-generated** (no valkey-benchmark involvement): `corpus_entries`
  blobs of `value_shape`, sizes drawn from `value_size_distribution`, written to a
  file. The **value-size distribution is encoded in the corpus** → no size-dist
  flag needed in valkey-benchmark.
- **Preload** the dataset (`key_count` keys) from the corpus (script-side, fast
  pipelined SET) → keyspace carries the corpus's content + size distribution; the
  dictionary trains on it.
- **SET payloads in the workload mix** are sourced by valkey-benchmark from the
  corpus → extension **`--value-data corpus:FILE`**.
- **Key selection** (read & write) follows `key_distribution` → extension
  **`--key-distribution uniform|zipf`** (hotness, exercises skip-hot-keys).

**Open sub-decisions:**

- **(Q3a) Key↔value mapping fidelity.** Verbatim reuse (key i → corpus[i mod E],
  many identical values → **overstates** compressibility) vs per-key
  unique-but-similar (most realistic, what dict compression targets, but cost to
  generate N distinct values). _Tentative: large corpus E (e.g. 50–100k) of
  unique-but-similar blobs, keys map round-robin (modest repetition); E is a
  fidelity dial toward key_count. Document that small E overstates ratio._
- **(Q3b) valkey-benchmark extension set + order** this locks (percentiles solved
  by parsing existing output, so the C list is just):
  1. **`--value-data corpus:FILE`** — corpus-backed SET payloads (write-path
     compressibility). _First C change._
  2. **`--key-distribution uniform|zipf [--zipf-theta]`** — hotset skew.
  3. **multi-key `MGET`/`MSET` (`--mget-keys N`)** — decompress-N-on-main-thread.
  Value-size distribution = corpus-encoded (no flag). A clean `--latency-dump` =
  later robustness nice-to-have (Q2a).
- **(Q3c) First-cut phasing within the model.** First real number = GET-only on a
  **script-preloaded** compressible corpus (zero C change), giving a Pareto point
  for the read path + memory. SET-in-the-mix arrives with extension #1. Confirm
  this is the on-ramp.

**Answer:** Confirmed, with two additions to the model (seed + size clamps) and one correction to Q3b.

**Data model (final shape):**
```json
"data_model": {
  "name": "json-mixed",
  "value_shape": "json",                           // kv | json | log | coordinates
  "value_size_distribution": "lognormal:512:1.6",  // or constant:512 | uniform:256:8192
  "value_size_min": 1,                             // clamp (bytes); default 1
  "value_size_max": 131072,                        // clamp (bytes); default a sane cap
  "seed": 1234,                                    // REQUIRED — reproducible corpus
  "corpus_entries": 50000,                         // representative blobs, script-generated
  "key_count": 1000000,                            // dataset size (keys preloaded)
  "key_distribution": "zipf:0.99"                  // or uniform
}
```

- **Seed — REQUIRED.** Corpus generation is script-side; the locked decision is
  "reproducible per seed." Same seed → identical corpus → runs comparable across
  configs and across days. (amz-orc already seeds via `keyspace.seed`; we make it
  first-class and mandatory.)
- **Value-size min/max — added as clamps.** `constant:N` needs neither;
  `uniform:MIN:MAX` carries them inline; **`lognormal:μ:σ` is unbounded** and needs
  explicit `[min,max]`. Two reasons: (a) avoid pathological huge values, and
  (b) deliberately straddle the `compression-min-value-size` /
  `compression-max-value-size` eligibility window under test. Defaults: `min=1`,
  `max` = a sane cap (e.g. 128 KiB, matching the v1 `compression-max-value-size`
  default).
- **Corpus = script-generated**, value-size distribution **encoded in the corpus**
  (no `--value-size-distribution` benchmark flag — only needed if/when corpus
  generation is ever moved into valkey-benchmark itself).
- **Preload** the `key_count` keyspace from the corpus (script-side pipelined SET).
- **SET payloads in the workload** sourced from the corpus → extension
  `--value-data corpus:FILE`.
- **Key selection** follows `key_distribution` → extension `--key-distribution`.

- **(Q3a) CONFIRMED.** Large corpus `E` (50–100k) of *unique-but-similar* blobs;
  keys map round-robin (modest repetition); `E` is the fidelity dial toward
  `key_count`. Document that small `E` overstates the ratio.

- **(Q3b) CONFIRMED — with `--latency-dump` correction.** The valkey-benchmark
  C-change list is exactly three, easy-first:
  1. **`--value-data corpus:FILE`** — corpus-backed SET payloads. _First C change._
  2. **`--key-distribution uniform|zipf [--zipf-theta]`** — hotset skew.
  3. **multi-key `MGET`/`MSET` (`--mget-keys N`)** — decompress-N-on-main-thread.

  Value-size distribution stays corpus-encoded (no flag). **`--latency-dump` is NOT
  on this list** — the full percentile distribution (incl. per-bucket cumulative
  counts) already exists in valkey-benchmark's stock end-of-run output, which Q2
  parses and merges with zero C change. `--latency-dump` is only a future
  *robustness* nice-to-have (emit machine-readable instead of parsing verbose
  text), already noted under Q2 — not a data-model extension.

- **(Q3c) CONFIRMED as the on-ramp.** First real number = **GET-only against a
  script-preloaded, already-compressed corpus** (zero benchmark C change) → a
  Pareto point for the read path + memory. SET-in-the-mix (write-path
  compression-enqueue cost + decompress-on-read) arrives with extension #1. The
  *how* of reaching the pre-compressed state, and how memory is sampled, is the
  subject of **Q4**.

_**Q3 status: CONFIRMED** (2026-06-23). Data model = value_shape +
value_size_distribution (+min/max clamps) + required seed + corpus_entries +
key_count + key_distribution. Three benchmark C-changes, easy-first, corpus first;
value-size stays corpus-encoded; `--latency-dump` is not a data-model extension.
GET-only-on-precompressed-corpus is the zero-C-change on-ramp._

---

## Q4. Run phases & memory-measurement methodology

Q3c confirmed the on-ramp (GET-only on a pre-compressed corpus) but deferred two
things to here: **how a run reaches the compressed steady state**, and **how
memory savings are measured** without being confounded by allocator behavior.

The governing insight: the report measures **two different things under two
different conditions**, and conflating them is the trap.

- **Memory saving is load-independent** → measure it *quiesced, at steady state,
  before any load*.
- **Latency penalty is load-dependent** → measure it *under the fixed-TPS load,
  after steady state* (so the sweeper isn't stealing CPU and reads actually hit
  compressed values, paying the real decompress cost).

**Proposed phased run, per (workload × config):**

| Phase | What | Why |
|---|---|---|
| 0. Start | server with the config's `--compression-*` flags (baseline = `off`) | |
| 1. **Dict acquisition** — config knob `dict_mode` | `import` (default): `COMPRESSION DICT-IMPORT` a pre-trained dict — deterministic, identical across configs, removes training variance from the comparison. `train` (optional): enable training, drive writes until `compression_active_dict_id != 0` — models real cold-start | Holds the dict constant when comparing configs; `train` is for the separate "customer-experience" story |
| 2. **Preload + drive to compressed steady state** | preload corpus (script-side pipelined SET); sweeper enabled; **poll INFO until `compression_candidates_pending == 0` AND `compression_compressed_objects` stable across two polls** | The "warmup". **No flush** — the corpus we preload is the dataset we measure. |
| 3. **Memory snapshot (quiesced)** | capture `used_memory`, `compression_ratio`, `compression_net_saved_bytes`, `compression_total_uncompressed/compressed_bytes`, `compression_compressed_objects` | The memory number, clean, before load perturbs it |
| 4. **Load + latency** | release FIFO barrier; open-loop `--rps`; warmup-skip; parse + merge end-of-run histograms; capture mpstat CPU; re-sample `used_memory` at end | The latency number, after steady state |
| 5. Teardown | stop server, clean isolated dir | |

**Memory metric — lead with `used_memory`, not RSS.**
- `used_memory` = jemalloc's *live-bytes* accounting. It drops correctly when the
  uncompressed sds is freed and a smaller compressed buffer is allocated — **no
  defrag needed** — and it's exactly what eviction / `maxmemory` see (in-tree
  design R2.8.1). This dissolves the "memory won't go down from the zmalloc POV /
  might need defrag" concern, which is an **RSS** problem, not a `used_memory` one.
- **RSS** (subject to fragmentation / page-return lag) is a **secondary** metric
  only, sampled after an optional `MEMORY PURGE`.

**Why no flush:** the dataset measured on IS the preloaded corpus; compressing it
in place (Phase 2) is simpler than train→flush→reload and is the real customer
path.

**dict_mode default = `import`:** holds the dictionary constant across configs so
the Pareto comparison isolates the config variable; `train` exists for the
realistic cold-start story (mirrors the in-tree post-S2 plan: import-dict on-ramp
now, auto-trained dicts later).

**Open sub-decisions for Q4:**

- **(Q4a) Measurement workload: read-only vs mixed.** On-ramp is GET-only (clean
  read-path isolation, Q3c). For the realistic run, does Phase 4 include writes
  (SET/APPEND → write-path compression-enqueue + decompress-on-read), or stay
  read-only for v1? _Tentative: GET-only on-ramp first; add a mixed profile once
  extension #1 (`--value-data corpus:FILE`) lands so SET payloads are
  compressible._
- **(Q4b) Steady-state detection robustness.** Is "`candidates_pending == 0` +
  `compressed_objects` stable across two polls" sufficient, or do we also gate on
  `compression_ratio` having plateaued? Settle-window interaction: the harness must
  set `compression-min-idle-seconds` low (or wait it out) so eligible values aren't
  skipped as "too fresh". _Tentative: poll both `candidates_pending` and a
  2-decimal-stable `compression_ratio`; set `compression-min-idle-seconds 0` in the
  bench config._
- **(Q4c) Iterations & memory variance.** Memory is near-deterministic per
  (workload, config, seed); do we still run N iterations for a variance band, or is
  1 enough for the memory number (with iterations mainly serving the latency
  histogram merge)? _Tentative: iterations serve latency; report memory from a
  single steady-state snapshot but assert it's within a small tolerance across
  iterations as a sanity check._
- **(Q4d) `MEMORY PURGE` + RSS secondary.** Include RSS-after-purge as a reported
  secondary from day one, or defer until someone asks? _Tentative: capture it
  (cheap), report it secondary, lead with `used_memory`._

**Answer:** CONFIRMED — distinct staged model, valkey-benchmark for all writes,
`--sequential` for exact population, explicit force-sweep gate, MAX `used_memory`
as the headline.

**Final phase model (per workload × config, repeated `iterations` times):**

| Phase | Action | Detection / exit |
|---|---|---|
| 0. Start | server with config's `--compression-*` flags (reference = `off`) | PING ready |
| 1. **Dict** | valkey-benchmark `SET` (corpus-backed) writes training data | poll INFO until `compression_active_dict_id != 0` → then `FLUSHALL` |
| 2. **Warmup** | valkey-benchmark `SET --sequential <key_count> --value-data corpus:FILE` → exactly `key_count` keys, each once → `COMPRESSION SWEEP FORCE` | poll INFO until `compression_sweeper_running == 0` AND `compression_candidates_pending == 0` (keyspace fully compressed, quiesced) |
| 3. **Load** | FIFO-barrier valkey-benchmark processes, full command mix, open-loop `--rps`, `--test-duration`; orchestrator periodically samples `used_memory` + mpstat CPU | processes self-exit on `--test-duration` |
| 4. **Aggregate** | merge per-command histograms + datapoints across `iterations`; delta vs `reference` config | — |

**Key resolutions:**

- **`--sequential <keyspacelen>` (amz_valkey-benchmark) replaces the random-prepopulate hack.**
  It writes keys `0..keyspacelen-1` each exactly once (global atomic counter
  `atomicGetIncr`), generating exactly `keyspacelen` requests — guaranteed full
  coverage, dataset sized *exactly* as configured (no coupon-collector gap, no
  oversized dataset). amz-orc's random `-n` prepopulate is flawed for the reason
  the user identified; `--sequential` is the fix and a strong upstream candidate
  (broadly useful, not compression-specific). Mutually exclusive with `-n` /
  `--test-duration`.
- **All writes go through valkey-benchmark** (training in phase 1, populate in
  phase 2) — no script-side SET population. **Corpus generation is the only
  script-side data step.**
- **Distinct stages, not mixed.** Relying on write-path opportunistic compression
  (per-SET) is unreliable: the training set is a subset of the keyspace, and
  internal mechanics (inbox/outbox queue limits, candidate drops, settle window)
  can leave written keys uncompressed. So: populate fully, then **`COMPRESSION
  SWEEP FORCE` once**, then **wait for the sweeper to complete** — a deterministic
  "everything compressed" gate before the measured load.
- **Flush** (end of phase 1) yields a clean, exactly-`key_count` dataset for
  phase 2 and discards training residue. The active dict **survives `FLUSHALL`
  by design** (only retiring dicts GC at `frame_refs == 0`; flush neither promotes
  a new dict nor flips the master switch) — no empirical verification needed.
- **Memory metric = MAX observed `used_memory` during load.** This is not merely
  conservative — it is the *correct* number: transient decompression views
  (design R2.5.7) are real memory the operator must provision for, so per workload
  we explicitly want the worst case. Lead with MAX; retain the full time series
  for the chart. **RSS dropped for v1** — `used_memory` is sufficient and more
  faithful to logical bytes saved (fragmentation would only mislead).
- **`iterations`** drives the histogram merge (sum per-command buckets across
  runs) plus a memory-MAX variance band. **`reference`** config
  (= `compression-master-switch off`) is the baseline every other config deltas
  against.

Resolves the open sub-decisions: **Q4a** (full command mix from the config, not
GET-only — supersedes Q3c's zero-C-change GET-only on-ramp; `--value-data` +
`--sequential` are day-one prerequisites); **Q4b** (pre-load gate = active-dict in
phase 1 + sweep-complete in phase 2 — deterministic, no ratio-plateau guessing);
**Q4c** (iterations); **Q4d** (used_memory sufficient; MAX is the right reduction).

_**Q4 status: CONFIRMED** (2026-06-23). Staged model: dict (train→flush) → warmup
(`--sequential` populate → force-sweep → wait quiesced) → load (full mix, open-loop,
poll used_memory+CPU) → aggregate (histogram merge over iterations, delta vs
reference). MAX used_memory headline; `--sequential` + `--value-data` are the
day-one benchmark prerequisites._

> **Amended by Q6 (2026-06-24).** The warmup/measurement portion of this model was
> refined — see Q6. Changes: the compression profile is **workload-developed via the
> hotness lever** (not force-everything-compressed); `compression-min-idle-seconds`
> is a **free experimental variable** (not pinned to 0); measurement uses a
> **signaled windowed-recording window inside one continuous load** (not a separate
> second load run, which had a profile-disturbing gap). The dict → `--sequential`
> populate → MAX-`used_memory` → `iterations` → `reference` → histogram-merge
> decisions here all stand.

---

## Q5. The compression-configuration surface — what we compare, and how it's expressed

Q1 fixed the Pareto (each point = a compression config). Q4 fixed `reference` =
the `off` config. Q5 defines the **set of configs** on that axis and **how each is
expressed in the run JSON**.

**Mapping to amz-orc.** A "config" is amz-orc's per-server entry: the *same*
valkey-server binary started with different `--compression-*` args. Reuse
`local-server-info.args` verbatim — the orchestrator appends the args at server
start (Q4 phase 0). The `reference` config is the one with
`--compression-master-switch off`.

**The headline sweep (from `research-rendering-proposal.md`).** The central
tradeoff knob is the **eligibility threshold** `compression-min-value-size`:
raising it compresses fewer (small) values → less memory saved but fewer reads
pay decompress. The proposal's own example axis is exactly this:
```
off (reference)
compression, min-value-size 256
compression, min-value-size 512
compression, min-value-size 1024
compression, min-value-size 4096
```
Secondary tradeoff knobs worth their own points: `compression-max-value-size`
(caps tail decompress latency by excluding big values), `compression-threads`
(write-path / warmup CPU), `compression-dict-size` (ratio).

**Proposed JSON shape (single layer, matrix deferred):**
```json
"reference_config": "off",
"configs": [
  { "name": "off",     "server_args": ["--compression-master-switch","off"] },
  { "name": "min-256", "server_args": ["--compression-master-switch","compression","--compression-min-value-size","256", ...] },
  { "name": "min-512", "server_args": ["--compression-master-switch","compression","--compression-min-value-size","512", ...] }
]
```
Each config gets its own full phased run (dict → flush → warmup+force-sweep →
load) × `iterations`; the `off` reference skips the dict/sweep phases.

**Base-config requirement (all compression configs).** The warmup force-sweep
must actually compress the freshly-`--sequential`-populated keyspace. The
eligibility predicate skips recently-touched keys (`compression-min-idle-seconds`,
default 60s), and the populate just wrote them all — so the base config must set
**`compression-min-idle-seconds 0`** (and a permissive `compression-min-savings-ratio`)
so force-sweep compresses everything deterministically. Same spirit as the in-tree
§7.1 transparency-test harness.

**Open sub-decisions:**

- **(Q5a) Raw `server_args` vs a structured `compression` block.** Raw args:
  flexible, passes any flag, no schema churn as the in-tree compression surface
  evolves; matches amz-orc's `args` exactly. Structured (`{min_value_size,
  max_value_size, threads, dict_size}` the orchestrator renders to flags):
  validated, self-documenting, but couples the bench schema to the compression
  flag set. _Tentative: raw `server_args` for v1 — the flag surface is still
  moving in-tree; revisit a structured block once it stabilizes._
- **(Q5b) The canonical/example config set.** _Tentative: ship the min-value-size
  eligibility sweep (off + 256/512/1024/4096) as the headline example; add a
  threads variant and a dict-size variant as separate examples. Hot-value-skip
  (`min-idle-seconds`/`lfu-threshold`) is an advanced scenario deferred — it
  conflicts with the deterministic full-compression warmup above._
- **(Q5c) Dict consistency across configs.** With `dict_mode=import`, share one
  imported dict (trained on the corpus) across all eligibility/threads configs so
  the comparison isolates the knob. Configs that change `dict-size` or training
  must import/train their own dict. _Tentative: shared dict for eligibility/threads
  sweeps; per-config dict for dict-size/training sweeps._
- **(Q5d) Per-config phase runs.** Each config is an independent server instance
  with its own dict→warmup→load phases; `off` skips dict/sweep. _Tentative: confirm._

**Answer:** Confirmed (surface only; canonical set → Q7).

- **(Q5a) BOTH structured + raw.** A config entry carries a structured
  `compression: { master_switch, threads, min_value_size, max_value_size,
  min_idle_seconds, dict_size, … }` block (validated, self-documenting, rendered to
  `--compression-*` flags by the orchestrator) **plus** a raw `extra_args`
  passthrough for anything the block doesn't cover or future flags. Both are merged
  into the server start command (structured first, then `extra_args` so raw can
  override). amz-orc itself has only the raw `args`; the structured block is our
  addition.
- **(Q5b) Canonical/example config set → deferred to Q7.**
- **(Q5c / Q5d) Each config is a self-contained run** with its own
  dict→populate→profile-prep→measure phases (per Q6). **No cross-config dict
  sharing**: `dict_mode=train` → each run trains its own (correct, esp. when
  sweeping `dict-size`); `dict_mode=import` → each imports (possibly the same bytes
  — that's just the import source). The `off` `reference` run skips the
  dict/compress phases.

_**Q5 status: CONFIRMED** (2026-06-24) for the surface — structured+raw config
expression, per-config self-contained runs, no cross-config dict sharing. Canonical
example config set deferred to Q7._

---

## Q6. Reaching & measuring a stable compression profile (amends Q4)

Q4 left the warmup as "force-everything-compressed, then load." Refinement showed
that's wrong on two counts: it pins `compression-min-idle-seconds` to 0 (destroying
it as an experimental variable) and it measures an artificial all-compressed state
rather than the realistic profile.

**The compression profile is an equilibrium of (corpus × workload × eligibility
config).** In v1, a read creates only a *transient* uncompressed view that is
restored to compressed at the event-loop boundary (R2.5.7); **only writes
permanently decompress**, and there is **no read-driven auto-demotion** (R2.5.6).
The steady-state partition is therefore: a key is **uncompressed iff its access
rate keeps `idle < min-idle-seconds`** (hot), else **compressed** (cold) — a
function of access rate vs `min-idle-seconds`, independent of start state.

**Start state: compressed.** Under a single shared key distribution (Q3) with
write-fraction > 0, start-compressed and start-uncompressed converge to the *same*
equilibrium (every hot key is write-hot too → its writes decompress it). We choose
**start-compressed** because (a) it matches operator intent when enabling
compression (memory-priority), and (b) it converges faster — only the **small hot
set** must flip (via writes) versus compressing the **large cold majority** via the
paced sweeper. The lone correctness exception — pure read-only (0% write) workloads,
where start-compressed would leave read-hot keys compressed — is **out of scope**
(unrealistic for the workloads we benchmark).

**`compression-min-idle-seconds` is the memory↔latency lever:** `0` ⇒ everything
compressed (max memory saved, max read-decompress latency); higher ⇒ hot keys stay
uncompressed (latency improves, less memory saved). It is a **free swept variable**,
not pinned.

**Phase model (per config × iteration):**

| Phase | Action | Exit |
|---|---|---|
| 1. Train | import dict (default) or train+flush | `compression_active_dict_id != 0` |
| 2. Populate | `--sequential <key_count> --value-data corpus` | exactly `key_count` keys |
| 3. Compress-all (deterministic start) | `min-idle-seconds=0` + `COMPRESSION SWEEP FORCE`, cranked pacing/threads | `compressed_objects == size-eligible count` |
| 4. Profile-prep (continuous load) | set **real** `min-idle-seconds` (lever) + real pacing; run the load workload; writes decompress hot keys | **plateau** (below) |
| 5. Measure (same continuous load) | at plateau, orchestrator releases the **record-start barrier** → all benchmark procs reset HDR histograms → measure for `measurement-duration`; poll `used_memory` (MAX) + CPU | duration elapsed → procs emit windowed histograms |
| 6. Aggregate | merge per-bucket histograms across procs + `iterations`; delta vs `reference` | — |

Phases 4 and 5 are **one uninterrupted load run** (no stop/restart) — see windowed
recording below.

**Plateau detection (phase 4 → 5 trigger).** Orchestrator polls INFO:
- **Primary:** `compression_compressed_objects` stable within ~1–2% over N
  consecutive polls (~10s apart) — the profile's defining quantity, less transient
  noise than `used_memory`.
- **Secondary cross-check:** `used_memory` stable.
- **Max-timeout → FAIL the run** (report "profile did not stabilize within Ts").
  No fallback measuring — we never measure a half-converged state on assumptions.

**Windowed measurement (no profile-disturbing gap).** A two-load split (stop
warmup-load, start measurement-load) leaves a multi-second zero-load gap during
which idle clocks advance — for small `min-idle-seconds` this flips the whole hot
set to eligible and the sweeper recompresses it, collapsing the profile right
before measurement. Instead: **one continuous load** with a **second FIFO barrier**
(mirroring the fork's existing start barrier). At plateau the orchestrator releases
it → all benchmark processes **simultaneously reset their HDR histograms** and begin
the measurement window (connections stay open → zero gap). At `measurement-duration`
they emit the **windowed** histogram; merge per Q2 (the emitted distribution now
covers only the measurement window — cleaner, no warmup pollution).

**Benchmark prerequisites (updated).** Adds a third valkey-benchmark feature beyond
`--value-data corpus:FILE` and `--sequential`: **windowed recording**
(reset-histogram + begin-measurement on a signal / 2nd FIFO barrier, then emit the
windowed histogram). This revises the earlier "no benchmark change for the boundary"
— windowed recording is a C change, but it is the only gap-free option and reuses
the fork's barrier/duration machinery (reasonable upstream candidate). Rejected
alternatives: a fixed `--warmup-duration` (requires predicting stabilization time —
exactly what the fail-on-timeout rule forbids); per-interval bucket-count dump +
post-hoc window selection (heavier output, more bookkeeping than one clean windowed
histogram).

_**Q6 status: CONFIRMED** (2026-06-24). Start-compressed; `min-idle-seconds` is the
free memory↔latency lever (0 = compress-everything). Phases: train → populate →
compress-all → profile-prep-to-plateau → signaled windowed measurement → aggregate.
Plateau = `compressed_objects` stable (used_memory cross-check), **fail on
max-timeout**. Measurement via one continuous load + 2nd-barrier windowed recording
(new benchmark prerequisite). Amends Q4._

> **Refined by design (2026-06-25):** record-start is a **signal**, not a FIFO
> barrier — the loaders are busy in their traffic loop and must not block. The
> start-load barrier stays a FIFO; record-start becomes a configurable signal.
> valkey-benchmark gets ONE new flag **`--record-start-signal <SIGNUM>`** (opt-in)
> that makes the **existing** `--warmup`→`--duration` warmup-exit reset (#2581)
> signal-triggered instead of fixed-time; the existing **`--duration`** is the
> measurement window and the existing **`--rps`** (#1761) provides the open-loop
> rate. The orchestrator sends `killpg(loader_pgid, SIGNUM)` (default `SIGUSR1`).
> See `design/detailed-design.md` §3.3 / R8.3.

---

## Q7. The canonical / example config set

Q5 deferred the actual config list to here. A "canonical config set" = the
`configs` array in a shipped example run-JSON: one swept compression knob across
several values + the `off` reference, producing the Pareto points for one
(workload, TPS).

**Which knob headlines the sweep?** Q6 established `compression-min-idle-seconds`
as *the* memory↔latency lever (0 = everything compressed → max memory / max
latency; higher = hot keys uncompressed → less memory / less latency). It traces a
clean, monotonic Pareto frontier for a given workload. The rendering proposal's
original example swept `compression-min-value-size` (the size-eligibility lever),
which is also valid but moves a different axis (which keys are eligible *by size*,
a static data property) rather than the hotness axis (dynamic, workload-driven).

**Proposed canonical set (headline example JSON):**
```json
"reference_config": "off",
"configs": [
  { "name": "off",      "compression": { "master_switch": "off" } },
  { "name": "idle-0",   "compression": { "master_switch": "compression", "min_idle_seconds": 0 } },
  { "name": "idle-10",  "compression": { "master_switch": "compression", "min_idle_seconds": 10 } },
  { "name": "idle-60",  "compression": { "master_switch": "compression", "min_idle_seconds": 60 } },
  { "name": "idle-300", "compression": { "master_switch": "compression", "min_idle_seconds": 300 } }
]
```
All other compression knobs (dict, min/max-value-size, threads, sweeper=enabled)
held fixed across the sweep so the Pareto isolates the lever; workload + corpus +
TPS fixed per the run.

**Open sub-decisions:**

- **(Q7a) Headline swept knob.** _Tentative: `min-idle-seconds` (the Q6 lever;
  cleanest monotonic Pareto). `min-value-size` as a strong secondary example._
- **(Q7b) Sweep points.** _Tentative: off + idle ∈ {0, 10, 60, 300} — 5 points,
  enough to show frontier shape without ballooning run time. Values are illustrative
  and should straddle the workload's inter-access-time distribution to show movement._
- **(Q7c) One example JSON or several.** _Tentative: ship ONE headline (idle sweep)
  for v1; add `min-value-size` and `dict-size` example JSONs as the repo matures._
- **(Q7d) What's held fixed.** _Tentative: base compression config (imported dict,
  fixed min/max-value-size, threads, sweeper=enabled) + workload + corpus + TPS;
  only the swept knob varies._

**Answer:** Two-config **before/after** example (not a sweep), modeling a realistic
mid-size application. Records the headline number (memory saved + latency penalty)
at one realistic operating point; the config-list mechanism (Q5) still supports
N configs, so a multi-point `min-idle` / `min-value` sweep can be added later for a
full Pareto frontier (a frontier wants ≥3 points; 2 points is a baseline + one dot).

**Canonical example — configs:**
```json
"reference_config": "off",
"configs": [
  { "name": "off", "compression": { "master_switch": "off" } },
  { "name": "compression-on", "compression": {
      "master_switch": "compression",
      "automatic_sweeper": "enabled",
      "min_value_size": 256,
      "max_value_size": 16384,
      "min_idle_seconds": 3
  }}
]
```
- **Sparse config blocks — unspecified knobs use server defaults** (threads,
  dict-size, min-savings-ratio, sweep pacing, etc. not listed → server default).
  Only what differs is specified.
- `automatic_sweeper: enabled` is required (Q6: the sweeper compresses cold keys
  during profile-prep).
- `max_value_size = 16384` is deliberately below the in-tree 128 KiB default —
  bounds tail decompress latency and keeps values small for the memory budget.
- `min_idle_seconds = 3` is exactly the small-idle case that would have broken the
  two-load split — it validates the Q6 windowed-recording decision (continuous
  load, no gap).

**Canonical example — workload + data model (instantiates Q2/Q3 for a normal
machine):**
- **Target TPS:** hundreds of KTPS (e.g. 200–300 K), open-loop `--rps`; connections
  sized per Q2b to sustain it.
- **Command mix:** 80/20 read/write (GET/SET).
- **key_count:** 1–3 M.
- **key_distribution:** `zipf` (a hotset, so `min_idle_seconds=3` yields a
  meaningful hot/cold split).
- **value_size_distribution:** centered ~512 B, clamped `[256, 16384]` to match the
  compression size bounds (so all values are size-eligible) — e.g.
  `lognormal:512:0.8` clamped.
- **Dataset-size guardrail (the "few GB" constraint):** the constraint is on the
  *dataset* (server memory), not the corpus *file*. Uncompressed dataset (the `off`
  reference) ≈ `key_count × avg_value × overhead` = 1–3 M × ~512–700 B ≈ **0.7–
  2.3 GB** → runs on a normal 8–16 GB machine. The **corpus file** is separate and
  tiny: `corpus_entries` ≈ 50–100 K blobs × ~700 B ≈ tens of MB; keys map
  round-robin onto them (Q3a).
- **dict_mode:** train+flush (v1; import deferred — **superseded by Q8**).

_**Q7 status: CONFIRMED** (2026-06-24). Canonical example = 2 configs (off +
compression-on @ min256 / max16K / idle3 / sweeper-enabled), modeling ~200–300 KTPS,
1–3 M keys, 80/20, zipf, ~512 B values clamped [256,16K], uncompressed dataset
≤ ~2.3 GB (normal-machine runnable). Sparse config blocks → unspecified knobs use
server defaults. Mechanism still supports N-config sweeps; multi-point frontier
example deferred._

---

## Q8. The run-JSON schema (input)

Assembles Q1–Q7 into the single document an operator writes to define a run. One
JSON = one (workload, target TPS) × list of configs → one results file (Q2c).

**Proposed schema:**
```json
{
  "description": "json-mixed, 250K TPS, 80/20, compression on vs off",
  "output_directory": "results/",
  "server_binary": "/path/to/valkey-server",
  "benchmark_binary": "/path/to/amz_valkey-benchmark",
  "iterations": 3,
  "reference_config": "off",

  "data_model": {
    "value_shape": "json",
    "value_size_distribution": "lognormal:512:0.8",
    "value_size_min": 256, "value_size_max": 16384,
    "seed": 1234,
    "corpus_entries": 50000,
    "key_count": 2000000,
    "key_distribution": "zipf:0.99"
  },

  "workload": {
    "target_tps": 250000,
    "commands": [ {"type":"get","ratio":0.8}, {"type":"set","ratio":0.2} ],
    "connections_total": 256,
    "max_clients_per_process": 64,
    "pipeline": 1,
    "measurement_duration_seconds": 60
  },

  "profile_prep": {
    "plateau_metric": "compression_compressed_objects",
    "plateau_tolerance_pct": 2,
    "plateau_window_polls": 3,
    "poll_interval_seconds": 10,
    "max_timeout_seconds": 900
  },

  "configs": [
    { "name": "off", "compression": { "master_switch": "off" } },
    { "name": "compression-on",
      "dict_mode": "import", "dict_source": "auto",
      "compression": { "master_switch":"compression", "automatic_sweeper":"enabled",
                       "min_value_size":256, "max_value_size":16384, "min_idle_seconds":3 } }
  ]
}
```

**Open sub-decisions:**

- **(Q8a) Single `server_binary` vs per-config binary.** Per Q5 configs are the
  same binary + different compression args. _Tentative: one top-level
  `server_binary`; configs carry only `compression`/`extra_args`. Optional
  per-config binary override deferred (would let us also compare builds, amz-orc
  style)._
- **(Q8b) Dict per config.** `dict_mode` (`import`|`train`) + `dict_source`
  (a path, or `"auto"` = orchestrator trains a dict from the generated corpus via
  the `gen-zstd-dict` helper). _Tentative: per-config; default `import` + `"auto"`._
- **(Q8c) `profile_prep` params top-level vs per-config.** _Tentative: top-level
  (shared detection policy); optional per-config override later._
- **(Q8d) Corpus provenance.** Auto-generated from `data_model` (reproducible per
  `seed`), not a supplied path; cache by a `(shape, seed, size-dist, entries)` hash
  so repeated runs reuse it. _Tentative: auto-generated + cached._
- **(Q8e) Connection sizing.** Explicit `connections_total` +
  `max_clients_per_process`; orchestrator derives per-command process counts and
  per-process `--rps` via the Q2b split math. _Tentative: explicit, per Q2b._

**Answer:** Confirmed with corrections — per-config `server_binary`, generic
`benchmark_binary`, train-only dict acquisition (import deferred), corpus
auto-generated by the orchestrator.

**Final schema:**
```json
{
  "description": "json-mixed, 250K TPS, 80/20, compression on vs off",
  "output_directory": "results/",
  "benchmark_binary": "/path/to/valkey-benchmark",   // OUR extended build: --value-data, --sequential, windowed-recording
  "server_binary": "/path/to/valkey-server",         // top-level DEFAULT; any config may override
  "iterations": 3,
  "reference_config": "off",                                 // name of the baseline config in configs[]; all deltas are vs this

  "data_model": {
    "value_shape": "json",
    "value_size_distribution": "lognormal:512:0.8",
    "value_size_min": 256, "value_size_max": 16384,
    "seed": 1234,
    "corpus_entries": 50000,
    "key_count": 2000000,
    "key_distribution": "zipf:0.99"
  },

  "workload": {
    "target_tps": 250000,
    "commands": [ {"type":"get","ratio":0.8}, {"type":"set","ratio":0.2} ],
    "connections_total": 256,
    "max_clients_per_process": 64,
    "pipeline": 1,
    "measurement_duration_seconds": 60
  },

  "profile_prep": {
    "plateau_metric": "compression_compressed_objects",
    "plateau_tolerance_pct": 2,
    "plateau_window_polls": 3,
    "poll_interval_seconds": 10,
    "max_timeout_seconds": 900
  },

  "configs": [
    { "name": "off", "compression": { "master_switch": "off" } },
    { "name": "compression-on",
      "compression": { "master_switch":"compression", "automatic_sweeper":"enabled",
                       "min_value_size":256, "max_value_size":16384, "min_idle_seconds":3 } }
  ]
}
```

**Resolutions:**
- **`benchmark_binary`** = our extended valkey-benchmark (not the `amz_` fork name) — carries `--value-data`, `--sequential`, windowed-recording.
- **(Q8a) `server_binary` is per-config with a top-level default.** A config may override `server_binary` to compare *builds* (e.g. reference on a pre-feature build vs compression on the feature build), not just compression args.
- **`reference_config`** names the baseline config in `configs[]`; every other config's memory-saved-% and latency-penalty-% are computed relative to it.
- **(Q8b) Train-only for v1 — import deferred.** Dict acquisition = **train+flush** (Q6 phase 1, train branch). No `dict_mode`/`dict_source` fields. **Supersedes Q6's "import (default)" framing and Q7's "dict_mode: import" line.** *Dependency:* requires in-tree server-side training (`COMPRESSION TRAIN` / auto-train, the S1.x track) to be working — the benchmark can't run end-to-end until that lands (import would have run today, but train is the chosen path).
- **(Q8c) `profile_prep` is top-level** (shared plateau policy); optional per-config override later.
- **(Q8d) Corpus is auto-generated by the orchestrator from `data_model`** (reproducible per `seed`), written to a file, and passed to valkey-benchmark via `--value-data corpus:FILE`. It is an internal artifact, not an operator-supplied path; cached by a `(shape, seed, size-dist, entries)` hash.
- **(Q8e) Connections & TPS division (Q2b):** both `connections_total` and `target_tps` are split by the **command ratios** — command *c* (ratio *r_c*) gets `r_c × connections_total` connections and `r_c × target_tps` rps. Each command's share is then split into `procs_c = ceil(connections_c / max_clients_per_process)` processes; each process gets `connections_c/procs_c` connections and `--rps = tps_c/procs_c`.

_**Q8 status: CONFIRMED** (2026-06-24). Run-JSON assembled: top-level
(description, output_directory, benchmark_binary, server_binary default, iterations,
reference) + data_model + workload + profile_prep + configs[] (each may override
server_binary). v1 train-only (import deferred — supersedes Q6/Q7 dict-import);
corpus auto-generated from data_model → file → `--value-data`; connections & TPS
split by command ratios then by `max_clients_per_process`._

---

## Q9. The orchestrator output — results JSON

One run (one workload × TPS × configs, Q2c) → **one results JSON**. This is the
artifact the implementation must produce and that we verify *before* building charts
(charts/outliers deferred). It must be self-contained enough that the deferred chart
layer needs no re-computation — i.e. it carries the headline numbers (memory saved,
latency penalty) **already computed as deltas vs `reference`**.

**Proposed shape:**
```json
{
  "schema_version": 1,
  "run": {
    "timestamp": "2026-06-24T23:00:00Z",
    "description": "...",
    "seed": 1234,
    "machine": { "cpus": 16, "numa": "...", "mem_gb": 32 },
    "binary_checksums": { "valkey-server": "sha256:...", "valkey-benchmark": "sha256:..." },
    "data_model": { ...echoed... },
    "workload": { "target_tps": 250000, "commands": [...], "connections_total": 256, ... }
  },
  "reference_config": "off",
  "configs": {
    "off": {
      "profile_prep": { "plateaued": true, "time_to_plateau_s": 45 },
      "iterations": [
        { "used_memory_max": 2310000000, "achieved_tps": 249800, "cpu_pct": 61.2,
          "latency_us": { "get": {"p50":..,"p99":..,"p999":..}, "set": {...}, "overall": {...} } }
      ],
      "aggregate": {
        "used_memory_max": 2310000000,
        "achieved_tps": 249800, "cpu_pct": 61.2,
        "compression": { "ratio": null, "compressed_objects": 0, "net_saved_bytes": 0, "...": "..." },
        "latency_us": { "get": {"p50":..,"p90":..,"p95":..,"p99":..,"p999":..,"p9999":..}, "set": {...}, "overall": {...} }
      }
    },
    "compression-on": {
      "profile_prep": { "plateaued": true, "time_to_plateau_s": 38 },
      "iterations": [ ... ],
      "aggregate": { "used_memory_max": ..., "compression": { "ratio": 0.41, ... }, "latency_us": {...}, ... },
      "delta_vs_reference": {
        "memory_saved_pct": 42.0,
        "cpu_delta_pct": 12.0,
        "latency_penalty": {
          "get": { "p99": {"abs_us": 0.21, "pct": 18.0}, "p999": {"abs_us": 0.9, "pct": 31.0} },
          "set": { ... }, "overall": { ... }
        }
      }
    }
  }
}
```

**Open sub-decisions:**

- **(Q9a) Per-command granularity.** We run per-command benchmark processes
  anyway, so per-command latency falls out naturally. _Tentative: record
  per-command **and** an `overall` (merged across commands) — the per-command
  detail feeds the eventual heatmap; overall is the headline._
- **(Q9b) Percentile set.** _Tentative: p50/p90/p95/p99/p99.9/p99.99 (+p99.999 if
  the windowed sample count supports it) — the full set the merged histogram makes
  available._
- **(Q9c) Store percentiles only, or also the merged per-bucket histogram?**
  Histogram bytes allow later re-analysis (different percentiles) but bloat the
  file. _Tentative: store computed percentiles in the results JSON; keep the raw
  merged histogram as a side artifact (separate file) if we want re-analysis, not
  inline._
- **(Q9d) Pre-compute deltas vs reference in the output?** Since charts are
  deferred, yes — the results JSON should carry `memory_saved_pct` +
  per-percentile `latency_penalty` (abs µs + %) so the headline is readable without
  the chart layer. _Tentative: pre-compute deltas inline._
- **(Q9e) Memory fields.** _Tentative: `used_memory_max` (headline, Q4) +
  the `compression_*` INFO snapshot at measurement end; the full polled
  used_memory time series as a side artifact (for the eventual memory-over-time
  chart), not inline._
- **(Q9f) profile-prep outcome.** _Tentative: record `plateaued` + `time_to_plateau_s`;
  on max-timeout, `{ "plateaued": false, "reason": "timeout" }` and the run is
  marked failed (Q6 fail-on-timeout)._
- **(Q9g) Aggregation across iterations.** _Tentative: latency = **merge per-bucket
  histograms across iterations** (Q2), then compute percentiles once; `used_memory_max`
  = max across iterations; CPU/achieved-TPS = mean. (Outlier filtering deferred — the
  per-iteration values are retained so it can be added later.)_

**Answer:** Reframed — clean separation of concerns. The **orchestrator collects
raw artifacts and decides run validity (success/failure)**; all statistical
reduction (histogram merge, percentiles, MAX `used_memory`, delta-vs-reference) and
chart generation move to a separate **post-processor** script (its own idea-honing,
deferred). This **supersedes** the rich-aggregated-JSON sketch above.

**Orchestrator output = a timestamped run directory containing:**

- **Provenance** (amz-orc style): echo of the input run-JSON, binary checksums
  (server + benchmark), machine info (CPU / NUMA / mem), seed, corpus hash.
- **Per config → per iteration**, the **full raw artifacts**:
  - every loader (valkey-benchmark) process's **raw output** — the windowed
    histogram dump (per-bucket counts / full end-of-run distribution) + stdout/stderr.
  - the **server log**.
  - **mpstat** CPU capture.
  - **INFO snapshots**: the `used_memory` poll time-series + `compression_*` fields
    captured during the measurement window, plus the profile-prep poll log
    (the plateau-detection trace).
- **`orchestrator.log`** — the orchestrator's own run log.
- **`run-status.json`** — the one thing the orchestrator *decides*: per-config (and
  overall) **success / failure + reason**. A config/run is **FAILED** if:
  - **achieved TPS < target TPS** (within tolerance) — the load could not be
    sustained, so latency at the intended operating point is not comparable
    (open-loop comparison requires actually hitting the target TPS);
  - the compression profile did **not plateau** within `max_timeout_seconds` (Q6);
  - benchmark/server logs contain errors (connection failures, crashes, zero-RPS).

The orchestrator does **not** compute merged histograms, percentiles, MAX, or
deltas — it stores the raw inputs for those. The **post-processor** (separate,
future idea-honing) consumes this directory, performs the Q2 histogram-merge +
Q4 MAX-`used_memory` + delta-vs-`reference` reduction, and renders the charts
(per `research-rendering-proposal.md`). (Q2's merge *method* stands; its *owner*
is the post-processor.)

_**Q9 status: CONFIRMED** (2026-06-24). Orchestrator = collect-all-raw + decide
success/failure. Output: timestamped dir with provenance + per-config/per-iteration
raw (benchmark windowed-histogram dumps, server logs, mpstat, INFO/`used_memory`
series, profile-prep poll log) + `orchestrator.log` + `run-status.json`
(success/fail + reason; FAIL on unmet target TPS, no plateau, or log errors). All
reduction (histogram merge, percentiles, MAX, deltas) + charts move to a separate
post-processor — its own idea-honing. Supersedes the rich-aggregated-JSON sketch
above._

---

## Q10. Integration & verification test suite

The orchestrator is a **measurement instrument** — its correctness gates every
number we report, so the suite is deliberately broad. Three tiers; binary-dependent
tests are **skipped** (not failed) when `valkey-server` / the extended
`valkey-benchmark` aren't built.

### Tier 1 — Unit (pure Python, no binaries)

- **Config parsing & validation:** valid config → expected internal model; each
  required field missing → specific error; invalid values rejected (non-positive
  `target_tps`, command `ratio`s that don't normalize, unknown command `type`,
  malformed `value_size_distribution`, `value_size_min > max`, `key_distribution`
  typo, `reference` not in `configs[]`, `max_clients_per_process < 1`); sparse
  `compression` block → only specified knobs rendered (server defaults the rest);
  structured block → correct `--compression-*` flag rendering; `extra_args` merged
  after structured (raw overrides); per-config `server_binary` override vs top-level
  default resolution.
- **Corpus generation (deterministic):** same `seed` → **byte-identical** corpus;
  exactly `corpus_entries` blobs; value sizes obey the distribution within
  `[min,max]` clamps (per-distribution statistical assertions — constant exact;
  uniform bounded+flat; lognormal mean/spread within tolerance; all clamped); each
  `value_shape` (kv/json/log/coordinates) yields well-formed blobs; corpus cache
  keyed by `(shape, seed, size-dist, entries)` → hit reuses, miss regenerates.
- **Connection→process split math (Q2b/Q8e):** for representative
  `(connections_total, max_clients_per_process, ratios, target_tps)` → assert
  per-command process counts, per-process connections, and per-process `--rps`
  exactly match the formula. Edges: ratio→<1 connection (min 1), single command,
  many commands.
- **Plateau-detection logic:** synthetic INFO series → fires at the right point on a
  stabilizing series; does NOT fire on a still-climbing series; **fails on
  max-timeout** for a never-stabilizing series.
- **Run-status decision logic:** synthetic per-config results → correct
  SUCCESS/FAILED + reason for achieved_tps-below-target, plateau-timeout,
  log-error, all-good; overall = AND of configs.

### Tier 2 — Component (need real valkey-server and/or extended valkey-benchmark)

- **valkey-benchmark modifications (precision-critical):**
  - **`--sequential <keyspacelen>`:** post-run `DBSIZE == keyspacelen` and a full
    scan confirms keys `0..keyspacelen-1` each exist exactly once (no gap, no
    overshoot) — the coverage guarantee the memory number rests on.
  - **`--value-data corpus:FILE`:** sampled stored values are corpus members; the
    dataset is genuinely compressible (compress a sample, assert ratio < 1).
  - **`--key-distribution zipf [--zipf-theta]`:** access counts are skewed
    (rank-frequency / chi-square check vs uniform).
  - **windowed recording (signal / 2nd FIFO barrier):** drive a *known* latency
    profile before the record-start signal and a *different* one after; assert the
    emitted windowed histogram reflects **only the post-signal** window; multi-proc:
    all processes reset on the single barrier.
- **Config application on a live server:** start from a config; `CONFIG GET
  compression-*` matches rendered flags; master-switch toggles (off→compression)
  take effect.
- **Phase transitions (real server):** Train → `active_dict_id != 0` then flush →
  `DBSIZE == 0`; Populate → `DBSIZE == key_count`; Compress-all (`min_idle=0` +
  force-sweep) → `compressed_objects == size-eligible count`; Profile-prep → after
  switching to real `min_idle` + load, plateau detected, hot keys observably
  uncompressed / cold compressed.
- **INFO polling & capture:** `used_memory` series + `compression_*` fields captured
  at cadence; profile-prep poll log written.

### Tier 3 — End-to-end (full tiny run on a throwaway server)

- **Success path:** small run (e.g. `key_count=20k`, short duration, off +
  compression-on) → **run-status SUCCESS**; full run directory present and
  well-formed — provenance (config echo, binary checksums, machine info, seed,
  corpus hash), per-config/per-iteration raw artifacts (benchmark windowed-histogram
  dumps, server logs, mpstat, INFO/used_memory series, profile-prep poll log),
  `orchestrator.log`, `run-status.json`.
- **Failure paths (each induced, each asserted FAILED with the right reason):**
  unmet target TPS (impossibly high `target_tps` → achieved < target); no plateau
  (tiny `max_timeout_seconds`); server crash / unreachable (kill mid-run); benchmark
  error (bad port / connection refusal surfaced from loader logs).
- **Artifact-contract assertions** (shared helper): every expected file exists, is
  non-empty, parses; `run-status.json` schema-validates; benchmark raw outputs parse
  into per-bucket histograms (so the post-processor can consume them).
- **Reproducibility:** same config+seed twice → identical corpus hash; results within
  a documented tolerance (sanity, not a hard gate).

### Harness

- **pytest**; binary-dependent tests gated by markers, skipped with a clear message
  when binaries aren't present.
- Throwaway servers on isolated ports + dirs (amz-orc `prepare_server_directory`
  pattern); always torn down via `valkey-cli shutdown nosave` — **never `pkill -f`**
  (the self-match footgun).
- Tier-1 runs on every change; tier-2/3 in CI with binaries built.

_**Q10 status: CONFIRMED** (2026-06-25). Three-tier suite (unit / component / e2e),
extensive coverage: config parse+validate+apply, deterministic corpus + distributions,
connection-split math, the valkey-benchmark modifications (`--sequential` exact
coverage, `--value-data` compressibility, `--key-distribution` skew, windowed
recording), phase transitions, INFO/artifact capture, rigorous failure detection
(unmet TPS, no plateau, crash, benchmark error). pytest; binary-dependent tiers
skipped when unbuilt._
