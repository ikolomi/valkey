# Idea Honing — Compression-benchmark POST-PROCESSOR

_Interactive Q&A to refine the rough idea into concrete requirements. One
question at a time; answers recorded (verbatim or summarized to the final
decision) as we go. Same method as the orchestrator's
`../idea-honing.md`._

_Context anchors (already settled, NOT up for re-litigation here):_
- _The **orchestrator** is a separate program that emits a timestamped **run
  directory** of raw artifacts + a `run-status.json` SUCCESS/FAILED verdict
  (layout: `../design/detailed-design.md` §5.2). The post-processor **consumes**
  that directory; the only coupling is the on-disk contract._
- _The analytical **content** is the north-star in
  `../research-rendering-proposal.md`: headline **Pareto** (memory-saved % vs
  latency-penalty per percentile), supporting per-percentile-delta /
  memory-breakdown / per-command-heatmap / operational-headroom / summary-table,
  everything **delta-from-baseline**._
- _The **reduction** is fixed in `../design/detailed-design.md` Appendix C: merge
  per-bucket histograms across processes+iterations → true percentiles
  (p50…p99.999, NOT averaged percentiles), MAX `used_memory`,
  delta-vs-`reference`, optional amz-orc 4-method outlier filtering on the
  iteration axis._
- _Only **SUCCESS** runs/iterations feed the reduction; FAILED ones are reported,
  not measured._

_So idea-honing here is about the genuinely-open decisions: form factor, the
parse/merge mechanics (the technical crux), output medium, v1 section scope, and
aggregation boundary._

---

## Q1. Primary output artifact, form factor, and invocation boundary

The orchestrator's Q1 anchored "what do we render / what is a data point" because
it drove the whole data model. The post-processor's content is already pinned
(rendering proposal), so its anchoring question is **what concrete artifact does
this tool emit, in what medium, how is it invoked, and where does it live** —
because that dictates language, dependencies (Plotly?), repo location, and the
testable boundary between reduction and rendering.

**Dimensions of this one question:**

- **(Q1a) Output medium.**
  1. **Self-contained interactive HTML** (Plotly via CDN; reuse amz-orc's
     `generate_html` scaffold + its absolute↔%-delta `switchMode` toggle). One
     shareable, screenshot-able file. _(tentative)_
  2. Static PNG charts + a Markdown summary table.
  3. Reduced-numbers JSON only; charts left to a separate viewer.
- **(Q1b) Also emit a machine-readable `report.json` of the reduced numbers**
  (merged per-percentile latencies, MAX `used_memory`, `compression_ratio`,
  delta-vs-reference) alongside the HTML? _(tentative: yes — split **reduction**
  (pure, unit-testable, emits `report.json`) from **rendering** (consumes
  `report.json` → HTML). Lets the numbers be verified without parsing charts and
  lets others re-chart.)_
- **(Q1c) Invocation + location.** Standalone Python under
  `utils/compression-benchmark/postprocessor/`, invoked
  `python3 postprocess.py <run-dir> [-o report.html]`; a strictly separate
  process from the orchestrator (run after it, by hand or a thin wrapper).
  _(tentative)_
- **(Q1d) Aggregation boundary.** One run-dir → one report (a single
  workload × target-TPS × config-set; the Pareto's multiple points come from the
  multiple **configs** within that one run-dir). Multi-run / TPS-sweep
  aggregation across several run-dirs is deferred. _(tentative)_

**Tentative recommendation:** self-contained interactive HTML (Plotly, amz-orc
scaffold) **+** a sibling `report.json` of the reduced numbers; standalone
`postprocess.py <run-dir>` under `utils/compression-benchmark/postprocessor/`;
one run-dir → one report; multi-run sweep deferred.

**Answer (RESOLVED):** Accept all four tentatives — self-contained interactive HTML
(Plotly, amz-orc scaffold) **+** sibling `report.json` of reduced numbers; reduction
split from rendering; standalone `postprocess.py <run-dir> [-o report.html]` under
`utils/compression-benchmark/postprocessor/`; one run-dir → one report; multi-run/TPS-sweep
aggregation deferred.

---

## Cross-cutting requirements (accepted, apply to all phases)

- **C1. Outlier detection IN SCOPE for v1.** Port amz-orc's **4-method consensus**
  (`find_consensus_outliers`: IQR / z-score / MAD / percentile-bound; flag when ≥N methods
  agree) operating on the **iteration axis** of a reduced metric. Outliers are flagged +
  reported, and excluded from the headline reduction (config: keep/drop, default drop) — never
  silently dropped without surfacing them.
- **C2. e2e tests IN SCOPE.** Mirror the orchestrator: drive the real `postprocess.py` over a
  real (small) orchestrator run-dir and assert the produced `report.json` numbers + that the HTML
  renders. (Detailed e2e matrix decided in the plan phase.)

---

## Q2. Configurable percentiles for the charts

User idea: `--memory-vs-latency-percentiles=p50,p99,p99.9` (default `p99`), and the same
parameterization for the other percentile-bearing charts.

**My take — yes, with one structural refinement that makes it cheap and correct:**

- **Reduction computes the FULL canonical percentile vector once** (p50, p90, p95, p99, p99.9,
  p99.99, p99.999) for every config × command, straight from the merged histogram, and stores it
  in `report.json`. **Percentile selection is then a pure *rendering-side view*** over
  already-reduced data — no re-reduction, no re-parsing. This keeps `--…-percentiles` a cosmetic
  filter and keeps the numbers verifiable independent of which charts you ask for.
- **Key geometric insight for the Pareto:** memory-saved % is **percentile-independent** (it's a
  steady-state memory property, not a latency property). So selecting multiple percentiles only
  moves a config's point **vertically** (Y = latency penalty) at a **fixed X** (memory saved).
  Rendering `p50,p99,p99.9` ⇒ each config becomes a short vertical stack of points at its X — the
  spread *is* the "how much worse does the tradeoff get at the tail" story. Clean, not cluttered.
- **Per-chart applicability:**
  - **Pareto** (`--memory-vs-latency-percentiles`, default `p99`): series per selected percentile.
  - **Per-percentile-delta chart**: percentile *is* the X-axis — the param selects which
    percentiles appear (default = full canonical set).
  - **Per-command heatmap** (`--heatmap-percentiles`): columns = selected percentiles (default
    e.g. `p50,p99,p99.9`).
  - **Summary table**: latency columns = selected percentiles.
  - **Memory breakdown**: not percentile-bearing — param N/A.
- **Knob shape:** a global default `--percentiles=p50,p99,p99.9` + optional per-chart overrides
  (`--memory-vs-latency-percentiles`, `--heatmap-percentiles`). Avoids repeating the list.

**Open sub-question (Q2a):** canonical-set-only vs arbitrary percentiles?
- **Canonical set** {50,90,95,99,99.9,99.99,99.999}: covers the proposed examples; no
  interpolation; tail resolution bounded by sample count.
- **Arbitrary** (e.g. `p99.95`): requires linear interpolation between merged-histogram buckets —
  standard and easy, but extreme-tail accuracy is capped by bucket granularity + sample count, so
  it can imply false precision.
- _Tentative: support arbitrary via interpolation (flexible), default to the canonical list, and
  document that tail percentiles beyond the sample budget are interpolation-limited._

**Answer (RESOLVED):**
- **(1) Confirmed:** reduction computes the full **canonical** percentile vector **once** into
  `report.json` for every config × command; rendering is a pure view over those numbers.
- **(2) Canonical set ONLY — no arbitrary, no interpolation:**
  **{p50, p90, p95, p99, p99.9, p99.99, p99.999}**.
- **NOTE:** the `--percentiles` CLI knob proposed here is **later DROPPED entirely by Q3a** — the
  set is tiny and every series is legend-toggleable, so runtime percentile configuration is
  ceremony. Reduction still computes all 7; rendering emits all 7. (Kept this question for the
  reasoning trail.)


---

## Q3. Chart interactivity (series select/deselect)

User: should charts be interactive so series can be selected/deselected?

**Yes — and most of it is native to Plotly, essentially free:**

- **Legend click → toggle a series' visibility; legend double-click → isolate it** (hide all
  others). This is the built-in series select/deselect the user asked for. Series = percentile
  (Pareto / per-percentile-delta) or config, grouped via `legendgroup` so related traces toggle
  together.
- **Hover tooltips** with exact values (memory-saved %, latency µs + %-delta, config name,
  percentile) — built-in.
- **Absolute ↔ %-delta `switchMode` toggle** (`updatemenus` buttons) — ported from amz-orc; flips
  the Y between absolute µs and %-penalty-vs-reference across all charts.
- Pan/zoom/box-select/reset + PNG export — built-in Plotly modebar.

**Answer (RESOLVED — Q3 + Q3a):**
- Charts are interactive (Plotly): **legend click toggles a series, double-click isolates**; hover
  tooltips (exact memory-saved %, latency µs + %-delta, config, percentile); absolute↔%-delta
  `switchMode` button (amz-orc); standard modebar (pan/zoom/box-select/PNG export).
- **Q3a — drop the percentile CLI knob; render ALL canonical percentiles, all toggleable.** The
  user's point: with a tiny set + legend toggle, configuring percentiles is pointless. So **no
  percentile flags at all.** Reduction computes all 7 (unchanged); rendering emits all 7
  everywhere.
- **Default visibility** (baked-in constants, not user-configurable):
  - **Pareto** — default-visible = **{p50, p99, p99.9}** (avoids 7 stacked points per config by
    default); the other four are present-but-legend-hidden, one click away.
  - **Per-percentile-delta chart / per-command heatmap / summary table** — show **all 7** (a
    7-point axis / 7-column heatmap / 7-column table is readable and complete).
- Net win: zero percentile flags to parse, validate, document, or test; full flexibility retained
  via the legend.



---

## Q4. The latency-merge source — a BLOCKING gap in the current artifacts (verified from source)

The post-processor's headline (latency **penalty per percentile**, merged across the loader
**processes** within a config and across **iterations**) needs a mergeable per-process latency
distribution. **Reading the actual code, that data is NOT captured today.**

**What I verified (`src/valkey-benchmark.c` + `lib/benchmark.py`):**
- valkey-benchmark uses **HdrHistogram** (`hdr_init`, range 10µs–3s, sig-figs = `--precision`).
  Non-`-q` output prints two latency sections:
  1. **"Latency by percentile distribution"** — HdrHistogram *percentile-iterator* points
     `P% <= V ms (cumulative count C)`, **µs-precision values** but the value points are
     **per-process** → not directly summable.
  2. **"Cumulative distribution of latencies"** — *linear-iterator* on a **fixed grid** (100µs
     buckets ≤2ms, then 1ms) → **identical edges across processes** → additively mergeable, but
     **100µs/1ms resolution is coarse**.
- **BUT `lib/benchmark.py` runs every loader with `-q`** (`populate_argv` L66, `loader_argv` L92).
  Under `-q`, valkey-benchmark prints only `<title>: <rps> requests per second, p50=<p50> msec`.
  The orchestrator captures `achieved_rps` and records loader entries as
  `{command,index,returncode,achieved_rps}` — **no percentile distribution, no histogram**.
  `info-measurement.json` has memory + compression INFO + rps, **no latency beyond p50**.

**⇒ The headline "latency penalty per p99/p99.9" is currently un-deliverable from the artifacts.**
The orchestrator must be enhanced to emit + capture per-process latency data — a **prerequisite
change** (touches the open PR #44 work; not merged yet, so fine, but it is scope to bless). Why we
missed it: design Appendix C *assumed* the loader stdout carried the distribution; the
implementation chose `-q`.

**Options for the capture/merge source (the real Q4 decision):**
- **(A) Drop `-q`; parse the linear "Cumulative distribution".** In-tree only; fixed grid → exact
  additive merge. **Risk: the 100µs grid may be too coarse** to resolve the penalty (localhost
  GET/SET p99 ≈ 150–400µs; a 10–50µs compression penalty can vanish inside a 100µs bucket).
- **(B) Drop `-q`; parse "Latency by percentile distribution" with `--precision 4`.** In-tree only;
  µs-resolution values, but per-process points ⇒ **approximate** re-binning to merge.
- **(C) Add a tiny `--latency-dump FILE` to valkey-benchmark** writing recorded raw hdr buckets
  (`low,high,count` via `hdr_iter_recorded_init`). **Lossless, µs-accurate, trivial + exact merge**
  (sum counts on the canonical hdr layout). Cons: one more in-tree C flag + tests — but consistent
  with the existing `--value-data` / `--record-start-signal` / `--key-distribution` additions.

**My recommendation: (C).** The whole deliverable is resolving small tail penalties; coarse/approx
merges undermine it, and the flag is small + self-contained. Orchestrator then drops `-q` on the
**measured** loaders (populate stays quiet), passes `--latency-dump`, captures the per-process dump
files beside loader stdout; the post-processor sums them. **Fallback (B) with `--precision 4`** if
we'd rather not touch the C benchmark again.

**Sequencing:** the post-processor *design* can proceed now (it just fixes the input contract to
whichever source we pick); its *implementation* depends on the orchestrator latency-capture change
landing first.

**Answer:** _(pending — need your call on A / B / C + accepting the orchestrator prerequisite)_

---

## Q4-addendum. Artifact-vs-PURPOSE gap audit (why the e2e missed it + what else is missing)

**Root cause of the miss:** the e2e suite asserts (a) the orchestrator produces the artifacts *it
was designed to* and (b) those are *internally self-consistent* (memory_max == max(series),
ratio ≈ compressed/uncompressed, loader rps re-parses). It was written against the
**implementation's own output contract**, never against the **downstream consumer's input
contract** (the post-processor didn't exist). "Output is sane" was operationalized as
"present fields are correct," NOT "the fields the goal requires are present." For a
memory-vs-**latency** benchmark, no test ever asked "is latency captured at all?" — it isn't.
**Soundness-of-present ≠ completeness-for-purpose.**

**Trust verdict:** the e2e are a solid **regression** net (orchestrator keeps doing what it does)
but are NOT a **fitness-for-purpose** guarantee. Fix = derive artifact requirements from this
contract and add e2e assertions keyed to it. Doing the post-processor PDD is what surfaces the
contract — so this is the right time.

**Full gap map (rendering proposal §1–6 + "metrics I would collect" vs captured artifacts):**

| Report need | Captured today? | Source / gap |
|---|---|---|
| **Aggregate latency p50…p99.9** (Pareto Y, §1/§2) | **❌ MISSING** | loaders run `-q` → only rps. **Needs Q4 (C).** |
| **Per-command latency** (heatmap §4) | **❌ MISSING** | recoverable: loaders are split per-command → per-process dump tagged by command (with C). |
| read-vs-write latency, timeout/error rate | ❌ MISSING | derivable from per-command latency + benchmark error counts (not captured). |
| memory total | ✅ | `used_memory_*`. |
| compressed / uncompressed bytes, ratio, net_saved | ✅ | INFO compression. |
| **fragmentation ratio, RSS, used_memory_dataset** (§3) | **❌ MISSING** | we capture only `used_memory`, not the INFO `memory` section. |
| **dictionary memory bytes, metadata overhead** (§3) | **❌ MISSING** | not in INFO compression field set (only `known_dicts` count). |
| eligible keys %, eligible bytes %, actually-compressed % | 🟡 PARTIAL | derivable from `compressed_objects`/`dbsize` + byte totals; "eligible" not directly exposed. |
| **main-thread vs compression-worker CPU split** (§5) | **❌ MISSING** | `mpstat.log` is best-effort *total host* CPU; no per-thread split. |
| compression queue depth | ✅ | `candidates_pending`. |
| compress/decompress ops/sec | ✅ | `compressions_per_sec` / `decompressions_per_sec`. |
| **avg compress / decompress time** | **❌ MISSING** | only `training_last_duration_ms` (training, not per-op). |
| skipped/rejected/backpressure | ✅ | `skipped_incompressible`, `candidates_dropped_total`, `sweep_backpressure_total`. |
| **evictions, OOM/rejected writes** (§5 stability) | **❌ MISSING** | INFO `stats` not captured. |
| dictionary rebuilds/misses | 🟡 PARTIAL | `known_dicts`/`active_dict_id`; no miss counter. |
| workload summary (TPS, mix, value/key dist, dataset) | ✅ | run-config echo + provenance. |

**So the latency gap is the cornerstone miss, but it is NOT the only one.** A correct
orchestrator-side capture pass (prerequisite to the post-processor) should add: per-process latency
(Q4-C), the INFO `memory` section (frag/rss/dataset), dictionary-memory + metadata if the feature
exposes them, INFO `stats` (evictions/OOM/expired), and ideally per-thread CPU. The post-processor
then degrades gracefully for anything still unavailable (render what exists, label the rest "n/a"),
and the **e2e must assert the orchestrator captures each field this contract needs.**

---

## Q5. Contract boundary — orchestrator owns a STABLE schema, decoupled from valkey-benchmark

**User insight (accepted):** valkey-benchmark is an internal load-gen detail of the orchestrator;
its output format must NOT be the orchestrator→post-processor contract. If valkey-benchmark changes
format, only the orchestrator's parser should break — loudly — not the whole system.

**Decision:**
- The orchestrator **parses** valkey-benchmark latency output and **emits its own stable schema** —
  a `latency` block inside the existing `info-measurement.json`: per-command histogram (bucket
  `[low, high, count]` arrays + `total_count`), **already summed across the iteration's loader
  processes**. NOT a separate format/tool (that would be overkill) — one defined block in JSON we
  already write.
- The **post-processor consumes that schema only** (never valkey-benchmark text); it merges across
  **iterations**, runs outlier detection (iteration axis), computes percentiles.
- valkey-benchmark format drift ⇒ contained to the orchestrator parser ⇒ caught by a **parser test
  pinned to a FROZEN sample** of valkey-benchmark output (asserts "parses" + "histogram correct").
  This is the test that should have existed and would have caught the original latency gap.
- This is why **Q4 = C** fits: raw hdr `--latency-dump` is the parser INPUT; the orchestrator turns
  it into the stable schema. Per-iteration histogram = natural unit (outlier detection is per-iter).

## Q6. Revised orchestrator capture pass (field scope — user-pruned)

Honest "why the rendering proposal asked" + verdict after the user's scope challenge:

| Field | Proposal rationale | Verdict |
|---|---|---|
| **Per-command latency histogram** | Pareto Y + heatmap; the cornerstone | **KEEP (Q4-C)** — stable schema per Q5 |
| main-thread vs worker CPU split; avg comp/decomp time | §5 deep-dive headroom | **DROP** — overkill; use **server-process CPU%** instead |
| dictionary memory; metadata overhead | §3 "why 28% not 45%" breakdown | **DROP v1** — explanatory, maybe unexposed; byte-totals already tell savings |
| dataset_mem | §3 | **DROP** |
| **mem_fragmentation_ratio + used_memory_rss** | §3 — savings can be eaten by fragmentation | **KEEP** — same INFO `memory` read, ~0 cost; **correctness guard on the headline** (avoid publishing fake `used_memory` savings), not a chart |
| **evicted_keys / OOM / rejected writes** | §5 stability | **KEEP as run-VALIDITY guard** — a memory benchmark that evicts/OOMs is invalid; one INFO `stats` read; ties to SUCCESS/FAILED, not a chart |
| compressed/uncompressed bytes, ratio, net_saved | §3 | already captured ✅ |
| queue depth, comp/decomp ops/sec, skip/backpressure | §5 | already captured ✅ |
| server-process CPU% | §5 headroom (pruned form) | **KEEP** — capture `valkey-server` process CPU (not host-total mpstat) |

**Net new orchestrator capture:** per-process→per-iteration latency histogram (stable schema) +
INFO `memory` (frag/RSS) + INFO `stats` (eviction/OOM guard) + server-process CPU%. Plus: the
parser frozen-sample test, and contract-driven e2e asserting every field this contract requires.

---

## Q6-revision (user) — RSS headline + eviction is a valid scenario

- **Q4 = C CONFIRMED.** The "Cumulative distribution" text *is* summable but only at 100µs (too
  coarse for a ~10–50µs penalty); "Latency by percentile distribution" is finer but per-process
  (not cleanly summable). Only **raw hdr buckets** give correct + µs-precise cross-process
  aggregation → add `--latency-dump` to the loader. No other clean way.
- **Memory headline = RSS, not used_memory.** RSS (`used_memory_rss`) is the physical RAM the user
  actually pays and the honest savings indicator; `used_memory` is logical and can **overstate**
  (freed pages not returned + fragmentation). **Primary "memory saved %" = RSS-based**, with
  `used_memory`-based shown alongside; the **gap = fragmentation/retention story**, which
  **active-defrag moves** (so defrag is a meaningful config dimension RSS reflects and used_memory
  doesn't). Capture the **whole INFO `memory` section** in one read (used_memory + rss + frag).
- **Evictions/OOM are NOT a validity gate — reverted.** A memory-pressure/eviction regime is a
  legitimate scenario; compression's effect on eviction rate is itself a signal. **Capture
  `evicted_keys`/OOM/rejected as reported metrics + interpretation context**, no run-failure policy.

**Net orchestrator capture pass (final for now):** per-process→per-iteration latency histogram
(Q4-C, stable schema per Q5) + full INFO `memory` (RSS headline, used_memory + frag) + INFO `stats`
eviction/OOM/rejected (reported metrics) + server-process CPU%. Dropped: CPU thread-split, per-op
timing, dict/metadata bytes, dataset_mem.

---

## Q6-clarification — the memory comparison metric is `used_memory_rss`

The three fields are NOT additive (verified `src/server.c`):
`used_memory_rss (physical, superset) ≈ used_memory (logical, subset) × mem_fragmentation_ratio
(derived RSS/used ratio)`.

- **Comparison metric = `used_memory_rss`.** `memory_saved% = (rss_baseline − rss_config) /
  rss_baseline`. It's the physical RAM the user actually pays.
- `used_memory` captured alongside = logical figure; the **rss − used gap = fragmentation/retention**
  (defrag-sensitive). `mem_fragmentation_ratio` is free from the same INFO read (derived).
- **RSS-noise mitigations (bake in):** (a) keep plateau/steady-state detection on the low-noise
  `used_memory`, but **sample RSS at that steady-state point**; (b) issue **`MEMORY PURGE`**
  (confirmed: calls `jemalloc_purge()`) immediately before the final RSS read so retained dirty
  pages return to the OS → fair cross-config physical comparison.
- Orchestrator change: currently captures only `used_memory_*`; add `used_memory_rss` (+ the INFO
  `memory` section) and the pre-read `MEMORY PURGE`.

## Q7 (RESOLVED) — analysis lives in the post-processor; orchestrator stays dumb
The orchestrator only **sums each iteration's loader processes into one per-command histogram** and
writes the stable schema. ALL analytical logic — merge across **iterations**, outlier detection,
percentile computation, deltas, rendering — lives in the **post-processor** (one testable place).

## Q8 (RESOLVED) — outlier detection on the iteration axis, flag-low-N / drop-high-N
- amz-orc 4-method consensus runs on the **iteration axis per (config, metric)**.
- **Low iteration counts (2–3): flag + warn only, never auto-drop** (dropping leaves 1–2 samples;
  consensus is untrustworthy at low N). **Auto-drop only at ≥ threshold (≈5 iterations).** Flagged
  outliers are always surfaced in the report, never silent.
- (Q8a) Driving metrics: the **headline latency percentile + RSS** per config (not every percentile
  independently — keeps rejection coherent). Other percentiles follow the kept-iteration set.

---

## Q6-correction (user) — RSS via periodic sampling, NO purge, NO special logic

Supersedes the Q6-clarification "mitigations":
- **No `MEMORY PURGE`.** A real server won't purge, so purging measures an artificially optimistic
  RSS the user never experiences. Retention/fragmentation IS part of the real footprint — capture
  RSS honestly with it. Defrag's effect is studied by **running an active-defrag-enabled config**
  (RSS moves), not by faking it with a purge.
- **No special steady-state read.** Measurement is during the load phase and the orchestrator
  already samples memory periodically (`used_memory_series`). Just **add `used_memory_rss` to that
  same periodic series** (RSS isn't captured today). A long-enough window + the existing
  plateau/window logic represents the real footprint. Report statistic (max vs steady-state-window)
  = design-phase nit.
- Keep `used_memory` alongside RSS (rss−used gap = fragmentation insight, free from same INFO read).

## Q10 (RESOLVED) — NO recommendation logic; instrument + visualization only
Drop the entire "Recommended configs" story (and any latency-budget threshold/policy). The tool
**measures** and **visualizes**; the Pareto + summary table are the decision surface. A human — or
an LLM fed `report.json` — draws conclusions. Removes rendering-proposal §"Recommended configs".

## Q9 (STILL OPEN) — work breakdown across the 3 pieces
(1) valkey-benchmark `--latency-dump` [C], (2) orchestrator capture+contract+e2e-rewrite,
(3) post-processor. **Pending user call:** one shared design (defining the contract once) + three
staged implementation plans/PRs in dependency order — vs splitting the orchestrator-capture work
into its own separate PDD.

---

## Q9 (RESOLVED) — one design, three staged plans/PRs
**Option A.** A single shared **design doc** defines the contract (latency schema + new captured
fields) once, covering all three components. Then **three staged implementation plans/PRs in
dependency order**: (1) valkey-benchmark `--latency-dump` → (2) orchestrator capture + contract +
e2e rewrite → (3) post-processor. Avoids re-litigating the schema across separate PDDs.

---

# IDEA-HONING COMPLETE — decision log

1. **Deliverable (Q1):** standalone `postprocess.py <run-dir> [-o report.html]` under
   `utils/compression-benchmark/postprocessor/`; emits a self-contained interactive **Plotly HTML**
   + a sibling **`report.json`** of reduced numbers. Reduction (pure, testable → report.json) split
   from rendering (report.json → HTML). One run-dir → one report; multi-run sweep deferred.
2. **Percentiles (Q2/Q3):** canonical set only **{p50,p90,p95,p99,p99.9,p99.99,p99.999}**, no
   arbitrary/interpolation. **No percentile CLI knob** — render all 7, legend-toggleable; Pareto
   default-visible {p50,p99,p99.9}, rest hidden-but-toggleable; other charts show all 7.
3. **Interactivity (Q3):** Plotly legend toggle/isolate, hover, absolute↔%-delta `switchMode`
   button, modebar.
4. **Latency source (Q4=C):** add valkey-benchmark **`--latency-dump`** (raw hdr buckets); only raw
   buckets give correct + µs-precise cross-process aggregation. The current `-q` output captures NO
   distribution — the cornerstone gap.
5. **Contract boundary (Q5):** orchestrator parses valkey-benchmark output → emits its OWN stable
   schema (`latency` block in `info-measurement.json`, per-command, per-iteration). Post-processor
   consumes the schema, never valkey-benchmark text. Frozen-sample parser test guards format drift.
6. **Captured fields (Q6, pruned):** per-iteration latency histogram (stable schema) + INFO
   `memory` (**RSS = headline memory metric**, used_memory + frag for context) + INFO `stats`
   (eviction/OOM/rejected = reported metrics, NOT a gate) + **server-process CPU%**. Dropped:
   CPU thread-split, per-op timing, dict/metadata bytes, dataset_mem.
7. **Memory metric (Q6):** comparison = **`used_memory_rss`** (`saved% = (rss_base−rss_cfg)/rss_base`);
   RSS via the existing periodic sampling (add it to the series); **no MEMORY PURGE, no special
   steady-state read** — long window represents real user experience honestly.
8. **Analysis location (Q7):** orchestrator stays dumb (sums processes → per-iteration histogram);
   ALL merge/percentile/outlier/delta/render logic lives in the post-processor.
9. **Outliers (Q8/C1):** amz-orc 4-method consensus on the **iteration axis per (config, metric)**;
   driving metrics = headline latency percentile + RSS; **flag+warn at low N (2–3), auto-drop only
   ≥~5 iterations**; flagged always surfaced.
10. **e2e (C2):** contract-driven — assert the produced files contain every field the contract
    requires (fixes the original "soundness-of-present ≠ completeness-for-purpose" trust gap).
11. **No recommendation logic (Q10):** measure + visualize only.
12. **Breakdown (Q9):** one design, three staged plans/PRs (benchmark flag → orchestrator → PP).

**NEXT:** write `design/detailed-design.md` (this folder) covering the contract + all three pieces.
