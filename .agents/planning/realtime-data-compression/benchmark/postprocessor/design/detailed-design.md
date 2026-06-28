# Detailed Design — Compression Benchmark Post-Processor (+ latency-capture contract)

_Design of record for the result-reduction + visualization stage and the orchestrator/benchmark
changes it depends on. Decisions trace to `../idea-honing.md` (Q1–Q10, decision log)._

## Table of contents
1. [Overview & scope](#1-overview--scope)
2. [Relationship to the orchestrator design (what this supersedes)](#2-relationship-to-the-orchestrator-design)
3. [The contract — stable artifact schema](#3-the-contract--stable-artifact-schema)
4. [Piece 1 — valkey-benchmark `--latency-dump`](#4-piece-1--valkey-benchmark---latency-dump)
5. [Piece 2 — orchestrator capture + contract](#5-piece-2--orchestrator-capture--contract)
6. [Piece 3 — the post-processor](#6-piece-3--the-post-processor)
7. [Data models](#7-data-models)
8. [Testing](#8-testing)
9. [Requirements list](#9-requirements-list)
10. [Sequencing — three staged plans/PRs](#10-sequencing)

---

## 1. Overview & scope

The orchestrator collects raw per-config/per-iteration artifacts and a SUCCESS/FAILED verdict; it
does **no** reduction or rendering. This effort delivers the **reduction + visualization** stage,
plus the two upstream changes required to capture the cornerstone latency data that the current
artifacts lack (see §2).

**Three components, hard dependency order (Q9):**
1. **valkey-benchmark `--latency-dump FILE`** — dumps the (already-windowed) raw hdr latency
   histogram of a loader process.
2. **Orchestrator capture + contract** — pass `--latency-dump` to measured loaders, parse each
   process dump, **sum across the iteration's processes per command**, and write a **stable
   `latency` schema** into `info-measurement.json`; also add INFO `memory` (RSS), INFO `stats`
   (eviction/OOM), and server-process CPU%. Add the frozen-sample parser test + contract-driven
   e2e.
3. **Post-processor** — `postprocess.py <run-dir>`: merge across iterations, detect outliers,
   compute true percentiles + deltas-vs-baseline → `report.json`; render a self-contained
   interactive Plotly **`report.html`**.

**In scope:** outlier detection (C1), e2e per piece (C2). **Out of scope:** recommendation/threshold
logic (Q10), multi-run/TPS-sweep aggregation (Q1d), arbitrary percentiles (Q2), dictionary/metadata
memory + per-thread CPU + per-op compression timing (Q6).

---

## 2. Relationship to the orchestrator design

This design **supersedes** parts of `../../design/detailed-design.md`:

| Orchestrator design | Status | Superseded by |
|---|---|---|
| **R6.1** headline = MAX `used_memory`; "RSS not used in v1" | **REVISED** | Headline = **`used_memory_rss`** (Q6); `used_memory`+frag kept for context; RSS added to the periodic series; **no MEMORY PURGE** (Q6-correction). |
| **R6.3** process-total CPU via mpstat | **REFINED** | **server-process CPU%** (the `valkey-server` process), not host-total. |
| **R6.4 / §5.5** capture windowed latency histogram raw; post-processor merges | **DESIGNED-BUT-NOT-IMPLEMENTED + format wrong** | Implementation used `-q` and stored only rps. Now actually implemented via **Piece 1/2**; format = **raw hdr buckets** (not the per-process "percentile distribution" text, which is not cleanly mergeable). |
| **R7.x** orchestrator collects, doesn't reduce | **UNCHANGED** | Reduction stays in the post-processor (Q7). |
| **R8.3** `--record-start-signal` windowing | **UNCHANGED, reused** | `--latency-dump` dumps the histogram this already windows. |
| New | **ADDED** | INFO `stats` eviction/OOM/rejected as **reported metrics** (not a validity gate, Q6-revision). |

**Trust-gap remediation:** the latency miss was both a missing test and a silent
implementation/design divergence. Piece 2 adds (a) a **frozen-sample parser test** (fails loudly if
valkey-benchmark output drifts) and (b) **contract-driven e2e** that assert the produced artifacts
contain **every field this contract requires** — not merely that present fields are self-consistent.

---

## 3. The contract — stable artifact schema

**Principle (Q5):** valkey-benchmark's output format is an internal detail of the orchestrator's
load generation. The orchestrator parses it and emits its **own** stable schema; the post-processor
consumes the schema, never valkey-benchmark text. Format drift breaks only the orchestrator parser
(caught by the frozen-sample test).

The contract is the existing per-iteration `info-measurement.json`, extended. New/changed blocks:

```jsonc
{
  // ...existing: used_memory_*, dbsize, achieved_tps, compression, compression_config, loaders...

  // NEW — per-command latency histogram, summed across THIS iteration's loader processes (Q7).
  "latency": {
    "hdr": { "lowest": 10, "highest": 3000000, "significant_figures": 3 },  // for merge-compat check
    "per_command": {
      "GET": { "total_count": 1234567, "buckets": [[<value_usec>, <count>], ...] },
      "SET": { "total_count":  308642, "buckets": [[<value_usec>, <count>], ...] }
    }
  },

  // NEW — memory: raw series only; the post-processor derives all stats (Q7). RSS = headline (Q6).
  "memory": {
    "used_memory_series":      [<bytes>, ...],   // already sampled today
    "used_memory_rss_series":  [<bytes>, ...],   // NEW — sampled alongside, same cadence, no purge
    "mem_fragmentation_ratio_series": [<float>, ...],  // free from the same INFO memory read
    "steady_state_window": [<i_start>, <i_end>]  // sample indices the orchestrator deems steady (plateau)
  },

  // NEW — stability metrics (reported, NOT a gate, Q6-revision).
  "stats": { "evicted_keys": 0, "rejected_connections": 0, "expired_keys": <n>, "keyspace_misses": <n> },

  // NEW — server process CPU over the window (Q6).
  "server_cpu": { "pct_user": <f>, "pct_system": <f>, "pct_total": <f> }
}
```

**Merge-compatibility:** every loader inits hdr with identical params (10µs–3s, same
`--precision`), so bucket value→index mapping is identical. Summing counts at matching `value_usec`
across processes (orchestrator) and across iterations (post-processor) is **exact**. The `hdr`
block lets consumers assert identical params before merging (else hard error).

---

## 4. Piece 1 — valkey-benchmark `--latency-dump`

**Flag:** `--latency-dump <FILE>` (opt-in; absent ⇒ benchmark unchanged). Composes with
`--record-start-signal` (the histogram is already reset at window start) and is **independent of
`-q`** (separate file ⇒ stdout/rps parsing untouched).

**Behavior:** after the measured window ends (normal end-of-run), iterate the latency histogram's
**recorded** buckets (`hdr_iter_recorded_init`) and write:

```
# hdr lowest=10 highest=3000000 sigfig=3 total_count=<N>
<value_usec>,<count>
... (one line per non-empty recorded bucket)
```

`value_usec` = `iter.value_iterated_to` (or `highest_equivalent_value`); `count` = `iter.count`.
Only non-empty buckets (sparse). One file per loader process (the orchestrator gives each process a
distinct path).

**Why recorded-iter, not the text distributions:** raw recorded buckets are µs-resolution and share
the canonical hdr layout ⇒ exact additive merge. The "percentile distribution" text is per-process
points (lossy to merge); the "cumulative distribution" text is a 100µs grid (too coarse for a
~10–50µs penalty). (idea-honing Q4.)

**Tests (gtest/Tcl):** record known values → dump → assert bucket counts + total; assert composition
with `--record-start-signal` (only windowed samples appear); assert absence leaves output unchanged.

---

## 5. Piece 2 — orchestrator capture + contract

**5.1 Loader invocation.** `loader_argv` (measured loaders only; populate stays as-is) gains
`--latency-dump <work_dir>/loader-<cmd>-<idx>.hist`. Keep `-q`.

**5.2 Parse + sum (the parser, with a frozen-sample test).** A new `lib/latency.py`:
- `parse_dump(path) -> {hdr, total_count, buckets:{value_usec:count}}` — parses the Piece-1 format.
- `sum_histograms([...]) -> merged` — asserts identical `hdr` params, sums counts by value.
- The orchestrator sums an iteration's per-command process dumps → the `latency.per_command` schema
  block (§3). **Frozen-sample test:** a checked-in real dump sample → assert exact parsed buckets;
  fails loudly on format drift.

**5.3 Memory/stats/CPU capture (`lib/phases.py`, `lib/server.py`).**
- Periodic sampler already polls `used_memory`; **also record `used_memory_rss` and
  `mem_fragmentation_ratio`** (same INFO `memory` read, same cadence) into their series. Record the
  **steady-state window indices** (from the existing plateau detection). **No purge.** Do NOT
  precompute median/max — the post-processor derives all stats from the raw series (Q7).
- Capture `evicted_keys`/`rejected_connections`/`expired_keys`/`keyspace_misses` (INFO `stats`) at
  window end → `stats` block. (`mem_fragmentation_ratio` is in the memory series, above.)
- Capture **server-process CPU%** for the window: sample the `valkey-server` PID
  (`/proc/<pid>/stat` utime+stime deltas over wall time, or `pidstat -p <pid>`). Replaces R6.3
  host-total mpstat.

**5.4 e2e rewrite (contract-driven, C2).** Replace self-referential assertions with: for a real
small run, assert `info-measurement.json` contains `latency.per_command[GET/SET]` with non-empty
buckets and `total_count>0`; `memory.used_memory_rss_*`; `stats`; `server_cpu`. Off-vs-compression:
assert latency present in both; assert a non-trivial histogram (e.g., reconstructed p50 within a
sane band). This makes the suite a completeness check, not just a consistency check.

---

## 6. Piece 3 — the post-processor

`utils/compression-benchmark/postprocessor/`, standalone `postprocess.py <run-dir> [-o report.html]`.
**Reduction is split from rendering** (Q1b): reduction is pure and emits `report.json`; rendering
consumes `report.json` → `report.html`.

**6.1 Reduction (`reduce.py`) — pure, unit-tested.**
1. **Discover** configs + iterations from the run-dir; read each iteration's `info-measurement.json`
   + `run-status.json` (skip FAILED iterations/configs — render-available, report the rest).
2. **Per-iteration percentiles:** from each iteration's per-command `latency` histogram, compute the
   canonical percentiles (and an aggregate over all commands). (Needed for outlier detection.)
3. **Outlier detection (C1/Q8):** amz-orc 4-method consensus (IQR/z-score/MAD/percentile-bound) on
   the **iteration axis per (config, metric)**, metrics = headline latency percentile + RSS.
   **flag+warn at N≤~4; auto-drop only at N≥5.** Flagged iterations always recorded in the report.
4. **Merge kept iterations:** sum the per-command histograms across kept iterations (exact) →
   compute final true percentiles (p50…p99.999) per command + aggregate. RSS = chosen statistic
   across kept iterations.
5. **Baseline + deltas:** identify the reference config (`run-config` reference_config). Compute
   `memory_saved% = (rss_base − rss_cfg)/rss_base`; per-percentile latency delta (absolute µs **and**
   %); CPU delta; report eviction/etc. as-is.
6. **Emit `report.json`** (§7) — all reduced numbers, deltas, outlier flags, provenance echo.

**6.2 Rendering (`render.py`) — `report.json` → self-contained Plotly HTML.**
- **Pareto** (headline): X = memory-saved % (RSS), Y = latency penalty; **series per canonical
  percentile**, default-visible {p50,p99,p99.9}, others legend-hidden (Q3). Memory-saved is
  percentile-independent ⇒ a config = a vertical stack at fixed X.
- **Per-percentile-delta** chart (all 7), **memory breakdown** (RSS vs used_memory gap =
  fragmentation), **memory-stability** table+chart (per-iteration & combined
  min/mean/median/p95/p99/max/stddev for RSS+used_memory — §7.4, sanity/completeness),
  **per-command latency heatmap** (commands × 7 percentiles), **operational
  headroom** (server CPU%, queue depth, comp/decomp ops/sec, eviction), **summary table**,
  **workload-summary** header (from run-config/provenance).
- **Interactivity (Q3):** legend toggle/isolate, hover, absolute↔%-delta `switchMode` button,
  modebar. Reuse amz-orc `generate_html` + `switchMode` scaffold (`../amz-orc-findings.md`).
- **Degrade gracefully:** any contract field absent ⇒ render available charts, mark the rest "n/a".

**6.3 CLI/deps.** `postprocess.py <run-dir> [-o report.html] [--json report.json]`. Plotly via CDN
(self-contained HTML, no server). Reduction has **no third-party deps** (pure stdlib) so it's
trivially testable; rendering needs only the Plotly JS (embedded/CDN), not python plotly.

---

## 7. Data models

**7.1 `info-measurement.json` `latency`/`memory`/`stats`/`server_cpu` blocks** — §3.

**7.2 valkey-benchmark `--latency-dump` file** — §4 (`# hdr ...` header + `value_usec,count` lines).

**7.3 `report.json`** (post-processor output; the LLM-feedable artifact, Q10):
```jsonc
{
  "workload": { /* echo: tps, mix, value/key dist, dataset, seed, corpus hash */ },
  "baseline": "off",
  "configs": {
    "compression-512": {
      "status": "SUCCESS",
      "iterations": { "kept": [0,2], "flagged_outliers": [{ "iter":1, "metric":"rss", "methods":["iqr","mad"] }] },
      "memory": { "rss_bytes": <median>, "used_memory_bytes": <median>, "frag_ratio": <f>,
                  "memory_saved_pct": <f>, "used_memory_saved_pct": <f>,
                  "rss_stats":  { "min":<n>,"mean":<n>,"median":<n>,"p95":<n>,"p99":<n>,"max":<n>,"stddev":<n>,"samples":<n> },
                  "used_stats": { "min":<n>,"mean":<n>,"median":<n>,"p95":<n>,"p99":<n>,"max":<n>,"stddev":<n>,"samples":<n> },
                  "per_iteration_rss_median": [<n>, ...] },
      "latency": {
        "aggregate": { "p50":{"usec":<f>,"delta_usec":<f>,"delta_pct":<f>}, ... "p99999":{...} },
        "per_command": { "GET": { "p99":{...}, ... }, "SET": {...} }
      },
      "cpu": { "pct_total": <f>, "delta_pct": <f> },
      "stats": { "evicted_keys": <n>, ... },
      "compression": { "ratio": <f>, "net_saved_bytes": <n>, ... }
    }
  }
}
```

**7.4 Memory statistic (RESOLVED) — headline = MEDIAN, full distribution reported.**
The reported per-config memory value (RSS and used_memory) = **median of the steady-state-window
samples** within each iteration, then combined across kept iterations. Rationale: steady-state
memory has occasional transient upward spikes (client/replication buffers, defrag passes, momentary
large values); **MAX** is decided by the single worst spike (unstable run-to-run), **mean** is
skewed proportionally to spikes, **median** reflects the sustained footprint and ≈ mean on a clean
plateau (so it loses nothing when there are no spikes). To keep the choice low-stakes and serve the
provisioning view, **the full memory distribution is reported**: per iteration and across
iterations — `min / mean / median / p95 / p99 / max / stddev / sample_count` for both
`used_memory_rss` and `used_memory`. Surfaced as a **memory-stability table + chart** (§6.2) for
sanity (did it plateau? spike? cross-iteration variance?) and completeness. Outlier detection (Q8)
prunes anomalous *iterations* before this combination.

---

## 8. Testing

- **Piece 1 (C, gtest/Tcl):** dump correctness, windowing composition, opt-in no-op.
- **Piece 2 (pytest unit + e2e):** `latency.py` parse/sum (incl. **frozen-sample** drift test +
  identical-hdr-params assertion); contract-driven e2e (§5.4) — artifacts contain every required
  field with sound values.
- **Piece 3 (pytest unit + e2e):** reduction unit tests with **synthetic histograms of known
  distribution** → assert merged percentiles within tolerance (cross-process + cross-iteration
  merge is exact, so this is tight); outlier-detection unit tests (flag-low-N / drop-high-N);
  delta-vs-baseline math; `report.json` schema; **e2e:** run `postprocess.py` over a real small
  run-dir → assert `report.json` numbers + `report.html` renders (non-empty, contains expected
  chart divs). Graceful-degradation test (missing field ⇒ "n/a", no crash).

---

## 9. Requirements list

**Piece 1 — benchmark**
- **P1.1** `--latency-dump FILE` opt-in; absent ⇒ unchanged; independent of `-q`.
- **P1.2** Dumps recorded hdr buckets (`value_usec,count`) + hdr-params header, after the windowed
  measurement.

**Piece 2 — orchestrator**
- **P2.1** Measured loaders pass `--latency-dump`; per-process `.hist` captured.
- **P2.2** `latency.py` parses + sums per-command across processes → `latency` schema; identical-hdr
  assertion; frozen-sample test.
- **P2.3** Capture `used_memory_rss` in the periodic series; `mem_fragmentation_ratio`; INFO `stats`
  (eviction/OOM/expired/misses); server-process CPU%. No purge.
- **P2.4** Contract-driven e2e: artifacts contain every contract field with sound values.

**Piece 3 — post-processor**
- **P3.1** `postprocess.py <run-dir>` → `report.json` + `report.html`; reduction split from render.
- **P3.2** Merge histograms (process already summed by orchestrator; PP sums across kept iterations)
  → true canonical percentiles; never average per-iteration percentiles.
- **P3.3** Outlier detection on iteration axis (consensus); flag-low-N, drop-high-N; always surfaced.
- **P3.4** All metrics as **delta-vs-baseline**; memory headline = RSS.
- **P3.5** Charts per §6.2; canonical percentiles all rendered + legend-toggleable; interactive
  (toggle/hover/absolute↔%); degrade gracefully on missing fields.
- **P3.6** No recommendation logic.

---

## 10. Sequencing

One design (this doc) → **three staged implementation plans/PRs** in dependency order:
1. **`plan-1-benchmark.md`** — `--latency-dump` (own CR on `src/`; clang-format-18; gtest/Tcl).
2. **`plan-2-orchestrator.md`** — capture + `latency.py` + frozen-sample test + contract-driven e2e
   (extends PR #44 territory; commits prefixed `feat(compression-benchmark):`).
3. **`plan-3-postprocessor.md`** — reduction + rendering + tests (new `postprocessor/` subtree).

Each plan TDD red→green; review-then-commit-then-proceed; commit only when asked; push to fork only.

---

## 11. Empirical hardening (post-implementation findings)

Running the instrument on a real Valkey build surfaced artifacts that, once fixed,
flipped the headline result from "compression costs memory" to the correct tradeoff.
These are now part of the design of record:

- **F1 — corpus must be realistically compressible.** The original `json` shape filled
  values with a random base-62 pad, which sits at the entropy floor (`zstd -19` = 0.74),
  so compression had nothing to win. Replaced with realistic customer/order records
  drawn from a bounded vocabulary (repeated keys + human-readable words) → `zstd -19`
  = 0.15, and the server compresses to ~0.29. **A compression benchmark is only as
  valid as its corpus's compressibility; validate it with an external compressor.**
- **F2 — compress-all must wait for TRUE completion, not a growth-plateau.** The setup
  detector watched `compressed_objects` growth and mistook the paced/back-pressured
  sweep's stalls for "done" — measuring a ~21%-compressed dataset. Fixed
  (`info.poll_until_swept`): complete only when the worker queue has **drained**
  (`candidates_pending == 0`) AND `compressed_objects` is **steady**; **fail the
  iteration** if it can't complete within the setup budget. After the fix compress-all
  reaches ~all eligible objects, so the memory comparison is steady-state.
- **F3 — `setup_timeout_seconds` is an orchestrator parameter** (default 180) bounding
  setup (auto-train + compress-all) before failing — large datasets need more.
- **F4 — RSS-headline validated.** With F1+F2, the 1M/60 s/active-defrag run shows
  **RSS −52.7%** (715→338 MB) for a **+54% p99 latency** penalty — the canonical
  memory↔latency tradeoff. `used_memory` dropped 396 MB vs 490 MB logical `net_saved`;
  the ~94 MB gap is read-path transient decompression views (capped at savings) for the
  Zipfian hot set. `used_memory` alone (≈flat in the buggy run) hid all of this — RSS is
  the right headline.
- **F5 — report surfaces measurement coverage.** The report now shows per-config request
  counts (⇒ tail-percentile sample counts) + iterations kept/total, so limited-sample
  tail noise (the 8 s run's ~24-sample p99.99) is visible at a glance, and memory is also
  shown as a saved-% delta (not just absolute).

**Takeaway:** the instrument's value came from the empirical loop catching these — a
stable orchestrator→post-processor contract + pure reduction made fast re-analysis (and
this debugging) possible.
