# Plan 3 — The post-processor (Piece 3)

## Status: DONE + GREEN (uncommitted)
All tasks complete. Full suite **163 passed** (~102s; +21 vs Plan-2 baseline). `postprocessor/`
(self-contained, stdlib): `reduce.py` (discover, exact histogram merge → true percentiles, median
memory + full stats, consensus outliers flag-low-N/drop-high-N, deltas → `report.json`), `render.py`
(self-contained interactive Plotly HTML — Pareto + per-percentile-delta + memory breakdown/stability
+ heatmap + headroom + summary, absolute↔% toggle, all-7-percentiles legend-toggleable), `postprocess.py`
CLI. Tests: 19 unit (incl. outlier-drop) + 2 real-pipeline e2e (incl. determinism). No C changes.


_Implements design §6, §7, P3.1–P3.6. Depends on Plan 2 (the stable contract). New subtree
`utils/compression-benchmark/postprocessor/`. Commit prefix `feat(compression-benchmark): …` + DCO._
_TDD red→green; reduction is pure stdlib (trivially testable); review→commit→proceed._

## A. Reduction (`reduce.py`) — pure, no third-party deps
- [ ] **T3.1 (RED)** `discover(run_dir)` → configs + per-config iteration dirs + baseline (from
  run-config `reference_config`); SUCCESS/FAILED from `run-status.json`; FAILED skipped, recorded.
- [ ] **T3.2 (RED)** `percentiles(histogram, [p50…p99999])` from summed buckets → canonical
  percentiles. **Synthetic test:** build a histogram of a KNOWN distribution → assert each
  percentile within tolerance (merge is exact, so tolerance is tight).
- [ ] **T3.3 (RED)** Cross-iteration merge: sum kept iterations' per-command histograms →
  final percentiles. Test: 3 identical iterations merge to the same percentiles (idempotence) and a
  process-split workload reconstructs the single-process percentiles (exactness).
- [ ] **T3.4 (RED)** Outlier consensus (C1/Q8): `find_consensus_outliers(values)` (IQR/z/MAD/pct);
  **N≤4 → flag only (no drop); N≥5 → drop flagged**; flagged always returned. Metrics = headline
  latency percentile + RSS. Port/adapt amz-orc `find_consensus_outliers` (`../amz-orc-findings.md`).
- [ ] **T3.5 (RED)** Memory stats (§7.4): from the rss/used series over the steady window →
  `min/mean/median/p95/p99/max/stddev/samples`; **headline = median**; per-iteration medians.
- [ ] **T3.6 (RED)** Deltas-vs-baseline: `memory_saved% = (rss_base−rss_cfg)/rss_base`; per-percentile
  latency delta (µs **and** %); CPU delta. Baseline rows = 0.
- [ ] **T3.7 (GREEN)** Implement A; `build_report(run_dir) -> report.json` (schema §7.3). Unit test
  the end-to-end reduction on a **fixture run-dir** (hand-built minimal artifacts) → assert
  `report.json` numbers.

## B. Rendering (`render.py`) — report.json → self-contained Plotly HTML
- [ ] **T3.8 (RED)** `render(report.json) -> html` produces non-empty HTML containing each expected
  chart container: pareto, percentile-delta, memory-breakdown, memory-stability, command-heatmap,
  headroom, summary-table, workload-summary header. Assert Plotly traces for the canonical
  percentiles on the pareto with default-visible {p50,p99,p99.9} and the rest `visible:"legendonly"`.
- [ ] **T3.9 (RED)** Interactivity assertions (Q3): legend toggling enabled; absolute↔%-delta
  `switchMode` button present (reuse amz-orc scaffold).
- [ ] **T3.10 (RED)** Graceful degradation: a `report.json` missing an optional field → render
  available charts, mark the rest "n/a", no crash.
- [ ] **T3.11 (GREEN)** Implement B; embed Plotly (CDN); pareto X = RSS memory-saved %, Y = latency
  penalty, series per percentile (memory-saved percentile-independent ⇒ vertical stack per config).

## C. CLI + e2e (rich coverage, C2)
- [ ] **T3.12 (GREEN)** `postprocess.py <run-dir> [-o report.html] [--json report.json]` wiring.
- [ ] **T3.13 (e2e RED→GREEN)** Drive the **real** pipeline end to end: run a small orchestrator run
  (off + ≥1 compression config, ≥2 iterations) → run `postprocess.py` on it →
  - assert `report.json`: baseline identified; each config has memory (median + full stats),
    per-command + aggregate percentiles, deltas (baseline = 0), outlier section, cpu, stats;
  - **soundness:** compression config's `memory_saved_pct` > 0; per-percentile latency monotonic
    (p50 ≤ p99 ≤ p99.9); merged total_count ≈ Σ per-iteration; deltas internally consistent
    (cfg − base);
  - assert `report.html` renders, is non-empty, and contains the expected chart divs.
- [ ] **T3.14 (e2e)** Multi-iteration determinism: same run-dir → identical `report.json` (pure
  reduction). Outlier path: inject an anomalous iteration → flagged (low N) / dropped (high N) and
  surfaced in `report.json`.
- [ ] **T3.15** Full suite green; commit; push (fork); CI `orchestrator-tests` green.

## Exit
`postprocess.py <run-dir>` → correct `report.json` + interactive `report.html`; reduction exact +
unit-proven; e2e covers the real orchestrator→post-processor pipeline + soundness + outliers +
determinism + graceful degradation.
