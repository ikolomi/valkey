# Plan 2 — Orchestrator capture + stable contract + e2e remediation (Piece 2)

## Status: DONE + GREEN (uncommitted)
All tasks complete. Full suite **142 passed** (~105s; +23 vs Plan-1 baseline). `lib/latency.py`
(parse/sum/capture + frozen-sample), `--latency-dump` wiring (keeps `-q`), RSS/frag series + INFO
`stats` + server-process CPU% captured into the stable `latency`/`memory`/`stats`/`server_cpu`
blocks of `info-measurement.json`; contract-driven e2e (`tests/e2e/test_contract.py`) assert every
required field with sound values. No C changes (all Python).


_Implements design §3, §5, P2.1–P2.4. Depends on Plan 1 (`--latency-dump` available). Extends the
existing orchestrator (PR #44 territory). Commit prefix `feat(compression-benchmark): …` + DCO `-s`._
_TDD red→green; targeted `git add`; push to fork only; review→commit→proceed._

## >>> This plan OWNS the orchestrator-e2e gap remediation <<<
The original e2e validated only that *present* fields were self-consistent — it never validated the
**valkey-benchmark latency output** (the cornerstone), because the orchestrator used `-q` and stored
none. Two tasks below close it permanently:
- **T2.6 — frozen-sample parser test:** a checked-in real `--latency-dump` sample → assert exact
  parsed buckets/total. Fails loudly if valkey-benchmark's format ever drifts (contains the blast
  radius to one localized parser).
- **T2.8 — contract-driven e2e rewrite:** assert produced artifacts contain **every contract field**
  (latency histogram, RSS series, stats, CPU) with **sound values**, not just internal consistency.

## A. Stable latency schema + parser (`lib/latency.py`)
- [ ] **T2.1 (RED)** Unit: `parse_dump(text)` on a small synthetic dump → `{hdr, total_count,
  buckets:{value:count}}`; malformed/empty → explicit error.
- [ ] **T2.2 (RED)** Unit: `sum_histograms([h1,h2])` sums counts by value; **mismatched `hdr`
  params → hard error** (merge-compat guard).
- [ ] **T2.3 (GREEN)** Implement `parse_dump` + `sum_histograms`.

## B. Loader wiring + per-iteration capture (`lib/benchmark.py`, `lib/phases.py`)
- [ ] **T2.4 (RED)** Unit: `loader_argv(...)` for a measured loader includes `--latency-dump
  <path>`; populate argv unchanged; `-q` retained.
- [ ] **T2.5 (GREEN)** Add `--latency-dump` path per measured loader process; after collect, group
  process dumps by command, `sum_histograms` per command → write the `latency.per_command` schema
  block (with `hdr`) into `info-measurement.json`.
- [ ] **T2.6 (RED→GREEN) FROZEN-SAMPLE TEST** Check in a real dump captured from the Plan-1 binary;
  unit test asserts `parse_dump` yields the exact expected buckets + total. *(Gap remediation #1.)*

## C. Memory / stats / CPU capture (`lib/phases.py`, `lib/server.py`)
- [ ] **T2.7a (RED)** Unit (`server.py`): INFO `memory` exposes `used_memory_rss` +
  `mem_fragmentation_ratio`; INFO `stats` exposes `evicted_keys`/`rejected_connections`/
  `expired_keys`/`keyspace_misses`; a process-CPU sampler returns user/system/total% for a PID.
- [ ] **T2.7b (GREEN)** Periodic sampler also records `used_memory_rss` + `mem_fragmentation_ratio`
  series (same cadence, **no purge**); record steady-state window indices; capture `stats` at window
  end; capture server-process CPU% (sample `valkey-server` PID `/proc/<pid>/stat` deltas over the
  window). Write `memory`/`stats`/`server_cpu` schema blocks. No precomputed stats (PP derives).

## D. Contract-driven e2e rewrite (Gap remediation #2)
- [ ] **T2.8 (RED→GREEN)** Rewrite `tests/e2e` artifact assertions to a **contract checklist**: for a
  real small run, for each SUCCESS config/iteration assert `info-measurement.json` has —
  `latency.per_command[GET]` and `[SET]` with non-empty `buckets` and `total_count>0`;
  `memory.used_memory_rss_series` non-empty and same length as `used_memory_series`;
  `mem_fragmentation_ratio_series`; `stats` keys present; `server_cpu.pct_total ≥ 0`. **Soundness
  checks:** reconstructed p50 from the histogram is within a sane localhost band; rss ≥ used_memory;
  Σ(per-command total_count) ≈ expected request volume. Keep the existing failure-path e2e.
- [ ] **T2.9** Run full suite (unit + e2e) green; `cr`/push updates PR #44; confirm CI
  (`orchestrator-tests`) green.

## Exit
`info-measurement.json` carries the full stable contract (§3); the latency cornerstone is captured
+ validated; e2e are contract-driven (completeness, not just consistency); format-drift is guarded.
