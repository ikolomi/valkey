# Implementation plan — Compression Benchmark Orchestrator (v1)

**Status:** draft • 2026-06-25
**Design of record:** [`../design/detailed-design.md`](../design/detailed-design.md) (R1.1–R9.2); idea-honing [`../idea-honing.md`](../idea-honing.md) (Q1–Q10)
**Methodology:** Test-Driven Development (red → green → refactor), iterative, dependency-aware.

---

## 1. Goals of this plan

1. Deliver the orchestrator **iteratively** — every increment is independently mergeable, leaves the test suite **green**, and moves toward a working end-to-end run.
2. Follow **TDD**: for each increment, the tests are written **first** (they fail), then the code is written to make them pass, then refactor.
3. Sequence around the **one hard dependency** (in-tree server training, S1.x): everything that doesn't need it ships first; the training-gated compression phases come last.
4. Treat the design's **§7.4 goal-coverage matrix as the acceptance gate** — a §1.3 goal without a green test is not done.

## 2. Methodology — how we build

- **TDD per increment.** Write the test (Tier 1/2/3 per the design §7) → watch it fail → implement the smallest code to pass → refactor. No production code without a failing test that demands it.
- **Iterative & vertical where possible.** Prefer increments that end in something runnable/observable. The biggest lever: the **`off` (reference) config path skips Train + Compress-all** (design §3.4), so a full end-to-end run of *just* the `off` config exercises the entire pipeline (corpus → populate → load → windowed measure → collect → `run-status.json`) **without any training**. We reach that milestone (M2) before depending on S1.x.
- **Dependency-aware ordering.** Tier-1 (pure Python, no binaries) → benchmark C-mods + orchestration plumbing (need a plain server, not training) → off-path e2e (no training) → compression phases (need S1.x) → hardening.
- **Green-always.** Tier-1 runs on every change; Tier-2/3 run in CI when binaries are built; binary-dependent tests **skip** (not fail) when binaries are absent (design §7 harness).
- **Goal-coverage gate.** The §7.4 matrix is tracked to completion in Phase F; each goal must have a cited green test.

## 3. Code layout (proposed — confirm with maintainers)

```
utils/compression-benchmark/            # the Python orchestrator subsystem
  orchestrator.py                       # CLI entry / run driver
  lib/{config,corpus,server,benchmark,info,phases,runstatus,provenance}.py
  configs/examples/canonical.json       # the Q7 2-config example
  tests/                                # pytest: unit/ (Tier1), component/ (Tier2), e2e/ (Tier3)
  README.md
src/valkey-benchmark.c                  # the 4 benchmark modifications (Appendix B)
```
The orchestrator is Python tooling (lives with `utils/`); the benchmark modifications live in the existing `src/valkey-benchmark.c`. Location is subject to maintainer preference (the docs note the subsystem is intended for in-tree integration).

## 4. Dependency map

| Work | Needs | Gate |
|---|---|---|
| Tier-1 core (config, corpus, split math, plateau, run-status) | nothing | none — **start here** |
| Benchmark C-mods (`--value-data`, `--sequential`, `--key-distribution`, windowed recording) | a plain `valkey-server` (no compression/training) | none |
| Orchestration plumbing (server lifecycle, INFO poll, loaders, barriers, artifacts) | a server with the compression feature **compiled** (for `INFO compression` fields) — training need **not** exist | none |
| **OFF-path e2e** (M2) | the above; `off` config skips Train/Compress-all | **none — the training-free proof of the instrument** |
| Compression phases (Train, Compress-all, Profile-prep) + compression-on e2e | **in-tree server training** (`COMPRESSION TRAIN` / S1.x) | **gated on S1.x** |

**Assumption to verify in Phase 0:** stock `valkey-benchmark` supports open-loop rate limiting (`--rps`). If it does not, that becomes a 5th benchmark modification (add to Phase B). (Risk §7.)

---

## 5. Phased, iterative, test-first plan

Each iteration: **(T)** tests written first → **(I)** implement to green → **(R)** refactor. Checkboxes track state.

### Phase A — Pure-Python core (Tier-1, no binaries) — UNBLOCKED

- [ ] **A0 Scaffold + harness.** (T) a smoke test that the package imports and that `needs_server`/`needs_benchmark` markers skip cleanly when binaries are absent. (I) repo skeleton, `pytest.ini` + markers, CI job running Tier-1 on every push.
- [ ] **A1 Config (`lib/config.py`).** (T) parse/validate/render: valid→model; each missing required field→specific error; invalid values rejected (non-positive `target_tps`, ratios not normalizing, unknown command `type`, malformed `value_size_distribution`, `min>max`, bad `key_distribution`, `reference_config` ∉ `configs[]`, `max_clients_per_process<1`); sparse `compression` block → only specified `--compression-*` flags; `extra_args` merged after structured (raw overrides); per-config `server_binary` vs top-level default. (I) implement. (design R3.1–R3.5, §7.1)
- [ ] **A2 Corpus (`lib/corpus.py`).** (T) same seed → byte-identical; exactly `corpus_entries`; sizes obey distribution within `[min,max]` (per-distribution stats); each `value_shape` well-formed; cache hit/miss by `(shape,seed,size-dist,entries)`. (I) implement. (R2.1–R2.5, §7.1)
- [ ] **A3 Pure helpers.** (T) connection→process split math (R5/Q8e) incl. edges; `info.detect_plateau` (fires on stabilizing series, not on climbing, **fails on max-timeout**); `runstatus` decision (synthetic results → SUCCESS/FAILED+reason; overall = AND). (I) implement `lib/benchmark.py:split_processes`, `lib/info.py:detect_plateau`, `lib/runstatus.py`.

**Phase A exit / M0:** entire **Tier-1 suite green**, no binaries required; CI runs it on every push.

### Phase B — valkey-benchmark modifications (Tier-2, plain server) — UNBLOCKED by training

Each is a C change in `src/valkey-benchmark.c` with a Tier-2 test written first.

- [x] **B0 Audit existing valkey-benchmark capabilities.** **DONE — finding (corrected):** the repo source `src/valkey-benchmark.c` already has **`--rps`** (token-bucket open-loop rate limiting, #1761), an **RPS histogram** (#2471), and **`--warmup <seconds>` + `--duration <seconds>`** (#2581). (My earlier "no `--rps`" claim was wrong — I grepped a stale `/usr/local/bin` binary, not the source.) **No `--rps` mod needed.** Crucially, the `--warmup`→`--duration` path (`src/valkey-benchmark.c` ~L2049) already implements *"reset all stats, then measure a bounded window"* — the exact seam windowed recording reuses (see B4). `--duration` is the measurement window (no new `--measurement-duration`).
- [ ] **B1 `--value-data corpus:FILE`** (R8.1) — **confirmed new (audit).** **Architecture fork (decision needed):** valkey-benchmark pre-formats the SET command **once** (the value, via `__data__`/`-d`, is baked in at build time) and per request rewrites only the 12-digit key in place — so all requests currently write the *same* value. Variable-length corpus values per request need either **(A)** per-request value (re)formatting — changes the perf-sensitive hot send path — or **(B)** fixed-size corpus entries that fit the in-place template — loses the value-size distribution (which R2.4 says lives in the corpus). Pick before implementing. (T) sampled stored values are corpus members + compress to ratio < 1.
- [x] **B2 `--sequential` (R8.2). DONE — satisfied by the existing in-tree flag, no C change.** The in-tree `--sequential` modifies `-r` to use a shared atomic counter (% keyspacelen) instead of random; `--sequential -r N -n N` yields keys `0..N-1` each exactly once. Verified by `tests/component/test_benchmark_sequential.py` (DBSIZE==N, boundary keys present, key N absent). The orchestrator composes `-t set -r <key_count> -n <key_count> --sequential` for the Populate phase.
- [ ] **B3 `--key-distribution uniform|zipf [--zipf-theta]`** (R8.x) — **confirmed new (audit)** (keys are uniform-random or sequential; no zipf). (T) access counts skewed (rank-frequency / chi-square vs uniform). (I) add a zipf draw in `replacePlaceholder` gated by the flag.
- [ ] **B4 Windowed recording** (R8.3) — **reuses the existing `--warmup`/`--duration` reset seam**; the only NEW flag is **`--record-start-signal <SIGNUM>`**. (I) when set, the loader starts in warmup mode and stays there (time-based warmup-exit disabled); an async-signal-safe handler sets an atomic; the timer callback then runs the *existing* warmup-exit block (`hdr_reset` + reset start/counters, ~L2049) to begin the measured window; **existing `--duration <seconds>` bounds it** (no new `--measurement-duration`). Socket I/O retries on `EINTR`; `killpg(SIGNUM)` for multi-proc simultaneity. (T) known pre/post-signal profiles → emitted histogram covers **only** the post-signal window; multi-proc reset on one `killpg`.

**Phase B exit:** four mods, each green; `valkey-benchmark` builds clean.

### Phase C — Orchestration plumbing (Tier-2, compression-compiled server, no training) — UNBLOCKED by training

- [x] **C1 Server lifecycle (`lib/server.py`).** **DONE** — start in its own temp dir under `servers_directory` (binary copied in, R9.2), `wait_ready` (PING), `cli`/`config_get`/`config_set`/`dbsize`/`flushall`/`info`/`compression`, `stop` (shutdown nosave) + `teardown`, context-manager. `lib/env.py` resolves server/benchmark/cli binaries. Tier-2 `tests/component/test_server.py` (2 tests) green against the real compression server. **Provenance (`lib/provenance.py`: binary SHA-256 + machine info, R9.1) still pending.**
- [x] **C2 INFO polling (`lib/info.py`).** **DONE (logic)** — `detect_plateau` + `poll_until_plateau` (injectable clock/sleep) + `Server.info()` section parse. `used_memory` time-series capture is a thin wrapper added with the measure phase (Phase D).
- [x] **C-extra — early compression-cycle integration test (leverages the merged hot path).** `lib/dictgen.py` trains a dict from corpus samples via `gen-zstd-dict` → base64 → `COMPRESSION DICT-IMPORT`. `tests/component/test_compression_cycle.py` drives the **real** hot path: import dict → populate compressible values → `COMPRESSION SWEEP FORCE` → `poll_until_plateau` on `compression_compressed_objects` → assert `compressed_objects>0` and `compression_ratio<1`. This validates dict-import + sweep + INFO + real compression **ahead of Phase E**, using `DICT-IMPORT` in place of the unlanded S1.x `COMPRESSION TRAIN`.
- [ ] **C3 Loader orchestration + barriers + artifacts (`lib/benchmark.py`).** (T) spawn loaders per the split math; **start-load FIFO barrier** releases all simultaneously; orchestrator sends the **record-start signal** (`killpg`); raw per-process outputs (`.hist`/stdout/stderr) + `mpstat` land in the run dir. (I) implement (R5.3, R6.3–R6.4, §3.3).

**Phase C exit / M1:** benchmark mods + plumbing green.

### Phase D — End-to-end OFF path (Tier-3, NO training dependency) — DE-RISK MILESTONE

> **Re-sequencing insight (2026-06-25 audit):** **M2 (the off-path e2e) needs NO new
> benchmark C-mods.** The `off` config skips profile-prep (nothing to stabilize), so its
> measure step is just `--rps --duration` over an existing-flag populate/load
> (`--sequential` + `-d`). Compressible corpus values (B1), zipf keys (B3), and
> signal-triggered windowed recording (B4) are needed only for the **compression-ON
> realistic runs (Phase E)** — so B1's architecture fork can be decided when we reach E,
> not before M2.

- [x] **D1 Phase machine (off path) + driver (`lib/phases.py`, `orchestrator.py`).** **DONE** — `run_off_iteration` (start → populate `--sequential` → open-loop load+measure via `run_loaders` behind the FIFO barrier → collect used_memory/INFO/loader artifacts → result) + `representative_datasize`; `orchestrator.run_file`/`main` (timestamped run dir, `run-config.json` echo, `corpus.ensure`, `provenance.json`, per-config×iteration dispatch, `runstatus.decide` → `run-status.json`, `--dry-run`); `lib/server.free_port`. `tests/e2e/test_off_path.py` (SUCCESS + artifact contract + reproducibility) green against real binaries.
- [x] **D2 Failure paths reachable without training.** **DONE** — e2e `target_tps_not_achieved` (target 50M) and `server_error` (bogus startup arg) via the full orchestrator; loader-failure detection (dead port → rc!=0 / no rps) in `tests/component/test_loaders.py`. (`benchmark_error` decision mapping is unit-tested in `test_helpers.py`.)

**Phase D exit / M2:** the **whole instrument is proven end-to-end on the `off` config** — corpus, populate, load, windowed measurement, artifact collection, validity verdict, reproducibility — *without S1.x*. Only the compression-specific phases remain.

### Phase E — Compression phases (Tier-3) — GATED on in-tree S1.x training

- [ ] **E1 Train phase.** (T) `COMPRESSION TRAIN` on written data → `compression_active_dict_id != 0`, then `FLUSHALL` → `DBSIZE == 0`. (I) implement (R4.1).
- [ ] **E2 Compress-all.** (T) `min-idle-seconds=0` + `COMPRESSION SWEEP FORCE` (cranked) → `compression_compressed_objects == size-eligible count`. (I) implement (R4.3).
- [ ] **E3 Profile-prep + plateau.** (T) switch to real `min-idle-seconds` + load → plateau detected; hot keys observably uncompressed / cold compressed; **no-plateau within `max_timeout_seconds` → FAILED `profile_not_stabilized`**. (I) implement (R4.4, R4.6).
- [ ] **E4 Canonical 2-config run.** (T, e2e) the Q7 example (`off` + `compression-on` @ 256/16K/idle-3/sweeper-enabled) → SUCCESS run-status; both configs' raw artifacts present for the post-processor. (I) wire the compression-on path end-to-end.

**Phase E exit / M3:** the **canonical example runs end-to-end**, producing a run directory the (future) post-processor can consume.

### Phase F — Hardening + goal-coverage closure

- [ ] **F1 Close the §7.4 goal-coverage matrix** — every §1.3 goal cites a green test (review gate).
- [ ] **F2** reproducibility soak; CI Tier-2/3 with binaries built; optional NUMA pinning; `--dry-run`.
- [ ] **F3** docs: `README.md`, run-JSON schema reference, how-to-run, the S1.x dependency note.

**Phase F exit / M4:** instrument ready; hand-off point for the **post-processor** (separate idea-honing).

---

## 6. Milestones

| Milestone | Gate | Notes |
|---|---|---|
| **M0** | Phase A | Tier-1 green, no binaries |
| **M1** | Phases B+C | benchmark mods + plumbing green |
| **M2** | Phase D | **OFF-path e2e — instrument proven minus compression** (training-free) |
| **M3** | Phase E | canonical 2-config run (needs S1.x) |
| **M4** | Phase F | goal matrix green; ready for the post-processor |

## 7. Risks & mitigations

| Risk | Mitigation |
|---|---|
| **S1.x training not landed** blocks Phase E | Phases A–D + B + C don't need it; deliver the instrument to **M2** (off-path proven). *Optional* unblock: use the already-merged `COMPRESSION DICT-IMPORT` as a **test-only fixture** to exercise the E phase machinery while S1.x lands (import stays out of the product path per Q8). |
| ~~Stock `valkey-benchmark` lacks `--rps`~~ — **resolved (B0)** | `--rps`, `--warmup`, `--duration`, RPS histogram already in source (#1761/#2471/#2581). Windowed recording reuses the `--warmup`/`--duration` reset seam — only `--record-start-signal` is new. |
| Signal/EINTR fragility in windowed recording | B4 tests the EINTR-retry + multi-proc `killpg` reset explicitly. |
| Plateau detection flaky (slow read-heavy convergence) | tunable `tolerance/window` + **fail-on-timeout** (never measure half-converged); A3 tests with synthetic series; E3 tests on a real workload. |
| Measurement precision regressions | the §7.4 matrix is a standing review gate; Tier-2 coverage/skew/windowing tests guard the instrument's core claims. |

## 8. Out of scope (v1)

Post-processor (histogram merge, percentiles, MAX, delta-vs-reference, charts, outliers — **separate idea-honing**); multi-key `MGET`/`MSET`; dict **import** as a product path; RSS memory metric; matrix runner (workload×TPS sweeps); read-only (0% write) workloads. (Design Appendix C/D.)

## 9. Cross-references

- Design: [`../design/detailed-design.md`](../design/detailed-design.md)
- Idea-honing: [`../idea-honing.md`](../idea-honing.md) (Q1–Q10)
- amz-orc reuse map: [`../amz-orc-findings.md`](../amz-orc-findings.md)
- Rendering (post-processor input): [`../research-rendering-proposal.md`](../research-rendering-proposal.md)

---

_This plan is a living document. Amend via the same review flow; keep the §7.4 goal-coverage matrix as the acceptance gate._
