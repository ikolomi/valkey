# Implementation plan — Compression Benchmark Orchestrator (v1)

**Status:** DONE (Phases A–F / M4) + extended • 2026-06-25
**Design of record:** [`../design/detailed-design.md`](../design/detailed-design.md) (R1.1–R9.2); idea-honing [`../idea-honing.md`](../idea-honing.md) (Q1–Q10)
**Methodology:** Test-Driven Development (red → green → refactor), iterative, dependency-aware.

> **STATUS BANNER.** Phases A–F are complete (M4). Work has continued **beyond this plan**:
> the separate **post-processor** (reduction + `report.json` + interactive `report.html`) and a
> round of **empirical hardening** (latency capture via `--latency-dump` + stable schema, RSS as
> the headline memory metric, server-process CPU, true compress-all completion,
> `setup_timeout_seconds`, realistic compressible corpus). Those are planned/tracked in
> [`../postprocessor/`](../postprocessor/) — `idea-honing.md`, `design/detailed-design.md`
> (§2 supersedes table, §11 empirical findings), `implementation/plan-{1,2,3}-*.md`. See the
> design-doc banner for the superseded v1 requirements (R6.1/R6.3/R6.4/§5.5).

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
- [x] **B1 `--value-data corpus:FILE`** (R8.1) — **DONE (2026-06-25).** confirmed new (audit). **DECISION (user): separate mutually-exclusive code path.** Corpus mode is incompatible with the existing in-place fixed-stride data mechanism (value baked once via `genBenchmarkRandomData`; per-request only the 12-digit `__rand_int__` key is poked). So when `--value-data corpus:FILE` is given, take a **completely separate command-generation path** (no placeholder/in-place reuse). Mechanics:
  - **Load once**: read the corpus file (orchestrator format `[4-byte BE len][bytes]*`, see `lib/corpus.write_corpus`) into one buffer; build an index array of `{const char *ptr; uint32_t len;}` pointing into it. Read-only + **static → shared lock-free** across client threads (no mutation issues).
  - **Per-request value provider** `corpusNext() -> (ptr, len)`: O(1), no copy, no alloc, no parse — atomic round-robin index `% corpus_entries` (matches Q3a round-robin key→corpus mapping). **Latency-critical**: the command-gen path must stay cheap (point at the entry; assemble RESP; at most one memcpy of the value bytes into the send buffer, or `writev` scatter-gather to avoid even that — TBD during impl).
  - **Multi-key commands** (e.g. MSET): fetch **N** entries per command (N successive `corpusNext`). B3/multi-key fan-out builds on this.
  - **Pipelining omitted in corpus mode for now** (pipeline=1); pipeline>1 corpus support deferred.
  - (T) Tier-2: stored values are corpus members + compress to ratio < 1.
  - **DONE outcome:** implemented in `src/valkey-benchmark.c` as the **minimum change** — corpus-only `--value-data corpus:FILE` (parseOptions; a single `config.value_corpus_path`, NULL = today's default random data — no `random`/`zero` modes added); `loadCorpus` (parses `[4B BE len][bytes]*` into a static `{ptr,len}` index over one buffer); `prepareCorpusSetHead` (precomputes `*3\r\n$3\r\nSET\r\n$<klen>\r\nkey<tag>:`); `buildCorpusSetObuf` (per-request rebuild in `writeHandler` when `g_corpus_set_active`: atomic round-robin entry + key digits + one small `snprintf` value-bulk header + one `memcpy` of value bytes, reusing obuf capacity); SET test routes through it; corpus forces `pipeline=1`. GET path + the existing in-place placeholder path untouched. Tier-2 `tests/component/test_value_data_corpus.py` green (members + variety + compressible). Help text omitted (the usage string-literal hit the C99 4095-char limit) — minor follow-up. **Orchestrator wiring** of `--value-data corpus:FILE` into `lib/benchmark.py` populate/loader argv is Phase E (consumed by `run_compression_iteration`). **Multi-key MSET (N values/command) deferred** (the provider supports N successive fetches; only the SET builder exists).
- [x] **B2 `--sequential` (R8.2). DONE — satisfied by the existing in-tree flag, no C change.** The in-tree `--sequential` modifies `-r` to use a shared atomic counter (% keyspacelen) instead of random; `--sequential -r N -n N` yields keys `0..N-1` each exactly once. Verified by `tests/component/test_benchmark_sequential.py` (DBSIZE==N, boundary keys present, key N absent). The orchestrator composes `-t set -r <key_count> -n <key_count> --sequential` for the Populate phase.
- [x] **B3 `--key-distribution uniform|zipf [--zipf-theta]`** (R8.x) — **DONE (2026-06-25).** confirmed new (audit). Added a Gray et al. / YCSB Zipfian generator (`zipfZeta`/`zipfInit`/`zipfNext`; `zetan`/`eta`/`alpha` precomputed once over `[0,keyspacelen)`, item 0 hottest) gated by `--key-distribution zipf` + `--zipf-theta` (default 0.99; validated `>0` and `!=1.0`, `-r>=2`). Wired into BOTH key-draw sites: `replacePlaceholder` (GET/INCR/placeholder path) and `buildCorpusSetObuf` (corpus SET path); sequential/uniform paths unchanged; `zipfInit` runs right after `parseOptions` so it covers both command paths. Tier-2 `tests/component/test_key_distribution.py` green (zipf hottest key ≫ uniform expectation and ≫ uniform run's hottest; head≫median; explicit/default uniform stays flat — via INCR counters as access counts).
- [x] **B4 Windowed recording** (R8.3) — **DONE (2026-06-25).** reuses the existing `--warmup`/`--duration` reset seam; the only NEW flag is **`--record-start-signal <SIGNUM>`**. When set: `config.warmup_duration` is forced > 0 so the loader enters warmup and `isBenchmarkFinished` waits there indefinitely; a `SA_RESTART` `sigaction` handler (`recordStartSignalHandler`) sets an async-signal-safe `g_record_start` flag; `showThroughput`'s warmup-exit is gated on that flag (instead of the timer) → runs the existing reset (`hdr_reset` + start/counters), then the existing `--duration` bounds the measured window. `SA_RESTART` keeps EINTR off the client I/O paths; the 250 ms throughput tick observes the flag promptly. Tier-2 `tests/component/test_record_start_signal.py` green (process stays in warmup past `--duration` until signaled, then finishes ~`--duration` after the signal). **`--warmup` and `--record-start-signal` are mutually exclusive** (rejected at parse time in both orders, matching the `-n`/`--duration` precedent — they are two ways to define the same warmup→measure boundary; combining them previously let the signal silently override the timed warmup). Multi-proc `killpg` reset is exercised at the orchestrator level (Phase E / `lib/benchmark.py`). Help text omitted (C99 4095-char usage-literal limit) — minor follow-up.

**Phase B exit:** ✅ **DONE (2026-06-25)** — B0/B2 satisfied by existing in-tree flags; B1 (`--value-data corpus:FILE`), B3 (`--key-distribution zipf [--zipf-theta]`), B4 (`--record-start-signal`) implemented + Tier-2-tested; `valkey-benchmark` builds clean with `-Werror`. (Deferred: multi-key MSET N-value corpus builder; `--value-data`/`--key-distribution`/`--record-start-signal` help-text lines — the usage string-literal is at the C99 4095-char limit.)

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

### Phase E — Compression phases (Tier-3)

> **DONE (2026-06-25).** Implemented `lib/phases.run_compression_iteration` end-to-end. **E1 uses server-side automatic first-training (S1.2, validated present in this checkout: `compression_train.c` + `BIO_COMPRESSION_TRAIN`, commit `f37298e79` by GilboaAWS, wired into `compressionCron`).** The orchestrator populates the keyspace with compression enabled and polls `compression_active_dict_id` until the server auto-trains+promotes a dict (the realistic "train on your data" path) — no `gen-zstd-dict`/`DICT-IMPORT`. NOTE: there is **no manual `COMPRESSION TRAIN` command** in this checkout (dispatch wires only STATUS/HELP/DICT-IMPORT/SWEEP; `compression.c` says "TRAIN … land in subsequent S2 PRs"), and only the first-training trigger is live (drift/refresh are stubbed `TODO`); auto first-training is the available + realistic path. The benchmark building blocks (corpus/zipf/record-start argv knobs + `spawn_loaders`/`record_start_loaders`/`collect_loaders`) landed first (increment 1). e2e `tests/e2e/test_compression_path.py` runs the tiny canonical off + compression-on run → SUCCESS + asserts real compression (`compressed_objects>0`, `ratio<1`, `plateaued`) via auto-training. **99 tests green.**

- [x] **E1 Train phase.** **DONE (auto-train).** Populate first (corpus values, compression enabled), then `_wait_active_dict` polls `compression_active_dict_id != 0` — the server's S1.2 first-training fires once DBSIZE ≥ `compression-dict-min-training-keys` (default 1000) and promotes a dict. No flush, no import. (R4.1; manual `COMPRESSION TRAIN` is a later in-tree PR.)
- [x] **E2 Compress-all.** **DONE** — save real `min-idle` via `config_get`, set `compression-min-idle-seconds 0`, `COMPRESSION SWEEP FORCE`, poll `compression_compressed_objects` to plateau, restore the real `min-idle`. (R4.3)
- [x] **E3 Profile-prep + plateau.** **DONE** — one continuous load (`spawn_loaders` with corpus/zipf/`--record-start-signal`) under the real `min-idle`; `poll_until_plateau` on `compression_compressed_objects` → `plateaued`; on timeout `plateaued=False` → runstatus maps to FAILED `profile_not_stabilized`. (R4.4, R4.6)
- [x] **E4 Canonical 2-config run.** **DONE** — `tests/e2e/test_compression_path.py` (tiny off + compression-on) → SUCCESS; per-config/iteration raw artifacts present; the measured window is started via `record_start_loaders(SIGUSR1)` after plateau, with `used_memory` sampled (MAX) during the window. (I) `orchestrator.run()` already dispatched compression configs to this path.

**Phase E exit / M3:** the **canonical example runs end-to-end**, producing a run directory the (future) post-processor can consume.

### Phase F — Hardening + goal-coverage closure

- [x] **F1 Close the §7.4 goal-coverage matrix** — **DONE (2026-06-25).** Every design §1.3 goal cites a green test (100 passed). As part of closing it, decoupled the setup-phase timeouts (auto-train + compress-all use a fixed `_SETUP_TIMEOUT_S`) from `profile_prep.max_timeout_seconds`, and added the missing induced-failure e2e (`profile_not_stabilized`). Matrix:

  | §1.3 goal | covering green test(s) |
  |---|---|
  | **Precision instrument** | exact coverage — `tests/component/test_benchmark_sequential.py` (`DBSIZE==N` + boundary keys); windowed recording reflects only the post-signal window — `tests/component/test_record_start_signal.py`; worst-case memory (MAX `used_memory` sampled across the window) — `tests/e2e/test_compression_path.py`; clean window (plateau before measure) — `test_compression_path.py` (`plateaued`); corpus compressibility / key skew — `test_value_data_corpus.py` / `test_key_distribution.py`. (True tail-percentile *merge* is post-processor scope, Appendix C.) |
  | **Reproducible** | byte-identical corpus per seed — `tests/unit/test_corpus.py`; e2e identical corpus hash — `tests/e2e/test_off_path.py`; provenance (sha256/machine/seed/corpus-hash) — `tests/unit/test_provenance.py` + `test_off_path.py` artifact contract. |
  | **Realistic** | start-compressed → equilibrium plateau under real load — `test_compression_path.py` (`compressed_objects>0` + `plateaued`); compress/ratio behavior — `tests/component/test_compression_cycle.py` (ratio<1). |
  | **Delta-from-baseline** | both `reference` (`off`) + `compression-on` configs run and retain raw artifacts — `test_compression_path.py`. (Delta *computation* is post-processor scope, Appendix C.) |
  | **Honest about validity** | induced FAILED e2e — `target_tps_not_achieved` + `server_error` (`test_off_path.py`), `profile_not_stabilized` (`test_compression_path.py`), `benchmark_error` (`tests/component/test_loaders.py` dead-port); decision logic + precedence — `tests/unit/test_helpers.py` (runstatus). |
  | **Normal-machine runnable** | tiny e2e runs within CI budget (`key_count≈2k`, short duration) — `test_off_path.py` + `test_compression_path.py`. (No hard dataset-size cap enforced — operators size their own runs; the tiny e2e is the budget guard.) |
- [~] **F2** hardening. **DONE:** (a) **`--dry-run`** — binary-independent; emits the full load
  plan (per-command process split via the R5 split math: procs/connections/rps + rendered
  `--compression-*` server args per config) so a run can be sanity-checked before launch
  (`orchestrator.build_plan` + `_format_plan`; Tier-1 `tests/unit/test_dry_run.py`). (b)
  **reproducibility** — already covered: byte-identical corpus per seed (`tests/unit/test_corpus.py`)
  + e2e identical corpus hash (`tests/e2e/test_off_path.py`); a long repeated-run "soak" is
  informational/slow and left as a manual step, not a CI gate (per §7.3 policy). (c) **CI** —
  `.github/workflows/compression-benchmark.yml`: path-filtered (runs only on
  `utils/compression-benchmark/**`, `src/compression*`, `src/valkey-benchmark.c`, the workflow
  file), builds the server+benchmark (`make BUILD_ZSTD=yes`, which also builds the
  `gen-zstd-dict` helper via `ALL_BUILD_PREREQUISITES`) and runs the full Tier-1/2/3 pytest
  with `VALKEY_SERVER`/`VALKEY_BENCHMARK`/`VALKEY_CLI` pointed at `src/`. Optional NUMA pinning
  not added (single-runner CI). **Phase F complete.**
- [x] **F3** docs — **DONE.** `README.md` refreshed: status table (Phases A–F), binary-independent
  `--dry-run` load-plan example, compression-ON-via-auto-training how-to, the **S1.x server-side-training
  dependency note** (no manual `COMPRESSION TRAIN`; first-training-only), and a **run-JSON schema-reference**
  pointer to design §5.1.

**Phase F exit / M4:** instrument ready; hand-off point for the **post-processor** (separate idea-honing).

- [x] **F4 (review hardening) — e2e coverage expansion.** **DONE.** On review the e2e suite was
  judged too thin (e2e is the most reliable signal). Expanded **6 → 22 e2e tests** covering both
  axes the review asked for: (a) **each config setting's effect** — `iterations`→N iteration
  dirs/verdicts, `reference_config` recorded, command-ratio/`connections_total`/`max_clients_per_process`
  → loader-process split (file counts), `target_tps` open-loop rate-limited (+ unmet-fails),
  `seed`→reproducible corpus, `key_count`→`dbsize`, `master_switch` off/compression→compressed
  objects 0/>0, `automatic_sweeper`/`min_value_size`/`max_value_size`/`min_idle_seconds`/`threads`
  applied (verified via captured `CONFIG GET`), `threads=0`→trains-but-doesn't-compress; (b)
  **output-file/data soundness** — `run-config.json` verbatim echo, full `provenance.json`,
  `run-status.json` structure, `orchestrator.log` markers, per-iteration `info-measurement.json`
  field consistency (memory MAX, byte totals vs ratio, net-saved>0, active dict, plateau, series),
  loader artifacts present + recorded rps re-parses from stdout. Two data-soundness additions to
  the artifact: `dbsize` and `compression_config` (CONFIG GET of 8 compression knobs — the
  size/threads knobs aren't in `INFO compression`). Session-scoped fixtures (`off_run`, `comp_run`)
  amortize the expensive runs. Full suite **119 passed**. Conscious gaps (covered elsewhere or not
  e2e-observable): `pipeline` (not in artifacts), `key_distribution zipf` (Tier-2
  `test_key_distribution`), deep value-size-distribution (Tier-1 `test_corpus`),
  `min/max_value_size` *behavioral exclusion* (starves auto-training → no clean e2e; covered by
  echo + the in-tree feature's own tests), standalone `master_switch=decompression` (a drain mode,
  not a coherent benchmark run).

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
