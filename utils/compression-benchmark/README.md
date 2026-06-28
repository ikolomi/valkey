# Compression Benchmark

Measures the **memory↔latency tradeoff** of Valkey's in-tree real-time value-compression
feature. Two components:

1. **Orchestrator** (`orchestrator.py`) — drives `valkey-benchmark` against `valkey-server`
   under a set of compression **configurations** at a fixed offered TPS (open-loop),
   collects precise raw artifacts into a **stable, versioned contract**, and decides each
   run's validity (SUCCESS / FAILED). It does **no** statistical reduction.
2. **Post-processor** (`postprocessor/`) — consumes a run directory and produces
   **`report.json`** (reduced numbers) + a self-contained interactive **`report.html`**
   (the headline **Pareto** memory-saved-% vs latency-penalty chart, per-percentile deltas,
   memory breakdown/stability, per-command heatmap, headroom, summary + measurement-coverage
   tables). Reduction is pure stdlib and split from rendering.

It is modeled on [`amz_redis-benchmark-orc`](https://github.com/ikolomi/amz_redis-benchmark-orc)
(process orchestration, FIFO-barrier launch, reproducibility rigor, CPU capture) but
**diverges** on three axes:

- compares **compression configs at the same offered TPS** (open-loop `--rps`), not a client sweep;
- computes **true tail percentiles by merging per-bucket histograms** (not averaging per-interval percentiles);
- adds the entire **memory + compressed-steady-state** dimension amz-orc lacks.

**The headline memory metric is `used_memory_rss`** (physical RAM the user actually pays),
with `used_memory` + fragmentation shown alongside — `used_memory` alone hides fragmentation
and the read-path's transient decompression views.

---

## Status — complete

| Area | What | State |
|---|---|---|
| Core | config / corpus / split-math / plateau / run-status | ✅ |
| benchmark flags | `--value-data corpus:FILE`, `--key-distribution zipf`, `--record-start-signal`, **`--latency-dump`** | ✅ |
| Orchestrator | server lifecycle, INFO polling, FIFO-barrier loaders, windowed measure, provenance | ✅ |
| OFF + Compression-ON | auto-train → compress-all (**true completion**) → profile-prep → windowed measure | ✅ |
| Stable contract | per-command **latency histogram**, **RSS/used/frag series**, INFO `stats`, server-process CPU% | ✅ |
| Post-processor | merge → true percentiles, consensus outliers, deltas, `report.json` + interactive `report.html` | ✅ |

> The compression-ON path acquires its dictionary via the server's **automatic
> first-training** (S1.2) — see the [dependency note](#dependency--server-side-training-s12).

---

## Prerequisites

- **Python 3.10+** and `pytest`. The orchestrator + post-processor have **no third-party
  Python dependencies** (stdlib only); `report.html` loads Plotly from a CDN.
- **Built Valkey binaries** (from the repo root, with the compression feature):

  ```sh
  make BUILD_ZSTD=yes        # builds src/valkey-server, src/valkey-cli, src/valkey-benchmark
                             # (and tests/helpers/gen-zstd-dict, a DICT-IMPORT test helper)
  ```

### Dependency — server-side training (S1.2)

The compression-ON path relies on the server's **automatic first-training**. With
`compression-master-switch compression`, once the keyspace reaches
`compression-dict-min-training-keys` (default `1000`) the server trains a ZSTD dictionary on
a `bio` thread and promotes it; the orchestrator polls `compression_active_dict_id` until it
is non-zero. The current in-tree feature has **no manual `COMPRESSION TRAIN`** and only the
**first-training** trigger is live (drift/refresh retraining is stubbed). A run therefore
needs `key_count ≥ compression-dict-min-training-keys`.

---

## Quick start

```sh
cd utils/compression-benchmark
export SRC="$(cd ../../src && pwd)"          # built binaries live in src/

# 1) Validate the config + print the load plan — no server started (works without binaries).
python3 orchestrator.py configs/examples/local-canonical-shaped.json --dry-run

# 2) Run it (off + compression-on, per the config).
python3 orchestrator.py configs/examples/local-canonical-shaped.json \
    --server-binary    "$SRC/valkey-server" \
    --benchmark-binary "$SRC/valkey-benchmark" \
    --out-root /tmp/cbench-results
# exit code 0 = overall SUCCESS, 1 = FAILED.

# 3) Produce the charts from the run directory.
python3 -m postprocessor.postprocess /tmp/cbench-results/<timestamp>
# → writes <run-dir>/report.json and <run-dir>/report.html  (open report.html in a browser)
```

Example configs in `configs/examples/`: `canonical.json` (perf-host target: 250k TPS / 2M keys),
`local-canonical-shaped.json` and `local-1m-defrag.json` (dev-box-tractable; the latter enables
`active-defrag` on both configs and uses a 60 s window).

For each config × iteration the orchestrator: starts a `valkey-server` **in its own temp home
dir** → **populates** exactly `key_count` keys (`--sequential -r N -n N`) → (compression-ON:
auto-trains a dict, then **compress-all** to true completion) → drives an **open-loop** load
(per-command FIFO-barrier-synchronized processes, `--rps`/`--duration`) → samples
`used_memory`/`used_memory_rss`/fragmentation + `INFO compression`/`stats` + server-process CPU
and each loader's **`--latency-dump`** histogram → writes a timestamped run directory + a
`run-status.json` verdict.

---

## The run-JSON config

One run-JSON = one `(workload, target TPS) × list of configs` → one run directory.

```jsonc
{
  "description": "off vs compression-on",
  "output_directory": "results/",                 // run dirs created here (or use --out-root)
  "servers_directory": "/tmp/cbench-servers",      // base for isolated per-server temp dirs
  "benchmark_binary": "valkey-benchmark",          // overridable with --benchmark-binary
  "server_binary":    "valkey-server",             // top-level default; per-config override allowed
  "iterations": 3,                                 // repeats per config (statistics + outlier detection)
  "reference_config": "off",                        // baseline config name; deltas are vs this
  "setup_timeout_seconds": 180,                     // OPTIONAL (default 180): max wall-time for a
                                                    //   compression config's setup (auto-train +
                                                    //   compress-all) before the iteration FAILS.
                                                    //   Large datasets need more (e.g. 900).

  "data_model": {                                  // dataset shape (corpus generated from this)
    "value_shape": "json",                         //   json = realistic, compressible records;
                                                    //   also kv | log | coordinates
    "value_size_distribution": "lognormal:512:0.8", //   constant:N | uniform:MIN:MAX | lognormal:MU:SIGMA
    "value_size_min": 256, "value_size_max": 8192, //   clamps (esp. for lognormal)
    "seed": 1234,                                   //   REQUIRED — reproducible corpus
    "corpus_entries": 50000,                        //   number of representative blobs
    "key_count": 1000000,                           //   dataset size (keys)
    "key_distribution": "zipf:0.99"                 //   uniform | zipf:THETA
  },

  "workload": {
    "target_tps": 10000,                            // offered TPS (open-loop, split by ratios)
    "commands": [ {"type":"get","ratio":0.8},
                  {"type":"set","ratio":0.2} ],     // ratios must sum to 1.0
    "connections_total": 16,
    "max_clients_per_process": 8,                   // single-threaded benchmark per process
    "pipeline": 1,
    "measurement_duration_seconds": 60              // longer windows → more tail samples (see coverage)
  },

  "profile_prep": {                                 // compression-ON plateau detection under load
    "plateau_metric": "compression_compressed_objects",
    "plateau_tolerance_pct": 5, "plateau_window_polls": 3,
    "poll_interval_seconds": 2, "max_timeout_seconds": 300
  },

  "configs": [
    { "name": "off", "compression": { "master_switch": "off" } },
    { "name": "compression-on",
      "compression": { "master_switch": "compression", "automatic_sweeper": "enabled",
                       "min_value_size": 256, "max_value_size": 8192, "min_idle_seconds": 2,
                       "threads": 4 },
      "extra_args": ["--activedefrag", "yes"] }     // raw server flags, appended after the structured ones
  ]
}
```

The per-config `compression` block is *sparse* — only knobs you set are rendered to
`--compression-*` flags. A raw `extra_args` list (e.g. to enable `active-defrag`) and a
per-config `server_binary` override are also supported. The **authoritative field-by-field
schema** is §5.1 of the [detailed design](../../.agents/planning/realtime-data-compression/benchmark/design/detailed-design.md);
invalid configs are rejected up front with a specific `ConfigError` (use `--dry-run`).

> **Corpus realism matters.** The `json` shape models realistic customer/order records
> (repeated keys + a bounded human-readable vocabulary) so it compresses like real data.
> A corpus filled with random bytes sits at the entropy floor and makes compression look
> useless — always sanity-check compressibility (e.g. `zstd -19` on a generated corpus).

---

## What it produces

```
<out-root>/<timestamp>/
  run-config.json          # verbatim echo of your input
  provenance.json          # binary SHA-256s, machine info, seed, corpus hash
  orchestrator.log
  run-status.json          # the verdict
  report.json              # (post-processor) reduced numbers: merged percentiles, memory
  report.html              #   stats, deltas-vs-baseline, outliers, measurement coverage + charts
  <config-name>/<iteration-N>/
    server.log
    mpstat.log                       # best-effort host CPU
    info-measurement.json            # the STABLE CONTRACT (below)
    load/loader-<cmd>-<i>.hist       # raw valkey-benchmark --latency-dump (recorded hdr buckets)
    load/loader-<cmd>-<i>.stdout/.stderr
```

`info-measurement.json` (the orchestrator→post-processor contract, design §3) carries, per
iteration: a per-command **`latency`** histogram (summed across the iteration's loader
processes, exact-merge-ready), a **`memory`** block (`used_memory` / `used_memory_rss` /
`mem_fragmentation_ratio` series + steady-state window — RSS is the headline), `stats`
(eviction/OOM, reported not gated), `server_cpu` (server-process %), plus `compression` INFO,
`compression_config`, `dbsize`, and per-loader rps. The format is valkey-benchmark-independent
(the orchestrator parses the raw dumps), guarded by a frozen-sample parser test.

A config iteration is **FAILED** (with a `reason`) when: achieved TPS < `target_tps`
(`target_tps_not_achieved`), the compression profile doesn't stabilize
(`profile_not_stabilized`), **compress-all doesn't finish within `setup_timeout_seconds`**, the
server crashes (`server_error`), or a loader errors (`benchmark_error`).

---

## The report (charts)
`report.html` is self-contained (Plotly via CDN) and interactive (legend toggle/isolate, hover).
A single **mode toggle** flips **all** comparison charts between **absolute** and **% vs baseline**
and relabels their axes accordingly. It includes:

- **Pareto** — memory saved (X) vs latency penalty (Y), one series per canonical percentile
  ({p50, p99, p99.9} visible by default; all 7 legend-toggleable). Toggle: X = saved bytes ↔ %,
  Y = penalty µs ↔ %.
- **Latency delta by percentile** (µs ↔ %).
- **Memory saved vs baseline** — one chart, X = `min…max` percentiles of the RSS/used sample
  series, **RSS + used_memory** series (toggle: bytes saved ↔ % saved).
- **Per-command latency heatmap** and **operational headroom** (server-PROCESS CPU%).
- **Summary** + **Measurement coverage & reliability** tables — the latter shows per-config request
  counts, tail-sample counts for **every** canonical percentile, and the per-iteration p99 spread,
  with **⚠ flags** for thin tails (< 100 samples) or high spread (> 30%, host-noise-confounded).

`report.json` is the same reduced data in machine-readable form (feed it to other tools / an LLM).

> **Tail-latency reliability.** p99.9+ is determined by very few samples and is highly sensitive to
> host contention. Because configs run **interleaved** but on a shared host the tail can still be
> noise-dominated — trust p50/headline-memory there, and treat ⚠-flagged percentiles cautiously.
> For trustworthy tails, run on a **quiet/dedicated host** with more iterations.

---

## Running the tests

Tier-1 (pure Python) runs everywhere; Tier-2/3 are tagged `needs_server` / `needs_benchmark`
and **skip** (not fail) when binaries are absent:

```sh
cd utils/compression-benchmark
python3 -m pytest -q                       # Tier-1 only (binary tests skip)

SRC="$(cd ../../src && pwd)"
VALKEY_SERVER="$SRC/valkey-server" VALKEY_BENCHMARK="$SRC/valkey-benchmark" \
    python3 -m pytest -q                   # full suite (Tier-1/2/3)
```

The benchmark's `--latency-dump` flag has its own integration tests in
`tests/integration/valkey-benchmark.tcl`.

---

## How it works (phases)

**Execution model.** Iterations are **interleaved across configs** (off, comp, off, comp, …) so
every config samples similar host conditions over the run's wall-clock — running all of one config
then the other lets a busy stretch bias whichever ran during it (the tail is contention-sensitive).
The orchestrator logs to the **console with timestamps + periodic heartbeats** during the long
phases (compress-all drain, profile-prep, measurement), and prints the `postprocess` command at the
end. Reduction uses the **median** over kept iterations and consensus **outlier detection** on the
iteration axis (flag at low iteration counts, drop at ≥ ~5).

- **OFF (reference) path** — `start → populate (--sequential) → open-loop load + measure
  (--rps/--duration, sampling memory/CPU + per-loader latency dumps) → collect → verdict`.
- **Compression-ON path** — `start → populate (corpus values, compression enabled) →
  auto-train (poll compression_active_dict_id) → compress-all (min-idle 0 + COMPRESSION SWEEP
  FORCE, waiting for the worker queue to **drain** + compressed-objects to hold **steady** —
  not a premature growth-plateau) → profile-prep under load to plateau → windowed measurement
  (record-start signal) → collect`. If compress-all can't complete within
  `setup_timeout_seconds`, the iteration fails (a half-compressed dataset can't be measured
  at steady state).
- **Post-processor** — discover SUCCESS iterations → merge per-command histograms across
  iterations → true percentiles + median memory stats → consensus outlier detection (flag at
  low iteration counts, drop at higher) → deltas vs baseline → `report.json` → render `report.html`.

---

## Layout

```
orchestrator.py                 # CLI entry / run driver
lib/config.py                   # run-JSON parse / validate / render
lib/corpus.py                   # deterministic corpus generation (realistic shapes) + cache
lib/benchmark.py                # split math + loader orchestration (FIFO barrier, --latency-dump)
lib/info.py                     # plateau + compress-all completion (poll_until_swept) detectors
lib/latency.py                  # parse/sum --latency-dump histograms → stable schema
lib/server.py                   # valkey-server lifecycle + INFO + process-CPU sampling
lib/provenance.py               # binary checksums, machine info, mpstat
lib/runstatus.py                # SUCCESS/FAILED decision
lib/phases.py                   # per-config-run phase machine (off + compression-on)
lib/env.py                      # binary resolution
postprocessor/reduce.py         # pure reduction → report.json
postprocessor/render.py         # report.json → interactive Plotly report.html
postprocessor/postprocess.py    # CLI
configs/examples/               # canonical.json + local-*.json runnable examples
tests/{unit,component,e2e,postprocessor}/   # Tier-1 / Tier-2 / Tier-3 + post-processor
```

---

## Design docs

- Orchestrator design / plan / idea-honing: `.agents/planning/realtime-data-compression/benchmark/`
  (`design/detailed-design.md`, `implementation/plan.md`, `idea-honing.md`, `amz-orc-findings.md`)
- Post-processor (+ latency-capture contract) design / plans / decisions:
  `.agents/planning/realtime-data-compression/benchmark/postprocessor/`
  (`design/detailed-design.md` incl. §11 empirical hardening, `implementation/plan-{1,2,3}-*.md`,
  `idea-honing.md`)
