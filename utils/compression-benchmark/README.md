# Compression Benchmark Orchestrator

A standalone Python orchestrator that measures the **memory↔latency tradeoff** of
Valkey's in-tree real-time value-compression feature. It drives `valkey-benchmark`
against `valkey-server` under a set of compression **configurations** at a fixed
offered TPS (open-loop), collects precise raw artifacts, and decides each run's
validity (SUCCESS / FAILED).

It is modeled on [`amz_redis-benchmark-orc`](https://github.com/ikolomi/amz_redis-benchmark-orc)
(process orchestration, FIFO-barrier launch, reproducibility rigor, CPU capture) but
**diverges** on three axes:

- compares **compression configs at the same offered TPS** (open-loop `--rps`), not a client sweep;
- computes **true tail percentiles by merging per-bucket histograms** (not averaging per-interval percentiles);
- adds the entire **memory + compressed-steady-state** dimension amz-orc lacks.

The orchestrator **collects raw artifacts and decides validity**. Statistical
reduction (histogram merge, percentiles, delta-vs-baseline) and the headline
**Pareto chart** (memory-saved % vs latency-penalty %) are a separate **post-processor**
(future work) that consumes the run directory.

---

## Status

| Phase | What | State |
|---|---|---|
| A (M0) | pure-Python core: config / corpus / split-math / plateau / run-status | ✅ done |
| C | server lifecycle, INFO polling, loader orchestration (FIFO barrier), provenance, dict-import | ✅ done |
| D (**M2**) | **OFF-config run end-to-end** (no training, no benchmark changes) + failure paths | ✅ **done** |
| B | benchmark flags `--value-data` / `--key-distribution` / `--record-start-signal` | ⏳ pending (needed for compression-ON realistic runs) |
| E | compression-ON path (train/compress-all/profile-prep-to-plateau) + canonical 2-config run | ⏳ pending |

> Today you can run the **off (reference) baseline** end-to-end. The compression-ON
> config path is in progress (Phase E). A real compression *cycle* is already exercised
> by the test suite via `COMPRESSION DICT-IMPORT` (see `tests/component/test_compression_cycle.py`).

---

## Prerequisites

- **Python 3.10+** and `pytest`.
- **Built Valkey binaries** (from the repo root, with the compression feature):

  ```sh
  make BUILD_ZSTD=yes        # produces src/valkey-server, src/valkey-cli, src/valkey-benchmark
  make -C tests/helpers gen-zstd-dict   # (only needed for compression-ON dict-import tests)
  ```

  The orchestrator core has **no third-party Python dependencies** (stdlib only).

---

## Quick start (how-to)

```sh
cd utils/compression-benchmark
export SRC="$(cd ../../src && pwd)"     # your built binaries live in src/

# 1) Validate the config and print the plan — no server is started.
python3 orchestrator.py configs/examples/off-baseline.json \
    --server-binary    "$SRC/valkey-server" \
    --benchmark-binary "$SRC/valkey-benchmark" \
    --dry-run
# → OK: 1 configs × 1 iterations; reference=off; server=.../valkey-server; benchmark=.../valkey-benchmark

# 2) Run it for real.
python3 orchestrator.py configs/examples/off-baseline.json \
    --server-binary    "$SRC/valkey-server" \
    --benchmark-binary "$SRC/valkey-benchmark" \
    --out-root /tmp/cbench-results
```

For each config × iteration this:

1. starts a `valkey-server` **in its own temporary home directory** (the binary is copied in),
2. **populates** exactly `key_count` keys (`--sequential -r N -n N`, each key once),
3. drives an **open-loop** load (per-command processes, FIFO-barrier-synchronized start, `--rps`/`--duration`),
4. samples `used_memory` + `INFO compression` + CPU (mpstat),
5. writes a **timestamped run directory** of raw artifacts and a **`run-status.json`** verdict.

The process exit code is `0` on overall SUCCESS, `1` on FAILED.

---

## The run-JSON config

One run-JSON = one `(workload, target TPS) × list of configs` → one run directory.
Annotated (`configs/examples/off-baseline.json`):

```jsonc
{
  "description": "off baseline — 80/20 GET/SET",
  "output_directory": "results/",                 // run dirs created here (or use --out-root)
  "servers_directory": "/tmp/cbench-servers",      // base for isolated per-server temp dirs
  "benchmark_binary": "valkey-benchmark",          // overridable with --benchmark-binary
  "server_binary":    "valkey-server",             // top-level default; per-config override allowed
  "iterations": 1,                                 // repeats per config (for statistics)
  "reference_config": "off",                        // baseline config name; deltas are vs this

  "data_model": {                                  // dataset shape (corpus is generated from this)
    "value_shape": "json",                         //   kv | json | log | coordinates
    "value_size_distribution": "constant:512",     //   constant:N | uniform:MIN:MAX | lognormal:MU:SIGMA
    "value_size_min": 64, "value_size_max": 16384, //   clamps (esp. for lognormal)
    "seed": 1234,                                   //   REQUIRED — reproducible corpus
    "corpus_entries": 50000,                        //   number of representative blobs
    "key_count": 1000000,                           //   dataset size (keys)
    "key_distribution": "uniform"                   //   uniform | zipf:THETA
  },

  "workload": {
    "target_tps": 50000,                            // offered TPS (open-loop, split by ratios)
    "commands": [ {"type":"get","ratio":0.8},
                  {"type":"set","ratio":0.2} ],     // ratios must sum to 1.0
    "connections_total": 50,
    "max_clients_per_process": 50,                  // single-threaded benchmark per process
    "pipeline": 1,
    "measurement_duration_seconds": 15
  },

  "profile_prep": {                                 // (compression-ON plateau detection — Phase E)
    "plateau_metric": "compression_compressed_objects",
    "plateau_tolerance_pct": 2, "plateau_window_polls": 3,
    "poll_interval_seconds": 5, "max_timeout_seconds": 600
  },

  "configs": [
    { "name": "off", "compression": { "master_switch": "off" } }
  ]
}
```

**Per-config `compression` block** is *sparse* — only the knobs you set are rendered to
`--compression-*` server flags; everything else uses the server default. A raw
`extra_args` list (appended after the structured flags, so it overrides) and a per-config
`server_binary` override are also supported. See `configs/examples/canonical.json` for the
2-config before/after (off + compression-on) target example.

---

## What it produces

```
<out-root>/<timestamp>/
  run-config.json          # echo of your input
  provenance.json          # binary SHA-256s, machine info, seed, corpus hash
  orchestrator.log
  run-status.json          # the verdict (below)
  <config-name>/<iteration-N>/
    server.log
    mpstat.log
    info-measurement.json  # used_memory (incl. MAX) + INFO compression + per-loader rps
    load/loader-<cmd>-<i>.stdout   # raw valkey-benchmark output (latency distribution)
    load/loader-<cmd>-<i>.stderr
```

`run-status.json`:

```json
{
  "overall": "SUCCESS",
  "configs": {
    "off": { "status": "SUCCESS", "iterations": [ { "status": "SUCCESS" } ] }
  }
}
```

A config iteration is **FAILED** (with a `reason`) when: the achieved TPS falls below
`target_tps` (`target_tps_not_achieved`), the compression profile doesn't stabilize
(`profile_not_stabilized`, Phase E), the server crashes (`server_error`), or a loader
errors (`benchmark_error`).

---

## Running the tests

Tier-1 (pure Python) runs everywhere; Tier-2/3 are tagged `needs_server` / `needs_benchmark`
and **skip** (not fail) when the binaries are absent:

```sh
cd utils/compression-benchmark
python3 -m pytest -q                       # Tier-1 only (binary tests skip)

SRC="$(cd ../../src && pwd)"
VALKEY_SERVER="$SRC/valkey-server" VALKEY_BENCHMARK="$SRC/valkey-benchmark" \
    python3 -m pytest -q                   # full suite (Tier-1/2/3)
```

---

## How it works (phases)

- **OFF (reference) path** — skips training/compression; the path proven end-to-end today:
  `start → populate (--sequential) → open-loop load + measure (--rps/--duration) → collect → verdict`.
- **Compression-ON path** (Phase E) — adds: acquire a dictionary (`COMPRESSION DICT-IMPORT`
  today, server-side `COMPRESSION TRAIN` once it lands) → compress-all (`min-idle 0` +
  `COMPRESSION SWEEP FORCE`) → profile-prep under load until the compressed-objects count
  **plateaus** → windowed measurement (record-start signal) → collect.

---

## Layout

```
orchestrator.py                 # CLI entry / run driver
lib/config.py                   # run-JSON parse / validate / render
lib/corpus.py                   # deterministic corpus generation + cache
lib/benchmark.py                # connection/TPS split math + loader orchestration (FIFO barrier)
lib/info.py                     # plateau detector + live poller
lib/server.py                   # valkey-server lifecycle (via valkey-cli)
lib/dictgen.py                  # train a dict via gen-zstd-dict → DICT-IMPORT (test-time)
lib/provenance.py               # binary checksums, machine info, mpstat
lib/runstatus.py                # SUCCESS/FAILED decision
lib/phases.py                   # per-config-run phase machine (off path; compression = Phase E)
lib/env.py                      # binary resolution
configs/examples/               # off-baseline.json (runnable), canonical.json (off + compression-on)
tests/{unit,component,e2e}/     # Tier-1 / Tier-2 / Tier-3
```

---

## Design docs

- Detailed design: `.agents/planning/realtime-data-compression/benchmark/design/detailed-design.md`
- Build plan: `.agents/planning/realtime-data-compression/benchmark/implementation/plan.md`
- Requirements Q&A: `.agents/planning/realtime-data-compression/benchmark/idea-honing.md`
- amz-orc reuse map: `.agents/planning/realtime-data-compression/benchmark/amz-orc-findings.md`
