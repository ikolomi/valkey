# Findings — `amz_redis-benchmark-orc` (source study)

_Studied commit `b39973e`-era source at https://github.com/ikolomi/amz_redis-benchmark-orc
(orchestrator.py, run_server_matrix.py, generate_server_graphs.py, config schemas).
Purpose: extract reusable mechanisms for our compression benchmark orchestrator,
and pin down exactly where we must diverge._

## TL;DR

amz-orc is a **closed-loop, multi-binary, throughput-scalability** harness:
sweep `total_clients`, compare valkey-server **binaries** on RPS/latency/CPU,
render interactive Plotly. We reuse ~70% of its plumbing verbatim, but our
**axes and statistics differ**: we fix TPS (open-loop `--rps`), compare
compression **configs**, lead with a **memory-vs-latency Pareto**, and **merge
histograms** instead of averaging per-interval percentiles. amz-orc has **no
memory metric and no compressed-steady-state notion** — those are our additions.

---

## 1. Reusable as-is (port with minimal change)

| Mechanism | Where | Note for us |
|---|---|---|
| **FIFO-barrier zero-skew launch** | `create_benchmark_process` (shell `read < fifo && bench …`) + `release_barrier` (write N lines) | Port verbatim. All per-command processes block on a named pipe; one write releases all simultaneously. Essential for clean concurrent multi-process load. |
| **`--test-duration` clean self-exit** | benchmark args; timer starts AFTER connections established | Port. No SIGTERM needed; processes exit on their own. Avoids the `pkill` self-match footgun noted in our SESSION_CHECKPOINT. |
| **connection→process split math** | `procs = max(1, ceil(conns / max_connections_per_benchmark_process))`, even distribution `ceil(conns/procs)` | Port. Single-threaded benchmark per process → this is how you reach high connection counts. Our `--rps` is divided the same way: per-proc rps = command-share-tps / procs. |
| **mpstat CPU capture** | `start_mpstat` (`mpstat -P ALL 1`) + `parse_mpstat_avg_cpu` (handles 12h/24h, last col = %idle) | Port. Process-total CPU is easy and free. (Per-thread worker-CPU split would need `/proc/<pid>/task` parsing — deferred, per rendering-proposal §6 tier.) |
| **Outlier filtering — 4-method consensus** | `find_consensus_outliers` (Modified Z 3.5 / IQR 1.5× / %Dev 15% / Grubbs 0.05; ≥2 agree) + `_norm_ppf`/`_grubbs_critical` | Port verbatim for the **per-iteration variance band** (memory %, RPS). NOTE: applies to the *iteration* axis, not within a single merged-histogram run. |
| **Reproducibility rigor** | isolated per-run server home dir (`prepare_server_directory` copies binary in), binary SHA256 (`sha256_file`), orchestrator SHA256, NUMA topology capture, full config copy into output | Port. Cheap, high-value provenance. |
| **NUMA pinning** | `build_numa_prefix` (`numactl --cpunodebind --membind`), server_node vs benchmark_node, `validate_numa` | Port (optional/off by default). Keeps load generator off the server's cores. |
| **Server lifecycle** | `start_server` / `wait_for_server_ready` (PING loop) / `flush_server` / `stop_server` / `find_cli_path` (redis-cli\|valkey-cli) | Port. We extend with compression-specific steady-state polling (see §3). |
| **local vs remote server abstraction** | `local-server-info` (binary+args, orchestrator manages lifecycle) vs `remote-server-info` (endpoint, validate+flush only) | Port. **Our "config" maps onto a `local-server-info` entry = same binary + different `--compression-*` args.** No new concept needed. |
| **Plotly self-contained HTML** | `generate_html` — CDN plotly, per-server color palette, legend toggle, log-x auto, **absolute↔% -delta mode switch in JS** (`switchMode`) | Reuse the scaffolding + the delta-mode switch. Replace the *chart set* (see §2). |
| **Matrix runner layer** | `run_server_matrix.py`: template × `dimensions.scale` → per-point orchestrator run → `_manifest.json` → graph generator | Reuse the **shape** (template × dimension → manifest), but our sweep dimension is **config list** (and later workload/TPS), not `scale`. |
| **dry-run + checksums + manifest** | both scripts | Port. |
| **pre-population** | `prepopulate_keys` via `benchmark -t set -n -r -d` | **Flawed: random `-r` addressing under-covers the keyspace** (coupon-collector: `-n`=keyspacelen ≈ 63% distinct), and bumping `-n` oversizes the dataset. We replace it with **valkey-benchmark `SET --sequential <key_count>`** (amz fork): keys `0..key_count-1` each written exactly once → exact, full coverage at the configured size. Strong upstream candidate. The "populate before measure" principle stands. |

---

## 2. Must diverge (decided in idea-honing Q1/Q2)

| Dimension | amz-orc | compression-bench | Why |
|---|---|---|---|
| **Primary x-axis** | `total_clients` (closed-loop scalability sweep) | compression **configs** at a **fixed TPS** (open-loop `--rps`) | Our headline is a memory-vs-latency **tradeoff**, not a load curve. Configs are the compared axis; client count is just a means to reach the TPS. |
| **Load model** | closed-loop (clients pull as fast as they can) | **open-loop, rate-limited** (`--rps` per process) | Comparing configs at the *same offered TPS* requires open-loop; under closed-loop each config reaches a different TPS and latency isn't comparable. |
| **Tail-percentile statistics** | **averages per-interval/per-process percentiles** (`mean(set_latencies["p99"])`), scales RPS by proc count | **merge per-bucket histograms** from each process's stock end-of-run "Latency by percentile distribution", sum counts, compute the true percentile | Averaging percentiles is statistically wrong for the tail — our headline Y-axis IS the tail (p99/p99.9). Single most important correctness improvement over amz-orc. **No valkey-benchmark C change needed** — per-bucket cumulative counts are already in stock output. |
| **What's compared** | server **binaries** | compression **configs** (often same binary, different `--compression-*` args; baseline = `off`) | Maps cleanly onto amz-orc's per-server `args`; the graph's delta-vs-reference mode already does config-vs-baseline %. |
| **Memory** | **none** | `used_memory` + `compression_ratio` + `compression_*` INFO fields, sampled at compressed steady state | The whole point of the feature. amz-orc never reads INFO memory. NEW subsystem. |
| **Headline chart** | RPS scalability lines | **Pareto: % memory saved (X) vs latency penalty (Y)**, one point per config; supporting per-percentile delta bars + memory breakdown | Per `research-rendering-proposal.md`. |
| **Keyspace / values** | uniform random keys (`-r`), fixed `-d` size | **corpus-backed values** (`--value-data corpus:FILE`) + **zipf keys** (`--key-distribution`) — value-size distribution lives *in the corpus* (no benchmark flag) | Need real compressibility + a hotset to exercise skip-hot-keys. These are the deferred valkey-benchmark extensions; first on-ramp needs zero C change (GET-only on script-preloaded corpus). |

---

## 3. Gaps amz-orc does not cover — net-new for us

1. **Compressed steady-state drive + detection.** amz-orc preloads then measures
   immediately. We must reach the steady compressed state first (dict active +
   all eligible values compressed) and *detect* it via INFO polling
   (`compression_active_dict_id != 0`, `compression_candidates_pending == 0`,
   `compression_compressed_objects` stable). See planning Q4 (run phases).
2. **Dict acquisition mode.** import (`COMPRESSION DICT-IMPORT`, deterministic,
   default for clean comparison) vs train (realistic cold-start, higher
   variance). amz-orc has no analog.
3. **Memory measurement methodology.** Lead with `used_memory` (live-bytes
   accounting, reflects compression without defrag); RSS only as a secondary
   after `MEMORY PURGE`. amz-orc has neither.
4. **Histogram merge from stock end-of-run output.** Replaces amz-orc's
   interval-CSV percentile averaging. The 1-second `--interval-metrics` CSV
   amz-orc relies on is, for us, only useful for drift-over-time (deferred).
5. **Corpus generation** (script-side, reproducible per seed) — see Q3.

---

## 4. Concrete "borrow list" for implementation

Lift these near-verbatim when we scaffold the orchestrator:
- FIFO barrier: `create_benchmark_process` shell-wrap + `release_barrier` + `wait_for_benchmark_processes`.
- `find_consensus_outliers` + `_norm_ppf` + `_t_critical` + `_grubbs_critical` (drop-in).
- `start_mpstat` / `stop_mpstat` / `parse_mpstat_avg_cpu`.
- `sha256_file`, isolated-dir prep, manifest writing, dry-run scaffolding.
- `wait_for_server_ready` / `flush_server` / `_redis_cli_cmd`.
- Plotly HTML scaffold + `switchMode` absolute↔delta JS (re-skin the chart set).

Rewrite (do not lift):
- `compute_iteration_summary` — replace percentile-averaging with histogram merge.
- chart construction in `generate_html` — replace scalability lines with the
  Pareto + per-percentile-delta + memory-breakdown set from `research-rendering-proposal.md`.
- data model / keyspace handling — corpus-backed, seeded (Q3).

---

## 5. One-line verdict

> amz-orc gives us the **process-orchestration, reproducibility, outlier, CPU,
> and rendering plumbing for free**. We replace its **axes (configs@fixedTPS,
> open-loop), its tail statistics (histogram merge), and add the entire
> memory + compressed-steady-state dimension** it never had.
