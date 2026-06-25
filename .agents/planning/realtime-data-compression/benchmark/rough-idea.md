# Rough idea — Inline-compression benchmarking subsystem

_Seed for the PDD process (idea-honing → design → plan → implement), mirroring
`.agents/planning/realtime-data-compression/`. Anchored on
**amz_redis-benchmark-orc** (server-config comparison + Python orchestration +
interactive graphs), not resp-bench._

## The goal (one line)

> Given a **data model** (data type, key distribution, value-size distribution),
> a **target TPS**, a **command mix (types + ratios)**, and a **compression
> configuration** (where "off" is just one configuration), **how does latency
> behave** — and what is the **memory** outcome?

Latency (and memory) are the **outputs**. The four bullets above are the
**inputs**. Client-count is *not* an input of interest — it is only a means to
drive a given TPS.

## Audience

Engineering — us + Valkey maintainers — to understand where the feature stands
(memory win vs latency/throughput cost). Not customer-facing marketing (yet).

## Locked decisions (from discussion 2026-06-22 — do not re-litigate)

- **Standalone, out-of-tree repo** (`ikolomi/valkey-compression-bench`), amz-orc
  model: Python orchestration + interactive Plotly graphs **wrapping
  valkey-benchmark**. Out of the Valkey source tree.
- **Everything that can be script-side, is**: corpus generation, memory
  tracking (INFO), server lifecycle, multi-process load (command ratios via
  concurrent processes, amz-orc style), outlier filtering, graph generation.
- **valkey-benchmark gets only the minimum that cannot be script-side**, added
  incrementally, easy-first: (1) corpus-backed command data, (2) non-uniform
  (zipf) key distribution, (3) multi-key commands (MGET/MSET). Deferred until
  scripts need them; the first results need **zero** valkey-benchmark change.
- **Phasing:** Phase 1 = standalone repo + the valkey-benchmark additions.
  Phase 2 = a thin in-tree `run.sh` subset (upstreamable). Not now.
- **Load model:** start closed-loop; `--rps` already exists in valkey-benchmark
  for open-loop/target-TPS when needed.
- **Corpora:** simulated synthetic-but-realistic (kv / json / log / coordinates),
  reproducible per seed. Good enough to start.

## Feasibility prototype (already built — informs design, is NOT the design)

`~/valkey-compression-bench` (commit b39973e) is a throwaway vertical slice that
proved the engine works end-to-end: server lifecycle, corpus preload,
drive-to-compressed (auto-train → sweep), a GET sweep, INFO capture, and a
Plotly report. First numbers (json, 3k keys): `compression_ratio ≈ 0.19`,
`used_memory −44%`. **Its config schema was invented unilaterally and is the
very thing this PDD process must design properly** — it stands only as proof
the moving parts connect.

## Open questions for idea-honing (the design we skipped)

1. **Rendering first.** What is the primary chart, what's on its axes, what are
   the series, what is a "data point"? (TPS as input; latency as output.)
2. **Workload model.** How are command types + ratios expressed in JSON
   (amz-orc multi-process style)? Single phase or phased?
3. **Inputs / dimensions.** Which inputs are fixed per run vs swept? (data model,
   TPS, command mix, compression config). Matrix templates deferred — start with
   a single JSON layer.
4. **Data point semantics.** Iterations, warmup, what's aggregated, how outliers
   handled, what TPS actually means when closed-loop.
5. **Compression configuration surface.** What set of compression configs do we
   compare (off / compression with N threads / value-size bounds / …)?
6. **Memory metric.** Which memory number is authoritative (used_memory vs RSS
   vs dataset), and when is it sampled relative to the load?

These are resolved interactively in `idea-honing.md`, one question at a time.
