#!/usr/bin/env python3
"""Compression Benchmark Orchestrator — run driver.

Reads a run-JSON, generates the corpus, and for each config × iteration drives the
phase machine, collecting raw artifacts into a timestamped run directory and writing
a SUCCESS/FAILED verdict (`run-status.json`). It does NOT reduce data or render charts
(that is the separate post-processor). Design: ../design/detailed-design.md.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from lib import config, corpus, phases, provenance, runstatus, server


def run(run_obj: config.RunConfig, raw: dict, server_binary: str,
        benchmark_binary: str, out_root: str | None = None) -> dict:
    out_root = out_root or run_obj.output_directory
    run_dir = os.path.join(out_root, time.strftime("%Y%m%dT%H%M%SZ", time.gmtime()))
    os.makedirs(run_dir, exist_ok=True)

    logf = open(os.path.join(run_dir, "orchestrator.log"), "w")

    def log(m):
        # Mirror to the console (stderr) with a timestamp so a human can follow a long
        # run live (heartbeats during compress-all / measurement), not just the file.
        line = f"[{time.strftime('%H:%M:%S')}] {m}"
        logf.write(line + "\n")
        logf.flush()
        print(line, file=sys.stderr, flush=True)

    log(f"run start: {run_obj.description!r} → {run_dir}")

    # Echo the input config and generate the (cached) corpus.
    with open(os.path.join(run_dir, "run-config.json"), "w") as f:
        json.dump(raw, f, indent=2)
    corpus_path = corpus.ensure(run_obj.data_model, os.path.join(out_root, ".corpus-cache"))
    log(f"corpus: {corpus_path}")

    prov = provenance.build_provenance(
        server_binary=server_binary, benchmark_binary=benchmark_binary,
        seed=run_obj.data_model.seed, corpus_path=corpus_path,
        config_echo={"description": run_obj.description,
                     "reference_config": run_obj.reference_config},
    )
    with open(os.path.join(run_dir, "provenance.json"), "w") as f:
        json.dump(prov, f, indent=2)

    config_results = {}
    for entry in run_obj.configs:
        iters = []
        # per-config override, else the resolved default passed in (CLI/env/JSON)
        sb = entry.server_binary or server_binary
        is_off = entry.compression.get("master_switch", "off") == "off"
        for it in range(run_obj.iterations):
            iter_dir = os.path.join(run_dir, entry.name, f"iteration-{it}")
            port = server.free_port()
            log(f"[{entry.name}] iteration {it} (port {port})")
            if is_off:
                res = phases.run_off_iteration(
                    run=run_obj, entry=entry, server_binary=sb,
                    benchmark_binary=benchmark_binary, iter_dir=iter_dir, port=port, log=log)
            else:
                res = phases.run_compression_iteration(
                    run=run_obj, entry=entry, server_binary=sb,
                    benchmark_binary=benchmark_binary, corpus_path=corpus_path,
                    iter_dir=iter_dir, port=port, log=log)
            iters.append(res)
        config_results[entry.name] = iters

    status = runstatus.decide(config_results)
    with open(os.path.join(run_dir, "run-status.json"), "w") as f:
        json.dump(status, f, indent=2)
    log(f"overall: {status['overall']}")
    log(f"run directory: {run_dir}")
    log(f"generate charts: python3 -m postprocessor.postprocess {run_dir}")
    logf.close()
    return {"run_dir": run_dir, "status": status}


def run_file(config_path: str, server_binary: str, benchmark_binary: str,
             out_root: str | None = None) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    run_obj = config.parse(raw)
    return run(run_obj, raw, server_binary, benchmark_binary, out_root)


def build_plan(run_obj, server_binary, benchmark_binary):
    """Build a structured dry-run plan (pure; no binaries required). Shows the
    per-command loader-process split (R5 split math) and each config's rendered
    --compression-* server args, so an operator can sanity-check a run before
    launching it."""
    from lib import benchmark as _bm

    def _fmt_dist(t):
        return ":".join(str(x) for x in t)

    wl = run_obj.workload
    dm = run_obj.data_model
    split = _bm.split_processes(
        wl.commands, wl.connections_total, wl.max_clients_per_process, wl.target_tps)
    return {
        "server_binary": server_binary or "<unresolved>",
        "benchmark_binary": benchmark_binary or "<unresolved>",
        "iterations": run_obj.iterations,
        "reference_config": run_obj.reference_config,
        "setup_timeout_seconds": run_obj.setup_timeout_seconds,
        "target_tps": wl.target_tps,
        "connections_total": wl.connections_total,
        "max_clients_per_process": wl.max_clients_per_process,
        "pipeline": wl.pipeline,
        "measurement_duration_seconds": wl.measurement_duration_seconds,
        "loader_processes_total": sum(s["n_procs"] for s in split),
        "workload_split": split,
        "configs": [
            {"name": e.name,
             "server_binary": e.server_binary or server_binary or "<unresolved>",
             "server_args": config.render_server_args(e)}
            for e in run_obj.configs
        ],
        "data_model": {
            "value_shape": dm.value_shape,
            "value_size_distribution": _fmt_dist(dm.value_size_distribution),
            "value_size_min": dm.value_size_min,
            "value_size_max": dm.value_size_max,
            "key_distribution": _fmt_dist(dm.key_distribution),
            "key_count": dm.key_count,
            "seed": dm.seed,
            "corpus_entries": dm.corpus_entries,
        },
    }


def _format_plan(plan) -> str:
    dm = plan["data_model"]
    lines = [
        "DRY RUN — no server or load is started.",
        f"  server_binary    : {plan['server_binary']}",
        f"  benchmark_binary : {plan['benchmark_binary']}",
        f"  iterations       : {plan['iterations']}   setup_timeout: {plan['setup_timeout_seconds']}s",
        f"  reference_config : {plan['reference_config']}",
        f"  target_tps       : {plan['target_tps']}  (connections_total={plan['connections_total']}, "
        f"max_clients_per_process={plan['max_clients_per_process']}, pipeline={plan['pipeline']}, "
        f"measure={plan['measurement_duration_seconds']}s)",
        f"  data_model       : shape={dm['value_shape']}  value_size={dm['value_size_distribution']} "
        f"(min {dm['value_size_min']}, max {dm['value_size_max']} B)",
        f"                     key_count={dm['key_count']}  key_distribution={dm['key_distribution']}  "
        f"corpus_entries={dm['corpus_entries']}  seed={dm['seed']}",
        f"  loader processes : {plan['loader_processes_total']} total",
    ]
    for s in plan["workload_split"]:
        rps = sum(p["rps"] for p in s["processes"])
        conns = sum(p["connections"] for p in s["processes"])
        lines.append(f"    {s['command']:<8} {s['n_procs']} proc(s), "
                     f"{conns} conn, {rps:.0f} rps")
    lines.append("  configs:")
    for c in plan["configs"]:
        lines.append(f"    {c['name']:<16} [{c['server_binary']}] {' '.join(c['server_args'])}")
    return "\n".join(lines)


def main(argv=None):
    from lib import env

    ap = argparse.ArgumentParser(description="Compression benchmark orchestrator")
    ap.add_argument("config", help="path to the run-JSON")
    ap.add_argument("--server-binary", default=None)
    ap.add_argument("--benchmark-binary", default=None)
    ap.add_argument("--out-root", default=None)
    ap.add_argument("--dry-run", action="store_true", help="validate config + show plan, do not run")
    args = ap.parse_args(argv)

    with open(args.config, "r", encoding="utf-8") as f:
        raw = json.load(f)
    run_obj = config.parse(raw)  # raises ConfigError on invalid

    sb = args.server_binary or run_obj.server_binary or env.server_binary_path()
    bb = args.benchmark_binary or run_obj.benchmark_binary or env.benchmark_binary_path()

    if args.dry_run:
        # Binary-independent: resolve leniently, never require a built server/benchmark.
        print(_format_plan(build_plan(run_obj, sb, bb)))
        return 0

    result = run(run_obj, raw, sb, bb, args.out_root)
    print(json.dumps(result["status"], indent=2))
    return 0 if result["status"]["overall"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
