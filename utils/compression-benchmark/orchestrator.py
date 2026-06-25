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
        logf.write(m + "\n")
        logf.flush()

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
            log(f"[{entry.name}] iteration {it} (port {port}, off={is_off})")
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
    logf.close()
    return {"run_dir": run_dir, "status": status}


def run_file(config_path: str, server_binary: str, benchmark_binary: str,
             out_root: str | None = None) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = json.load(f)
    run_obj = config.parse(raw)
    return run(run_obj, raw, server_binary, benchmark_binary, out_root)


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
        print(f"OK: {len(run_obj.configs)} configs × {run_obj.iterations} iterations; "
              f"reference={run_obj.reference_config}; server={sb}; benchmark={bb}")
        return 0

    result = run(run_obj, raw, sb, bb, args.out_root)
    print(json.dumps(result["status"], indent=2))
    return 0 if result["status"]["overall"] == "SUCCESS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
