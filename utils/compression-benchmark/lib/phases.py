"""Per-config-run phase machine (design §3.4).

v1 implements the **off (reference) path**, which skips Train + Compress-all and is
therefore runnable end-to-end with no training (the M2 de-risk milestone): start →
populate (exact coverage) → load+measure (open-loop) → collect → iteration result.
The compression-on path (Train/Compress-all/Profile-prep) is Phase E.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess

from lib import benchmark, config, provenance, server


def representative_datasize(dm: config.DataModel) -> int:
    """A single representative value size for the off baseline (corpus-backed
    variable values are Phase E / B1). Derived from the size distribution, clamped."""
    kind = dm.value_size_distribution[0]
    if kind == "constant":
        size = dm.value_size_distribution[1]
    elif kind == "uniform":
        size = (dm.value_size_distribution[1] + dm.value_size_distribution[2]) // 2
    else:  # lognormal: use the median target (mu)
        size = int(dm.value_size_distribution[1])
    return max(dm.value_size_min, min(dm.value_size_max, int(size)))


def _copy_server_log(srv, iter_dir):
    try:
        src = os.path.join(srv.home_dir, "server.log")
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(iter_dir, "server.log"))
    except Exception:
        pass


def run_off_iteration(*, run, entry, server_binary, benchmark_binary, iter_dir,
                      port, host="127.0.0.1", log=lambda m: None):
    """Run one off-config iteration; return a runstatus iteration dict."""
    os.makedirs(iter_dir, exist_ok=True)
    dm, wl = run.data_model, run.workload
    datasize = representative_datasize(dm)

    crashed = False
    bench_err = False
    achieved = 0.0
    used_mem_max = 0
    srv = server.Server(server_binary, os.path.join(iter_dir, "srv"), entry.name, port,
                        args=config.render_server_args(entry))
    try:
        srv.start()

        # Populate: exactly key_count keys, each once (existing-flags exact coverage).
        pr = subprocess.run(
            benchmark.populate_argv(benchmark_binary, host, port, dm.key_count, datasize),
            capture_output=True, text=True, timeout=900,
        )
        if pr.returncode != 0:
            bench_err = True
            log(f"[{entry.name}] populate failed (rc={pr.returncode}): {pr.stderr.strip()}")

        used_mem_pre = int(srv.info("memory").get("used_memory", "0"))

        # Load + measure (off skips profile-prep): open-loop, duration-bounded.
        split = benchmark.split_processes(wl.commands, wl.connections_total,
                                          wl.max_clients_per_process, wl.target_tps)
        loaders = benchmark.build_load_argvs(
            benchmark_binary, host, port, split, wl.measurement_duration_seconds,
            dm.key_count, datasize, wl.pipeline,
        )
        mpstat = provenance.start_mpstat(os.path.join(iter_dir, "mpstat.log"))
        results = benchmark.run_loaders(loaders, os.path.join(iter_dir, "load"))
        provenance.stop_mpstat(mpstat)

        used_mem_post = int(srv.info("memory").get("used_memory", "0"))
        used_mem_max = max(used_mem_pre, used_mem_post)
        achieved = sum((r["achieved_rps"] or 0.0) for r in results)
        if any(r["returncode"] != 0 or r["achieved_rps"] is None for r in results):
            bench_err = True
            log(f"[{entry.name}] one or more loaders errored")

        with open(os.path.join(iter_dir, "info-measurement.json"), "w") as f:
            json.dump({
                "used_memory_pre": used_mem_pre,
                "used_memory_post": used_mem_post,
                "used_memory_max": used_mem_max,
                "achieved_tps": achieved,
                "compression": srv.info("compression"),
                "loaders": [{k: r[k] for k in ("command", "index", "returncode", "achieved_rps")}
                            for r in results],
            }, f, indent=2)
    except Exception as e:  # server failed to start / died, or control error
        crashed = True
        log(f"[{entry.name}] iteration error: {e}")
    finally:
        _copy_server_log(srv, iter_dir)
        srv.teardown()

    return {
        "achieved_tps": achieved,
        "target_tps": wl.target_tps,
        "plateaued": None,          # off config: profile-prep N/A
        "crashed": crashed,
        "benchmark_error": bench_err,
        "used_memory_max": used_mem_max,
    }


def run_compression_iteration(**kwargs):
    """Compression-on path (Train/Compress-all/Profile-prep) — Phase E."""
    raise NotImplementedError("compression-on phase machine is Phase E (gated on S1.x training)")
