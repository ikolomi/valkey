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
import signal as _signal
import subprocess
import time as _time

from lib import benchmark, config, info, provenance, server

# Generous fixed timeout for the SETUP phases (auto-train + compress-all). These are
# not the measured profile; only profile-prep is gated by profile_prep.max_timeout_seconds
# (R4.6 fail-on-timeout → profile_not_stabilized).
_SETUP_TIMEOUT_S = 180


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


def _wait_active_dict(srv, timeout):
    """Poll until the server has auto-trained and promoted an active dictionary
    (S1.2 first-training fires once DBSIZE >= compression-dict-min-training-keys)."""
    deadline = _time.monotonic() + timeout
    while _time.monotonic() < deadline:
        if int(srv.info("compression").get("compression_active_dict_id", "0")) != 0:
            return True
        _time.sleep(0.5)
    return False


def _sample_used_memory_until_done(spawned, srv, interval, max_wait):
    """Poll `used_memory` while the loaders run the measured window; return the
    series (the MAX is the headline memory metric, R6.1; transient decompression
    views are real memory to provision for)."""
    series = []
    deadline = _time.monotonic() + max_wait
    while any(s["proc"].poll() is None for s in spawned):
        try:
            series.append(int(srv.info("memory").get("used_memory", "0")))
        except Exception:
            pass
        if _time.monotonic() >= deadline:
            break
        _time.sleep(interval)
    return series


def run_compression_iteration(*, run, entry, server_binary, benchmark_binary,
                              corpus_path, iter_dir, port, host="127.0.0.1",
                              log=lambda m: None):
    """Compression-on path (design §3.4 / Q6): Train (dict-import) → Populate →
    Compress-all → Profile-prep-to-plateau → signaled windowed Measure → collect.

    Dict acquisition is via `COMPRESSION DICT-IMPORT` (a dict trained out-of-process
    by `gen-zstd-dict`) because server-side `COMPRESSION TRAIN` (S1.x) isn't landed.
    """
    os.makedirs(iter_dir, exist_ok=True)
    dm, wl, pp = run.data_model, run.workload, run.profile_prep
    datasize = representative_datasize(dm)
    key_dist = dm.key_distribution if dm.key_distribution[0] == "zipf" else None
    sig = int(_signal.SIGUSR1)

    crashed = False
    bench_err = False
    achieved = 0.0
    used_mem_max = 0
    plateaued = None

    def compressed_objects():
        return int(srv.info("compression").get("compression_compressed_objects", "0"))

    srv = server.Server(server_binary, os.path.join(iter_dir, "srv"), entry.name, port,
                        args=config.render_server_args(entry))
    try:
        srv.start()

        # E2 Populate first: exactly key_count keys, corpus-backed (compressible)
        # values. No dict exists yet, so they store uncompressed — the server then
        # auto-trains a dict on this keyspace (the realistic "train on your data" path).
        pr = subprocess.run(
            benchmark.populate_argv(benchmark_binary, host, port, dm.key_count,
                                    value_corpus=corpus_path),
            capture_output=True, text=True, timeout=1800)
        if pr.returncode != 0:
            bench_err = True
            log(f"[{entry.name}] populate failed (rc={pr.returncode}): {pr.stderr.strip()}")

        # E1 Train: server-side automatic first-training (S1.2 BIO_COMPRESSION_TRAIN).
        # The cron fires training once DBSIZE >= compression-dict-min-training-keys and
        # promotes a dict; poll until it's active. (A manual `COMPRESSION TRAIN` command
        # is a later PR; auto-training is the available — and realistic — path.)
        if not _wait_active_dict(srv, _SETUP_TIMEOUT_S):
            raise RuntimeError(
                f"server did not auto-train an active dict within {_SETUP_TIMEOUT_S}s "
                f"(need >= compression-dict-min-training-keys eligible keys)")
        log(f"[{entry.name}] auto-trained active dict id="
            f"{srv.info('compression').get('compression_active_dict_id')}")

        # E3 Compress-all (deterministic start): min-idle 0 + force sweep, wait to plateau.
        real_min_idle = srv.config_get("compression-min-idle-seconds")
        srv.config_set("compression-min-idle-seconds", 0)
        srv.compression("sweep", "force")
        ca = info.poll_until_plateau(
            compressed_objects, tolerance_pct=pp.plateau_tolerance_pct,
            window_polls=pp.plateau_window_polls, poll_interval=pp.poll_interval_seconds,
            max_timeout=_SETUP_TIMEOUT_S)
        log(f"[{entry.name}] compress-all: plateaued={ca['plateaued']} "
            f"objects={ca['series'][-1] if ca['series'] else 0}")
        srv.config_set("compression-min-idle-seconds", real_min_idle)  # restore the lever

        # E4 Profile-prep + E5 Measure: ONE continuous load. Release the barrier, poll the
        # equilibrium plateau (FAIL on timeout), fire the record-start signal, then measure.
        split = benchmark.split_processes(wl.commands, wl.connections_total,
                                          wl.max_clients_per_process, wl.target_tps)
        loaders = benchmark.build_load_argvs(
            benchmark_binary, host, port, split, wl.measurement_duration_seconds,
            dm.key_count, datasize=datasize, pipeline=wl.pipeline,
            value_corpus=corpus_path, key_dist=key_dist, record_start_signal=sig)
        mpstat = provenance.start_mpstat(os.path.join(iter_dir, "mpstat.log"))
        spawned = benchmark.spawn_loaders(loaders, os.path.join(iter_dir, "load"))

        prep = info.poll_until_plateau(
            compressed_objects, tolerance_pct=pp.plateau_tolerance_pct,
            window_polls=pp.plateau_window_polls, poll_interval=pp.poll_interval_seconds,
            max_timeout=pp.max_timeout_seconds)
        plateaued = prep["plateaued"]
        if not plateaued:
            log(f"[{entry.name}] profile did not stabilize within {pp.max_timeout_seconds}s")

        # Begin the measured window (always signal — never leave loaders hung).
        benchmark.record_start_loaders(spawned, sig)
        mem_series = _sample_used_memory_until_done(
            spawned, srv, interval=0.5, max_wait=wl.measurement_duration_seconds + 30)
        results = benchmark.collect_loaders(spawned)
        provenance.stop_mpstat(mpstat)

        used_mem_max = max(mem_series) if mem_series else int(
            srv.info("memory").get("used_memory", "0"))
        achieved = sum((r["achieved_rps"] or 0.0) for r in results)
        if any(r["returncode"] != 0 or r["achieved_rps"] is None for r in results):
            bench_err = True
            log(f"[{entry.name}] one or more loaders errored")

        with open(os.path.join(iter_dir, "info-measurement.json"), "w") as f:
            json.dump({
                "used_memory_series": mem_series,
                "used_memory_max": used_mem_max,
                "achieved_tps": achieved,
                "plateaued": plateaued,
                "compress_all_series": ca["series"],
                "profile_prep_series": prep["series"],
                "compression": srv.info("compression"),
                "loaders": [{k: r[k] for k in ("command", "index", "returncode", "achieved_rps")}
                            for r in results],
            }, f, indent=2)
    except Exception as e:  # server failed / control error
        crashed = True
        log(f"[{entry.name}] iteration error: {e}")
    finally:
        _copy_server_log(srv, iter_dir)
        srv.teardown()

    return {
        "achieved_tps": achieved,
        "target_tps": wl.target_tps,
        "plateaued": plateaued,
        "crashed": crashed,
        "benchmark_error": bench_err,
        "used_memory_max": used_mem_max,
    }
