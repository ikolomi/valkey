"""Shared helpers for Tier-3 (e2e) tests: build tiny run-JSON configs and execute
them through the orchestrator. Imported by conftest.py (session fixtures) and by the
config-effect tests that need their own dedicated runs.
"""

import json
import os

import pytest

import orchestrator
from lib import env


def base_cfg(out_dir, servers_dir):
    return {
        "description": "e2e fixture run",
        "output_directory": str(out_dir),
        "servers_directory": str(servers_dir),
        "benchmark_binary": env.benchmark_binary_path(),
        "server_binary": env.server_binary_path(),
        "iterations": 1,
        "reference_config": "off",
        "data_model": {
            "value_shape": "kv", "value_size_distribution": "constant:512",
            "value_size_min": 1, "value_size_max": 16384, "seed": 4242,
            "corpus_entries": 4000, "key_count": 2000, "key_distribution": "zipf:0.99",
        },
        "workload": {
            "target_tps": 2000,
            "commands": [{"type": "get", "ratio": 0.8}, {"type": "set", "ratio": 0.2}],
            "connections_total": 8, "max_clients_per_process": 4, "pipeline": 1,
            "measurement_duration_seconds": 2,
        },
        "profile_prep": {
            "plateau_metric": "compression_compressed_objects", "plateau_tolerance_pct": 8,
            "plateau_window_polls": 3, "poll_interval_seconds": 1, "max_timeout_seconds": 90,
        },
        "configs": [{"name": "off", "compression": {"master_switch": "off"}}],
    }


def run_cfg(cfg, work_dir):
    path = os.path.join(str(work_dir), "run.json")
    with open(path, "w") as f:
        json.dump(cfg, f)
    res = orchestrator.run_file(path, cfg["server_binary"], cfg["benchmark_binary"])
    return {"run_dir": res["run_dir"], "status": res["status"], "cfg": cfg}


def need_binaries():
    if env.server_binary_path() is None or env.benchmark_binary_path() is None:
        pytest.skip("valkey-server / valkey-benchmark not available")


# Distinctive, non-default compression knobs so the INFO echo is unambiguous
# (defaults: min_value_size 256, max_value_size 131072, min_idle_seconds 60, threads 1).
COMP_KNOBS = {
    "master_switch": "compression",
    "automatic_sweeper": "enabled",
    "min_value_size": 128,
    "max_value_size": 8192,
    "min_idle_seconds": 2,
    "threads": 2,
}
