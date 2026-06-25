"""Phase E end-to-end: a tiny canonical run (off + compression-on) through the
orchestrator must succeed, and the compression-on iteration must show that
compression actually happened (compressed objects + ratio < 1) captured in its
info-measurement.json. The compression-on path acquires its dict via DICT-IMPORT
(S1.x COMPRESSION TRAIN not landed), so it needs the gen-zstd-dict helper.
Needs valkey-server + valkey-benchmark; skipped if gen-zstd-dict is absent.
"""

import json
import os

import pytest

import orchestrator
from lib import env

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _cfg(tmp_path):
    return {
        "description": "phase-E tiny canonical (off vs compression-on)",
        "output_directory": str(tmp_path / "results"),
        "servers_directory": str(tmp_path / "servers"),
        "benchmark_binary": env.benchmark_binary_path(),
        "server_binary": env.server_binary_path(),
        "iterations": 1,
        "reference_config": "off",
        "data_model": {
            "value_shape": "kv",
            "value_size_distribution": "constant:512",
            "value_size_min": 1,
            "value_size_max": 16384,
            "seed": 1234,
            "corpus_entries": 4000,
            "key_count": 2000,
            "key_distribution": "zipf:0.99",
        },
        "workload": {
            "target_tps": 2000,
            "commands": [{"type": "get", "ratio": 0.8}, {"type": "set", "ratio": 0.2}],
            "connections_total": 20,
            "max_clients_per_process": 20,
            "pipeline": 1,
            "measurement_duration_seconds": 2,
        },
        "profile_prep": {
            "plateau_metric": "compression_compressed_objects",
            "plateau_tolerance_pct": 8,
            "plateau_window_polls": 3,
            "poll_interval_seconds": 1,
            "max_timeout_seconds": 90,
        },
        "configs": [
            {"name": "off", "compression": {"master_switch": "off"}},
            {"name": "compression-on", "compression": {
                "master_switch": "compression",
                "automatic_sweeper": "enabled",
                "min_value_size": 64,
                "max_value_size": 0,
                "min_idle_seconds": 3,
                "min_savings_ratio": 0,
                "threads": 1,
            }},
        ],
    }


def test_compression_on_run_succeeds_and_compresses(tmp_path):
    sb = env.server_binary_path()

    cfg = _cfg(tmp_path)
    path = tmp_path / "run.json"
    path.write_text(json.dumps(cfg))

    res = orchestrator.run_file(str(path), sb, env.benchmark_binary_path())
    assert res["status"]["overall"] == "SUCCESS", res["status"]

    # The compression-on iteration must have actually compressed the keyspace.
    info_path = os.path.join(res["run_dir"], "compression-on", "iteration-0",
                             "info-measurement.json")
    assert os.path.exists(info_path), "missing compression-on measurement artifact"
    data = json.load(open(info_path))
    comp = data["compression"]
    assert int(comp["compression_compressed_objects"]) > 0, comp
    assert 0.0 < float(comp["compression_ratio"]) < 1.0, comp
    assert data["used_memory_max"] > 0
    assert data.get("plateaued") is True, data
