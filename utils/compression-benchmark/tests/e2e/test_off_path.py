"""D1 Tier-3 end-to-end test: the orchestrator drives a full OFF-config run with no
training (M2 de-risk milestone) — corpus → populate → open-loop load+measure →
collect → run-status SUCCESS — and the run directory satisfies the artifact contract.
Needs valkey-server + valkey-benchmark.
"""

import json
import os

import pytest

import orchestrator
from lib import corpus, env

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _run_json(tmp_path):
    return {
        "description": "e2e off-path",
        "output_directory": str(tmp_path / "results"),
        "servers_directory": str(tmp_path / "servers"),
        "benchmark_binary": env.benchmark_binary_path(),
        "server_binary": env.server_binary_path(),
        "iterations": 1,
        "reference_config": "off",
        "data_model": {
            "value_shape": "json", "value_size_distribution": "constant:256",
            "value_size_min": 64, "value_size_max": 16384, "seed": 7,
            "corpus_entries": 500, "key_count": 20000, "key_distribution": "uniform",
        },
        "workload": {
            "target_tps": 2000,
            "commands": [{"type": "get", "ratio": 0.8}, {"type": "set", "ratio": 0.2}],
            "connections_total": 8, "max_clients_per_process": 4, "pipeline": 1,
            "measurement_duration_seconds": 3,
        },
        "profile_prep": {
            "plateau_metric": "compression_compressed_objects", "plateau_tolerance_pct": 2,
            "plateau_window_polls": 3, "poll_interval_seconds": 1, "max_timeout_seconds": 60,
        },
        "configs": [{"name": "off", "compression": {"master_switch": "off"}}],
    }


def test_off_path_e2e_success_and_artifact_contract(tmp_path):
    cfg = _run_json(tmp_path)
    cfg_path = tmp_path / "run.json"
    cfg_path.write_text(json.dumps(cfg))

    result = orchestrator.run_file(str(cfg_path), cfg["server_binary"], cfg["benchmark_binary"])
    status = result["status"]
    run_dir = result["run_dir"]

    # verdict
    assert status["overall"] == "SUCCESS", status
    assert status["configs"]["off"]["status"] == "SUCCESS"

    # top-level artifacts
    for name in ("provenance.json", "run-config.json", "orchestrator.log", "run-status.json"):
        assert os.path.getsize(os.path.join(run_dir, name)) > 0, name

    # per-iteration artifacts
    it_dir = os.path.join(run_dir, "off", "iteration-0")
    assert os.path.getsize(os.path.join(it_dir, "server.log")) > 0
    im = json.load(open(os.path.join(it_dir, "info-measurement.json")))
    assert im["used_memory_max"] > 0
    assert len(im["loaders"]) >= 2 and all(l["returncode"] == 0 for l in im["loaders"])
    # loader stdout artifacts exist and parsed to a positive rps
    for ld in im["loaders"]:
        assert ld["achieved_rps"] and ld["achieved_rps"] > 0

    # provenance carries reproducibility info
    prov = json.load(open(os.path.join(run_dir, "provenance.json")))
    assert prov["seed"] == 7
    assert prov["corpus"]["sha256"].startswith("sha256:")
    assert prov["binary_checksums"]["valkey-server"].startswith("sha256:")

    # reproducibility: same seed → identical cached corpus (cache hit, same path)
    dm = orchestrator.config.parse(cfg).data_model
    cache = os.path.join(cfg["output_directory"], ".corpus-cache")
    assert corpus.ensure(dm, cache) == corpus.corpus_path(dm, cache)


def test_off_path_honors_passed_server_binary_over_json(tmp_path):
    # run-JSON has a bogus server_binary; the path passed to run_file must win.
    cfg = _run_json(tmp_path)
    cfg["data_model"]["key_count"] = 5000
    cfg["workload"]["measurement_duration_seconds"] = 2
    cfg["server_binary"] = "/nonexistent/valkey-server"
    path = tmp_path / "run.json"
    path.write_text(json.dumps(cfg))

    res = orchestrator.run_file(str(path), env.server_binary_path(), env.benchmark_binary_path())
    assert res["status"]["overall"] == "SUCCESS", res["status"]


def test_off_path_unmet_target_tps_fails(tmp_path):
    cfg = _run_json(tmp_path)
    cfg["data_model"]["key_count"] = 5000
    cfg["workload"]["target_tps"] = 50_000_000        # unreachable with 8 connections
    cfg["workload"]["measurement_duration_seconds"] = 2
    path = tmp_path / "run.json"
    path.write_text(json.dumps(cfg))

    res = orchestrator.run_file(str(path), cfg["server_binary"], cfg["benchmark_binary"])
    assert res["status"]["overall"] == "FAILED"
    it = res["status"]["configs"]["off"]["iterations"][0]
    assert it["reason"] == "target_tps_not_achieved"
    assert it["achieved_tps"] < it["target_tps"]


def test_off_path_server_crash_fails(tmp_path):
    cfg = _run_json(tmp_path)
    cfg["data_model"]["key_count"] = 5000
    # a bogus startup arg makes valkey-server exit during start → server_error
    cfg["configs"] = [{"name": "off",
                       "compression": {"master_switch": "off"},
                       "extra_args": ["--not-a-real-config", "1"]}]
    path = tmp_path / "run.json"
    path.write_text(json.dumps(cfg))

    res = orchestrator.run_file(str(path), cfg["server_binary"], cfg["benchmark_binary"])
    assert res["status"]["overall"] == "FAILED"
    it = res["status"]["configs"]["off"]["iterations"][0]
    assert it["reason"] == "server_error"

