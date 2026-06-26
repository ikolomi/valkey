"""Tier-3 e2e — each configuration setting has its expected end-to-end effect.

Strategy:
- **Echo**: the server's ``INFO compression`` section reflects the configured knobs, so
  one compression run proves master_switch / automatic_sweeper / min_value_size /
  max_value_size / min_idle_seconds / threads all round-tripped config → server → INFO.
- **Behavioral**: ``master_switch=off`` and ``threads=0`` must yield zero compression even
  though (for threads=0) a dictionary still trains on ``bio``.
- **Load shaping**: open-loop ``--rps`` keeps achieved TPS from blowing past ``target_tps``.
- **Validation**: the run entry point rejects an invalid config before doing any work.
"""

import json
import os

import pytest

import orchestrator
from e2e_lib import base_cfg, need_binaries, run_cfg
from lib import config

NEEDS = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _im(run_dir, cfg_name):
    with open(os.path.join(run_dir, cfg_name, "iteration-0", "info-measurement.json")) as f:
        return json.load(f)


@pytest.mark.needs_server
@pytest.mark.needs_benchmark
def test_compression_knobs_echoed_in_info(comp_run):
    """The distinctive COMP_KNOBS must be reflected by the server — master_switch and
    automatic_sweeper appear in INFO compression; the size/threads knobs are captured via
    CONFIG GET into `compression_config`. Proof that the structured `compression` block
    rendered to `--compression-*` flags and the server applied each one."""
    im = _im(comp_run["run_dir"], "compression-on")
    comp = im["compression"]
    cfg = im["compression_config"]
    assert comp["compression_master_switch"] == "compression"
    assert comp["compression_automatic_sweeper"] == "enabled"
    assert cfg["compression-master-switch"] == "compression"
    assert cfg["compression-automatic-sweeper"] == "enabled"
    assert int(cfg["compression-min-value-size"]) == 128
    assert int(cfg["compression-max-value-size"]) == 8192
    assert int(cfg["compression-min-idle-seconds"]) == 2
    assert int(cfg["compression-threads"]) == 2


@pytest.mark.needs_server
@pytest.mark.needs_benchmark
def test_off_config_does_no_compression(off_run):
    """master_switch=off → the server reports off and never compresses."""
    comp = _im(off_run["run_dir"], "off")["compression"]
    assert comp["compression_master_switch"] == "off"
    assert int(comp["compression_compressed_objects"]) == 0
    assert int(comp["compression_active_dict_id"]) == 0


@pytest.mark.needs_server
@pytest.mark.needs_benchmark
def test_reference_config_is_recorded(comp_run):
    prov = json.load(open(os.path.join(comp_run["run_dir"], "provenance.json")))
    assert prov["config"]["reference_config"] == "off"
    assert set(comp_run["status"]["configs"]) == {"off", "compression-on"}


@pytest.mark.needs_server
@pytest.mark.needs_benchmark
def test_target_tps_is_open_loop_rate_limited(off_run):
    """Open-loop `--rps` caps the rate: 8 closed-loop connections would do far more than
    target_tps; achieved staying near target proves the rate limit is applied."""
    target = off_run["cfg"]["workload"]["target_tps"]
    im0 = _im(off_run["run_dir"], "off")
    assert 0 < im0["achieved_tps"] <= target * 1.5


@pytest.mark.needs_server
@pytest.mark.needs_benchmark
def test_threads_zero_disables_compression(tmp_path):
    """compression-threads=0 with master=compression: a dict still trains (on bio), but the
    worker pool is empty so nothing is compressed — the run still completes successfully."""
    need_binaries()
    cfg = base_cfg(tmp_path / "results", tmp_path / "servers")
    cfg["reference_config"] = "compression-on"
    cfg["configs"] = [{"name": "compression-on", "compression": {
        "master_switch": "compression", "automatic_sweeper": "enabled",
        "min_value_size": 128, "min_idle_seconds": 0, "threads": 0,
    }}]
    res = run_cfg(cfg, tmp_path)

    assert res["status"]["configs"]["compression-on"]["status"] == "SUCCESS", res["status"]
    im = _im(res["run_dir"], "compression-on")
    comp = im["compression"]
    assert int(im["compression_config"]["compression-threads"]) == 0
    assert int(comp["compression_active_dict_id"]) != 0, "dict trains on bio regardless of threads"
    assert int(comp["compression_compressed_objects"]) == 0, "threads=0 → no compression work"


def test_invalid_reference_config_rejected_before_run(tmp_path):
    """The run entry point validates the config (reference_config must name a real config)
    and raises before starting any server — no binaries required."""
    cfg = base_cfg(tmp_path / "results", tmp_path / "servers")
    cfg["reference_config"] = "does-not-exist"
    path = tmp_path / "run.json"
    path.write_text(json.dumps(cfg))
    with pytest.raises(config.ConfigError):
        orchestrator.run_file(str(path), cfg["server_binary"] or "x",
                              cfg["benchmark_binary"] or "x")
