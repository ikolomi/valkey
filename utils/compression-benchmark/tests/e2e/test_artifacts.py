"""Tier-3 e2e — output-file / data soundness.

Asserts that every artifact the orchestrator writes exists and contains sound,
internally-consistent data. Reuses the session-scoped ``off_run`` / ``comp_run``
fixtures (conftest.py) so the expensive runs happen once.
"""

import json
import os

import pytest

from lib import benchmark, config

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _load(run_dir, *parts):
    with open(os.path.join(run_dir, *parts)) as f:
        return json.load(f)


# --------------------------------------------------------------------------- #
# Top-level artifacts (off_run)
# --------------------------------------------------------------------------- #

def test_run_config_is_exact_echo(off_run):
    echo = _load(off_run["run_dir"], "run-config.json")
    assert echo == off_run["cfg"], "run-config.json must be a verbatim echo of the input"


def test_provenance_is_sound(off_run):
    prov = _load(off_run["run_dir"], "provenance.json")
    assert prov["seed"] == off_run["cfg"]["data_model"]["seed"]
    assert prov["binary_checksums"]["valkey-server"].startswith("sha256:")
    assert prov["binary_checksums"]["valkey-benchmark"].startswith("sha256:")
    assert prov["corpus"]["sha256"].startswith("sha256:")
    assert os.path.exists(prov["corpus"]["path"])
    assert prov["machine"]["cpus"] and prov["machine"]["cpus"] > 0
    assert prov["machine"]["platform"] and prov["machine"]["python"]
    assert prov["config"]["reference_config"] == "off"
    assert prov["timestamp"]


def test_run_status_structure(off_run):
    st = _load(off_run["run_dir"], "run-status.json")
    assert st["overall"] in ("SUCCESS", "FAILED")
    assert set(st["configs"]) == {"off"}
    # iterations knob: 2 requested → 2 iteration verdicts
    assert len(st["configs"]["off"]["iterations"]) == 2
    assert st == off_run["status"]  # returned status matches the written file


def test_orchestrator_log_has_markers(off_run):
    with open(os.path.join(off_run["run_dir"], "orchestrator.log")) as f:
        text = f.read()
    assert "run start" in text
    assert "overall:" in text


def test_iterations_knob_produces_n_dirs(off_run):
    for it in range(2):
        d = os.path.join(off_run["run_dir"], "off", f"iteration-{it}")
        assert os.path.isdir(d), d
        assert os.path.getsize(os.path.join(d, "server.log")) > 0
        assert os.path.exists(os.path.join(d, "info-measurement.json"))


# --------------------------------------------------------------------------- #
# Per-iteration info-measurement soundness (off_run)
# --------------------------------------------------------------------------- #

def test_off_info_measurement_fields_sound(off_run):
    key_count = off_run["cfg"]["data_model"]["key_count"]
    for it in range(2):
        im = _load(off_run["run_dir"], "off", f"iteration-{it}", "info-measurement.json")
        assert isinstance(im["used_memory_pre"], int) and im["used_memory_pre"] > 0
        assert isinstance(im["used_memory_post"], int) and im["used_memory_post"] > 0
        assert im["used_memory_max"] == max(im["used_memory_pre"], im["used_memory_post"])
        assert im["dbsize"] == key_count, "populate must cover exactly key_count keys"
        assert im["achieved_tps"] > 0
        # off config: the server reports compression off and nothing compressed
        comp = im["compression"]
        assert comp["compression_master_switch"] == "off"
        assert int(comp["compression_compressed_objects"]) == 0


def test_off_loader_split_matches_config_and_artifacts(off_run):
    """The number of loader processes per command equals the R5 split math, each has
    a non-empty stdout/stderr artifact, and the recorded achieved_rps matches the
    value re-parsed from the stdout file."""
    run_obj = config.parse(off_run["cfg"])
    wl = run_obj.workload
    split = {s["command"]: s["n_procs"]
             for s in benchmark.split_processes(wl.commands, wl.connections_total,
                                                wl.max_clients_per_process, wl.target_tps)}
    # get ratio .8, conns 8 → 6 conns, ceil(6/4)=2 procs; set ratio .2 → 2 conns, 1 proc
    assert split == {"get": 2, "set": 1}

    for it in range(2):
        it_dir = os.path.join(off_run["run_dir"], "off", f"iteration-{it}")
        load_dir = os.path.join(it_dir, "load")
        im = _load(it_dir, "info-measurement.json")

        for cmd, n in split.items():
            files = [f for f in os.listdir(load_dir)
                     if f.startswith(f"loader-{cmd}-") and f.endswith(".stdout")]
            assert len(files) == n, f"{cmd}: expected {n} loader stdout files, got {files}"

        assert len(im["loaders"]) == sum(split.values())
        for ld in im["loaders"]:
            base = f"loader-{ld['command']}-{ld['index']}"
            out_path = os.path.join(load_dir, base + ".stdout")
            assert os.path.getsize(out_path) > 0
            assert os.path.exists(os.path.join(load_dir, base + ".stderr"))
            assert ld["returncode"] == 0
            assert ld["achieved_rps"] and ld["achieved_rps"] > 0
            # the recorded rps is exactly what the stdout artifact parses to
            reparsed = benchmark.parse_achieved_rps(open(out_path).read())
            assert reparsed == ld["achieved_rps"]


# --------------------------------------------------------------------------- #
# Compression-run soundness (comp_run)
# --------------------------------------------------------------------------- #

def test_comp_run_overall_success(comp_run):
    st = comp_run["status"]
    assert st["overall"] == "SUCCESS", st
    assert st["configs"]["off"]["status"] == "SUCCESS"
    assert st["configs"]["compression-on"]["status"] == "SUCCESS"


def test_comp_info_measurement_compressed_and_consistent(comp_run):
    im = _load(comp_run["run_dir"], "compression-on", "iteration-0", "info-measurement.json")
    comp = im["compression"]

    # actually compressed
    objs = int(comp["compression_compressed_objects"])
    assert objs > 0, comp
    ratio = float(comp["compression_ratio"])
    assert 0.0 < ratio < 1.0, comp

    # byte totals are internally consistent with the ratio and net savings
    unc = int(comp["compression_total_uncompressed_bytes"])
    cmp_ = int(comp["compression_total_compressed_bytes"])
    assert 0 < cmp_ < unc, comp
    assert abs(ratio - cmp_ / unc) < 0.05, (ratio, cmp_, unc)
    assert int(comp["compression_net_saved_bytes"]) > 0

    # a dictionary was trained + promoted
    assert int(comp["compression_active_dict_id"]) != 0
    assert int(comp["compression_known_dicts"]) >= 1

    # plateau + memory series soundness
    assert im["plateaued"] is True
    assert isinstance(im["used_memory_series"], list) and len(im["used_memory_series"]) > 0
    assert im["used_memory_max"] == max(im["used_memory_series"])
    assert isinstance(im["compress_all_series"], list) and len(im["compress_all_series"]) > 0
    assert isinstance(im["profile_prep_series"], list) and len(im["profile_prep_series"]) > 0
    assert im["dbsize"] == comp_run["cfg"]["data_model"]["key_count"]


def test_comp_loaders_and_artifacts(comp_run):
    it_dir = os.path.join(comp_run["run_dir"], "compression-on", "iteration-0")
    im = _load(it_dir, "info-measurement.json")
    load_dir = os.path.join(it_dir, "load")
    assert len(im["loaders"]) >= 2
    for ld in im["loaders"]:
        out_path = os.path.join(load_dir, f"loader-{ld['command']}-{ld['index']}.stdout")
        assert os.path.getsize(out_path) > 0
        assert ld["returncode"] == 0
        assert ld["achieved_rps"] and ld["achieved_rps"] > 0
