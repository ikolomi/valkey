"""Plan 3 — postprocess.py CLI (T3.12). Builds a minimal run-dir and runs the CLI."""

import json
import os

from postprocessor import postprocess


def _write(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f)


def _minimal_run(root):
    _write(os.path.join(root, "run-config.json"),
           {"reference_config": "off", "workload": {"target_tps": 1, "commands": []},
            "data_model": {"key_distribution": "zipf:0.99", "seed": 1, "key_count": 1,
                           "value_size_distribution": "constant:512"}})
    _write(os.path.join(root, "run-status.json"),
           {"overall": "SUCCESS", "configs": {"off": {"status": "SUCCESS",
                                                       "iterations": [{"status": "SUCCESS"}]}}})
    im = {
        "latency": {"hdr": {"lowest": 10, "highest": 3000000, "sigfig": 3},
                    "per_command": {"get": {"total_count": 2, "buckets": [[50, 2]]}}},
        "memory": {"used_memory_series": [900], "used_memory_rss_series": [1000],
                   "mem_fragmentation_ratio_series": [1.1], "steady_state_window": [0, 0]},
        "server_cpu": {"pct_total": 30.0}, "stats": {"evicted_keys": 0},
        "compression": {},
    }
    _write(os.path.join(root, "off", "iteration-0", "info-measurement.json"), im)


def test_cli_writes_report_json_and_html(tmp_path):
    root = str(tmp_path / "run")
    _minimal_run(root)
    rc = postprocess.main([root])
    assert rc == 0
    jpath = os.path.join(root, "report.json")
    hpath = os.path.join(root, "report.html")
    assert os.path.exists(jpath) and os.path.exists(hpath)
    rep = json.load(open(jpath))
    assert rep["baseline"] == "off" and "off" in rep["configs"]
    html = open(hpath).read()
    assert html.startswith("<!DOCTYPE html>") and "chart-pareto" in html


def test_cli_custom_output_paths(tmp_path):
    root = str(tmp_path / "run")
    _minimal_run(root)
    jpath = str(tmp_path / "r.json")
    hpath = str(tmp_path / "r.html")
    postprocess.main([root, "-o", hpath, "--json", jpath])
    assert os.path.exists(jpath) and os.path.exists(hpath)
