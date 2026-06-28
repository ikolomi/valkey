"""Plan 3 — build_report over a hand-built fixture run directory (T3.7).

Constructs a minimal run-dir with a known off baseline and a compression config that
uses less RSS but has higher latency, then asserts the reduced report.json numbers
(memory_saved_pct, per-percentile latency deltas, baseline-is-zero).
"""

import json
import os

from postprocessor import reduce


def _write(path, obj):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f)


def _im(rss, get_lat, set_lat, cpu=42.0):
    """info-measurement.json with flat rss series + single-bucket per-command latency."""
    return {
        "latency": {
            "hdr": {"lowest": 10, "highest": 3000000, "sigfig": 3},
            "per_command": {
                "get": {"total_count": 10, "buckets": [[get_lat, 10]]},
                "set": {"total_count": 10, "buckets": [[set_lat, 10]]},
            },
        },
        "memory": {
            "used_memory_series": [rss - 100, rss - 100, rss - 100],
            "used_memory_rss_series": [rss, rss, rss],
            "mem_fragmentation_ratio_series": [1.1, 1.1, 1.1],
            "steady_state_window": [0, 2],
        },
        "server_cpu": {"pct_user": cpu * 0.6, "pct_system": cpu * 0.4, "pct_total": cpu},
        "stats": {"evicted_keys": 0, "rejected_connections": 0, "expired_keys": 0, "keyspace_misses": 0},
        "compression": {"compression_ratio": "2.50", "compression_net_saved_bytes": "12345",
                        "compression_compressed_objects": "999"},
    }


def _build_run_dir(root):
    _write(os.path.join(root, "run-config.json"), {
        "description": "fixture", "reference_config": "off",
        "workload": {"target_tps": 2000, "commands": [{"type": "get", "ratio": 0.8}]},
        "data_model": {"value_size_distribution": "constant:512", "key_distribution": "zipf:0.99",
                       "key_count": 2000, "seed": 4242},
    })
    _write(os.path.join(root, "provenance.json"), {"corpus": {"sha256": "sha256:deadbeef"}})
    _write(os.path.join(root, "run-status.json"), {
        "overall": "SUCCESS",
        "configs": {
            "off": {"status": "SUCCESS", "iterations": [{"status": "SUCCESS"}, {"status": "SUCCESS"}]},
            "comp": {"status": "SUCCESS", "iterations": [{"status": "SUCCESS"}, {"status": "SUCCESS"}]},
        },
    })
    for it in (0, 1):
        _write(os.path.join(root, "off", f"iteration-{it}", "info-measurement.json"),
               _im(1000, 100, 100))
        _write(os.path.join(root, "comp", f"iteration-{it}", "info-measurement.json"),
               _im(700, 140, 120))  # less RSS, higher latency


def test_build_report_numbers(tmp_path):
    root = str(tmp_path / "run")
    _build_run_dir(root)
    rep = reduce.build_report(root)

    assert rep["baseline"] == "off"
    assert rep["workload"]["corpus_sha256"] == "sha256:deadbeef"
    assert set(rep["configs"]) == {"off", "comp"}

    off, comp = rep["configs"]["off"], rep["configs"]["comp"]

    # memory: off rss 1000, comp rss 700 → 30% saved; baseline saves 0
    assert off["memory"]["rss_bytes"] == 1000
    assert comp["memory"]["rss_bytes"] == 700
    assert abs(off["memory"]["memory_saved_pct"]) < 1e-9
    assert abs(comp["memory"]["memory_saved_pct"] - 30.0) < 1e-9
    assert comp["memory"]["rss_stats"]["median"] == 700

    # latency: comp get p99 = 140 vs baseline 100 → +40us / +40%
    g99 = comp["latency"]["per_command"]["get"]["p99"]
    assert g99["usec"] == 140
    assert g99["delta_usec"] == 40
    assert abs(g99["delta_pct"] - 40.0) < 1e-9
    # baseline deltas are zero
    assert off["latency"]["per_command"]["get"]["p99"]["delta_usec"] == 0

    # aggregate present with all canonical percentiles
    assert set(comp["latency"]["aggregate"]) == {"p50", "p90", "p95", "p99", "p99.9", "p99.99", "p99.999"}
    # compression + kept iterations
    assert comp["compression"]["compressed_objects"] == "999"
    assert comp["iterations"]["kept"] == [0, 1]
    # measurement coverage: 2 iterations merged, get+set each 10/iter → 40 total requests
    assert comp["samples"]["requests_total"] == 40
    assert comp["samples"]["per_command"] == {"get": 20, "set": 20}
    assert comp["samples"]["iterations_kept"] == 2 and comp["samples"]["iterations_total"] == 2
    assert comp["samples"]["memory_samples"] == off["samples"]["memory_samples"] > 0
    assert comp["samples"]["per_iteration_p99"] == [140, 140]  # aggregate p99 per iteration


def test_build_report_flags_and_drops_outlier_iteration(tmp_path):
    """Inject an anomalous iteration into a 5-iteration config: consensus flags it and
    (N≥5) drops it, surfaced in report.json; the headline median is not polluted."""
    root = str(tmp_path / "run")
    _write(os.path.join(root, "run-config.json"),
           {"reference_config": "off", "workload": {"commands": []},
            "data_model": {"key_distribution": "zipf:0.99", "seed": 1, "key_count": 1,
                           "value_size_distribution": "constant:512"}})
    _write(os.path.join(root, "provenance.json"), {"corpus": {"sha256": "sha256:x"}})
    rss_vals = [700, 710, 705, 715, 9999]  # slight spread + one clear anomaly (idx 4)
    _write(os.path.join(root, "run-status.json"), {
        "overall": "SUCCESS",
        "configs": {
            "off": {"status": "SUCCESS", "iterations": [{"status": "SUCCESS"}]},
            "comp": {"status": "SUCCESS", "iterations": [{"status": "SUCCESS"}] * len(rss_vals)},
        },
    })
    _write(os.path.join(root, "off", "iteration-0", "info-measurement.json"), _im(1000, 100, 100))
    for i, rss in enumerate(rss_vals):
        _write(os.path.join(root, "comp", f"iteration-{i}", "info-measurement.json"),
               _im(rss, 140, 120))

    rep = reduce.build_report(root)
    comp = rep["configs"]["comp"]
    flagged = {f["iter"] for f in comp["iterations"]["flagged_outliers"]}
    assert 4 in flagged, "the 9999 iteration must be flagged"
    assert 4 not in comp["iterations"]["kept"]
    assert comp["iterations"]["kept"] == [0, 1, 2, 3]
    # headline RSS median is over kept iterations only (700–715), not polluted by 9999
    assert comp["memory"]["rss_bytes"] <= 715
