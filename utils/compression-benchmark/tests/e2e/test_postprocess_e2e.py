"""Tier-3 e2e — the REAL orchestrator → post-processor pipeline (Plan 3, T3.13/T3.14).

Reuses the session-scoped ``comp_run`` fixture (a real off + compression-on run), runs
``postprocess.main`` on it, and asserts the produced ``report.json`` numbers + soundness
and that ``report.html`` renders. Determinism: reducing twice yields identical JSON.
"""

import json
import os

import pytest

from postprocessor import postprocess, reduce

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]

_PERCENTILES = ["p50", "p90", "p95", "p99", "p99.9", "p99.99", "p99.999"]


def test_postprocess_real_pipeline(comp_run):
    run_dir = comp_run["run_dir"]
    rc = postprocess.main([run_dir])
    assert rc == 0

    rep = json.load(open(os.path.join(run_dir, "report.json")))
    assert rep["baseline"] == "off"
    assert {"off", "compression-on"} <= set(rep["configs"])

    for name, c in rep["configs"].items():
        # memory: RSS headline + full stats present and positive
        assert c["memory"]["rss_bytes"] > 0
        assert c["memory"]["rss_stats"]["median"] > 0
        assert c["memory"]["memory_saved_pct"] is not None  # a real number vs baseline
        # latency: all canonical percentiles present, monotonic non-decreasing, in hdr range
        agg = c["latency"]["aggregate"]
        assert set(agg) == set(_PERCENTILES)
        usecs = [agg[p]["usec"] for p in _PERCENTILES]
        assert all(u is not None for u in usecs)
        assert usecs == sorted(usecs), f"percentiles not monotonic for {name}: {usecs}"
        assert 10 <= usecs[0] and usecs[-1] <= 3_000_000
        # per-command present
        assert {"get", "set"} <= set(c["latency"]["per_command"])

    # delta internal consistency: compression-on p99 delta == its usec - off usec
    off99 = rep["configs"]["off"]["latency"]["aggregate"]["p99"]["usec"]
    comp = rep["configs"]["compression-on"]["latency"]["aggregate"]["p99"]
    assert comp["delta_usec"] == comp["usec"] - off99
    # off is the baseline → its deltas are zero
    assert rep["configs"]["off"]["latency"]["aggregate"]["p99"]["delta_usec"] == 0

    # report.html renders, self-contained, with the chart containers
    html = open(os.path.join(run_dir, "report.html")).read()
    assert html.startswith("<!DOCTYPE html>")
    for div in ("chart-pareto", "chart-percentile-delta", "chart-memory-breakdown",
                "chart-heatmap", "table-summary"):
        assert f'id="{div}"' in html


def test_reduction_is_deterministic(comp_run):
    a = reduce.build_report(comp_run["run_dir"])
    b = reduce.build_report(comp_run["run_dir"])
    assert json.dumps(a, sort_keys=True) == json.dumps(b, sort_keys=True)
