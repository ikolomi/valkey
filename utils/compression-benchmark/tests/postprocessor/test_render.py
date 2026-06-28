"""Plan 3 Tier-1 tests — rendering (``postprocessor/render.py``).

We don't run a browser; instead we assert the Python-built Plotly figure dicts
(``_figures``) and that the self-contained HTML document carries every expected chart
container, the Plotly CDN, the absolute↔% mode toggle, and degrades gracefully.
"""

from postprocessor import render


def _report():
    def cfg(saved, g99, cpu):
        labels = ["p50", "p90", "p95", "p99", "p99.9", "p99.99", "p99.999"]
        agg = {l: {"usec": g99, "delta_usec": (g99 - 100), "delta_pct": (g99 - 100)} for l in labels}
        return {
            "status": "SUCCESS",
            "iterations": {"kept": [0, 1], "flagged_outliers": []},
            "memory": {"rss_bytes": 1000 - saved * 10, "used_memory_bytes": 900,
                       "frag_ratio": 1.1, "memory_saved_pct": saved, "used_memory_saved_pct": saved,
                       "rss_stats": {"min": 700, "median": 700, "max": 720, "p95": 715, "p99": 718,
                                     "mean": 705, "stddev": 5, "samples": 6},
                       "used_stats": {"min": 600, "median": 610, "max": 620, "p95": 618, "p99": 619,
                                      "mean": 611, "stddev": 4, "samples": 6},
                       "per_iteration_rss_median": [700, 700]},
            "latency": {"aggregate": agg,
                        "per_command": {"get": agg, "set": agg}},
            "cpu": {"pct_total": cpu, "delta_pct": 5.0},
            "stats": {"evicted_keys": 0, "rejected_connections": 0, "expired_keys": 0, "keyspace_misses": 0},
            "compression": {"ratio": "2.5", "net_saved_bytes": "123", "compressed_objects": "999"},
            "samples": {"requests_total": 480000, "per_command": {"get": 384000, "set": 96000},
                        "memory_samples": 60, "iterations_kept": 2, "iterations_total": 3},
        }
    return {
        "workload": {"target_tps": 2000, "key_distribution": "zipf:0.99",
                     "value_size_distribution": "constant:512", "seed": 4242,
                     "commands": [{"type": "get", "ratio": 0.8}]},
        "baseline": "off",
        "configs": {"off": cfg(0.0, 100, 40.0), "comp": cfg(30.0, 140, 52.0)},
    }


def test_figures_pareto_per_percentile_visibility():
    figs = render._figures(_report())
    pareto = figs["pareto"]
    names = [t["name"] for t in pareto["traces"]]
    assert names == ["p50", "p90", "p95", "p99", "p99.9", "p99.99", "p99.999"]
    vis = {t["name"]: t.get("visible", True) for t in pareto["traces"]}
    assert vis["p50"] is True and vis["p99"] is True and vis["p99.9"] is True
    assert vis["p90"] == "legendonly" and vis["p99.99"] == "legendonly"


def test_figures_present():
    figs = render._figures(_report())
    for key in ("pareto", "percentile-delta", "memory-saved", "memory-breakdown",
                "memory-stability", "heatmap", "headroom"):
        assert key in figs and figs[key]["traces"] is not None


def test_render_html_has_all_containers_and_toggle():
    html = render.render(_report())
    for div in ("chart-pareto", "chart-percentile-delta", "chart-memory-saved",
                "chart-memory-breakdown", "chart-memory-stability", "chart-heatmap",
                "chart-headroom", "table-summary", "table-coverage"):
        assert f'id="{div}"' in html, f"missing chart container {div}"
    assert "plotly" in html.lower() and "Plotly.newPlot" in html
    assert 'id="mode-toggle"' in html  # absolute↔% switch
    assert "zipf:0.99" in html  # workload header rendered
    assert "480000" in html      # measurement-coverage request count rendered
    assert html.strip().startswith("<!DOCTYPE html>")


def test_render_graceful_degradation_missing_latency():
    rep = _report()
    rep["configs"]["comp"]["latency"] = {"aggregate": {}, "per_command": {}}
    html = render.render(rep)  # must not raise
    assert "chart-pareto" in html
