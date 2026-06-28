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
                       "rss_stats": {"min": 700, "p25": 702, "median": 700, "p75": 710, "p90": 713,
                                     "p95": 715, "p99": 718, "max": 720, "mean": 705, "stddev": 5, "samples": 6},
                       "used_stats": {"min": 600, "p25": 605, "median": 610, "p75": 615, "p90": 617,
                                      "p95": 618, "p99": 619, "max": 620, "mean": 611, "stddev": 4, "samples": 6},
                       "per_iteration_rss_median": [700, 700]},
            "latency": {"aggregate": agg,
                        "per_command": {"get": agg, "set": agg}},
            "cpu": {"pct_total": cpu, "delta_pct": 5.0},
            "stats": {"evicted_keys": 0, "rejected_connections": 0, "expired_keys": 0, "keyspace_misses": 0},
            "compression": {"ratio": "2.5", "net_saved_bytes": "123", "compressed_objects": "999"},
            "samples": {"requests_total": 480000, "per_command": {"get": 384000, "set": 96000},
                        "per_iteration_p99": [100, 100], "memory_samples": 60,
                        "iterations_kept": 2, "iterations_total": 3},
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
    for key in ("pareto", "percentile-delta", "memory-saved", "heatmap", "headroom"):
        assert key in figs and figs[key]["traces"] is not None
    # consolidated: the old absolute-memory + rss-distribution charts are gone
    assert "memory-breakdown" not in figs and "memory-stability" not in figs


def test_memory_saved_has_rss_and_used_series_over_percentiles():
    figs = render._figures(_report())
    ms = figs["memory-saved"]
    names = [t["name"] for t in ms["traces"]]
    assert names == ["RSS", "used_memory"]            # single non-baseline config → bare labels
    assert ms["traces"][0]["x"] == ["min", "p25", "median", "p75", "p90", "p95", "p99", "max"]
    # abs mode = bytes saved, pct mode = % saved (both present for the toggle)
    assert "abs" in ms["modes"] and "pct" in ms["modes"]
    assert "bytes" in ms["modes"]["abs"]["ytitle"] and "%" in ms["modes"]["pct"]["ytitle"]


def test_unified_toggle_modes_on_all_comparison_charts():
    figs = render._figures(_report())
    for key in ("pareto", "percentile-delta", "memory-saved"):
        m = figs[key]["modes"]
        assert set(m) == {"abs", "pct"}
        assert m["abs"]["ytitle"] and m["pct"]["ytitle"]
    # pareto toggles the X axis too (memory saved bytes ↔ %)
    assert figs["pareto"]["modes"]["abs"]["xtitle"] != figs["pareto"]["modes"]["pct"]["xtitle"]


def test_heatmap_labels_decluttered_for_single_config():
    figs = render._figures(_report())
    hm = figs["heatmap"]
    # single non-baseline config → bare command rows (no redundant "comp/" prefix)
    assert hm["traces"][0]["y"] == ["get", "set"]
    assert hm["layout"]["yaxis"]["automargin"] is True
    assert hm["layout"]["xaxis"]["title"] == "percentile"


def test_render_html_has_all_containers_and_toggle():
    html = render.render(_report())
    for div in ("chart-pareto", "chart-percentile-delta", "chart-memory-saved",
                "chart-heatmap", "chart-headroom", "table-summary", "table-coverage"):
        assert f'id="{div}"' in html, f"missing chart container {div}"
    # the consolidated-away charts must not appear
    assert 'id="chart-memory-breakdown"' not in html and 'id="chart-memory-stability"' not in html
    assert "plotly" in html.lower() and "Plotly.newPlot" in html
    assert 'id="mode-toggle"' in html and "Show % vs baseline" in html
    assert "Plotly.relayout" in html  # toggle updates axis titles
    assert "zipf:0.99" in html      # workload header rendered
    assert "480000" in html          # measurement-coverage request count rendered
    assert "⚠" in html               # thin-tail reliability flag (p99.99/p99.999 < 100 samples)
    assert html.strip().startswith("<!DOCTYPE html>")


def test_render_graceful_degradation_missing_latency():
    rep = _report()
    rep["configs"]["comp"]["latency"] = {"aggregate": {}, "per_command": {}}
    html = render.render(rep)  # must not raise
    assert "chart-pareto" in html
