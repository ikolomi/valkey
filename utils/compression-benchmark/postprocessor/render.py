"""Render a ``report.json`` (from :mod:`postprocessor.reduce`) into a self-contained
interactive Plotly HTML report.

Design §6.2 / idea-honing Q3: charts are interactive (legend toggle/isolate, hover),
all 7 canonical percentiles are emitted with the Pareto defaulting to {p50,p99,p99.9}
visible and the rest legend-hidden, plus an absolute↔%-delta mode toggle. Everything is
delta-from-baseline. Plotly is loaded from CDN; no Python plotting dependency. The
figures are built in Python (testable) and rendered by a tiny generic JS loop. Missing
fields degrade gracefully (charts render with gaps; never raises).
"""

from __future__ import annotations

import html
import json

_PLOTLY_CDN = "https://cdn.plot.ly/plotly-2.27.0.min.js"
PERCENTILE_ORDER = ["p50", "p90", "p95", "p99", "p99.9", "p99.99", "p99.999"]
DEFAULT_VISIBLE = {"p50", "p99", "p99.9"}


def _g(d, *path, default=None):
    for k in path:
        if not isinstance(d, dict):
            return default
        d = d.get(k)
    return d if d is not None else default


def _figures(report):
    configs = report.get("configs", {})
    baseline = report.get("baseline")
    names = list(configs)
    others = [n for n in names if n != baseline]

    # --- Pareto: X = memory saved %, Y = latency penalty; one trace per percentile.
    pareto_traces = []
    for p in PERCENTILE_ORDER:
        pareto_traces.append({
            "type": "scatter", "mode": "markers+text", "name": p,
            "x": [_g(configs, n, "memory", "memory_saved_pct") for n in others],
            "y": [_g(configs, n, "latency", "aggregate", p, "delta_usec") for n in others],
            "customdata": [_g(configs, n, "latency", "aggregate", p, "delta_pct") for n in others],
            "text": others, "textposition": "top center",
            "visible": True if p in DEFAULT_VISIBLE else "legendonly",
        })
    pareto = {
        "div": "chart-pareto", "toggle": True, "traces": pareto_traces,
        "layout": {"title": "Memory saved vs latency penalty (Pareto)",
                   "xaxis": {"title": "Memory saved vs baseline (%)"},
                   "yaxis": {"title": "Latency penalty vs baseline (µs)"},
                   "hovermode": "closest"},
    }

    # --- Per-percentile delta: X = percentile, one trace per (non-baseline) config.
    pd_traces = []
    for n in others:
        pd_traces.append({
            "type": "scatter", "mode": "lines+markers", "name": n,
            "x": PERCENTILE_ORDER,
            "y": [_g(configs, n, "latency", "aggregate", p, "delta_usec") for p in PERCENTILE_ORDER],
            "customdata": [_g(configs, n, "latency", "aggregate", p, "delta_pct") for p in PERCENTILE_ORDER],
        })
    percentile_delta = {
        "div": "chart-percentile-delta", "toggle": True, "traces": pd_traces,
        "layout": {"title": "Latency delta vs baseline, by percentile "
                            "(tail percentiles use fewer samples — see Measurement coverage)",
                   "xaxis": {"title": "percentile"}, "yaxis": {"title": "Δ latency vs baseline (µs)"}},
    }

    # --- Memory saved vs baseline (%), the delta view (consistent with latency-delta).
    mem_saved = {
        "div": "chart-memory-saved", "traces": [
            {"type": "bar", "name": "RSS saved % (headline)", "x": names,
             "y": [_g(configs, n, "memory", "memory_saved_pct") for n in names]},
            {"type": "bar", "name": "used_memory saved %", "x": names,
             "y": [_g(configs, n, "memory", "used_memory_saved_pct") for n in names]},
        ],
        "layout": {"title": "Memory saved vs baseline (%) — higher is better",
                   "barmode": "group", "yaxis": {"title": "% saved vs baseline"}},
    }

    # --- Absolute memory breakdown: RSS (headline) vs used_memory, MEDIAN over the
    # steady-state window per config.
    mem_breakdown = {
        "div": "chart-memory-breakdown", "traces": [
            {"type": "bar", "name": "RSS (physical)", "x": names,
             "y": [_g(configs, n, "memory", "rss_bytes") for n in names]},
            {"type": "bar", "name": "used_memory (logical)", "x": names,
             "y": [_g(configs, n, "memory", "used_memory_bytes") for n in names]},
        ],
        "layout": {"title": "Absolute memory — MEDIAN over the steady-state window "
                            "(RSS vs used_memory; gap = fragmentation)",
                   "barmode": "group", "yaxis": {"title": "bytes"}},
    }

    # --- Memory stability: RSS median with min–max whiskers per config.
    med = [_g(configs, n, "memory", "rss_stats", "median") for n in names]
    mx = [_g(configs, n, "memory", "rss_stats", "max") for n in names]
    mn = [_g(configs, n, "memory", "rss_stats", "min") for n in names]
    mem_stability = {
        "div": "chart-memory-stability", "traces": [{
            "type": "scatter", "mode": "markers", "name": "RSS median (min–max)",
            "x": names, "y": med,
            "error_y": {"type": "data", "symmetric": False,
                        "array": [(a - b) if (a is not None and b is not None) else None for a, b in zip(mx, med)],
                        "arrayminus": [(b - a) if (a is not None and b is not None) else None for a, b in zip(mn, med)]},
        }],
        "layout": {"title": "RSS stability (median + min–max across kept iterations)",
                   "yaxis": {"title": "bytes"}},
    }

    # --- Heatmap: rows = config/command, cols = percentile, z = Δ latency (µs).
    z, yrows = [], []
    for n in others:
        for cmd in sorted(_g(configs, n, "latency", "per_command", default={})):
            yrows.append(f"{n}/{cmd}")
            z.append([_g(configs, n, "latency", "per_command", cmd, p, "delta_usec")
                      for p in PERCENTILE_ORDER])
    heatmap = {
        "div": "chart-heatmap", "traces": [{
            "type": "heatmap", "x": PERCENTILE_ORDER, "y": yrows, "z": z, "colorscale": "Reds",
        }],
        "layout": {"title": "Per-command latency penalty (µs) vs baseline"},
    }

    # --- Operational headroom: server-process CPU% per config.
    headroom = {
        "div": "chart-headroom", "traces": [{
            "type": "bar", "name": "server CPU %", "x": names,
            "y": [_g(configs, n, "cpu", "pct_total") for n in names],
        }],
        "layout": {"title": "Operational headroom — server-process CPU%",
                   "yaxis": {"title": "CPU %"}},
    }

    return {"pareto": pareto, "percentile-delta": percentile_delta,
            "memory-saved": mem_saved, "memory-breakdown": mem_breakdown,
            "memory-stability": mem_stability, "heatmap": heatmap, "headroom": headroom}


def _fmt(v, suffix=""):
    if v is None:
        return "n/a"
    if isinstance(v, float):
        return f"{v:.1f}{suffix}"
    return f"{v}{suffix}"


def _workload_header(report):
    wl = report.get("workload", {})
    items = [
        ("target TPS", wl.get("target_tps")),
        ("commands", ", ".join(f"{c.get('type')}={c.get('ratio')}" for c in (wl.get("commands") or []))),
        ("connections / max-per-proc / pipeline",
         f"{wl.get('connections_total')} / {wl.get('max_clients_per_process')} / {wl.get('pipeline')}"),
        ("measurement window", f"{wl.get('measurement_duration_seconds')} s × {wl.get('iterations')} iterations"),
        ("value sizes", f"{wl.get('value_size_distribution')}  (min {wl.get('value_size_min')}, max {wl.get('value_size_max')} B)"),
        ("key distribution", wl.get("key_distribution")),
        ("key count / corpus entries", f"{wl.get('key_count')} / {wl.get('corpus_entries')}"),
        ("seed / corpus sha256", f"{wl.get('seed')} / {wl.get('corpus_sha256')}"),
        ("baseline", report.get("baseline")),
    ]
    rows = "".join(f"<tr><th>{html.escape(str(k))}</th><td>{html.escape(str(v))}</td></tr>"
                   for k, v in items)
    return f'<div id="workload"><h2>Workload</h2><table>{rows}</table></div>'


def _coverage_table(report):
    """Measurement coverage — so the reader can judge tail reliability: a p99.9 is
    only ~0.1% of the request count, p99.99 ~0.01% — small counts ⇒ noisy tails."""
    configs = report.get("configs", {})
    head = ("<tr><th>Config</th><th>iterations (kept/total)</th><th>requests measured</th>"
            "<th>≈p99.9 samples</th><th>≈p99.99 samples</th><th>memory samples</th></tr>")
    rows = []
    for n, c in configs.items():
        s = _g(c, "samples", default={})
        req = s.get("requests_total")
        s999 = f"{req/1000:.0f}" if isinstance(req, (int, float)) else "n/a"
        s9999 = f"{req/10000:.0f}" if isinstance(req, (int, float)) else "n/a"
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(n))}</td>"
            f"<td>{s.get('iterations_kept')}/{s.get('iterations_total')}</td>"
            f"<td>{req if req is not None else 'n/a'}</td>"
            f"<td>{s999}</td><td>{s9999}</td>"
            f"<td>{s.get('memory_samples')}</td>"
            "</tr>")
    note = ('<p style="color:#666;font-size:0.9em">Tail percentiles use few samples '
            '(≈ requests × the tail fraction). Treat p99.9 / p99.99 with &lt; a few hundred '
            'samples as noisy — widen the measurement window or add iterations to tighten them.</p>')
    return f'<div id="table-coverage"><h2>Measurement coverage</h2><table>{head}{"".join(rows)}</table>{note}</div>'


def _summary_table(report):
    configs = report.get("configs", {})
    head = ("<tr><th>Config</th><th>Mem saved %</th><th>p50 Δµs</th><th>p99 Δµs</th>"
            "<th>p99.9 Δµs</th><th>CPU %</th><th>flagged outliers</th></tr>")
    rows = []
    for n, c in configs.items():
        agg = _g(c, "latency", "aggregate", default={})
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(n))}</td>"
            f"<td>{_fmt(_g(c, 'memory', 'memory_saved_pct'), '%')}</td>"
            f"<td>{_fmt(_g(agg, 'p50', 'delta_usec'))}</td>"
            f"<td>{_fmt(_g(agg, 'p99', 'delta_usec'))}</td>"
            f"<td>{_fmt(_g(agg, 'p99.9', 'delta_usec'))}</td>"
            f"<td>{_fmt(_g(c, 'cpu', 'pct_total'))}</td>"
            f"<td>{len(_g(c, 'iterations', 'flagged_outliers', default=[]))}</td>"
            "</tr>")
    return f'<div id="table-summary"><h2>Summary</h2><table>{head}{"".join(rows)}</table></div>'


_JS = """
var FIGS = __FIGS_JSON__;
var ORIG = {};
Object.keys(FIGS).forEach(function(k){
  var f = FIGS[k];
  Plotly.newPlot(f.div, f.traces, f.layout, {responsive: true});
  if (f.toggle) {
    ORIG[f.div] = { abs: f.traces.map(function(t){ return t.y; }),
                    pct: f.traces.map(function(t){ return t.customdata; }) };
  }
});
var pctMode = false;
function toggleMode(){
  pctMode = !pctMode;
  document.getElementById('mode-toggle').textContent =
      pctMode ? 'Show absolute (µs)' : 'Show % delta';
  Object.keys(ORIG).forEach(function(div){
    Plotly.restyle(div, {y: pctMode ? ORIG[div].pct : ORIG[div].abs});
  });
}
"""


def render(report):
    """Return a self-contained interactive HTML report string."""
    figs = _figures(report)
    figs_json = json.dumps(figs)
    divs = "\n".join(
        f'<div id="{figs[k]["div"]}" class="chart"></div>' for k in figs)
    return (
        "<!DOCTYPE html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n"
        "<title>Compression Benchmark Report</title>\n"
        f'<script src="{_PLOTLY_CDN}"></script>\n'
        "<style>body{font-family:sans-serif;margin:1.5rem;}"
        ".chart{width:100%;max-width:1100px;height:460px;margin:1rem 0;}"
        "table{border-collapse:collapse;}th,td{border:1px solid #ccc;padding:4px 8px;text-align:left;}"
        "#mode-toggle{padding:6px 12px;margin:0.5rem 0;cursor:pointer;}</style>\n"
        "</head><body>\n"
        "<h1>Compression Benchmark Report</h1>\n"
        f"{_workload_header(report)}\n"
        '<button id="mode-toggle" onclick="toggleMode()">Show % delta</button>\n'
        f"{divs}\n"
        f"{_summary_table(report)}\n"
        f"{_coverage_table(report)}\n"
        f"<script>\n{_JS.replace('__FIGS_JSON__', figs_json)}\n</script>\n"
        "</body></html>\n"
    )
