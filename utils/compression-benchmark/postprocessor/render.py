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
    base = configs.get(baseline, {})

    def saved_bytes(b, c):
        return (b - c) if (b is not None and c is not None) else None

    def saved_pct(b, c):
        return (100.0 * (b - c) / b) if (b not in (None, 0) and c is not None) else None

    # --- Pareto: X = memory saved, Y = latency penalty; one trace per percentile.
    # Toggle (abs ↔ %): X = saved bytes ↔ saved %, Y = penalty µs ↔ %.
    base_rss = _g(base, "memory", "rss_bytes")
    x_abs = [[saved_bytes(base_rss, _g(configs, n, "memory", "rss_bytes")) for n in others]]
    x_pct = [[_g(configs, n, "memory", "memory_saved_pct") for n in others]]
    pareto_traces, py_abs, py_pct = [], [], []
    for p in PERCENTILE_ORDER:
        ya = [_g(configs, n, "latency", "aggregate", p, "delta_usec") for n in others]
        yp = [_g(configs, n, "latency", "aggregate", p, "delta_pct") for n in others]
        py_abs.append(ya)
        py_pct.append(yp)
        pareto_traces.append({
            "type": "scatter", "mode": "markers+text", "name": p,
            "x": x_abs[0], "y": ya, "text": others, "textposition": "top center",
            "visible": True if p in DEFAULT_VISIBLE else "legendonly",
        })
    pareto = {
        "div": "chart-pareto", "traces": pareto_traces,
        "layout": {"title": "Memory saved vs latency penalty (Pareto)",
                   "xaxis": {"title": "Memory saved vs baseline (bytes)"},
                   "yaxis": {"title": "Latency penalty vs baseline (µs)"}, "hovermode": "closest"},
        "modes": {
            "abs": {"x": x_abs * len(PERCENTILE_ORDER), "y": py_abs,
                    "xtitle": "Memory saved vs baseline (bytes)", "ytitle": "Latency penalty vs baseline (µs)"},
            "pct": {"x": x_pct * len(PERCENTILE_ORDER), "y": py_pct,
                    "xtitle": "Memory saved vs baseline (%)", "ytitle": "Latency penalty vs baseline (%)"},
        },
    }

    # --- Per-percentile latency delta: X = percentile, one trace per non-baseline config.
    pd_traces, pd_abs, pd_pct = [], [], []
    for n in others:
        ya = [_g(configs, n, "latency", "aggregate", p, "delta_usec") for p in PERCENTILE_ORDER]
        yp = [_g(configs, n, "latency", "aggregate", p, "delta_pct") for p in PERCENTILE_ORDER]
        pd_abs.append(ya)
        pd_pct.append(yp)
        pd_traces.append({"type": "scatter", "mode": "lines+markers", "name": n,
                          "x": PERCENTILE_ORDER, "y": ya})
    percentile_delta = {
        "div": "chart-percentile-delta", "traces": pd_traces,
        "layout": {"title": "Latency delta vs baseline, by percentile "
                            "(tail percentiles use fewer samples — see Measurement coverage)",
                   "xaxis": {"title": "percentile"}, "yaxis": {"title": "Δ latency vs baseline (µs)"}},
        "modes": {
            "abs": {"y": pd_abs, "ytitle": "Δ latency vs baseline (µs)"},
            "pct": {"y": pd_pct, "ytitle": "Δ latency vs baseline (%)"},
        },
    }

    # --- Memory saved vs baseline, by percentile of the steady-window sample series.
    # One chart, RSS + used_memory series per non-baseline config; toggle bytes ↔ %.
    _MEM_PCTS = ["min", "p25", "median", "p75", "p90", "p95", "p99", "max"]
    ms_traces, ms_abs, ms_pct = [], [], []
    for n in others:
        for label, stat in (("RSS", "rss_stats"), ("used_memory", "used_stats")):
            bytes_y = [saved_bytes(_g(base, "memory", stat, k), _g(configs, n, "memory", stat, k)) for k in _MEM_PCTS]
            pct_y = [saved_pct(_g(base, "memory", stat, k), _g(configs, n, "memory", stat, k)) for k in _MEM_PCTS]
            ms_abs.append(bytes_y)
            ms_pct.append(pct_y)
            ms_traces.append({"type": "scatter", "mode": "lines+markers",
                              "name": (f"{n} {label}" if len(others) > 1 else label),
                              "x": _MEM_PCTS, "y": bytes_y})
    mem_saved = {
        "div": "chart-memory-saved", "traces": ms_traces,
        "layout": {"title": "Memory saved vs baseline, by percentile of the RSS/used sample series "
                            "(higher is better)",
                   "xaxis": {"title": "percentile of the memory sample series"},
                   "yaxis": {"title": "Memory saved vs baseline (bytes)"}},
        "modes": {
            "abs": {"y": ms_abs, "ytitle": "Memory saved vs baseline (bytes)"},
            "pct": {"y": ms_pct, "ytitle": "Memory saved vs baseline (%)"},
        },
    }

    # --- Heatmap: rows = command (or config/command if >1 config), cols = percentile.
    z, yrows = [], []
    single = len(others) == 1
    for n in others:
        for cmd in sorted(_g(configs, n, "latency", "per_command", default={})):
            yrows.append(cmd if single else f"{n}/{cmd}")
            z.append([_g(configs, n, "latency", "per_command", cmd, p, "delta_usec")
                      for p in PERCENTILE_ORDER])
    heatmap = {
        "div": "chart-heatmap", "traces": [{
            "type": "heatmap", "x": PERCENTILE_ORDER, "y": yrows, "z": z, "colorscale": "Reds",
            "colorbar": {"title": "Δ µs"},
        }],
        "layout": {"title": "Per-command latency penalty (µs) vs baseline",
                   "xaxis": {"title": "percentile"},
                   "yaxis": {"title": ("command" if single else "config / command"),
                             "automargin": True}},
    }

    # --- Operational headroom: server-PROCESS CPU% per config (not system-wide).
    headroom = {
        "div": "chart-headroom", "traces": [{
            "type": "bar", "name": "valkey-server process CPU %", "x": names,
            "y": [_g(configs, n, "cpu", "pct_total") for n in names],
        }],
        "layout": {"title": "Operational headroom — valkey-server PROCESS CPU% "
                            "(all server threads incl. compression workers; not system-wide)",
                   "yaxis": {"title": "CPU % (process)"}},
    }

    return {"pareto": pareto, "percentile-delta": percentile_delta,
            "memory-saved": mem_saved, "heatmap": heatmap, "headroom": headroom}


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
    """Measurement coverage + reliability flags. A percentile p is determined by the
    samples in its tail ≈ requests × (1 − p/100); a thin tail (< 100) is flagged ⚠. A
    large per-iteration p99 spread is flagged too — it means host conditions varied
    across iterations (the tail is environment-confounded, not a config property)."""
    pcts = [("p50", 0.5), ("p90", 0.1), ("p95", 0.05), ("p99", 0.01),
            ("p99.9", 1e-3), ("p99.99", 1e-4), ("p99.999", 1e-5)]
    THIN = 100
    configs = report.get("configs", {})
    head = ("<tr><th>Config</th><th>iterations<br>(kept/total)</th><th>requests</th>"
            "<th>p99 per-iter<br>(µs, spread)</th>"
            + "".join(f"<th>≈{lbl}<br>tail samples</th>" for lbl, _ in pcts) + "</tr>")
    rows = []
    any_thin = False
    for n, c in configs.items():
        s = _g(c, "samples", default={})
        req = s.get("requests_total")
        # per-iteration p99 spread → within-run instability (host noise)
        p99s = s.get("per_iteration_p99") or []
        if p99s:
            lo, hi, md = min(p99s), max(p99s), sorted(p99s)[len(p99s) // 2]
            rel = (hi - lo) / md if md else 0
            spread = f"{lo:.0f}–{hi:.0f}" + (" ⚠" if rel > 0.3 else "")
        else:
            spread = "n/a"
        cells = []
        for _, frac in pcts:
            if isinstance(req, (int, float)):
                cnt = round(req * frac)
                thin = cnt < THIN
                any_thin = any_thin or thin
                cells.append(f'<td>{cnt}{" ⚠" if thin else ""}</td>')
            else:
                cells.append("<td>n/a</td>")
        rows.append(
            "<tr>"
            f"<td>{html.escape(str(n))}</td>"
            f"<td>{s.get('iterations_kept')}/{s.get('iterations_total')}</td>"
            f"<td>{req if req is not None else 'n/a'}</td>"
            f"<td>{spread}</td>"
            f"{''.join(cells)}</tr>")
    note = ('<p style="color:#666;font-size:0.9em">⚠ flags unreliable readings: a tail with '
            f'&lt; {THIN} samples, or a per-iteration p99 spread &gt; 30% (host conditions varied '
            'across iterations — the tail is environment-confounded). "tail samples" ≈ requests × '
            '(1 − p/100). Widen the window, add iterations, or run on a quiet/dedicated host to '
            'tighten the tail.</p>')
    return f'<div id="table-coverage"><h2>Measurement coverage &amp; reliability</h2><table>{head}{"".join(rows)}</table>{note}</div>'


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
var MODES = {};
Object.keys(FIGS).forEach(function(k){
  var f = FIGS[k];
  Plotly.newPlot(f.div, f.traces, f.layout, {responsive: true});
  if (f.modes) MODES[f.div] = f.modes;   // charts that support absolute↔% toggling
});
var pctMode = false;
function toggleMode(){
  pctMode = !pctMode;
  var m = pctMode ? 'pct' : 'abs';
  document.getElementById('mode-toggle').textContent =
      pctMode ? 'Show absolute values' : 'Show % vs baseline';
  Object.keys(MODES).forEach(function(div){
    var md = MODES[div][m];
    var up = {};
    if (md.y) up.y = md.y;
    if (md.x) up.x = md.x;
    Plotly.restyle(div, up);                       // swap the data
    var rl = {};
    if (md.ytitle) rl['yaxis.title.text'] = md.ytitle;
    if (md.xtitle) rl['xaxis.title.text'] = md.xtitle;
    Plotly.relayout(div, rl);                       // and the axis labels
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
        '<button id="mode-toggle" onclick="toggleMode()">Show % vs baseline</button>\n'
        f"{divs}\n"
        f"{_summary_table(report)}\n"
        f"{_coverage_table(report)}\n"
        f"<script>\n{_JS.replace('__FIGS_JSON__', figs_json)}\n</script>\n"
        "</body></html>\n"
    )
