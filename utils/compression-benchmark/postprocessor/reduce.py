"""Pure reduction: orchestrator run directory → ``report.json`` numbers.

Self-contained (stdlib only) so it is trivially unit-testable and decoupled from the
orchestrator's load-generation internals — it consumes only the stable
``info-measurement.json`` contract (design §3). Histograms arrive already summed
across each iteration's loader processes (``latency.per_command[...].buckets`` as a
sorted ``[[value_usec, count], ...]`` list); here we sum them across **kept** iterations
and compute true percentiles — never an average of per-iteration percentiles.
"""

from __future__ import annotations

import json
import math
import os
import statistics

# Canonical percentile set (idea-honing Q2 — no arbitrary percentiles).
CANONICAL_PERCENTILES = [50, 90, 95, 99, 99.9, 99.99, 99.999]


def _pct_label(p):
    return "p" + ("%f" % p).rstrip("0").rstrip(".")


# --------------------------------------------------------------------------- #
# Histogram math
# --------------------------------------------------------------------------- #

def percentile_from_buckets(buckets, pct):
    """Percentile value (usec) from a ``[[value, count], ...]`` list (ascending by
    value). Returns ``None`` for an empty histogram."""
    total = sum(c for _, c in buckets)
    if total <= 0:
        return None
    rank = (pct / 100.0) * total
    cum = 0
    for value, count in buckets:
        cum += count
        if cum >= rank:
            return value
    return buckets[-1][0]


def percentiles(buckets, pcts=None):
    """``{label: value}`` for the canonical percentile set (or ``pcts``)."""
    pcts = pcts if pcts is not None else CANONICAL_PERCENTILES
    return {_pct_label(p): percentile_from_buckets(buckets, p) for p in pcts}


def merge_bucket_lists(lists):
    """Sum a list of ``[[value, count], ...]`` histograms by value → one ascending
    list. Exact (identical hdr bucket layout), so percentiles of the merge are true,
    not averaged."""
    acc = {}
    for bl in lists:
        for value, count in bl:
            acc[value] = acc.get(value, 0) + count
    return [[v, acc[v]] for v in sorted(acc)]


# --------------------------------------------------------------------------- #
# Statistics + outliers
# --------------------------------------------------------------------------- #

def _pct_of_sorted(s, pct):
    if not s:
        return None
    idx = min(max(0, math.ceil((pct / 100.0) * len(s)) - 1), len(s) - 1)
    return s[idx]


def memory_stats(series):
    """Distribution stats for a memory series (design §7.4). Headline = ``median``;
    the full set is reported for sanity + provisioning view. ``None`` if empty."""
    if not series:
        return None
    s = sorted(series)
    return {
        "min": s[0], "max": s[-1],
        "mean": statistics.fmean(s),
        "median": statistics.median(s),
        "p95": _pct_of_sorted(s, 95),
        "p99": _pct_of_sorted(s, 99),
        "stddev": statistics.pstdev(s),
        "samples": len(s),
    }


def consensus_outliers(values, min_methods=2):
    """Indices flagged by ≥ ``min_methods`` of four methods (IQR / z-score / MAD /
    percentile-bound) — amz-orc-style consensus (C1). Conservative by design: at very
    small N the methods rarely agree, which is the intended behavior (see
    :func:`select_iterations`)."""
    n = len(values)
    if n == 0:
        return set()
    counts = [0] * n
    s = sorted(values)

    q1, q3 = _pct_of_sorted(s, 25), _pct_of_sorted(s, 75)
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    for i, x in enumerate(values):
        if x < lo or x > hi:
            counts[i] += 1

    mean, std = statistics.fmean(values), statistics.pstdev(values)
    if std > 0:
        for i, x in enumerate(values):
            if abs(x - mean) / std > 3.0:
                counts[i] += 1

    med = statistics.median(values)
    mad = statistics.median([abs(x - med) for x in values])
    if mad > 0:
        for i, x in enumerate(values):
            if 0.6745 * abs(x - med) / mad > 3.5:
                counts[i] += 1

    p5, p95 = _pct_of_sorted(s, 5), _pct_of_sorted(s, 95)
    for i, x in enumerate(values):
        if x < p5 or x > p95:
            counts[i] += 1

    return {i for i, c in enumerate(counts) if c >= min_methods}


def select_iterations(values, n_threshold=5):
    """Apply the flag-low-N / drop-high-N policy (Q8): below ``n_threshold`` iterations
    flag-only (consensus is untrustworthy at low N); at/above, drop flagged. Never
    drops everything. Returns ``(kept_indices, flagged_indices)``."""
    flagged = consensus_outliers(values)
    n = len(values)
    if n >= n_threshold and 0 < len(flagged) < n:
        kept = [i for i in range(n) if i not in flagged]
    else:
        kept = list(range(n))
    return kept, flagged


def delta(base, value):
    """Absolute + percentage delta of ``value`` vs ``base`` (``pct`` None if base 0)."""
    ab = value - base
    return {"abs": ab, "pct": (100.0 * ab / base) if base else None}


# --------------------------------------------------------------------------- #
# Discovery + full report assembly
# --------------------------------------------------------------------------- #

def _read_json(*parts):
    with open(os.path.join(*parts)) as f:
        return json.load(f)


def discover(run_dir):
    """Map a run directory from ``run-config.json`` + ``run-status.json``: the baseline
    config name and, per config, its status and per-iteration {index, dir, status}."""
    cfg = _read_json(run_dir, "run-config.json")
    status = _read_json(run_dir, "run-status.json")
    out = {"baseline": cfg.get("reference_config"),
           "overall": status.get("overall"), "configs": {}}
    for name, cstat in status.get("configs", {}).items():
        iters = [{"index": idx,
                  "dir": os.path.join(run_dir, name, f"iteration-{idx}"),
                  "status": istat.get("status")}
                 for idx, istat in enumerate(cstat.get("iterations", []))]
        out["configs"][name] = {"status": cstat.get("status"), "iterations": iters}
    return out


def _success_iterations(disc_cfg):
    data = []
    for it in disc_cfg["iterations"]:
        if it["status"] != "SUCCESS":
            continue
        p = os.path.join(it["dir"], "info-measurement.json")
        if os.path.exists(p):
            with open(p) as f:
                data.append({"index": it["index"], "im": json.load(f)})
    return data


def _steady_slice(im):
    mem = im.get("memory") or {}
    rss = mem.get("used_memory_rss_series") or []
    used = mem.get("used_memory_series") or []
    win = mem.get("steady_state_window") or [0, max(0, len(rss) - 1)]
    a, b = win[0], win[1]
    return rss[a:b + 1], used[a:b + 1]


def _aggregate_buckets(im):
    pcmd = (im.get("latency") or {}).get("per_command") or {}
    return merge_bucket_lists([v["buckets"] for v in pcmd.values()])


def _reduce_config(iter_data, n_threshold=5):
    """Reduce a config's SUCCESS iterations: outlier-select on RSS-median + aggregate
    p99 (Q8a), merge kept iterations' histograms → true percentiles, and compute
    median memory headline + full distribution stats."""
    n = len(iter_data)
    rss_pools, used_pools, rss_medians, p99s = [], [], [], []
    for d in iter_data:
        rss_s, used_s = _steady_slice(d["im"])
        rss_pools.append(rss_s)
        used_pools.append(used_s)
        rss_medians.append(statistics.median(rss_s) if rss_s else 0)
        p99s.append(percentile_from_buckets(_aggregate_buckets(d["im"]), 99) or 0)

    kept_rss, fl_rss = select_iterations(rss_medians, n_threshold)
    kept_p99, fl_p99 = select_iterations(p99s, n_threshold)
    dropped = (set(range(n)) - set(kept_rss)) | (set(range(n)) - set(kept_p99))
    kept = [i for i in range(n) if i not in dropped] or list(range(n))
    flagged = ([{"iter": iter_data[i]["index"], "metric": "rss"} for i in sorted(fl_rss)] +
               [{"iter": iter_data[i]["index"], "metric": "p99"} for i in sorted(fl_p99)])

    kept_data = [iter_data[i] for i in kept]
    commands = set()
    for d in kept_data:
        commands |= set((d["im"].get("latency") or {}).get("per_command", {}).keys())
    per_command_pcts = {}
    for cmd in sorted(commands):
        bl = [d["im"]["latency"]["per_command"][cmd]["buckets"]
              for d in kept_data if cmd in (d["im"].get("latency") or {}).get("per_command", {})]
        per_command_pcts[cmd] = percentiles(merge_bucket_lists(bl))
    aggregate_pcts = percentiles(merge_bucket_lists([_aggregate_buckets(d["im"]) for d in kept_data]))

    rss_pool = [x for i in kept for x in rss_pools[i]]
    used_pool = [x for i in kept for x in used_pools[i]]
    rss_med_kept = [rss_medians[i] for i in kept]
    used_med_kept = [statistics.median(used_pools[i]) if used_pools[i] else 0 for i in kept]
    rss_headline = statistics.median(rss_med_kept) if rss_med_kept else 0
    used_headline = statistics.median(used_med_kept) if used_med_kept else 0

    cpus = [d["im"].get("server_cpu", {}).get("pct_total") for d in kept_data]
    cpus = [c for c in cpus if c is not None]
    stats_keys = ("evicted_keys", "rejected_connections", "expired_keys", "keyspace_misses")
    stats = {k: max((d["im"].get("stats", {}) or {}).get(k, 0) for d in kept_data) for k in stats_keys} \
        if kept_data else {}
    comp = (kept_data[-1]["im"].get("compression") or {}) if kept_data else {}

    return {
        "kept": [iter_data[i]["index"] for i in kept],
        "flagged_outliers": flagged,
        "rss_headline": rss_headline,
        "used_headline": used_headline,
        "frag_ratio": (rss_headline / used_headline) if used_headline else None,
        "rss_stats": memory_stats(rss_pool),
        "used_stats": memory_stats(used_pool),
        "per_iteration_rss_median": rss_med_kept,
        "aggregate_pcts": aggregate_pcts,
        "per_command_pcts": per_command_pcts,
        "cpu_total": statistics.fmean(cpus) if cpus else None,
        "stats": stats,
        "compression": {
            "ratio": comp.get("compression_ratio"),
            "net_saved_bytes": comp.get("compression_net_saved_bytes"),
            "compressed_objects": comp.get("compression_compressed_objects"),
        },
    }


def _pct_block(cfg_pcts, base_pcts):
    """Per-percentile {usec, delta_usec, delta_pct} vs baseline."""
    out = {}
    for label, value in cfg_pcts.items():
        b = base_pcts.get(label) if base_pcts else None
        if value is None:
            out[label] = {"usec": None, "delta_usec": None, "delta_pct": None}
        elif b is None:
            out[label] = {"usec": value, "delta_usec": None, "delta_pct": None}
        else:
            d = delta(b, value)
            out[label] = {"usec": value, "delta_usec": d["abs"], "delta_pct": d["pct"]}
    return out


def build_report(run_dir):
    """Reduce an orchestrator run directory into the ``report.json`` structure
    (design §7.3): per-config merged percentiles, median memory + full stats, and all
    metrics as deltas vs the baseline config."""
    disc = discover(run_dir)
    baseline = disc["baseline"]
    runcfg = _read_json(run_dir, "run-config.json")
    try:
        prov = _read_json(run_dir, "provenance.json")
    except FileNotFoundError:
        prov = {}

    reduced = {name: _reduce_config(_success_iterations(c))
               for name, c in disc["configs"].items()
               if _success_iterations(c)}
    base = reduced.get(baseline)

    configs = {}
    for name, r in reduced.items():
        base_rss = base["rss_headline"] if base else None
        base_used = base["used_headline"] if base else None
        mem_saved = (100.0 * (base_rss - r["rss_headline"]) / base_rss
                     if base_rss else None)
        used_saved = (100.0 * (base_used - r["used_headline"]) / base_used
                      if base_used else None)
        cpu_delta = (delta(base["cpu_total"], r["cpu_total"])["pct"]
                     if base and base.get("cpu_total") and r.get("cpu_total") else None)
        configs[name] = {
            "status": disc["configs"][name]["status"],
            "iterations": {"kept": r["kept"], "flagged_outliers": r["flagged_outliers"]},
            "memory": {
                "rss_bytes": r["rss_headline"],
                "used_memory_bytes": r["used_headline"],
                "frag_ratio": r["frag_ratio"],
                "memory_saved_pct": mem_saved,
                "used_memory_saved_pct": used_saved,
                "rss_stats": r["rss_stats"],
                "used_stats": r["used_stats"],
                "per_iteration_rss_median": r["per_iteration_rss_median"],
            },
            "latency": {
                "aggregate": _pct_block(r["aggregate_pcts"], base["aggregate_pcts"] if base else None),
                "per_command": {
                    cmd: _pct_block(pcts, (base["per_command_pcts"].get(cmd) if base else None))
                    for cmd, pcts in r["per_command_pcts"].items()
                },
            },
            "cpu": {"pct_total": r["cpu_total"], "delta_pct": cpu_delta},
            "stats": r["stats"],
            "compression": r["compression"],
        }

    wl = runcfg.get("workload", {})
    dm = runcfg.get("data_model", {})
    return {
        "workload": {
            "description": runcfg.get("description"),
            "target_tps": wl.get("target_tps"),
            "commands": wl.get("commands"),
            "value_size_distribution": dm.get("value_size_distribution"),
            "key_distribution": dm.get("key_distribution"),
            "key_count": dm.get("key_count"),
            "seed": dm.get("seed"),
            "corpus_sha256": (prov.get("corpus") or {}).get("sha256"),
        },
        "baseline": baseline,
        "overall": disc["overall"],
        "configs": configs,
    }
