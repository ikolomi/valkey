"""Tier-3 e2e — CONTRACT-DRIVEN soundness (Plan 2, T2.8 / C2).

This is the remediation for the original e2e gap: the old tests only checked that
*present* fields were self-consistent and never validated that the **cornerstone
latency histogram** (or RSS / stats / CPU) was captured at all. These tests assert
the produced ``info-measurement.json`` contains **every field the orchestrator→
post-processor contract requires** (design §3) **with sound values** — i.e.
completeness, not just internal consistency.
"""

import json
import os

import pytest

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _load(run_dir, *parts):
    with open(os.path.join(run_dir, *parts)) as f:
        return json.load(f)


def _iteration_dirs(run_dir, cfg_name):
    base = os.path.join(run_dir, cfg_name)
    its = sorted(d for d in os.listdir(base) if d.startswith("iteration-"))
    assert its, f"no iteration dirs under {base}"
    return its


def _percentile(buckets, pct):
    """Reconstruct a percentile value (usec) from a sorted [[value, count], ...] list."""
    total = sum(c for _, c in buckets)
    target = (pct / 100.0) * total
    cum = 0
    for value, count in buckets:
        cum += count
        if cum >= target:
            return value
    return buckets[-1][0]


def _median(xs):
    s = sorted(xs)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2.0


# --------------------------------------------------------------------------- #
# Latency histogram — the cornerstone the old e2e missed entirely
# --------------------------------------------------------------------------- #

def test_latency_histogram_present_and_sound_off(off_run):
    for it in _iteration_dirs(off_run["run_dir"], "off"):
        im = _load(off_run["run_dir"], "off", it, "info-measurement.json")
        lat = im.get("latency")
        assert lat is not None, "latency block MISSING (the original gap)"
        assert lat["hdr"]["lowest"] == 10 and lat["hdr"]["highest"] == 3000000
        pc = lat["per_command"]
        assert "get" in pc and "set" in pc, f"per-command latency missing: {list(pc)}"
        for cmd in ("get", "set"):
            b = pc[cmd]
            assert b["total_count"] > 0
            assert len(b["buckets"]) >= 1
            # our cross-process merge is exact: bucket counts sum to total_count
            assert sum(c for _, c in b["buckets"]) == b["total_count"]
            # buckets sorted by value (deterministic round-trip)
            vals = [v for v, _ in b["buckets"]]
            assert vals == sorted(vals)
            # reconstructed p50 is a plausible localhost latency within hdr range
            p50 = _percentile(b["buckets"], 50)
            assert 10 <= p50 <= 3000000


def test_latency_histogram_present_compression(comp_run):
    for it in _iteration_dirs(comp_run["run_dir"], "compression-on"):
        im = _load(comp_run["run_dir"], "compression-on", it, "info-measurement.json")
        lat = im.get("latency")
        assert lat is not None
        for cmd in ("get", "set"):
            assert lat["per_command"][cmd]["total_count"] > 0


# --------------------------------------------------------------------------- #
# Memory: RSS series is the headline metric (Q6) — must be present + sound
# --------------------------------------------------------------------------- #

def test_memory_block_present_and_sound(off_run):
    for it in _iteration_dirs(off_run["run_dir"], "off"):
        im = _load(off_run["run_dir"], "off", it, "info-measurement.json")
        mem = im.get("memory")
        assert mem is not None, "memory block MISSING"
        u = mem["used_memory_series"]
        r = mem["used_memory_rss_series"]
        frag = mem["mem_fragmentation_ratio_series"]
        assert u and r and frag, "memory series must be non-empty"
        assert len(r) == len(u) == len(frag), "memory series lengths must match"
        assert mem["steady_state_window"] == [0, len(u) - 1]
        assert all(x > 0 for x in u) and all(x > 0 for x in r)
        # RSS is the physical superset of logical used_memory (robust: compare medians)
        assert _median(r) >= _median(u)


def test_memory_block_present_compression(comp_run):
    for it in _iteration_dirs(comp_run["run_dir"], "compression-on"):
        im = _load(comp_run["run_dir"], "compression-on", it, "info-measurement.json")
        mem = im.get("memory")
        assert mem is not None
        assert mem["used_memory_rss_series"] and all(x > 0 for x in mem["used_memory_rss_series"])


# --------------------------------------------------------------------------- #
# Stability stats (reported, not gated) + server-process CPU
# --------------------------------------------------------------------------- #

def test_stats_and_cpu_blocks_present(off_run):
    for it in _iteration_dirs(off_run["run_dir"], "off"):
        im = _load(off_run["run_dir"], "off", it, "info-measurement.json")
        st = im.get("stats")
        assert st is not None
        for k in ("evicted_keys", "rejected_connections", "expired_keys", "keyspace_misses"):
            assert k in st and isinstance(st[k], int) and st[k] >= 0
        cpu = im.get("server_cpu")
        assert cpu is not None, "server_cpu block MISSING"
        assert cpu["pct_user"] >= 0 and cpu["pct_system"] >= 0 and cpu["pct_total"] >= 0
        # a server under load for ~2s should burn some CPU
        assert cpu["pct_total"] > 0
