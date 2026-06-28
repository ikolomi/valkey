"""Tier-2 cross-validation — our histogram-derived percentiles vs valkey-benchmark's
OWN hdr percentile computation (the independent oracle), on the *same* run.

Guards the dump→parse→percentile pipeline: a regression in `--latency-dump`,
`lib.latency`, or `reduce.percentile_from_buckets` would make these diverge. This is
the answer to "are the high-percentile latency numbers bogus?" — they must match
valkey-benchmark's own hdr percentiles to within bucket resolution.
"""

import re
import subprocess
import sys

import pytest

from lib import env, latency, server

sys.path.insert(0, "postprocessor")
import reduce  # noqa: E402

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _parse_summary_msec(text):
    """Parse valkey-benchmark's 'latency summary (msec)' → {p50,p95,p99} in µs.
    The header line is followed by 'avg min p50 p95 p99 max'."""
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        if "latency summary (msec)" in ln:
            nums = re.findall(r"[0-9]+\.[0-9]+", lines[i + 2])
            avg, mn, p50, p95, p99, mx = (float(x) for x in nums[:6])
            return {"p50": p50 * 1000, "p95": p95 * 1000, "p99": p99 * 1000}
    raise AssertionError("no latency summary in valkey-benchmark output")


def test_our_percentiles_match_valkey_benchmark(tmp_path):
    sb, bb = env.server_binary_path(), env.benchmark_binary_path()
    srv = server.Server(sb, str(tmp_path / "srv"), "val", server.free_port())
    srv.start()
    try:
        dump = str(tmp_path / "val.hist")
        # NO -q → valkey-benchmark prints its own percentiles; --latency-dump → our histogram.
        r = subprocess.run(
            [bb, "-h", "127.0.0.1", "-p", str(srv.port), "-t", "set",
             "-n", "200000", "-c", "8", "-r", "100000", "--latency-dump", dump],
            capture_output=True, text=True, timeout=300)
        assert r.returncode == 0, r.stderr
        vb = _parse_summary_msec(r.stdout)
    finally:
        srv.teardown()

    h = latency.parse_dump_file(dump)
    buckets = [[v, c] for v, c in sorted(h["buckets"].items())]
    assert h["total_count"] == 200000

    for label in ("p50", "p95", "p99"):
        ours = reduce.percentile_from_buckets(buckets, float(label[1:]))
        # within ~one hdr bucket: generous bound that still catches a broken calc (≥2× off)
        tol = max(30.0, 0.20 * vb[label])
        assert abs(ours - vb[label]) <= tol, (
            f"{label}: ours={ours}us vs valkey-benchmark={vb[label]}us (tol {tol:.0f})")
