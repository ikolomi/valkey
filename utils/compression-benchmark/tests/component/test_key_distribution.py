"""B3 Tier-2 test for R8.x (``--key-distribution uniform|zipf [--zipf-theta]``).

A Zipfian key distribution gives the workload a hotset (a few keys take most of
the traffic) so the compression skip-hot-keys policy can be exercised. We verify
the skew directly: run INCR over a small keyspace so each key's counter *is* its
access count, then assert the distribution is strongly skewed (vs the flat
counts a uniform distribution would produce). Needs valkey-server +
valkey-benchmark (with the B3 change).
"""

import socket
import subprocess

import pytest

from lib import env, server

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _run_incr(srv, bench, n, m, extra):
    r = subprocess.run(
        [bench, "-h", "127.0.0.1", "-p", str(srv.port), "-t", "incr",
         "-r", str(n), "-n", str(m), "-q"] + extra,
        capture_output=True, text=True, timeout=120,
    )
    assert r.returncode == 0, r.stderr
    counts = [int(srv.cli("get", "counter:%012d" % i) or 0) for i in range(n)]
    assert sum(counts) == m, "every INCR must land on some counter (sum != n requests)"
    return counts


def test_key_distribution_zipf_is_skewed(tmp_path):
    bench = env.benchmark_binary_path()
    binpath = env.server_binary_path()
    n, m = 100, 20000

    with server.Server(binpath, str(tmp_path), "kd", _free_port()) as srv:
        # Uniform (default): counts should be roughly flat.
        srv.flushall()
        uni = _run_incr(srv, bench, n, m, [])
        uni_sorted = sorted(uni, reverse=True)

        # Zipf: a few keys dominate.
        srv.flushall()
        zipf = _run_incr(srv, bench, n, m, ["--key-distribution", "zipf", "--zipf-theta", "0.99"])
        zipf_sorted = sorted(zipf, reverse=True)

        uniform_expect = m / n  # 200

        # The hottest zipf key is far above the uniform expectation ...
        assert zipf_sorted[0] > 5 * uniform_expect, (zipf_sorted[0], uniform_expect)
        # ... and far above the uniform run's hottest key (which hovers near expect).
        assert zipf_sorted[0] > 3 * uni_sorted[0], (zipf_sorted[0], uni_sorted[0])
        # Strong head-vs-median skew under zipf, but not under uniform.
        zipf_median = zipf_sorted[len(zipf_sorted) // 2]
        assert zipf_sorted[0] > 10 * max(1, zipf_median)
        assert uni_sorted[0] < 3 * uniform_expect, uni_sorted[0]


def test_key_distribution_uniform_is_default_and_explicit(tmp_path):
    bench = env.benchmark_binary_path()
    binpath = env.server_binary_path()
    n, m = 100, 20000
    with server.Server(binpath, str(tmp_path), "kd2", _free_port()) as srv:
        srv.flushall()
        explicit = _run_incr(srv, bench, n, m, ["--key-distribution", "uniform"])
        # Explicit uniform stays roughly flat (no single key dominates).
        assert sorted(explicit, reverse=True)[0] < 3 * (m / n)
