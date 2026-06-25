"""B2 Tier-2 test for R8.2 (exact, full, deterministic keyspace coverage).

The in-tree `valkey-benchmark --sequential` modifies `-r` to replace `__rand_int__`
with a shared atomic counter (% keyspacelen) instead of random. With
`--sequential -r N -n N`, N atomic increments mod N yield keys 0..N-1 each exactly
once — which is exactly R8.2. This test verifies that guarantee against the built
binary (so no C change is needed if it holds). Needs valkey-server + valkey-benchmark.
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


def test_sequential_gives_exact_full_coverage(tmp_path):
    binpath = env.server_binary_path()
    bench = env.benchmark_binary_path()
    n = 1000
    with server.Server(binpath, str(tmp_path), "seq", _free_port()) as srv:
        srv.flushall()
        r = subprocess.run(
            [bench, "-h", "127.0.0.1", "-p", str(srv.port), "-t", "set",
             "-r", str(n), "-n", str(n), "--sequential", "-q"],
            capture_output=True, text=True, timeout=120,
        )
        assert r.returncode == 0, r.stderr

        # exactly N distinct keys ⇒ each of 0..N-1 produced exactly once (no dup, no gap)
        assert srv.dbsize() == n

        # boundary keys exist in the expected key:<12-digit> form; key N (overshoot) does not
        for i in (0, n // 2, n - 1):
            assert srv.cli("exists", "key:%012d" % i) == "1", i
        assert srv.cli("exists", "key:%012d" % n) == "0"
