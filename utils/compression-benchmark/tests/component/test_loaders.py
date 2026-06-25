"""C3b Tier-2 test for lib/benchmark loader orchestration (FIFO barrier + spawn +
artifact collection, R5.3/R6.4). Needs valkey-server + valkey-benchmark.
"""

import os
import socket
import subprocess

import pytest

from lib import benchmark, config, env, server

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_run_loaders_barrier_spawn_and_collect(tmp_path):
    binpath = env.server_binary_path()
    bench = env.benchmark_binary_path()
    n = 500
    with server.Server(binpath, str(tmp_path / "srv"), "off", _free_port(),
                       args=["--compression-master-switch", "off"]) as srv:
        # populate exactly N keys (existing-flags exact coverage)
        subprocess.run(benchmark.populate_argv(bench, "127.0.0.1", srv.port, n, 64),
                       capture_output=True, timeout=60, check=True)
        assert srv.dbsize() == n

        # open-loop load: 80/20 get/set, tiny duration, behind the FIFO barrier
        split = benchmark.split_processes(
            [config.Command("get", 0.8), config.Command("set", 0.2)],
            connections_total=8, max_clients_per_process=4, target_tps=2000,
        )
        loaders = benchmark.build_load_argvs(bench, "127.0.0.1", srv.port, split,
                                             duration=2, key_count=n, datasize=64)
        results = benchmark.run_loaders(loaders, str(tmp_path / "load"))

        # all loaders ran (barrier released), exited cleanly, produced artifacts + rps
        assert len(results) == len(loaders)
        assert all(r["returncode"] == 0 for r in results), results
        for r in results:
            assert r["achieved_rps"] and r["achieved_rps"] > 0
            assert os.path.getsize(r["stdout_path"]) > 0
            assert os.path.exists(r["stderr_path"])


def test_run_loaders_dead_port_reports_failure(tmp_path):
    """Loaders pointed at a closed port fail (rc!=0 / no rps) — the signal the off
    phase maps to FAILED `benchmark_error` (decision logic unit-tested separately)."""
    bench = env.benchmark_binary_path()
    dead_port = _free_port()  # nothing is listening here
    split = benchmark.split_processes([config.Command("get", 1.0)], 2, 4, 100)
    loaders = benchmark.build_load_argvs(bench, "127.0.0.1", dead_port, split,
                                         duration=2, key_count=100)
    results = benchmark.run_loaders(loaders, str(tmp_path / "load"))
    assert results
    assert all(r["returncode"] != 0 or not r["achieved_rps"] for r in results), results
