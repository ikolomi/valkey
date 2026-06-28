"""A3 Tier-1 tests — connection/TPS split math (R5), plateau detector (R4.6),
run-status decision (R7.3)."""

import pytest

from lib import benchmark, info, runstatus
from lib.config import Command


# --------------------------------------------------------------------------- #
# split_processes (R5 / Q8e)
# --------------------------------------------------------------------------- #

def test_split_canonical():
    cmds = [Command("get", 0.8), Command("set", 0.2)]
    res = benchmark.split_processes(cmds, connections_total=256,
                                    max_clients_per_process=64, target_tps=250000)
    get = next(r for r in res if r["command"] == "get")
    sett = next(r for r in res if r["command"] == "set")

    assert get["n_procs"] == 4                       # ceil(round(0.8*256)=205 / 64)
    assert sum(p["connections"] for p in get["processes"]) == 205
    assert all(p["rps"] == pytest.approx(50000) for p in get["processes"])
    assert sum(p["rps"] for p in get["processes"]) == pytest.approx(0.8 * 250000)

    assert sett["n_procs"] == 1                       # round(0.2*256)=51 <= 64
    assert sett["processes"][0]["connections"] == 51
    assert sett["processes"][0]["rps"] == pytest.approx(50000)


def test_split_even_distribution():
    cmds = [Command("get", 1.0)]
    res = benchmark.split_processes(cmds, 205, 64, 100000)
    procs = res[0]["processes"]
    conns = [p["connections"] for p in procs]
    assert sum(conns) == 205
    assert max(conns) - min(conns) <= 1          # as even as possible
    assert min(conns) >= 1                        # never a zero-connection process


def test_split_min_one_connection():
    cmds = [Command("get", 0.999), Command("set", 0.001)]
    res = benchmark.split_processes(cmds, 100, 64, 100000)
    sett = next(r for r in res if r["command"] == "set")
    assert sett["n_procs"] == 1
    assert sum(p["connections"] for p in sett["processes"]) == 1   # 0 → bumped to 1


def test_split_single_command_total_connections():
    res = benchmark.split_processes([Command("get", 1.0)], 200, 64, 100000)
    assert sum(p["connections"] for p in res[0]["processes"]) == 200


def test_populate_argv_exact_coverage():
    argv = benchmark.populate_argv("vb", "127.0.0.1", 7000, key_count=2000, datasize=512)
    assert argv[:2] == ["vb", "-h"]
    assert "--sequential" in argv
    assert argv[argv.index("-t") + 1] == "set"
    assert argv[argv.index("-r") + 1] == "2000"
    assert argv[argv.index("-n") + 1] == "2000"
    assert argv[argv.index("-d") + 1] == "512"


def test_loader_argv_open_loop():
    argv = benchmark.loader_argv("vb", "h", 7000, "get", connections=52, rps=50000,
                                 duration=60, key_count=2000, datasize=512, pipeline=1)
    assert argv[argv.index("-t") + 1] == "get"
    assert argv[argv.index("-c") + 1] == "52"
    assert argv[argv.index("--rps") + 1] == "50000"
    assert argv[argv.index("--duration") + 1] == "60"
    assert "-n" not in argv  # open-loop: duration-bounded, not request-count-bounded


def test_loader_argv_rps_floor_one():
    argv = benchmark.loader_argv("vb", "h", 7000, "get", 1, rps=0.4, duration=10, key_count=10)
    assert argv[argv.index("--rps") + 1] == "1"   # never 0 (which would mean unlimited)


def test_build_load_argvs_one_per_process():
    split = benchmark.split_processes([Command("get", 0.8), Command("set", 0.2)],
                                      256, 64, 250000)
    loaders = benchmark.build_load_argvs("vb", "127.0.0.1", 7000, split,
                                         duration=60, key_count=2000)
    assert len(loaders) == sum(e["n_procs"] for e in split)   # 4 + 1 = 5
    gets = [l for l in loaders if l["command"] == "get"]
    assert len(gets) == 4
    assert all(l["argv"][l["argv"].index("--rps") + 1] == "50000" for l in gets)


# --------------------------------------------------------------------------- #
# detect_plateau (R4.6)
# --------------------------------------------------------------------------- #

def test_plateau_fires_on_stabilizing_series():
    series = [100, 200, 300, 400, 500, 1000, 1005, 1003, 1004]
    assert info.detect_plateau(series, tolerance_pct=2, window_polls=3) is True


def test_plateau_not_on_climbing_series():
    assert info.detect_plateau([100, 200, 300, 400, 500], 2, 3) is False


def test_plateau_needs_enough_samples():
    assert info.detect_plateau([1000], 2, 3) is False


def test_plateau_never_on_monotone_climb_models_timeout():
    # A series that never stabilizes → detector stays False; the driver enforces
    # the actual max-timeout (→ FAILED profile_not_stabilized).
    assert info.detect_plateau(list(range(1, 60)), 2, 5) is False


def test_plateau_flat_series():
    assert info.detect_plateau([500, 500, 500], 2, 3) is True


def test_plateau_tolerance_boundary():
    assert info.detect_plateau([100, 101, 102], 5, 3) is True    # ~1.96% <= 5
    assert info.detect_plateau([100, 110, 120], 5, 3) is False   # ~16.7% > 5


def test_poll_until_plateau_fires():
    vals = iter([100, 200, 300, 1000, 1005, 1003, 1004] + [1004] * 5)
    res = info.poll_until_plateau(
        lambda: next(vals), tolerance_pct=2, window_polls=3, poll_interval=0,
        max_timeout=100, clock=lambda: 0.0, sleep=lambda s: None,
    )
    assert res["plateaued"] is True


def test_poll_until_plateau_times_out():
    t = {"now": 0.0}
    n = iter(range(10_000))
    res = info.poll_until_plateau(
        lambda: next(n) * 100, tolerance_pct=2, window_polls=3, poll_interval=1.0,
        max_timeout=5, clock=lambda: t["now"], sleep=lambda s: t.__setitem__("now", t["now"] + s),
    )
    assert res["plateaued"] is False
    assert res["elapsed"] >= 5


# --------------------------------------------------------------------------- #
# runstatus.decide (R7.3)
# --------------------------------------------------------------------------- #

def ok_iter(**over):
    it = {"achieved_tps": 249000, "target_tps": 250000, "plateaued": True,
          "crashed": False, "benchmark_error": False}
    it.update(over)
    return it


def test_all_success():
    res = runstatus.decide({"off": [ok_iter()], "compression-on": [ok_iter()]})
    assert res["overall"] == "SUCCESS"
    assert res["configs"]["off"]["status"] == "SUCCESS"


def test_unmet_target_tps():
    res = runstatus.decide({"c": [ok_iter(achieved_tps=188000)]}, tps_tolerance=0.05)
    it = res["configs"]["c"]["iterations"][0]
    assert it["status"] == "FAILED"
    assert it["reason"] == "target_tps_not_achieved"
    assert it["achieved_tps"] == 188000 and it["target_tps"] == 250000
    assert res["overall"] == "FAILED"


def test_no_plateau():
    res = runstatus.decide({"c": [ok_iter(plateaued=False)]})
    assert res["configs"]["c"]["iterations"][0]["reason"] == "profile_not_stabilized"


def test_server_crash():
    res = runstatus.decide({"c": [ok_iter(crashed=True)]})
    assert res["configs"]["c"]["iterations"][0]["reason"] == "server_error"


def test_benchmark_error():
    res = runstatus.decide({"c": [ok_iter(benchmark_error=True)]})
    assert res["configs"]["c"]["iterations"][0]["reason"] == "benchmark_error"


def test_failure_reason_precedence_crash_wins():
    res = runstatus.decide({"c": [ok_iter(crashed=True, achieved_tps=1, benchmark_error=True)]})
    assert res["configs"]["c"]["iterations"][0]["reason"] == "server_error"


def test_off_config_plateau_not_applicable():
    # off config skips compress phases → plateaued=None must not fail it.
    res = runstatus.decide({"off": [ok_iter(plateaued=None)]})
    assert res["configs"]["off"]["status"] == "SUCCESS"


def test_config_fails_if_any_iteration_fails():
    res = runstatus.decide({"c": [ok_iter(), ok_iter(crashed=True)]})
    assert res["configs"]["c"]["status"] == "FAILED"
    assert res["overall"] == "FAILED"


# --- compression-mode argv knobs (Phase E prerequisites: B1/B3/B4) ----------- #

def test_populate_argv_corpus_replaces_random_data():
    from lib import benchmark
    a = benchmark.populate_argv("vb", "h", 1, 100, datasize=512, value_corpus="/tmp/c.bin")
    assert "--value-data" in a and "corpus:/tmp/c.bin" in a
    assert "-d" not in a  # corpus supplies the values
    assert "--sequential" in a
    b = benchmark.populate_argv("vb", "h", 1, 100, datasize=512)
    assert "-d" in b and "--value-data" not in b  # default unchanged


def test_loader_argv_set_corpus_zipf_record_start():
    from lib import benchmark
    s = benchmark.loader_argv("vb", "h", 1, "set", 10, 5000, 30, 100,
                              value_corpus="/c.bin", key_dist=("zipf", 0.99),
                              record_start_signal=10)
    assert "--value-data" in s and "corpus:/c.bin" in s and "-d" not in s
    assert s[s.index("--key-distribution") + 1] == "zipf"
    assert s[s.index("--zipf-theta") + 1] == "0.99"
    assert s[s.index("--record-start-signal") + 1] == "10"
    assert "--duration" in s


def test_loader_argv_get_skips_value_data_keeps_keydist():
    from lib import benchmark
    g = benchmark.loader_argv("vb", "h", 1, "get", 10, 5000, 30, 100,
                              value_corpus="/c.bin", key_dist=("zipf", 0.99),
                              record_start_signal=10)
    # GET carries no value → no --value-data even in corpus mode; key dist still applies.
    assert "--value-data" not in g and "-d" in g
    assert "--key-distribution" in g and "--record-start-signal" in g


def test_loader_argv_uniform_and_defaults_are_backward_compatible():
    from lib import benchmark
    u = benchmark.loader_argv("vb", "h", 1, "set", 10, 5000, 30, 100, key_dist=("uniform",))
    assert "--key-distribution" not in u  # uniform = default, omitted
    o = benchmark.loader_argv("vb", "h", 1, "get", 10, 5000, 30, 100)
    assert "-d" in o and "--value-data" not in o
    assert "--key-distribution" not in o and "--record-start-signal" not in o


# --------------------------------------------------------------------------- #
# --latency-dump wiring (Plan 2, P2.1) — measured loaders dump raw hdr buckets
# --------------------------------------------------------------------------- #

def test_loader_argv_latency_dump_appends_flag():
    argv = benchmark.loader_argv("vb", "h", 7000, "get", 8, rps=5000, duration=10,
                                 key_count=100, latency_dump="/tmp/x.hist")
    assert argv[argv.index("--latency-dump") + 1] == "/tmp/x.hist"
    assert "-q" in argv  # dump is independent of -q


def test_loader_argv_no_latency_dump_by_default():
    argv = benchmark.loader_argv("vb", "h", 7000, "get", 8, rps=5000, duration=10, key_count=100)
    assert "--latency-dump" not in argv


def test_build_load_argvs_load_dir_sets_per_process_hist_path():
    split = benchmark.split_processes([Command("get", 0.8), Command("set", 0.2)],
                                      8, 4, 2000)
    loaders = benchmark.build_load_argvs("vb", "127.0.0.1", 7000, split,
                                         duration=10, key_count=100, load_dir="/run/load")
    for spec in loaders:
        expected = f"/run/load/loader-{spec['command']}-{spec['index']}.hist"
        assert spec["hist_path"] == expected
        assert spec["argv"][spec["argv"].index("--latency-dump") + 1] == expected


def test_build_load_argvs_without_load_dir_has_no_dump():
    split = benchmark.split_processes([Command("get", 1.0)], 4, 4, 2000)
    loaders = benchmark.build_load_argvs("vb", "127.0.0.1", 7000, split,
                                         duration=10, key_count=100)
    for spec in loaders:
        assert spec.get("hist_path") is None
        assert "--latency-dump" not in spec["argv"]


# --------------------------------------------------------------------------- #
# poll_until_swept — compress-all completion (waits for the worker queue to drain
# + compressed_objects to hold steady; NOT a premature growth-plateau). Fixes the
# bug where a paced/back-pressured sweep stalls growth while candidates_pending>0.
# --------------------------------------------------------------------------- #

class _Clk:
    def __init__(self):
        self.t = 0.0

    def __call__(self):
        return self.t

    def sleep(self, s):
        self.t += s


def test_poll_until_swept_waits_for_drain_then_completes():
    clk = _Clk()
    # (compressed_objects, candidates_pending): growth, then a STALL with pending>0
    # (the old bug would fire here), then resume, then drain + steady.
    seq = iter([(100, 50), (215, 80), (215, 80), (215, 80), (400, 60),
                (810, 10), (810, 0), (810, 0), (810, 0), (810, 0), (810, 0)])
    res = info.poll_until_swept(lambda: next(seq), poll_interval=2, max_timeout=1000,
                                stable_polls=4, clock=clk, sleep=clk.sleep)
    assert res["completed"] is True
    assert res["series"][-1] == 810  # completed at the drained/steady value, not the 215 stall


def test_poll_until_swept_does_not_complete_while_backlogged():
    clk = _Clk()
    # compressed_objects steady at 215 BUT candidates_pending stays 80 → still working
    res = info.poll_until_swept(lambda: (215, 80), poll_interval=2, max_timeout=20,
                                stable_polls=4, clock=clk, sleep=clk.sleep)
    assert res["completed"] is False  # never drained → times out (→ setup failure)


def test_poll_until_swept_noop_when_nothing_eligible():
    clk = _Clk()
    # nothing to compress: compressed stays 0, pending 0 → completes after grace
    res = info.poll_until_swept(lambda: (0, 0), poll_interval=2, max_timeout=1000,
                                stable_polls=3, start_grace_polls=2, clock=clk, sleep=clk.sleep)
    assert res["completed"] is True
