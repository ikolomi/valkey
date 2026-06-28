"""Tier-1 (no binaries): --dry-run validates the config and emits the load plan
(per-command process split + rendered server args) without starting anything (F2)."""
import json
from pathlib import Path

import pytest

import orchestrator
from lib import config

CANONICAL = Path(__file__).resolve().parents[2] / "configs" / "examples" / "canonical.json"


def _run_obj():
    return config.parse(json.loads(CANONICAL.read_text()))


def test_build_plan_structure_no_binaries():
    plan = orchestrator.build_plan(_run_obj(), None, None)

    assert plan["server_binary"] == "<unresolved>"
    assert plan["benchmark_binary"] == "<unresolved>"
    assert plan["iterations"] == 3
    assert plan["reference_config"] == "off"
    assert plan["target_tps"] == 250000
    assert plan["data_model"]["key_count"] == 2000000
    assert plan["data_model"]["seed"] == 1234
    # enriched data model + workload knobs (idea-honing UX feedback)
    dm = plan["data_model"]
    assert dm["value_shape"] == "json"
    assert dm["value_size_distribution"] == "lognormal:512.0:0.8"
    assert dm["value_size_min"] == 256 and dm["value_size_max"] == 16384
    assert dm["key_distribution"] == "zipf:0.99"
    assert dm["corpus_entries"] == 50000
    assert plan["setup_timeout_seconds"] == 180.0       # default
    assert plan["max_clients_per_process"] == 64 and plan["pipeline"] == 1
    assert plan["measurement_duration_seconds"] == 60

    split = {s["command"]: s for s in plan["workload_split"]}
    assert set(split) == {"get", "set"}
    for cmd, ratio in (("get", 0.8), ("set", 0.2)):
        s = split[cmd]
        assert s["n_procs"] >= 1
        # per-process rps sums to the command's tps share; connections to round(ratio*total)
        assert abs(sum(p["rps"] for p in s["processes"]) - ratio * 250000) < 1e-6
        assert sum(p["connections"] for p in s["processes"]) == max(1, round(ratio * 256))
    assert plan["loader_processes_total"] == sum(s["n_procs"] for s in plan["workload_split"])

    cfgs = {c["name"]: c for c in plan["configs"]}
    assert set(cfgs) == {"off", "compression-on"}
    assert cfgs["off"]["server_args"] == ["--compression-master-switch", "off"]
    on = cfgs["compression-on"]["server_args"]
    assert "--compression-master-switch" in on and "compression" in on
    assert "--compression-min-idle-seconds" in on


def test_dry_run_cli_exits_zero_and_prints_plan(capsys):
    rc = orchestrator.main([str(CANONICAL), "--dry-run"])
    assert rc == 0
    out = capsys.readouterr().out
    assert "compression-on" in out and "off" in out
    assert "get" in out and "set" in out
    assert "shape=json" in out and "zipf:0.99" in out and "lognormal:512.0:0.8" in out
    assert "setup_timeout" in out


def test_dry_run_invalid_config_raises(tmp_path):
    bad = tmp_path / "bad.json"
    bad.write_text(json.dumps({"description": "missing everything"}))
    with pytest.raises(config.ConfigError):
        orchestrator.main([str(bad), "--dry-run"])


def test_iteration_order_interleaves_configs():
    class _E:
        def __init__(self, n):
            self.name = n

    a, b = _E("off"), _E("comp")
    order = [(it, e.name) for it, e in orchestrator._iteration_order([a, b], 3)]
    # iteration-major: configs alternate so both sample similar conditions over time
    assert order == [(0, "off"), (0, "comp"), (1, "off"), (1, "comp"), (2, "off"), (2, "comp")]
