"""A1 Tier-1 tests for lib/config.py — parse, validate, render (design §7.1)."""

import copy

import pytest

from lib import config


def valid_run():
    """A minimal valid run-JSON dict (the Q7 canonical 2-config example)."""
    return {
        "description": "json-mixed, 250K TPS, 80/20, compression on vs off",
        "output_directory": "results/",
        "servers_directory": "/tmp/valkey-bench-servers",
        "benchmark_binary": "/path/to/valkey-benchmark",
        "server_binary": "/path/to/valkey-server",
        "iterations": 3,
        "reference_config": "off",
        "data_model": {
            "value_shape": "json",
            "value_size_distribution": "lognormal:512:0.8",
            "value_size_min": 256,
            "value_size_max": 16384,
            "seed": 1234,
            "corpus_entries": 50000,
            "key_count": 2000000,
            "key_distribution": "zipf:0.99",
        },
        "workload": {
            "target_tps": 250000,
            "commands": [
                {"type": "get", "ratio": 0.8},
                {"type": "set", "ratio": 0.2},
            ],
            "connections_total": 256,
            "max_clients_per_process": 64,
            "pipeline": 1,
            "measurement_duration_seconds": 60,
        },
        "profile_prep": {
            "plateau_metric": "compression_compressed_objects",
            "plateau_tolerance_pct": 2,
            "plateau_window_polls": 3,
            "poll_interval_seconds": 10,
            "max_timeout_seconds": 900,
        },
        "configs": [
            {"name": "off", "compression": {"master_switch": "off"}},
            {
                "name": "compression-on",
                "compression": {
                    "master_switch": "compression",
                    "automatic_sweeper": "enabled",
                    "min_value_size": 256,
                    "max_value_size": 16384,
                    "min_idle_seconds": 3,
                },
            },
        ],
    }


# ---- happy path ----

def test_parse_valid():
    run = config.parse(valid_run())
    assert run.reference_config == "off"
    assert run.iterations == 3
    assert run.server_binary == "/path/to/valkey-server"
    assert [c.name for c in run.configs] == ["off", "compression-on"]
    assert run.data_model.value_shape == "json"
    assert run.data_model.value_size_distribution == ("lognormal", 512.0, 0.8)
    assert run.data_model.key_distribution == ("zipf", 0.99)
    assert run.workload.target_tps == 250000
    assert len(run.workload.commands) == 2
    assert run.profile_prep.plateau_window_polls == 3


# ---- required fields ----

@pytest.mark.parametrize(
    "field",
    [
        "output_directory",
        "servers_directory",
        "benchmark_binary",
        "server_binary",
        "iterations",
        "reference_config",
        "data_model",
        "workload",
        "profile_prep",
        "configs",
    ],
)
def test_missing_required_field(field):
    d = valid_run()
    del d[field]
    with pytest.raises(config.ConfigError) as e:
        config.parse(d)
    assert field in str(e.value)


# ---- value validation ----

def test_iterations_must_be_positive():
    d = valid_run()
    d["iterations"] = 0
    with pytest.raises(config.ConfigError):
        config.parse(d)


def test_target_tps_must_be_positive():
    d = valid_run()
    d["workload"]["target_tps"] = 0
    with pytest.raises(config.ConfigError):
        config.parse(d)


def test_ratios_must_normalize_to_one():
    d = valid_run()
    d["workload"]["commands"] = [
        {"type": "get", "ratio": 0.7},
        {"type": "set", "ratio": 0.2},  # sums to 0.9
    ]
    with pytest.raises(config.ConfigError):
        config.parse(d)


def test_unknown_command_type_rejected():
    d = valid_run()
    d["workload"]["commands"] = [{"type": "frobnicate", "ratio": 1.0}]
    with pytest.raises(config.ConfigError):
        config.parse(d)


def test_multikey_command_requires_keys():
    d = valid_run()
    d["workload"]["commands"] = [{"type": "mget", "ratio": 1.0}]  # missing "keys"
    with pytest.raises(config.ConfigError):
        config.parse(d)
    d["workload"]["commands"] = [{"type": "mget", "ratio": 1.0, "keys": 10}]
    config.parse(d)  # ok


@pytest.mark.parametrize("bad", ["lognormal:foo", "weird", "uniform:10", "constant:"])
def test_value_size_distribution_malformed(bad):
    d = valid_run()
    d["data_model"]["value_size_distribution"] = bad
    with pytest.raises(config.ConfigError):
        config.parse(d)


def test_value_size_distribution_forms_parse():
    for s, expected in [
        ("constant:512", ("constant", 512)),
        ("uniform:256:8192", ("uniform", 256, 8192)),
        ("lognormal:512:1.6", ("lognormal", 512.0, 1.6)),
    ]:
        d = valid_run()
        d["data_model"]["value_size_distribution"] = s
        assert config.parse(d).data_model.value_size_distribution == expected


def test_value_size_min_max():
    d = valid_run()
    d["data_model"]["value_size_min"] = 1000
    d["data_model"]["value_size_max"] = 500
    with pytest.raises(config.ConfigError):
        config.parse(d)


@pytest.mark.parametrize("bad", ["zipf", "zipf:0", "bogus"])
def test_key_distribution_malformed(bad):
    d = valid_run()
    d["data_model"]["key_distribution"] = bad
    with pytest.raises(config.ConfigError):
        config.parse(d)


def test_key_distribution_forms_parse():
    d = valid_run()
    d["data_model"]["key_distribution"] = "uniform"
    assert config.parse(d).data_model.key_distribution == ("uniform",)
    d["data_model"]["key_distribution"] = "zipf:0.99"
    assert config.parse(d).data_model.key_distribution == ("zipf", 0.99)


def test_reference_config_must_exist():
    d = valid_run()
    d["reference_config"] = "nope"
    with pytest.raises(config.ConfigError):
        config.parse(d)


def test_max_clients_per_process_min():
    d = valid_run()
    d["workload"]["max_clients_per_process"] = 0
    with pytest.raises(config.ConfigError):
        config.parse(d)


def test_duplicate_config_names_rejected():
    d = valid_run()
    d["configs"].append({"name": "off", "compression": {"master_switch": "off"}})
    with pytest.raises(config.ConfigError):
        config.parse(d)


# ---- rendering ----

def test_sparse_compression_renders_only_specified():
    run = config.parse(valid_run())
    off = next(c for c in run.configs if c.name == "off")
    assert config.render_server_args(off) == ["--compression-master-switch", "off"]


def test_structured_render_all_knobs():
    run = config.parse(valid_run())
    on = next(c for c in run.configs if c.name == "compression-on")
    args = config.render_server_args(on)
    # every specified knob is rendered as --compression-<kebab> <value>
    assert "--compression-master-switch" in args
    assert args[args.index("--compression-master-switch") + 1] == "compression"
    assert "--compression-automatic-sweeper" in args
    assert "--compression-min-value-size" in args
    assert args[args.index("--compression-min-value-size") + 1] == "256"
    assert "--compression-min-idle-seconds" in args
    assert args[args.index("--compression-min-idle-seconds") + 1] == "3"


def test_extra_args_appended_after_structured_so_raw_overrides():
    d = valid_run()
    d["configs"][1]["compression"]["threads"] = 1
    d["configs"][1]["extra_args"] = ["--compression-threads", "4"]
    run = config.parse(d)
    on = next(c for c in run.configs if c.name == "compression-on")
    args = config.render_server_args(on)
    # structured threads=1 rendered, then extra_args threads=4 after it (last wins)
    first = args.index("--compression-threads")
    last = len(args) - 1 - args[::-1].index("--compression-threads")
    assert first < last
    assert args[last + 1] == "4"


def test_per_config_server_binary_override():
    d = valid_run()
    d["configs"][1]["server_binary"] = "/feature/valkey-server"
    run = config.parse(d)
    off = next(c for c in run.configs if c.name == "off")
    on = next(c for c in run.configs if c.name == "compression-on")
    assert config.resolve_server_binary(off, run) == "/path/to/valkey-server"
    assert config.resolve_server_binary(on, run) == "/feature/valkey-server"
