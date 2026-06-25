"""Run-JSON parsing, validation, and server-arg rendering.

Design of record: ``design/detailed-design.md`` §2.3 (R3.1–R3.5), §5.1 (schema),
§7.1 (validation). Pure Python, stdlib only — Tier-1.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

# v1 command vocabulary (Q2/Q7). Multi-key commands need a `keys` count.
KNOWN_COMMANDS = {"get", "set", "mget", "mset"}
MULTIKEY_COMMANDS = {"mget", "mset"}
VALUE_SHAPES = {"kv", "json", "log", "coordinates"}

REQUIRED_TOP = [
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
]


class ConfigError(Exception):
    """Raised when a run-JSON document fails validation."""


# --------------------------------------------------------------------------- #
# Data model
# --------------------------------------------------------------------------- #

@dataclass
class DataModel:
    value_shape: str
    value_size_distribution: tuple
    value_size_min: int
    value_size_max: int
    seed: int
    corpus_entries: int
    key_count: int
    key_distribution: tuple


@dataclass
class Command:
    type: str
    ratio: float
    keys: int | None = None


@dataclass
class Workload:
    target_tps: int
    commands: list[Command]
    connections_total: int
    max_clients_per_process: int
    pipeline: int
    measurement_duration_seconds: int


@dataclass
class ProfilePrep:
    plateau_metric: str
    plateau_tolerance_pct: float
    plateau_window_polls: int
    poll_interval_seconds: float
    max_timeout_seconds: float


@dataclass
class ConfigEntry:
    name: str
    compression: dict
    extra_args: list[str] = field(default_factory=list)
    server_binary: str | None = None


@dataclass
class RunConfig:
    output_directory: str
    servers_directory: str
    benchmark_binary: str
    server_binary: str
    iterations: int
    reference_config: str
    data_model: DataModel
    workload: Workload
    profile_prep: ProfilePrep
    configs: list[ConfigEntry]
    description: str = ""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #

def _req(d: dict, key: str, ctx: str) -> Any:
    if not isinstance(d, dict) or key not in d:
        where = f" in {ctx}" if ctx else ""
        raise ConfigError(f"missing required field: {key}{where}")
    return d[key]


def _positive_int(v: Any, name: str) -> int:
    if not isinstance(v, int) or isinstance(v, bool) or v <= 0:
        raise ConfigError(f"{name} must be a positive integer (got {v!r})")
    return v


def _positive_num(v: Any, name: str) -> float:
    if isinstance(v, bool) or not isinstance(v, (int, float)) or v <= 0:
        raise ConfigError(f"{name} must be a positive number (got {v!r})")
    return float(v)


def _parse_size_dist(s: Any) -> tuple:
    if not isinstance(s, str):
        raise ConfigError(f"malformed value_size_distribution: {s!r}")
    parts = s.split(":")
    kind = parts[0]
    try:
        if kind == "constant" and len(parts) == 2:
            return ("constant", int(parts[1]))
        if kind == "uniform" and len(parts) == 3:
            lo, hi = int(parts[1]), int(parts[2])
            if lo > hi:
                raise ValueError
            return ("uniform", lo, hi)
        if kind == "lognormal" and len(parts) == 3:
            mu, sigma = float(parts[1]), float(parts[2])
            if mu <= 0 or sigma <= 0:
                raise ValueError
            return ("lognormal", mu, sigma)
    except ValueError:
        pass
    raise ConfigError(
        f"malformed value_size_distribution: {s!r} "
        f"(expected constant:N | uniform:MIN:MAX | lognormal:MU:SIGMA)"
    )


def _parse_key_dist(s: Any) -> tuple:
    if s == "uniform":
        return ("uniform",)
    if isinstance(s, str) and s.startswith("zipf"):
        parts = s.split(":")
        if len(parts) == 2:
            try:
                theta = float(parts[1])
            except ValueError:
                theta = -1.0
            if theta > 0:
                return ("zipf", theta)
    raise ConfigError(
        f"malformed key_distribution: {s!r} (expected uniform | zipf:THETA)"
    )


# --------------------------------------------------------------------------- #
# Section parsers
# --------------------------------------------------------------------------- #

def _parse_data_model(d: dict) -> DataModel:
    shape = _req(d, "value_shape", "data_model")
    if shape not in VALUE_SHAPES:
        raise ConfigError(f"unknown value_shape: {shape!r} (expected one of {sorted(VALUE_SHAPES)})")
    dist = _parse_size_dist(_req(d, "value_size_distribution", "data_model"))
    vmin = _positive_int(_req(d, "value_size_min", "data_model"), "value_size_min")
    vmax = _positive_int(_req(d, "value_size_max", "data_model"), "value_size_max")
    if vmin > vmax:
        raise ConfigError(f"value_size_min ({vmin}) > value_size_max ({vmax})")
    seed = _req(d, "seed", "data_model")
    if not isinstance(seed, int) or isinstance(seed, bool):
        raise ConfigError(f"seed must be an integer (got {seed!r})")
    return DataModel(
        value_shape=shape,
        value_size_distribution=dist,
        value_size_min=vmin,
        value_size_max=vmax,
        seed=seed,
        corpus_entries=_positive_int(_req(d, "corpus_entries", "data_model"), "corpus_entries"),
        key_count=_positive_int(_req(d, "key_count", "data_model"), "key_count"),
        key_distribution=_parse_key_dist(_req(d, "key_distribution", "data_model")),
    )


def _parse_commands(raw: Any) -> list[Command]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError("workload.commands must be a non-empty list")
    cmds: list[Command] = []
    total = 0.0
    for c in raw:
        ctype = _req(c, "type", "workload.commands[]")
        if ctype not in KNOWN_COMMANDS:
            raise ConfigError(f"unknown command type: {ctype!r} (known: {sorted(KNOWN_COMMANDS)})")
        ratio = _req(c, "ratio", "workload.commands[]")
        ratio = _positive_num(ratio, "command ratio")
        keys = c.get("keys")
        if ctype in MULTIKEY_COMMANDS:
            if not isinstance(keys, int) or isinstance(keys, bool) or keys < 1:
                raise ConfigError(f"command {ctype!r} requires integer 'keys' >= 1")
        total += ratio
        cmds.append(Command(type=ctype, ratio=ratio, keys=keys))
    if abs(total - 1.0) > 1e-6:
        raise ConfigError(f"command ratios must normalize to 1.0 (sum={total})")
    return cmds


def _parse_workload(d: dict) -> Workload:
    return Workload(
        target_tps=_positive_int(_req(d, "target_tps", "workload"), "target_tps"),
        commands=_parse_commands(_req(d, "commands", "workload")),
        connections_total=_positive_int(_req(d, "connections_total", "workload"), "connections_total"),
        max_clients_per_process=_positive_int(
            _req(d, "max_clients_per_process", "workload"), "max_clients_per_process"
        ),
        pipeline=_positive_int(_req(d, "pipeline", "workload"), "pipeline"),
        measurement_duration_seconds=_positive_int(
            _req(d, "measurement_duration_seconds", "workload"), "measurement_duration_seconds"
        ),
    )


def _parse_profile_prep(d: dict) -> ProfilePrep:
    metric = _req(d, "plateau_metric", "profile_prep")
    if not isinstance(metric, str) or not metric:
        raise ConfigError("profile_prep.plateau_metric must be a non-empty string")
    window = _positive_int(_req(d, "plateau_window_polls", "profile_prep"), "plateau_window_polls")
    if window < 2:
        raise ConfigError("profile_prep.plateau_window_polls must be >= 2")
    return ProfilePrep(
        plateau_metric=metric,
        plateau_tolerance_pct=_positive_num(
            _req(d, "plateau_tolerance_pct", "profile_prep"), "plateau_tolerance_pct"
        ),
        plateau_window_polls=window,
        poll_interval_seconds=_positive_num(
            _req(d, "poll_interval_seconds", "profile_prep"), "poll_interval_seconds"
        ),
        max_timeout_seconds=_positive_num(
            _req(d, "max_timeout_seconds", "profile_prep"), "max_timeout_seconds"
        ),
    )


def _parse_configs(raw: Any) -> list[ConfigEntry]:
    if not isinstance(raw, list) or not raw:
        raise ConfigError("configs must be a non-empty list")
    entries: list[ConfigEntry] = []
    seen: set[str] = set()
    for c in raw:
        name = _req(c, "name", "configs[]")
        if name in seen:
            raise ConfigError(f"duplicate config name: {name!r}")
        seen.add(name)
        compression = c.get("compression", {})
        if not isinstance(compression, dict):
            raise ConfigError(f"config {name!r}: 'compression' must be an object")
        extra = c.get("extra_args", [])
        if not isinstance(extra, list) or not all(isinstance(x, str) for x in extra):
            raise ConfigError(f"config {name!r}: 'extra_args' must be a list of strings")
        entries.append(
            ConfigEntry(
                name=name,
                compression=compression,
                extra_args=list(extra),
                server_binary=c.get("server_binary"),
            )
        )
    return entries


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def parse(d: dict) -> RunConfig:
    """Validate a run-JSON dict and return a :class:`RunConfig`."""
    if not isinstance(d, dict):
        raise ConfigError("run config must be a JSON object")
    for f in REQUIRED_TOP:
        _req(d, f, "")

    configs = _parse_configs(d["configs"])
    reference = d["reference_config"]
    names = {c.name for c in configs}
    if reference not in names:
        raise ConfigError(f"reference_config {reference!r} is not one of configs {sorted(names)}")

    return RunConfig(
        output_directory=d["output_directory"],
        servers_directory=d["servers_directory"],
        benchmark_binary=d["benchmark_binary"],
        server_binary=d["server_binary"],
        iterations=_positive_int(d["iterations"], "iterations"),
        reference_config=reference,
        data_model=_parse_data_model(d["data_model"]),
        workload=_parse_workload(d["workload"]),
        profile_prep=_parse_profile_prep(d["profile_prep"]),
        configs=configs,
        description=d.get("description", ""),
    )


def load(path: str) -> RunConfig:
    """Read a run-JSON file from ``path`` and parse it."""
    with open(path, "r", encoding="utf-8") as fh:
        return parse(json.load(fh))


def render_server_args(entry: ConfigEntry) -> list[str]:
    """Render a config's structured ``compression`` block to ``--compression-*``
    flags (sparse: only specified knobs), then append ``extra_args`` so raw
    overrides win (last value on the command line takes effect)."""
    args: list[str] = []
    for key, value in entry.compression.items():
        args.append("--compression-" + key.replace("_", "-"))
        args.append(str(value))
    args.extend(entry.extra_args)
    return args


def resolve_server_binary(entry: ConfigEntry, run: RunConfig) -> str:
    """Per-config ``server_binary`` override, else the top-level default."""
    return entry.server_binary or run.server_binary
