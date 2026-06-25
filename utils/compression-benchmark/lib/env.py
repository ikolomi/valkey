"""Binary resolution for Tier-2/3 tests and the orchestrator.

Resolves `valkey-server` / `valkey-benchmark` / `valkey-cli` from explicit env vars
(`VALKEY_SERVER`, `VALKEY_BENCHMARK`, `VALKEY_CLI`), then PATH, then — for the cli —
alongside the resolved server binary (the common in-tree `src/` layout).
"""

from __future__ import annotations

import os
import shutil


def resolve_binary(env_var: str, names: list[str]) -> str | None:
    p = os.environ.get(env_var)
    if p and os.path.exists(p) and os.access(p, os.X_OK):
        return p
    for name in names:
        found = shutil.which(name)
        if found:
            return found
    return None


def server_binary_path() -> str | None:
    return resolve_binary("VALKEY_SERVER", ["valkey-server"])


def benchmark_binary_path() -> str | None:
    return resolve_binary("VALKEY_BENCHMARK", ["valkey-benchmark"])


def cli_binary_path(server_binary: str | None = None) -> str | None:
    p = os.environ.get("VALKEY_CLI")
    if p and os.path.exists(p) and os.access(p, os.X_OK):
        return p
    if server_binary:
        cand = os.path.join(os.path.dirname(os.path.abspath(server_binary)), "valkey-cli")
        if os.path.exists(cand) and os.access(cand, os.X_OK):
            return cand
    return shutil.which("valkey-cli")
