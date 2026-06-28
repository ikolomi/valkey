"""A0 smoke test: package imports and the binary-gated markers skip cleanly."""

import importlib

import pytest

LIB_MODULES = [
    "config",
    "corpus",
    "benchmark",
    "info",
    "phases",
    "runstatus",
    "provenance",
]


def test_lib_modules_import():
    for mod in LIB_MODULES:
        importlib.import_module(f"lib.{mod}")


@pytest.mark.needs_server
def test_needs_server_marker_skips_without_binary():
    # Runs only if a valkey-server binary is available; otherwise skipped by conftest.
    assert True


@pytest.mark.needs_benchmark
def test_needs_benchmark_marker_skips_without_binary():
    # Runs only if the extended valkey-benchmark is available; otherwise skipped.
    assert True
