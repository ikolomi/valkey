"""pytest harness for the compression-benchmark orchestrator.

Tier-1 (unit) tests are pure Python and always run. Tier-2/3 tests are tagged
``needs_server`` / ``needs_benchmark`` and are *skipped* (not failed) when the
required binaries are not available, per design §7.
"""

import os
import sys

# Ensure the package root is importable before importing `lib` (independent of
# the pytest `pythonpath` ini timing).
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import pytest

from lib import env


def pytest_collection_modifyitems(config, items):
    no_server = env.server_binary_path() is None
    no_bench = env.benchmark_binary_path() is None
    skip_server = pytest.mark.skip(
        reason="needs valkey-server (set VALKEY_SERVER or put it on PATH)"
    )
    skip_bench = pytest.mark.skip(
        reason="needs extended valkey-benchmark (set VALKEY_BENCHMARK or put it on PATH)"
    )
    for item in items:
        if "needs_server" in item.keywords and no_server:
            item.add_marker(skip_server)
        if "needs_benchmark" in item.keywords and no_bench:
            item.add_marker(skip_bench)
