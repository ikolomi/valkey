"""Shared Tier-3 (e2e) fixtures.

The full orchestrator runs (server + load + measure) are expensive, so the two
canonical runs are **session-scoped** and reused by many cheap assertion tests:

- ``off_run``  — the reference/off config, ``iterations=2`` (exercises the iterations
  knob and the off→no-compression behavior).
- ``comp_run`` — off + a compression-on config with **distinctive** compression knobs
  (``e2e_lib.COMP_KNOBS``) so the INFO echo can prove each knob was applied end-to-end.

Both skip cleanly when the binaries are absent.
"""

import pytest

from e2e_lib import COMP_KNOBS, base_cfg, need_binaries, run_cfg


@pytest.fixture(scope="session")
def off_run(tmp_path_factory):
    need_binaries()
    d = tmp_path_factory.mktemp("off_run")
    cfg = base_cfg(d / "results", d / "servers")
    cfg["iterations"] = 2
    return run_cfg(cfg, d)


@pytest.fixture(scope="session")
def comp_run(tmp_path_factory):
    need_binaries()
    d = tmp_path_factory.mktemp("comp_run")
    cfg = base_cfg(d / "results", d / "servers")
    cfg["configs"].append({"name": "compression-on", "compression": dict(COMP_KNOBS)})
    return run_cfg(cfg, d)
