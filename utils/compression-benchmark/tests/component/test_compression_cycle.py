"""Tier-2 integration test exercising the real compression hot path via the
orchestrator: DICT-IMPORT a trained dict, populate compressible values, run
`COMPRESSION SWEEP FORCE`, poll `INFO compression` to a plateau, and assert that
compression actually happened. Needs a built valkey-server + `gen-zstd-dict`.
"""

import socket

import pytest

from lib import config, corpus, dictgen, env, info, server

pytestmark = pytest.mark.needs_server


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_compression_cycle_compresses_keys(tmp_path):
    binpath = env.server_binary_path()
    gen = dictgen.resolve_gen_zstd_dict(binpath)
    if gen is None:
        pytest.skip("gen-zstd-dict helper not built (BUILD_ZSTD=yes)")

    # 1. compressible corpus samples (constant 512B json) → train a dict from them
    dm = config.DataModel("json", ("constant", 512), 256, 16384, 7, 4000, 100000, ("uniform",))
    blobs = corpus.generate_blobs(dm)
    dict_b64 = dictgen.to_base64(dictgen.train_dict(blobs, gen, str(tmp_path / "dict")))

    # 2. start a compression-enabled server (eligibility wide open so the sweep compresses)
    entry = config.ConfigEntry("compression-on", {
        "master_switch": "compression",
        "automatic_sweeper": "enabled",
        "min_value_size": 64,
        "max_value_size": 0,
        "min_idle_seconds": 0,
        "min_savings_ratio": 0,
        "threads": 1,
    })
    srv = server.Server(binpath, str(tmp_path / "srv"), "compression-on", _free_port(),
                        args=config.render_server_args(entry))
    srv.start()
    try:
        # 3. install the trained dict → active dict id becomes non-zero
        srv.compression("dict-import", dict_b64)
        assert int(srv.info("compression").get("compression_active_dict_id", "0")) != 0

        # 4. populate compressible values (json corpus blobs are pure ASCII, no spaces)
        srv.flushall()
        n = 500
        for i in range(n):
            srv.cli("set", f"k{i}", blobs[i % len(blobs)].decode("ascii"))
        assert srv.dbsize() == n

        # 5. force a sweep and poll INFO until compressed_objects plateaus
        srv.compression("sweep", "force")
        res = info.poll_until_plateau(
            lambda: int(srv.info("compression").get("compression_compressed_objects", "0")),
            tolerance_pct=1, window_polls=3, poll_interval=0.2, max_timeout=30,
        )
        assert res["plateaued"] is True, res["series"]

        # 6. compression actually happened, with real savings
        final = srv.info("compression")
        assert int(final["compression_compressed_objects"]) > 0
        ratio = float(final.get("compression_ratio", "1"))
        assert 0.0 < ratio < 1.0, f"compression_ratio={ratio}"
    finally:
        srv.teardown()
