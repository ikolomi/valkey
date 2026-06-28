"""C1 Tier-2 test for lib/server.py — server lifecycle against a real,
compression-enabled valkey-server (design §3.2, R9.2). Needs a built server.

Run with: VALKEY_SERVER=/path/to/src/valkey-server python3 -m pytest tests/component
"""

import os
import socket

import pytest

from lib import config, env, server

pytestmark = pytest.mark.needs_server


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _compression_on_entry():
    return config.ConfigEntry(
        name="compression-on",
        compression={
            "master_switch": "compression",
            "automatic_sweeper": "enabled",
            "min_value_size": 64,
            "min_idle_seconds": 0,
        },
    )


def test_server_lifecycle_config_data_and_info(tmp_path):
    binpath = env.server_binary_path()
    args = config.render_server_args(_compression_on_entry())
    srv = server.Server(
        server_binary=binpath,
        servers_directory=str(tmp_path),
        name="compression-on",
        port=_free_port(),
        args=args,
    )
    srv.start()
    home = srv.home_dir
    try:
        # ready
        assert srv.ping() is True

        # R9.2: runs from its own temp home dir with the binary copied in
        assert os.path.isdir(home)
        assert os.path.exists(os.path.join(home, os.path.basename(binpath)))

        # rendered compression args were applied at startup
        assert srv.config_get("compression-master-switch") == "compression"
        assert srv.config_get("compression-automatic-sweeper") == "enabled"

        # master-switch toggle takes effect at runtime
        srv.config_set("compression-master-switch", "off")
        assert srv.config_get("compression-master-switch") == "off"

        # data plane
        srv.flushall()
        assert srv.dbsize() == 0
        srv.cli("set", "k", "v")
        assert srv.dbsize() == 1

        # INFO compression section is present and parsed
        info = srv.info("compression")
        assert any(key.startswith("compression_") for key in info)
    finally:
        srv.teardown()

    # teardown stops the server and removes the isolated dir
    assert not os.path.exists(home)


def test_server_context_manager_tears_down(tmp_path):
    binpath = env.server_binary_path()
    with server.Server(binpath, str(tmp_path), "off", _free_port(),
                       args=["--compression-master-switch", "off"]) as srv:
        assert srv.ping() is True
        home = srv.home_dir
    assert not os.path.exists(home)
