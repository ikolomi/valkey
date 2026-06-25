"""valkey-server lifecycle for the orchestrator (design §3.2, R9.2).

Each server runs from its own temporary home directory under ``servers_directory``
with the resolved ``server_binary`` copied in (amz-orc ``prepare_server_directory``
pattern), and is controlled over RESP via ``valkey-cli``. Stdlib only.
"""

from __future__ import annotations

import os
import shutil
import socket
import subprocess
import time

from lib import env


def free_port() -> int:
    """Return an ephemeral free TCP port on localhost."""
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Server:
    def __init__(self, server_binary, servers_directory, name, port,
                 args=None, cli_binary=None, copy_binary=True):
        self.server_binary = server_binary
        self.servers_directory = servers_directory
        self.name = name
        self.port = port
        self.args = list(args or [])
        self.cli_binary = cli_binary or env.cli_binary_path(server_binary)
        self.copy_binary = copy_binary
        self.home_dir = os.path.join(servers_directory, f"{name}-{port}")
        self.proc = None

    # -- lifecycle -------------------------------------------------------- #

    def start(self, ready_timeout=15.0):
        os.makedirs(self.home_dir, exist_ok=True)
        exec_bin = self.server_binary
        if self.copy_binary:
            dst = os.path.join(self.home_dir, os.path.basename(self.server_binary))
            shutil.copy2(self.server_binary, dst)
            os.chmod(dst, 0o755)
            exec_bin = dst
        cmd = [
            exec_bin,
            "--port", str(self.port),
            "--dir", self.home_dir,
            "--save", "",
            "--appendonly", "no",
            "--logfile", "server.log",
        ] + self.args
        self.proc = subprocess.Popen(
            cmd, cwd=self.home_dir, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
        )
        self.wait_ready(ready_timeout)
        return self

    def wait_ready(self, timeout):
        deadline = time.time() + timeout
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError(
                    f"server exited early (rc={self.proc.returncode}); "
                    f"see {os.path.join(self.home_dir, 'server.log')}"
                )
            try:
                if self.cli("ping") == "PONG":
                    return
            except Exception:
                pass
            time.sleep(0.1)
        raise TimeoutError(f"server not ready within {timeout}s on port {self.port}")

    def stop(self, timeout=10):
        if self.proc is None:
            return
        try:
            subprocess.run(
                [self.cli_binary, "-p", str(self.port), "shutdown", "nosave"],
                capture_output=True, text=True, timeout=timeout,
            )
        except Exception:
            pass
        try:
            self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait(timeout=5)
        self.proc = None

    def teardown(self):
        self.stop()
        shutil.rmtree(self.home_dir, ignore_errors=True)

    def __enter__(self):
        return self.start()

    def __exit__(self, *exc):
        self.teardown()
        return False

    # -- control ---------------------------------------------------------- #

    def cli(self, *args, timeout=10):
        cmd = [self.cli_binary, "-p", str(self.port), *map(str, args)]
        r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if r.returncode != 0:
            raise RuntimeError(
                f"valkey-cli {args} failed (rc={r.returncode}): "
                f"{r.stderr.strip() or r.stdout.strip()}"
            )
        return r.stdout.strip()

    def ping(self):
        try:
            return self.cli("ping") == "PONG"
        except Exception:
            return False

    def config_get(self, key):
        out = self.cli("config", "get", key)
        lines = out.split("\n")
        return lines[1] if len(lines) >= 2 else ""

    def config_set(self, key, value):
        self.cli("config", "set", key, str(value))

    def dbsize(self):
        return int(self.cli("dbsize"))

    def flushall(self):
        self.cli("flushall")

    def compression(self, *subargs):
        return self.cli("compression", *subargs)

    def info(self, section: str = "") -> dict:
        out = self.cli("info", section) if section else self.cli("info")
        d = {}
        for line in out.splitlines():
            line = line.strip()
            if not line or line.startswith("#") or ":" not in line:
                continue
            k, v = line.split(":", 1)
            d[k] = v
        return d
