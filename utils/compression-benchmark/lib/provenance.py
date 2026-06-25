"""Provenance for reproducibility (R9.1): binary SHA-256, machine info, corpus
hash, and best-effort mpstat CPU capture. Stdlib only.
"""

from __future__ import annotations

import hashlib
import os
import platform
import shutil
import subprocess
import time


def sha256_file(path: str, _bufsize: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(_bufsize), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def machine_info() -> dict:
    info = {
        "cpus": os.cpu_count(),
        "platform": platform.platform(),
        "machine": platform.machine(),
        "python": platform.python_version(),
        "mem_total_bytes": None,
        "numa": None,
    }
    try:
        info["mem_total_bytes"] = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    except (ValueError, OSError, AttributeError):
        pass
    numactl = shutil.which("numactl")
    if numactl:
        try:
            r = subprocess.run([numactl, "--hardware"], capture_output=True, text=True, timeout=5)
            if r.returncode == 0 and r.stdout.strip():
                info["numa"] = r.stdout.strip().splitlines()[0]
        except Exception:
            pass
    return info


def build_provenance(*, server_binary: str, benchmark_binary: str, seed: int,
                     corpus_path: str | None = None, config_echo=None) -> dict:
    prov = {
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "seed": seed,
        "binary_checksums": {
            "valkey-server": sha256_file(server_binary),
            "valkey-benchmark": sha256_file(benchmark_binary),
        },
        "machine": machine_info(),
    }
    if corpus_path:
        prov["corpus"] = {"path": corpus_path, "sha256": sha256_file(corpus_path)}
    if config_echo is not None:
        prov["config"] = config_echo
    return prov


def start_mpstat(out_path: str, interval: int = 1):
    """Start `mpstat -P ALL <interval>` writing to ``out_path``. Returns the Popen,
    or ``None`` if mpstat isn't installed (best-effort CPU capture, R6.3)."""
    mpstat = shutil.which("mpstat")
    if not mpstat:
        return None
    fh = open(out_path, "w")
    proc = subprocess.Popen([mpstat, "-P", "ALL", str(interval)], stdout=fh,
                            stderr=subprocess.DEVNULL)
    proc._out_fh = fh  # keep a handle to close on stop
    return proc


def stop_mpstat(proc) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except Exception:
        proc.kill()
    try:
        proc._out_fh.close()
    except Exception:
        pass
