"""Test-time dictionary acquisition via the `gen-zstd-dict` helper.

v1's product dict path is server-side training (`COMPRESSION TRAIN`, S1.x). Until that
lands, integration tests install a dict via `COMPRESSION DICT-IMPORT`, trained
out-of-process by `tests/helpers/gen-zstd-dict` (design §7.6) — kept outside the SUT
so a server training bug can't mask itself. The helper reads samples from stdin as
``[4-byte big-endian length][bytes]`` (the same format as :func:`lib.corpus.write_corpus`)
and writes the trained dict to ``argv[1]``.
"""

from __future__ import annotations

import base64
import os
import subprocess

from lib import corpus


def resolve_gen_zstd_dict(server_binary: str | None = None) -> str | None:
    """Find the `gen-zstd-dict` helper: ``$VALKEY_DICTGEN``, else `tests/helpers/`
    relative to the server binary's repo (``src/`` → repo root)."""
    p = os.environ.get("VALKEY_DICTGEN")
    if p and os.path.exists(p) and os.access(p, os.X_OK):
        return p
    if server_binary:
        repo = os.path.dirname(os.path.dirname(os.path.abspath(server_binary)))
        cand = os.path.join(repo, "tests", "helpers", "gen-zstd-dict")
        if os.path.exists(cand) and os.access(cand, os.X_OK):
            return cand
    return None


def train_dict(samples: list[bytes], gen_zstd_dict_path: str, work_dir: str,
               timeout: float = 120.0) -> bytes:
    """Train a ZSTD dictionary from ``samples`` and return its raw bytes."""
    os.makedirs(work_dir, exist_ok=True)
    samples_path = os.path.join(work_dir, "samples.bin")
    out_path = os.path.join(work_dir, "dict.bin")
    corpus.write_corpus(samples, samples_path)
    with open(samples_path, "rb") as fin:
        r = subprocess.run(
            [gen_zstd_dict_path, out_path], stdin=fin,
            capture_output=True, timeout=timeout,
        )
    if r.returncode != 0:
        raise RuntimeError(
            f"gen-zstd-dict failed (rc={r.returncode}): {r.stderr.decode(errors='replace').strip()}"
        )
    with open(out_path, "rb") as f:
        return f.read()


def to_base64(dict_bytes: bytes) -> str:
    return base64.b64encode(dict_bytes).decode("ascii")
