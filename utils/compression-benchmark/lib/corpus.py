"""Deterministic corpus generation + caching.

Design of record: ``design/detailed-design.md`` §2.2 (R2.1–R2.5), §5.4. The corpus
is generated script-side from the ``data_model``, reproducible per ``seed`` (R2.3),
and consumed by ``valkey-benchmark`` via ``--value-data corpus:FILE``. Stdlib only.

Corpus file format (§5.4): a flat sequence of length-prefixed blobs —
``[4-byte big-endian length][bytes]`` repeated ``corpus_entries`` times.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import random
import string
import struct

from lib.config import DataModel

_ALNUM = string.ascii_letters + string.digits


# --------------------------------------------------------------------------- #
# Size sampling
# --------------------------------------------------------------------------- #

def _clamp(n: int, lo: int, hi: int) -> int:
    return max(lo, min(hi, n))


def _sample_size(rng: random.Random, dist: tuple, lo: int, hi: int) -> int:
    kind = dist[0]
    if kind == "constant":
        size = dist[1]
    elif kind == "uniform":
        size = rng.randint(dist[1], dist[2])
    elif kind == "lognormal":
        mu, sigma = dist[1], dist[2]
        size = round(math.exp(rng.normalvariate(math.log(mu), sigma)))
    else:  # pragma: no cover - config validation prevents this
        raise ValueError(f"unknown size distribution {dist!r}")
    return _clamp(int(size), lo, hi)


def _alnum(rng: random.Random, n: int) -> str:
    if n <= 0:
        return ""
    return "".join(rng.choices(_ALNUM, k=n))


# --------------------------------------------------------------------------- #
# Value-shape generators — each returns exactly `size` bytes
# --------------------------------------------------------------------------- #

def _gen_json(rng: random.Random, size: int, i: int) -> bytes:
    base = json.dumps({"id": i, "pad": ""}, separators=(",", ":"))
    filler_len = size - len(base)
    pad = _alnum(rng, filler_len)  # alnum is never JSON-escaped → length is exact
    s = json.dumps({"id": i, "pad": pad}, separators=(",", ":"))
    return s.encode("ascii")


def _build_truncate(rng: random.Random, size: int, head: str, make_token) -> bytes:
    parts = [head]
    total = len(head)
    n = 0
    while total < size:
        tok = make_token(rng, n)
        parts.append(tok)
        total += len(tok)
        n += 1
    return "".join(parts)[:size].encode("ascii")


def _gen_kv(rng: random.Random, size: int) -> bytes:
    return _build_truncate(rng, size, "", lambda r, n: f"f{n}={_alnum(r, r.randint(3, 12))};")


def _gen_log(rng: random.Random, size: int) -> bytes:
    level = rng.choice(["INFO", "WARN", "ERROR", "DEBUG"])
    head = f"2026-06-25T12:00:00Z {level} "
    return _build_truncate(rng, size, head, lambda r, n: f"k{n}={_alnum(r, r.randint(3, 10))} ")


def _gen_coordinates(rng: random.Random, size: int) -> bytes:
    def tok(r, n):
        return f"{round(r.uniform(-90, 90), 6)},{round(r.uniform(-180, 180), 6)} "
    return _build_truncate(rng, size, "", tok)


_SHAPES = {
    "json": lambda rng, size, i: _gen_json(rng, size, i),
    "kv": lambda rng, size, i: _gen_kv(rng, size),
    "log": lambda rng, size, i: _gen_log(rng, size),
    "coordinates": lambda rng, size, i: _gen_coordinates(rng, size),
}


# --------------------------------------------------------------------------- #
# Public API
# --------------------------------------------------------------------------- #

def generate_blobs(dm: DataModel) -> list[bytes]:
    """Generate ``dm.corpus_entries`` blobs, deterministic for ``dm.seed`` (R2.3)."""
    rng = random.Random(dm.seed)
    gen = _SHAPES[dm.value_shape]
    out: list[bytes] = []
    for i in range(dm.corpus_entries):
        size = _sample_size(rng, dm.value_size_distribution, dm.value_size_min, dm.value_size_max)
        out.append(gen(rng, size, i))
    return out


def corpus_hash(dm: DataModel) -> str:
    """Stable hash of the corpus-defining fields (Q8d cache key)."""
    key = json.dumps(
        {
            "shape": dm.value_shape,
            "dist": list(dm.value_size_distribution),
            "min": dm.value_size_min,
            "max": dm.value_size_max,
            "seed": dm.seed,
            "entries": dm.corpus_entries,
        },
        sort_keys=True,
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]


def corpus_path(dm: DataModel, cache_dir: str) -> str:
    return os.path.join(cache_dir, f"corpus-{corpus_hash(dm)}.bin")


def write_corpus(blobs: list[bytes], path: str) -> None:
    """Write length-prefixed blobs atomically (tmp + rename)."""
    tmp = path + ".tmp"
    with open(tmp, "wb") as fh:
        for b in blobs:
            fh.write(struct.pack(">I", len(b)))
            fh.write(b)
    os.replace(tmp, path)


def read_corpus(path: str) -> list[bytes]:
    out: list[bytes] = []
    with open(path, "rb") as fh:
        while True:
            hdr = fh.read(4)
            if not hdr:
                break
            (n,) = struct.unpack(">I", hdr)
            out.append(fh.read(n))
    return out


def ensure(dm: DataModel, cache_dir: str) -> str:
    """Return the corpus file path, generating+caching it on a miss (R2.3)."""
    path = corpus_path(dm, cache_dir)
    if os.path.exists(path):
        return path
    os.makedirs(cache_dir, exist_ok=True)
    write_corpus(generate_blobs(dm), path)
    return path
