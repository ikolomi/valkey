"""A2 Tier-1 tests for lib/corpus.py — deterministic corpus generation + cache."""

import json
import os
import re
import statistics

import pytest

from lib import config, corpus


def dm(**over):
    base = dict(
        value_shape="json",
        value_size_distribution=("lognormal", 512.0, 0.8),
        value_size_min=256,
        value_size_max=16384,
        seed=1234,
        corpus_entries=2000,
        key_count=100000,
        key_distribution=("uniform",),
    )
    base.update(over)
    return config.DataModel(**base)


# ---- count + clamps ----

def test_exact_count():
    assert len(corpus.generate_blobs(dm())) == 2000


def test_sizes_within_clamps():
    blobs = corpus.generate_blobs(dm())
    assert all(256 <= len(b) <= 16384 for b in blobs)


# ---- determinism ----

def test_deterministic_same_seed():
    assert corpus.generate_blobs(dm()) == corpus.generate_blobs(dm())


def test_different_seed_differs():
    assert corpus.generate_blobs(dm(seed=1)) != corpus.generate_blobs(dm(seed=2))


# ---- size distributions ----

def test_constant_distribution():
    blobs = corpus.generate_blobs(dm(value_size_distribution=("constant", 512)))
    assert all(len(b) == 512 for b in blobs)


def test_uniform_distribution_in_range():
    blobs = corpus.generate_blobs(
        dm(value_size_distribution=("uniform", 256, 8192), value_size_max=8192)
    )
    sizes = [len(b) for b in blobs]
    assert all(256 <= s <= 8192 for s in sizes)
    assert statistics.pstdev(sizes) > 0  # genuinely spread, not constant


def test_lognormal_median_near_mu():
    blobs = corpus.generate_blobs(dm(value_size_distribution=("lognormal", 512.0, 0.8)))
    med = statistics.median(len(b) for b in blobs)
    assert 0.5 * 512 <= med <= 2.0 * 512


# ---- value shapes well-formed ----

def test_shape_json_is_valid_json():
    for b in corpus.generate_blobs(dm(value_shape="json", corpus_entries=200)):
        obj = json.loads(b)
        assert isinstance(obj, dict)


def test_shape_kv_has_separators():
    for b in corpus.generate_blobs(dm(value_shape="kv", corpus_entries=200)):
        assert b"=" in b and b";" in b


def test_shape_log_has_timestamp_and_level():
    pat = re.compile(rb"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\S* (INFO|WARN|ERROR|DEBUG)")
    for b in corpus.generate_blobs(dm(value_shape="log", corpus_entries=200)):
        assert pat.match(b), b[:40]


def test_shape_coordinates_has_pairs():
    for b in corpus.generate_blobs(dm(value_shape="coordinates", corpus_entries=200)):
        assert b"," in b and any(48 <= c <= 57 for c in b)  # has a digit


# ---- file write/read + determinism ----

def test_write_read_roundtrip(tmp_path):
    blobs = corpus.generate_blobs(dm(corpus_entries=100))
    p = tmp_path / "c.bin"
    corpus.write_corpus(blobs, str(p))
    assert corpus.read_corpus(str(p)) == blobs


def test_write_is_byte_deterministic(tmp_path):
    blobs = corpus.generate_blobs(dm(corpus_entries=100))
    a, b = tmp_path / "a.bin", tmp_path / "b.bin"
    corpus.write_corpus(blobs, str(a))
    corpus.write_corpus(blobs, str(b))
    assert a.read_bytes() == b.read_bytes()


# ---- cache hit/miss ----

def test_cache_hit_does_not_regenerate(tmp_path):
    cache = str(tmp_path)
    p1 = corpus.ensure(dm(corpus_entries=100), cache)
    assert os.path.exists(p1)
    mtime1 = os.path.getmtime(p1)
    p2 = corpus.ensure(dm(corpus_entries=100), cache)
    assert p2 == p1
    assert os.path.getmtime(p2) == mtime1  # not regenerated


def test_cache_miss_on_param_change(tmp_path):
    cache = str(tmp_path)
    p_seed1 = corpus.ensure(dm(seed=1, corpus_entries=100), cache)
    p_seed2 = corpus.ensure(dm(seed=2, corpus_entries=100), cache)
    assert p_seed1 != p_seed2
