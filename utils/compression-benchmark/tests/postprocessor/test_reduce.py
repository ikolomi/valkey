"""Plan 3 Tier-1 tests — pure reduction pieces (``postprocessor/reduce.py``).

These exercise the histogram→percentile math, cross-iteration bucket merge, memory
statistics, and consensus outlier detection (flag-low-N / drop-high-N) with synthetic
inputs of known shape, so the assertions are tight.
"""

import math

from postprocessor import reduce


# --- percentile_from_buckets ------------------------------------------------ #

_TEN = [[v, 1] for v in (10, 20, 30, 40, 50, 60, 70, 80, 90, 100)]  # 10 buckets, total 10


def test_percentile_from_buckets():
    assert reduce.percentile_from_buckets(_TEN, 50) == 50
    assert reduce.percentile_from_buckets(_TEN, 90) == 90
    assert reduce.percentile_from_buckets(_TEN, 100) == 100
    assert reduce.percentile_from_buckets(_TEN, 10) == 10


def test_percentile_from_buckets_empty_is_none():
    assert reduce.percentile_from_buckets([], 50) is None


def test_percentiles_canonical_labels():
    pc = reduce.percentiles(_TEN)
    assert set(pc) == {"p50", "p90", "p95", "p99", "p99.9", "p99.99", "p99.999"}
    assert pc["p50"] == 50 and pc["p90"] == 90


# --- merge_bucket_lists ----------------------------------------------------- #

def test_merge_bucket_lists_sums_by_value_sorted():
    merged = reduce.merge_bucket_lists([[[10, 1], [20, 2]], [[20, 3], [30, 1]]])
    assert merged == [[10, 1], [20, 5], [30, 1]]


def test_merge_bucket_lists_exact_percentile_after_merge():
    # two processes each [[10,1]..[100,1]] → merged each value count 2, total 20
    merged = reduce.merge_bucket_lists([_TEN, _TEN])
    assert reduce.percentile_from_buckets(merged, 50) == 50  # exact, not averaged


# --- memory_stats ----------------------------------------------------------- #

def test_memory_stats():
    s = reduce.memory_stats([10, 20, 30, 40, 50])
    assert s["min"] == 10 and s["max"] == 50
    assert s["mean"] == 30 and s["median"] == 30
    assert s["samples"] == 5
    assert math.isclose(s["stddev"], math.sqrt(200), rel_tol=1e-9)  # population std


def test_memory_stats_empty_is_none():
    assert reduce.memory_stats([]) is None


# --- consensus_outliers + select_iterations (Q8) ---------------------------- #

def test_consensus_outliers_flags_clear_outlier():
    flagged = reduce.consensus_outliers([10, 10, 11, 9, 10, 100])
    assert 5 in flagged
    assert flagged - {5} == set()  # only the 100 is flagged


def test_select_iterations_drops_only_at_high_n():
    vals_high = [10, 10, 11, 9, 10, 100]   # N=6 ≥ 5 → drop
    kept, flagged = reduce.select_iterations(vals_high, n_threshold=5)
    assert 5 in flagged and 5 not in kept and kept == [0, 1, 2, 3, 4]

    vals_low = [10, 11, 10, 100]           # N=4 < 5 → flag only, keep all
    kept2, flagged2 = reduce.select_iterations(vals_low, n_threshold=5)
    assert 3 in flagged2 and kept2 == [0, 1, 2, 3]


def test_select_iterations_never_drops_everything():
    kept, flagged = reduce.select_iterations([5, 5, 5, 5, 5], n_threshold=5)
    assert kept  # no false positives on a flat series


# --- deltas ----------------------------------------------------------------- #

def test_delta_pct_and_abs():
    d = reduce.delta(base=100.0, value=120.0)
    assert d["abs"] == 20.0
    assert math.isclose(d["pct"], 20.0)
    d0 = reduce.delta(base=0.0, value=5.0)
    assert d0["abs"] == 5.0 and d0["pct"] is None  # guard divide-by-zero
