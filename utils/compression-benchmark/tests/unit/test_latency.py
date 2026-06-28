"""Plan 2 Tier-1 tests — latency dump parsing + lossless cross-process merge
(``lib/latency.py``). The dump format is valkey-benchmark ``--latency-dump`` output
(Plan 1): a ``# hdr lowest=.. highest=.. sigfig=.. total_count=..`` header then
``value_usec,count`` lines for each recorded bucket.
"""

import pytest

from lib import latency

_DUMP_A = """# hdr lowest=10 highest=3000000 sigfig=3 total_count=6
16,1
24,2
32,3
"""

_DUMP_B = """# hdr lowest=10 highest=3000000 sigfig=3 total_count=4
24,1
40,3
"""


def test_parse_dump_basic():
    h = latency.parse_dump(_DUMP_A)
    assert h["hdr"] == {"lowest": 10, "highest": 3000000, "sigfig": 3}
    assert h["total_count"] == 6
    assert h["buckets"] == {16: 1, 24: 2, 32: 3}
    assert sum(h["buckets"].values()) == h["total_count"]


def test_parse_dump_rejects_empty():
    with pytest.raises(ValueError):
        latency.parse_dump("")


def test_parse_dump_rejects_missing_header():
    with pytest.raises(ValueError):
        latency.parse_dump("16,1\n24,2\n")


def test_parse_dump_rejects_count_mismatch():
    # header says 99 but buckets sum to 6 → truncation/corruption guard
    bad = "# hdr lowest=10 highest=3000000 sigfig=3 total_count=99\n16,1\n24,2\n32,3\n"
    with pytest.raises(ValueError):
        latency.parse_dump(bad)


def test_sum_histograms_adds_counts_by_value():
    merged = latency.sum_histograms([latency.parse_dump(_DUMP_A), latency.parse_dump(_DUMP_B)])
    assert merged["hdr"] == {"lowest": 10, "highest": 3000000, "sigfig": 3}
    assert merged["total_count"] == 10
    # 24 overlaps (2+1); 16,32 from A; 40 from B
    assert merged["buckets"] == {16: 1, 24: 3, 32: 3, 40: 3}
    assert sum(merged["buckets"].values()) == merged["total_count"]


def test_sum_histograms_single():
    merged = latency.sum_histograms([latency.parse_dump(_DUMP_A)])
    assert merged["buckets"] == {16: 1, 24: 2, 32: 3}
    assert merged["total_count"] == 6


def test_sum_histograms_rejects_mismatched_hdr():
    other = "# hdr lowest=1 highest=3000000 sigfig=3 total_count=1\n50,1\n"
    with pytest.raises(ValueError):
        latency.sum_histograms([latency.parse_dump(_DUMP_A), latency.parse_dump(other)])


def test_sum_histograms_rejects_empty_list():
    with pytest.raises(ValueError):
        latency.sum_histograms([])


def test_capture_per_command_sums_processes(tmp_path):
    # two GET loader processes + one SET process, each with a dump file
    g0 = tmp_path / "loader-get-0.hist"; g0.write_text(_DUMP_A)   # total 6
    g1 = tmp_path / "loader-get-1.hist"; g1.write_text(_DUMP_B)   # total 4
    s0 = tmp_path / "loader-set-0.hist"; s0.write_text(_DUMP_A)   # total 6
    results = [
        {"command": "get", "index": 0, "hist_path": str(g0)},
        {"command": "get", "index": 1, "hist_path": str(g1)},
        {"command": "set", "index": 0, "hist_path": str(s0)},
    ]
    block = latency.capture_per_command(results)
    assert block["hdr"] == {"lowest": 10, "highest": 3000000, "sigfig": 3}
    assert set(block["per_command"]) == {"get", "set"}
    assert block["per_command"]["get"]["total_count"] == 10   # 6 + 4
    assert block["per_command"]["set"]["total_count"] == 6
    # buckets are a sorted [[value, count], ...] list (deterministic round-trip)
    get_buckets = dict(block["per_command"]["get"]["buckets"])
    assert get_buckets == {16: 1, 24: 3, 32: 3, 40: 3}
    assert block["per_command"]["get"]["buckets"] == sorted(block["per_command"]["get"]["buckets"])


def test_capture_per_command_none_when_no_dumps():
    results = [{"command": "get", "index": 0, "hist_path": None},
               {"command": "set", "index": 0}]  # missing key entirely
    assert latency.capture_per_command(results) is None


# --------------------------------------------------------------------------- #
# T2.6 — FROZEN SAMPLE: a real --latency-dump captured from the Plan-1 binary.
# Guards against valkey-benchmark output-format drift: if the dump format ever
# changes, this test fails loudly (localizing the blast radius to the parser).
# --------------------------------------------------------------------------- #

import os as _os

_FIXTURE = _os.path.join(_os.path.dirname(__file__), "fixtures", "latency-dump-sample.hist")


def test_frozen_real_dump_parses_exactly():
    h = latency.parse_dump_file(_FIXTURE)
    assert h["hdr"] == {"lowest": 10, "highest": 3000000, "sigfig": 3}
    assert h["total_count"] == 30
    assert h["buckets"] == {
        48: 5, 56: 3, 64: 2, 72: 4, 80: 5, 88: 1, 96: 1, 104: 2,
        112: 1, 120: 1, 136: 1, 2256: 1, 2304: 1, 2328: 1, 2376: 1,
    }
    assert sum(h["buckets"].values()) == 30
