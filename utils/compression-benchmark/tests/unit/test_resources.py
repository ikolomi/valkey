"""Plan 2 Tier-1 tests — server-process CPU sampling (T2.7).

The pure pieces: parsing /proc/<pid>/stat utime+stime (robust to a comm with spaces
and parentheses), and turning a jiffy delta over a wall window into CPU percentages.
"""

from lib import phases, server


def test_parse_proc_stat_cpu_handles_comm_with_spaces_and_parens():
    # comm "(valkey srv)" contains a space; utime is field 14, stime field 15.
    stat = ("4242 (valkey srv) S 1 4242 4242 0 -1 4194560 1000 0 0 0 "
            "123 45 0 0 20 0 1 0 9999 0 0")
    utime, stime = server._parse_proc_stat_cpu(stat)
    assert utime == 123
    assert stime == 45


def test_cpu_pct_basic():
    # 100 utime jiffies + 50 stime jiffies over 1.0s at 100 Hz → 100% user, 50% sys.
    blk = phases._cpu_pct((10, 5), (110, 55), wall_seconds=1.0, clk_tck=100)
    assert blk["pct_user"] == 100.0
    assert blk["pct_system"] == 50.0
    assert blk["pct_total"] == 150.0


def test_cpu_pct_none_on_bad_inputs():
    assert phases._cpu_pct(None, (1, 1), 1.0, 100) is None
    assert phases._cpu_pct((1, 1), (2, 2), 0.0, 100) is None
