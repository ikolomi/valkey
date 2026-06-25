"""INFO polling + plateau detection.

A3 implements the pure plateau detector (R4.6). INFO polling against a live server
and the ``used_memory`` time-series capture land in Phase C.
"""

from __future__ import annotations


def detect_plateau(series, tolerance_pct, window_polls):
    """Return ``True`` iff the trailing ``window_polls`` samples of ``series`` are
    stable within ``tolerance_pct`` — ``(max-min)/max*100 <= tolerance_pct`` (R4.6).

    The polling driver appends each new sample and calls this every tick; it stops
    and measures on ``True``, or FAILs (``profile_not_stabilized``) once the elapsed
    time exceeds ``max_timeout_seconds`` without a ``True``. A never-stabilizing
    series therefore stays ``False`` here by construction.
    """
    if len(series) < window_polls:
        return False
    window = series[-window_polls:]
    hi, lo = max(window), min(window)
    if hi == 0:
        return lo == 0  # all-zero window is trivially stable
    return (hi - lo) / hi * 100.0 <= tolerance_pct


def poll_until_plateau(sample_fn, tolerance_pct, window_polls, poll_interval,
                       max_timeout, clock=None, sleep=None):
    """Poll ``sample_fn()`` every ``poll_interval`` seconds, appending to a series,
    until :func:`detect_plateau` is True or ``max_timeout`` elapses (R4.6).

    Returns ``{"plateaued": bool, "series": [...], "elapsed": float}``. On timeout
    ``plateaued`` is False → the driver maps that to FAILED ``profile_not_stabilized``.
    ``clock``/``sleep`` are injectable for testing.
    """
    import time as _time

    clock = clock or _time.monotonic
    sleep = sleep or _time.sleep
    series = []
    start = clock()
    while True:
        series.append(sample_fn())
        if detect_plateau(series, tolerance_pct, window_polls):
            return {"plateaued": True, "series": series, "elapsed": clock() - start}
        if clock() - start >= max_timeout:
            return {"plateaued": False, "series": series, "elapsed": clock() - start}
        sleep(poll_interval)

