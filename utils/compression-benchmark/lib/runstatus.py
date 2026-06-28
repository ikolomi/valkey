"""Run success/failure decision (R7.3).

A config iteration is FAILED if (in precedence order): the server crashed
(``server_error``); the benchmark errored (``benchmark_error``); the achieved TPS
fell below ``target_tps*(1-tps_tolerance)`` (``target_tps_not_achieved``); or the
compression profile did not plateau (``profile_not_stabilized``). ``plateaued=None``
means "not applicable" (e.g. the reference/off config skips the compress phases) and
never fails the iteration. A config is SUCCESS iff all its iterations are; overall is
the AND of all configs.
"""

from __future__ import annotations


def _iter_status(it: dict, tps_tolerance: float) -> dict:
    if it.get("crashed"):
        return {"status": "FAILED", "reason": "server_error"}
    if it.get("benchmark_error"):
        return {"status": "FAILED", "reason": "benchmark_error"}
    achieved = it.get("achieved_tps")
    target = it.get("target_tps")
    if achieved is not None and target is not None and achieved < target * (1 - tps_tolerance):
        return {
            "status": "FAILED",
            "reason": "target_tps_not_achieved",
            "achieved_tps": achieved,
            "target_tps": target,
        }
    if it.get("plateaued", True) is False:
        return {"status": "FAILED", "reason": "profile_not_stabilized"}
    return {"status": "SUCCESS"}


def decide(config_results: dict, tps_tolerance: float = 0.05) -> dict:
    """Decide per-config and overall SUCCESS/FAILED from per-iteration results.

    ``config_results`` maps config name → list of iteration dicts, each with
    ``achieved_tps``, ``target_tps``, ``plateaued`` (bool|None), ``crashed`` (bool),
    ``benchmark_error`` (bool).
    """
    configs = {}
    overall_ok = True
    for name, iters in config_results.items():
        iter_out = [_iter_status(it, tps_tolerance) for it in iters]
        ok = all(i["status"] == "SUCCESS" for i in iter_out)
        configs[name] = {"status": "SUCCESS" if ok else "FAILED", "iterations": iter_out}
        overall_ok = overall_ok and ok
    return {"overall": "SUCCESS" if overall_ok else "FAILED", "configs": configs}
