"""Parse + losslessly merge valkey-benchmark ``--latency-dump`` histograms.

The orchestrator owns this stable representation (design Q5): it parses each loader
process's raw hdr dump (Plan 1 format) and sums them — per command, across the
iteration's processes — into the ``latency`` block of ``info-measurement.json``. The
post-processor later sums those per-iteration histograms across iterations. Because
every loader inits hdr with identical parameters, bucket value→count mappings are
identical, so summing counts by value is **exact** (no re-binning).

Dump format (Plan 1)::

    # hdr lowest=10 highest=3000000 sigfig=3 total_count=<N>
    <value_usec>,<count>
    ... (one line per non-empty recorded bucket)

Parsed form: ``{"hdr": {"lowest", "highest", "sigfig"}, "total_count": int,
"buckets": {value_usec: count}}``.
"""

from __future__ import annotations

import os
import re

_HDR_RE = re.compile(
    r"^#\s*hdr\s+lowest=(\d+)\s+highest=(\d+)\s+sigfig=(\d+)\s+total_count=(\d+)\s*$"
)


def parse_dump(text):
    """Parse a single ``--latency-dump`` file's contents. Raises ``ValueError`` on an
    empty/headerless/corrupt dump or when the bucket counts don't sum to the header's
    ``total_count`` (truncation guard)."""
    if not text or not text.strip():
        raise ValueError("empty latency dump")
    lines = text.strip().splitlines()
    m = _HDR_RE.match(lines[0].strip())
    if not m:
        raise ValueError(f"latency dump missing/!malformed hdr header: {lines[0]!r}")
    lowest, highest, sigfig, total_count = (int(g) for g in m.groups())

    buckets = {}
    for ln in lines[1:]:
        ln = ln.strip()
        if not ln or ln.startswith("#"):
            continue
        try:
            v_str, c_str = ln.split(",")
            value, count = int(v_str), int(c_str)
        except ValueError:
            raise ValueError(f"malformed latency dump bucket line: {ln!r}")
        # Identical hdr params ⇒ each value appears once, but be defensive and add.
        buckets[value] = buckets.get(value, 0) + count

    if sum(buckets.values()) != total_count:
        raise ValueError(
            f"latency dump count mismatch: header total_count={total_count} "
            f"but buckets sum to {sum(buckets.values())} (truncated/corrupt dump)")

    return {"hdr": {"lowest": lowest, "highest": highest, "sigfig": sigfig},
            "total_count": total_count, "buckets": buckets}


def sum_histograms(parsed_list):
    """Sum a non-empty list of parsed histograms by value. All must share identical
    hdr params (``ValueError`` otherwise — merging across incompatible layouts is
    unsound). Returns a parsed-form histogram."""
    if not parsed_list:
        raise ValueError("sum_histograms requires at least one histogram")
    hdr = parsed_list[0]["hdr"]
    merged = {}
    total = 0
    for h in parsed_list:
        if h["hdr"] != hdr:
            raise ValueError(
                f"cannot merge histograms with differing hdr params: {hdr} vs {h['hdr']}")
        for value, count in h["buckets"].items():
            merged[value] = merged.get(value, 0) + count
        total += h["total_count"]
    return {"hdr": dict(hdr), "total_count": total, "buckets": merged}


def parse_dump_file(path):
    """Convenience: read + :func:`parse_dump` a file path."""
    with open(path, "r") as f:
        return parse_dump(f.read())


def to_schema(merged):
    """Render a parsed/merged histogram into the JSON-serializable schema block stored
    in ``info-measurement.json`` (``buckets`` as a sorted ``[[value, count], ...]``
    list so it round-trips deterministically)."""
    return {
        "hdr": dict(merged["hdr"]),
        "total_count": merged["total_count"],
        "buckets": [[v, merged["buckets"][v]] for v in sorted(merged["buckets"])],
    }


def capture_per_command(loader_results):
    """Given collected loader results (each may carry a ``hist_path``), parse the
    per-process ``--latency-dump`` files and sum them **per command** into the stable
    ``latency`` schema block (design §3)::

        {"hdr": {...}, "per_command": {"<cmd>": {"total_count": N, "buckets": [[v, c], ...]}}}

    Returns ``None`` if no dumps are present (e.g. a benchmark without the flag), so
    callers can omit the block cleanly. Process dumps share identical hdr params, so
    the per-command sum is exact (``sum_histograms`` enforces this)."""
    by_cmd = {}
    for r in loader_results:
        hp = r.get("hist_path") if isinstance(r, dict) else None
        if not hp or not os.path.exists(hp):
            continue
        by_cmd.setdefault(r["command"], []).append(parse_dump_file(hp))
    if not by_cmd:
        return None
    hdr = None
    per_command = {}
    for cmd, hists in by_cmd.items():
        merged = sum_histograms(hists)
        if hdr is None:
            hdr = merged["hdr"]
        sch = to_schema(merged)
        per_command[cmd] = {"total_count": sch["total_count"], "buckets": sch["buckets"]}
    return {"hdr": hdr, "per_command": per_command}
