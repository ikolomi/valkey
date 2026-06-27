"""CLI: reduce an orchestrator run directory → ``report.json`` + ``report.html``.

Usage:
    python3 -m postprocessor.postprocess <run-dir> [-o report.html] [--json report.json]
    python3 postprocessor/postprocess.py <run-dir>

Reduction is split from rendering (idea-honing Q1b): ``report.json`` holds the reduced
numbers; ``report.html`` is the self-contained interactive Plotly view of it.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

try:  # package invocation (python -m postprocessor.postprocess)
    from postprocessor import reduce, render
except ImportError:  # direct script invocation (python postprocessor/postprocess.py)
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    import reduce  # type: ignore
    import render  # type: ignore


def main(argv=None):
    ap = argparse.ArgumentParser(description="Reduce + visualize a compression-benchmark run.")
    ap.add_argument("run_dir", help="orchestrator run directory (the <timestamp> dir)")
    ap.add_argument("-o", "--output", default=None, help="HTML output path (default: <run-dir>/report.html)")
    ap.add_argument("--json", dest="json_out", default=None,
                    help="reduced-numbers JSON path (default: <run-dir>/report.json)")
    args = ap.parse_args(argv)

    if not os.path.isdir(args.run_dir):
        ap.error(f"run directory not found: {args.run_dir}")

    report = reduce.build_report(args.run_dir)
    json_path = args.json_out or os.path.join(args.run_dir, "report.json")
    html_path = args.output or os.path.join(args.run_dir, "report.html")
    with open(json_path, "w") as f:
        json.dump(report, f, indent=2)
    with open(html_path, "w") as f:
        f.write(render.render(report))
    print(f"wrote {json_path}")
    print(f"wrote {html_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
