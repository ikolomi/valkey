"""Compression-benchmark POST-PROCESSOR (Plan 3).

Consumes an orchestrator run directory (the stable ``info-measurement.json`` contract,
design §3) and produces:
  - ``report.json`` — reduced numbers (true merged percentiles, memory stats, deltas
    vs baseline, outlier flags); pure/testable, no third-party deps.
  - ``report.html`` — a self-contained interactive Plotly report rendered from it.

Reduction (``reduce``) is split from rendering (``render``); ``postprocess`` is the CLI.
"""
