# Plan 1 — valkey-benchmark `--latency-dump` (Piece 1)

_Implements design §4 / P1.1–P1.2. Dependency: none (lands first). Own CR on `src/`._
_TDD red→green; clang-format-18 on touched C; build with `-Werror`; review→commit→proceed._
_Commit prefix: conventional + DCO `-s` (this is a `src/valkey-benchmark.c` change, not the
benchmark subproject — use `feat(benchmark): …` or as the repo convention dictates)._

## Scope
Add opt-in `--latency-dump <FILE>`: after the (possibly windowed) measurement, write the loader
process's recorded hdr latency buckets so they can be summed losslessly across processes/iterations.
Independent of `-q`; composes with `--record-start-signal`.

## Pre-work (grounding)
- [ ] Read the existing flag-parse block + `--record-start-signal` windowing in
  `src/valkey-benchmark.c` (hdr reset at window start) and the recorded-iterator API in
  `deps/hdr_histogram/hdr_histogram.h` (`hdr_iter_recorded_init`, `iter.value_iterated_to`,
  `iter.count`, `total_count`).
- [ ] Find the benchmark integration test home (`tests/integration/*benchmark*` or equivalent) and
  the unit-test convention.

## Status: DONE + GREEN (uncommitted)
Implemented in `src/valkey-benchmark.c`; 3 Tcl tests pass; full benchmark suite green;
clang-format-18.1.8 clean; build `-Werror` clean. Windowing-composition test deferred to Plan 2
(Tcl blocking exec can't deliver the async signal) and replaced with a `-q`-independence test.

## Tasks (test-first)
- [x] **T1.1 (RED→GREEN)** dump exists + header + **Σcount == total == n** (exactness).
- [x] **T1.2 (revised)** `-q` independence: `-q --latency-dump` still writes the dump + still prints
  the rps line (windowing composition → Plan 2 e2e).
- [x] **T1.3** buckets are valid latencies within `[lowest,highest]`, counts > 0, ≥1 bucket.
- [x] **T1.4 (GREEN)** `--latency-dump <FILE>` parsed into `config.latency_dump_file`;
  `dumpLatencyHistogram()` writes the hdr header + recorded `value_usec,count` via
  `hdr_iter_recorded_init`; called at the top of `showReport()` (independent of `-q`/`--csv`).
- [x] **T1.5** `--help` usage line added after `-q` (ColumnLimit:0 ⇒ no width concern).
- [x] **T1.6** clang-format-18 clean (no changes); `make -C src valkey-benchmark
  SERVER_CFLAGS=-Werror`; tests green.

## Exit
`--latency-dump` produces a lossless, parseable per-process histogram; tests green; format frozen as
the input to Plan 2's parser (a copy of a real dump becomes Plan 2's frozen sample).
