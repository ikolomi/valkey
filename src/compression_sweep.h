/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef VALKEY_COMPRESSION_SWEEP_H
#define VALKEY_COMPRESSION_SWEEP_H

/*
 * compression_sweep.h — background sweeper that drives the keyspace
 * toward the master-switch-declared state.
 *
 * The sweeper is one of the three orthogonal mechanics in the v1
 * declarative-master-switch design (PR-B / detailed-design.md §2.1):
 *
 *   1. Master switch (compression-master-switch enum: off /
 *      compression / decompression) — operator declares desired state.
 *   2. Sweeper (THIS FILE) — drives keyspace toward declared state.
 *      Two configs:
 *        compression-automatic-sweeper (enabled / disabled) —
 *           gates AUTOMATIC scheduling: master-switch transitions
 *           and interval-based re-runs. Manual FORCE bypasses it.
 *        compression-automatic-sweeper-interval (seconds, default 0) —
 *           0   = single pass on automatic trigger, then idle.
 *           N>0 = re-run pass every N seconds after the previous
 *                 pass completed (only when sweeper=enabled).
 *   3. Read-path transient view — read-time decompression (in
 *      compression.c).
 *
 * Operator escape hatch:
 *   COMPRESSION SWEEP FORCE — one-shot pass, regardless of the
 *   compression-automatic-sweeper config; uses the master switch's
 *   current direction; rejected if master=off; **preempts any
 *   in-flight scan and starts fresh from cursor 0**.
 *
 * Internal model
 * --------------
 *
 * Two booleans + one timestamp + the iteration cursor capture
 * everything (much simpler than the 4-state machine an earlier draft
 * used; reverted because the apply-hook-driven model removes the
 * polling-vs-edge tension that the state machine was managing):
 *
 *   scan_in_progress    1 iff cursor is mid-pass (set when a pass
 *                       starts; cleared on completion).
 *   enable_once         1 iff a one-shot scan has been requested by
 *                       an apply hook or FORCE; consumed by the next
 *                       cron tick that runs a scan.
 *   last_completion_ms  mstime() of the most recent pass completion;
 *                       0 before the first pass. Used by the interval
 *                       check to decide if a periodic re-run is due.
 *
 * Two helper actions on those booleans:
 *
 *   resetScanStateAndEnableOnce()
 *     cursor=0; current_db=0; scan_in_progress=0; enable_once=1.
 *     Called by master-switch transitions to {compression,
 *     decompression}, by sweeper config disabled→enabled, and by
 *     FORCE. Aborts any in-flight scan and schedules a fresh pass on
 *     the next cron tick.
 *
 *   abortScan()
 *     cursor=0; current_db=0; scan_in_progress=0; enable_once=0.
 *     Called by master-switch → off and by sweeper config
 *     enabled→disabled. Aborts in-flight work; no replacement
 *     scheduled.
 *
 * Cron tick:
 *
 *   if master == off: return.                          // no direction
 *   if !enable_once && !scan_in_progress
 *      && !nextIntervalElapsed(): return.              // nothing to do
 *   enable_once = 0; scan_in_progress = 1.
 *   run sweep budget.
 *   if pass complete: scan_in_progress=0;
 *                     last_completion_ms=now;
 *                     log + bump passes_completed.
 *
 *   nextIntervalElapsed() returns true iff
 *     compression-automatic-sweeper == enabled
 *     AND compression-automatic-sweeper-interval > 0
 *     AND last_completion_ms != 0
 *     AND (now - last_completion_ms) >= interval*1000.
 *
 * Per-key behavior depends on the master switch's CURRENT value
 * (read on every cron tick, not snapshotted at pass start — mid-pass
 * direction changes resolve at the next tick if the apply hook
 * resets cursor + schedules):
 *
 *   master == compression    -> compressionEnqueueCandidate(...) for
 *                               each RAW string value (existing
 *                               eligibility predicate filters).
 *
 *   master == decompression  -> compressionPermanentlyDecompress(...)
 *                               for each compressed value, on the
 *                               main thread (worker pool is unused
 *                               in this direction).
 *
 *   master == off            -> cron returns early; nothing runs.
 *
 * Per-tick CPU budget: compression-sweep-max-cpu-pct (default 25%)
 * of the cron-tick wall time. Each tick scans up to that many
 * microseconds of work before yielding back to the event loop.
 *
 * Thread safety: EVERY function in this file runs on the main thread.
 * The sweeper interacts with the worker pool only via
 * compressionEnqueueCandidate (main-thread producer side).
 */

#include "server.h"

#include <stddef.h>
#include <stdint.h>

/*
 * Lifecycle. Init zeroes the file-static state struct (zero-cost when
 * the feature is off). If the boot config has compression-automatic-
 * sweeper=enabled and master ∈ {compression, decompression}, init
 * also arms enable_once so the first cron tick after boot runs a
 * pass (matches the disabled→enabled apply-hook behavior at runtime).
 * Shutdown clears state. Both are called from compression.c's
 * compressionInit / compressionShutdown.
 */
void compressionSweepInit(void);
void compressionSweepShutdown(void);

/*
 * Cron tick. Called from compressionCron once per serverCron iteration.
 * Non-blocking. See file-level docstring for the full state-machine
 * pseudocode.
 */
void compressionSweepCron(void);

/*
 * Operator-driven force-pass entry point. Called from the
 * COMPRESSION SWEEP FORCE command handler.
 *
 *   master=off: returns COMPRESSION_SWEEP_FORCE_REJECTED. The command
 *               handler translates this to a -ERR reply.
 *
 *   otherwise:  resets cursor + arms enable_once. The next cron tick
 *               starts a fresh pass from the top, regardless of
 *               compression-automatic-sweeper. **Preempts any
 *               in-flight scan** — that's the user-facing semantic
 *               of "do a pass now, from scratch".
 *
 * Idempotent — repeated calls during a single cron-tick window
 * collapse into one fresh pass.
 */
#define COMPRESSION_SWEEP_FORCE_OK 0
#define COMPRESSION_SWEEP_FORCE_REJECTED 1
int compressionSweepForce(void);

/*
 * Apply-hook callbacks (called from compression.c apply hooks
 * synchronously on every CONFIG SET that changes the value):
 *
 *   compressionSweepNotifyMasterSwitchChanged(new_master)
 *     new_master == off                  -> abortScan()
 *     compression-automatic-sweeper=on   -> resetScanStateAndEnableOnce()
 *     compression-automatic-sweeper=off  -> nothing (sweeper config
 *                                          gates automatic scheduling)
 *
 *   compressionSweepNotifyAutomaticSweeperChanged(new_value)
 *     new_value == enabled, master != off -> resetScanStateAndEnableOnce()
 *     new_value == disabled               -> abortScan()
 *     new_value == enabled, master == off -> nothing (no direction)
 *
 * Why apply-hook driven (not cron-poll driven): the cron polls every
 * ~100ms; faster transitions like compression→off→compression within
 * one tick window would be invisible to a polled-comparison model.
 * Apply hooks fire synchronously on every CONFIG SET, so no edge is
 * missed.
 */
void compressionSweepNotifyMasterSwitchChanged(void);
void compressionSweepNotifyAutomaticSweeperChanged(void);

/*
 * INFO accessors. Stable across versions; safe to call any time.
 *
 *   compressionSweepIsRunning()   1 iff a pass is currently mid-flight
 *                                 (rendered as compression_sweeper_running).
 *   compressionSweepGetPassesCompleted()  cumulative completed passes.
 *   compressionSweepGetKeysProcessed()    cumulative keys scanned.
 */
int compressionSweepIsRunning(void);
uint64_t compressionSweepGetPassesCompleted(void);
uint64_t compressionSweepGetKeysProcessed(void);

/*
 * Test-only accessors. Declared here (not behind an ifdef) so unit
 * tests can include the production header without macro juggling.
 * Production callers should not use these.
 */
void testOnlyCompressionSweepReset(void);
void testOnlyCompressionSweepRunOneTick(void);

#endif /* VALKEY_COMPRESSION_SWEEP_H */
