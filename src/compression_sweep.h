/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_sweep.h — keyspace sweep state machine.
 *
 * The sweep walks the keyspace shard-at-a-time across cron ticks,
 * applying either the compress eligibility predicate (and enqueuing
 * to the worker pool) or in-place permanent-decompress for every
 * visited string value.
 *
 * Triggered exclusively by the operator-issued `COMPRESSION SWEEP
 * [direction=compress|decompress]` command. Per R2.1.6, master-switch
 * toggles do NOT auto-trigger sweeps in either direction; existing
 * in-memory state is preserved across `compression-enabled` toggles
 * and conversion is operator-driven. The auto-trigger affordance is
 * an opt-in v2 extension (Appendix D —
 * `compression-auto-sweep-on-{enable,disable}`).
 *
 * Pacing: per-tick wall-time budget computed from `compression-sweep-
 * max-cpu-pct` and `server.hz`. Default 25% × 1000ms / 10 hz = 25 ms
 * per tick. The budget bounds main-thread time spent on the sweep;
 * the worker pool absorbs the queued compression work asynchronously.
 *
 * Mirrors compression_train.c's pattern: monotime-budgeted state
 * machine with a persistent (DB index, kvstore cursor) cursor across
 * cron ticks. Two states (IDLE / SCANNING) plus a direction property;
 * the authoritative state-machine table lives in
 * detailed-design.md §3.4 and covers every (state × trigger ×
 * master-switch) cell. R2.1.3, R2.1.4, R2.1.6, R2.5.7, and R2.9.2
 * all read against that table.
 *
 * Single-flight: only one sweep can run at a time. The
 * compressionSweepRequest API is the low-level state-machine entry
 * point and silently drops a second request while SCANNING; the
 * COMPRESSION SWEEP command handler layered on top adds the friendly
 * error message and the direction-conditional master-switch guard
 * (compress requires the master switch on; decompress does not).
 * v1 does not queue requests or support direction-change while
 * SCANNING — the operator's escape hatch for an in-flight COMPRESS
 * sweep is to toggle `compression-enabled` off, which the cron
 * observes and aborts on. A SCANNING(DECOMPRESS) sweep has no abort
 * affordance (idempotent; runs to completion).
 *
 * Only one sweep state machine instance exists per process — the
 * keyspace is a singleton.
 */

#ifndef VALKEY_COMPRESSION_SWEEP_H
#define VALKEY_COMPRESSION_SWEEP_H

/* Sweep direction. The integer values match the public
 * `compressionSweep(client *c, int direction)` API in compression.h
 * (`1 = compress, -1 = decompress`). */
#define COMPRESSION_SWEEP_DIR_COMPRESS 1
#define COMPRESSION_SWEEP_DIR_DECOMPRESS (-1)

/* Lifecycle — called by compressionInit / compressionShutdown. */
void compressionSweepInit(void);
void compressionSweepRelease(void);

/* Called from compressionCron each tick. Drives the state machine
 * per the table in detailed-design.md §3.4. Summary of the cases the
 * cron MUST handle directly (the command handler sits in front for
 * the request side of the table):
 *
 *   IDLE.requested(COMPRESS) + master_off  → defensive clear of the
 *     queued request without entering SCANNING. The command handler
 *     should already have rejected; reaching the cron with this
 *     state means a future caller bypassed it.
 *   IDLE.requested(D) + (otherwise)        → enterScanning(D).
 *   SCANNING(COMPRESS) + master_off        → abort, log NOTICE,
 *     enterIdle. Honors R2.1.4 — "new writes stop being compressed"
 *     extends to background work. In-flight worker jobs already
 *     enqueued by sweepScanCallback before this tick still complete
 *     and install (workers don't observe the master switch — R2.4 /
 *     R2.11.4); the sweep state machine is the authoritative lever.
 *   SCANNING(DECOMPRESS) + (any switch)    → advanceScan(). The
 *     decompress sweep is the canonical drain path for R2.1.4 and
 *     runs regardless of master-switch state.
 *   SCANNING(COMPRESS) + master_on         → advanceScan().
 */
void compressionSweepCron(void);

/* Low-level state-machine entry point: queue a sweep request. Sets
 * the `requested` flag plus the direction; compressionSweepCron picks
 * it up on the next tick.
 *
 * Returns 1 if the request was queued, 0 if a sweep is already
 * SCANNING (single-flight rejection). The 0-return is informational
 * for the caller; correctness does not depend on it.
 *
 * NOTE: this API does NOT enforce the master-switch / direction
 * constraints. Those live one layer up in `compressionSweep` (the
 * COMPRESSION SWEEP command handler in compression.c), which rejects
 * COMPRESS requests when the master switch is off and renders
 * single-flight rejections as a clear error message. Internal
 * callers (currently none in v1; the auto-trigger affordance was
 * removed per R2.1.6) MUST do their own guarding before reaching
 * here. The cron's defensive clear (see compressionSweepCron above)
 * catches the COMPRESS-while-disabled corner case as a backstop.
 *
 * Direction must be COMPRESSION_SWEEP_DIR_COMPRESS or
 * COMPRESSION_SWEEP_DIR_DECOMPRESS. */
int compressionSweepRequest(int direction);

/* Returns 1 if the state machine is currently in SCANNING, 0 if IDLE.
 *
 * Used by:
 *   - the transient-view drain in `compressionBeforeSleep` (R2.5.7),
 *     paired with `compressionSweepCurrentDirection()` to decide
 *     whether to cooperate with an in-flight decompress sweep
 *   - unit tests verifying state transitions
 *
 * Cheap (single read of the file-static state from the main thread;
 * no atomics). */
int compressionSweepIsScanning(void);

/* Returns the direction of the in-flight sweep
 * (COMPRESSION_SWEEP_DIR_COMPRESS or COMPRESSION_SWEEP_DIR_DECOMPRESS),
 * or 0 if no sweep is currently scanning. Used by the transient-view
 * drain in `compressionBeforeSleep` to cooperate with an
 * operator-initiated decompress sweep — when this returns
 * COMPRESSION_SWEEP_DIR_DECOMPRESS, the drain switches to
 * permanent-decompress mode (R2.5.7) instead of restoring the
 * compressed form. Cheap (single int read; no atomics, single-threaded
 * access from the main thread). */
int compressionSweepCurrentDirection(void);

/* Test-only: synchronously advance the sweep until completion or
 * `max_iterations` cron ticks elapsed. Returns the number of ticks
 * actually executed. Allows unit tests to drive the state machine
 * without a real serverCron loop. */
int compressionSweepDriveForTesting(int max_iterations);

#endif /* VALKEY_COMPRESSION_SWEEP_H */
