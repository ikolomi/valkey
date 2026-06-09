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
 * to the worker pool) or the in-place permanent-decompress (per
 * R2.1.4) for every visited string value.
 *
 * Triggers (any of):
 *   - master-switch transition `compression-enabled no → yes`
 *     (auto-initiates a compress-direction sweep — R2.1.3)
 *   - explicit `COMPRESSION SWEEP [direction=compress|decompress]`
 *     command (operator-driven; covers the R2.1.4 disable-side drain
 *     when run with direction=decompress)
 *
 * Pacing: per-tick wall-time budget computed from `compression-sweep-
 * max-cpu-pct` and `server.hz`. Default 25% × 1000ms / 10 hz = 25 ms
 * per tick. The budget bounds main-thread time spent on the sweep;
 * the worker pool absorbs the queued compression work asynchronously.
 *
 * Mirrors compression_train.c's pattern: monotime-budgeted state
 * machine with a persistent (DB index, kvstore cursor) cursor across
 * cron ticks. Single sweep can run at a time — overlapping requests
 * collapse to one (last-direction-wins for the IDLE→SCANNING
 * transition; an in-flight sweep cannot be re-targeted mid-run).
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

/* Called from compressionCron each tick. Advances the scan if a sweep
 * is in progress; honors the per-tick wall-time budget; transitions
 * the state machine. No-op when the master switch is off OR no sweep
 * is requested OR a previous sweep is already scanning (one at a
 * time). */
void compressionSweepCron(void);

/* Request a sweep. Sets the `requested` flag plus the direction;
 * compressionSweepCron picks it up on the next tick.
 *
 * Returns 1 if the request was queued, 0 if a sweep was already
 * in flight (the request is dropped silently — operators can re-issue
 * after the in-flight sweep completes). The 0-return is informational
 * only; correctness does not depend on it.
 *
 * Direction must be COMPRESSION_SWEEP_DIR_COMPRESS or
 * COMPRESSION_SWEEP_DIR_DECOMPRESS. */
int compressionSweepRequest(int direction);

/* Test-only: introspect current state. Returns 1 if the state machine
 * is in SCANNING, 0 if IDLE. Tests use this to verify a request was
 * accepted. */
int compressionSweepIsScanning(void);

/* Test-only: synchronously advance the sweep until completion or
 * `max_iterations` cron ticks elapsed. Returns the number of ticks
 * actually executed. Allows unit tests to drive the state machine
 * without a real serverCron loop. */
int compressionSweepDriveForTesting(int max_iterations);

#endif /* VALKEY_COMPRESSION_SWEEP_H */
