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
 *   2. Sweeper (THIS FILE; compression-active-sweeper enum: enabled /
 *      disabled) — drives keyspace toward declared state.
 *   3. Read-path transient view — read-time decompression (already in
 *      compression.c).
 *
 * Plus a periodic re-run knob:
 *   compression-active-sweeper-interval (seconds, default 0)
 *     0  = single pass on direction change, then idle.
 *     N>0 = re-run pass every N seconds after the previous pass
 *           completed.
 *
 * And an operator escape hatch:
 *   COMPRESSION SWEEP FORCE — one-shot pass, regardless of the
 *   compression-active-sweeper config; uses the master-switch's
 *   current direction; rejected if master=off.
 *
 * State machine (R2.10.1 surfaces these via the
 * compression_active_sweeper_state INFO field):
 *
 *     +-----------+
 *     | DISABLED  | (sweeper config = disabled and no FORCE pending)
 *     +-----------+
 *           ^                       ^
 *      cfg disabled              cfg enabled
 *           |                       |
 *     +-----------+ direction +-----------+
 *     |   IDLE    | <-------> | SCANNING  |
 *     +-----------+   change  +-----------+
 *           ^                       |
 *      interval                     v
 *      expires            (pass complete; interval=0 -> IDLE,
 *                          interval>0 -> SLEEPING)
 *           |
 *     +-----------+
 *     | SLEEPING  | (only entered when interval > 0)
 *     +-----------+
 *
 * Per-key behavior depends on the master switch's CURRENT value
 * (read on every cron tick, not snapshotted at pass start — mid-pass
 * direction changes resolve at the next tick):
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
 *   master == off            -> sweeper has no direction; idles. The
 *                               compression-active-sweeper config is
 *                               still honored, but every cron tick
 *                               is a no-op.
 *
 * Per-tick CPU budget: compression-sweep-max-cpu-pct (default 25%)
 * of the cron-tick wall time. Each tick scans up to that many
 * microseconds of work before yielding back to the event loop.
 *
 * Thread safety: EVERY function in this file runs on the main thread.
 * The sweeper interacts with the worker pool only via
 * compressionEnqueueCandidate (which is itself main-thread-only on
 * the producer side; the worker pool's thread-safety contract is
 * documented in compression_workers.c).
 */

#include "server.h"

#include <stddef.h>

/*
 * Sweeper state machine values. Reported by compressionSweepGetState
 * and rendered as the compression_active_sweeper_state INFO field.
 *
 * Values stable across versions — operators / dashboards may match
 * on the string names. Keep names in sync with sweeperStateName() in
 * compression_sweep.c.
 */
#define COMPRESSION_SWEEPER_STATE_DISABLED 0 /* config=disabled, no FORCE pending */
#define COMPRESSION_SWEEPER_STATE_IDLE 1     /* config=enabled, no current pass, no pending interval */
#define COMPRESSION_SWEEPER_STATE_SCANNING 2 /* a pass is in progress (cursor advancing) */
#define COMPRESSION_SWEEPER_STATE_SLEEPING 3 /* post-pass, waiting interval seconds */

/*
 * Lifecycle. Init creates the file-static state struct (zero-cost when
 * the feature is off). Shutdown clears it. Both are called from
 * compression.c's compressionInit / compressionShutdown, in that order
 * relative to other compression subsystems.
 */
void compressionSweepInit(void);
void compressionSweepShutdown(void);

/*
 * Cron tick. Called from compressionCron once per serverCron iteration.
 * Reads the master-switch + sweeper config, advances the state machine,
 * and (if SCANNING) does up to compression-sweep-max-cpu-pct of work
 * before returning. Non-blocking.
 */
void compressionSweepCron(void);

/*
 * Operator-driven force-pass entry point. Called from the
 * COMPRESSION SWEEP FORCE command handler.
 *
 *   master=off: returns COMPRESSION_SWEEP_FORCE_REJECTED. The command
 *               handler translates this to a -ERR reply.
 *
 *   otherwise:  arms a force-pending flag. The next cron tick picks
 *               it up and transitions to SCANNING regardless of
 *               compression-active-sweeper / current state. Returns
 *               COMPRESSION_SWEEP_FORCE_OK.
 *
 * Idempotent — repeated calls during a single cron-tick window
 * collapse into a single force-pass.
 */
#define COMPRESSION_SWEEP_FORCE_OK 0
#define COMPRESSION_SWEEP_FORCE_REJECTED 1
int compressionSweepForce(void);

/*
 * Edge-trigger helper for master-switch transitions.
 *
 * Called from the master-switch apply hook (compression.c) on every
 * non-equal value change. The sweeper consumes the flag in its next
 * cron tick to detect direction changes that occur faster than cron
 * polling could observe (R2.1.5 — direction-change semantics must not
 * miss compression→off→compression sub-tick edges).
 */
void compressionSweepNotifyMasterSwitchChanged(void);

/*
 * State + counter accessors. Used by the INFO renderer (compression.c)
 * and by gtests. Stable values across versions; safe to call at any
 * time.
 */
int compressionSweepGetState(void);
const char *compressionSweepStateName(int state);
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
