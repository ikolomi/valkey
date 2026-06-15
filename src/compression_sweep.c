/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_sweep.c — sweeper engine.
 *
 * One file-static state struct. All entry points run on the main
 * thread (compressionCron, command handler, apply hooks, gtest
 * helpers). The sweeper interacts with the worker pool only via
 * compressionEnqueueCandidate (main-thread producer side) and with
 * the registry only via compressionPermanentlyDecompress (main thread).
 *
 * Internal model
 * --------------
 *
 * Two booleans + one timestamp + the iteration cursor capture
 * everything (header docstring has the full prose). Apply hooks
 * mutate state synchronously on every CONFIG SET; the cron tick
 * just consumes that state and runs work. There is no separate
 * polling-based edge detector — the model that used one (an
 * earlier 4-state machine) had a sub-tick edge bug because the
 * cron polls at ~100ms while CONFIG SETs can run faster. The
 * simpler apply-hook-driven model is immune by construction.
 */

#include "compression_sweep.h"
#include "compression.h"
#include "compression_registry.h"
#include "kvstore.h"
#include "server.h"

#include <stdint.h>
#include <stdio.h>

/* ========================================================================
 * State
 * ======================================================================== */

/* All sweeper state lives here. Touching it requires the main thread. */
typedef struct compressionSweepState {
    /* Mid-pass marker. Set when a pass starts; cleared on completion.
     * Distinct from `cursor != 0` because cursor can be 0 at any
     * intra-pass moment (kvstoreScan returns 0 between DB hand-offs);
     * scan_in_progress reflects the user-visible "is a scan running"
     * state without depending on the cursor's per-tick rhythm. */
    int scan_in_progress;

    /* One-shot trigger. Set by apply hooks (master change, sweeper
     * disabled→enabled) and by FORCE. Consumed by the next cron tick
     * that runs work. Persists across cron ticks until consumed. */
    int enable_once;

    /* Iteration cursor. Both fields are reset to 0 on pass start
     * (resetScanStateAndEnableOnce or completion). During SCANNING,
     * current_db points at the DB whose keyspace is currently being
     * iterated; cursor is its kvstoreScan cursor. */
    int current_db;
    unsigned long long cursor;

    /* mstime() of the most recent pass completion. Zero before the
     * first pass. Used by nextIntervalElapsed() to gate periodic
     * re-runs. */
    mstime_t last_completion_ms;

    /* Rate-limit cache for the master=compression + threads=0
     * non-functional warning. */
    mstime_t last_warning_ms;

    /* Cumulative INFO counters (lifetime of the process). */
    uint64_t passes_completed;
    uint64_t keys_processed;
} compressionSweepState;

static compressionSweepState sweep;

/* ========================================================================
 * State-mutation helpers (called from cron + apply hooks + FORCE)
 * ======================================================================== */

/* Reset cursor and arm enable_once. Aborts any in-flight pass and
 * schedules a fresh one for the next cron tick. Called by:
 *   - master-switch transitions to {compression, decompression} when
 *     compression-automatic-sweeper=enabled
 *   - compression-automatic-sweeper transitions disabled→enabled
 *     when master ∈ {compression, decompression}
 *   - COMPRESSION SWEEP FORCE (when master ≠ off)
 */
static void resetScanStateAndEnableOnce(void) {
    sweep.cursor = 0;
    sweep.current_db = 0;
    sweep.scan_in_progress = 0;
    sweep.enable_once = 1;
}

/* Abort any in-flight pass without scheduling a replacement. Called by:
 *   - master-switch transitions to off
 *   - compression-automatic-sweeper transitions enabled→disabled
 */
static void abortScan(void) {
    sweep.cursor = 0;
    sweep.current_db = 0;
    sweep.scan_in_progress = 0;
    sweep.enable_once = 0;
}

/* Decide if a periodic interval-driven re-run is due. Returns true iff:
 *   - sweeper config = enabled, AND
 *   - interval > 0 (interval=0 means "no periodic re-runs"), AND
 *   - at least one pass has completed (last_completion_ms != 0), AND
 *   - elapsed since last completion >= interval seconds. */
static int nextIntervalElapsed(void) {
    if (server.compression_automatic_sweeper != COMPRESSION_AUTOMATIC_SWEEPER_ENABLED) {
        return 0;
    }
    int interval = server.compression_automatic_sweeper_interval;
    if (interval <= 0) return 0;
    if (sweep.last_completion_ms == 0) return 0;
    mstime_t elapsed = mstime() - sweep.last_completion_ms;
    return elapsed >= (mstime_t)interval * 1000;
}

/* ========================================================================
 * Apply-hook callbacks
 * ======================================================================== */

void compressionSweepNotifyMasterSwitchChanged(void) {
    int master = server.compression_master_switch;
    int sweeper = server.compression_automatic_sweeper;

    if (master == COMPRESSION_MASTER_OFF) {
        /* No direction; abort any in-flight scan unconditionally. */
        abortScan();
        return;
    }
    /* Master is compression or decompression. Schedule a fresh pass
     * iff the operator has automatic scheduling enabled. With sweeper
     * disabled the operator wants to FORCE manually; do nothing. */
    if (sweeper == COMPRESSION_AUTOMATIC_SWEEPER_ENABLED) {
        resetScanStateAndEnableOnce();
    }
}

void compressionSweepNotifyAutomaticSweeperChanged(void) {
    int master = server.compression_master_switch;
    int sweeper = server.compression_automatic_sweeper;

    if (sweeper == COMPRESSION_AUTOMATIC_SWEEPER_DISABLED) {
        /* Symmetric to enable: abort whatever was in flight. The
         * operator just declared "no automatic background work". */
        abortScan();
        return;
    }
    /* Sweeper just enabled. Kick a fresh pass — but only if there's
     * a direction. master=off + sweeper=enabled is a documented
     * non-functional state (R2.1.6); the next master change handles
     * scheduling. */
    if (master != COMPRESSION_MASTER_OFF) {
        resetScanStateAndEnableOnce();
    }
}

/* ========================================================================
 * INFO accessors
 * ======================================================================== */

int compressionSweepIsRunning(void) {
    return sweep.scan_in_progress;
}

uint64_t compressionSweepGetPassesCompleted(void) {
    return sweep.passes_completed;
}

uint64_t compressionSweepGetKeysProcessed(void) {
    return sweep.keys_processed;
}

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

void compressionSweepInit(void) {
    memset(&sweep, 0, sizeof(sweep));
    /* Boot config might land in a state where the sweeper should
     * already be scheduled (sweeper=enabled + master ∈ {compression,
     * decompression}). The apply hook fires only on CONFIG SET, not
     * on boot, so handle that case here — same logic as
     * compressionSweepNotifyAutomaticSweeperChanged()'s enabled
     * branch. */
    if (server.compression_automatic_sweeper == COMPRESSION_AUTOMATIC_SWEEPER_ENABLED &&
        server.compression_master_switch != COMPRESSION_MASTER_OFF) {
        resetScanStateAndEnableOnce();
    }
}

void compressionSweepShutdown(void) {
    memset(&sweep, 0, sizeof(sweep));
}

/* ========================================================================
 * Force entry point
 * ======================================================================== */

int compressionSweepForce(void) {
    if (server.compression_master_switch == COMPRESSION_MASTER_OFF) {
        return COMPRESSION_SWEEP_FORCE_REJECTED;
    }
    /* FORCE preempts: any in-flight automatic scan is aborted and a
     * fresh pass starts from cursor 0 on the next cron tick. The
     * operator-facing semantic is "do a pass now, from scratch,
     * regardless of what was running". */
    resetScanStateAndEnableOnce();
    return COMPRESSION_SWEEP_FORCE_OK;
}

/* ========================================================================
 * Per-key callback
 * ======================================================================== */

typedef struct sweepCbCtx {
    int master;
    int dbid;
} sweepCbCtx;

static void sweepScanCallback(void *privdata, void *entry, int didx) {
    UNUSED(didx);
    sweepCbCtx *ctx = privdata;
    robj *val = entry;
    sweep.keys_processed++;

    if (ctx->master == COMPRESSION_MASTER_COMPRESSION) {
        /* Eligibility predicate inside compressionEnqueueCandidate
         * filters non-string values, non-RAW encodings, hot keys,
         * etc. Sweeper just hands every entry to the producer. */
        compressionEnqueueCandidate(NULL, val, ctx->dbid);
    } else if (ctx->master == COMPRESSION_MASTER_DECOMPRESSION) {
        /* Drain mode. Permanently decompress every compressed value;
         * non-compressed values are a cheap no-op. */
        if (val->encoding == OBJ_ENCODING_COMPRESSED) {
            compressionPermanentlyDecompress(val);
        }
    }
    /* master == off cannot reach here — sweepCron returns early. */
}

/* ========================================================================
 * Per-tick budget + cron
 * ======================================================================== */

/* Wall-clock budget in microseconds for one cron tick. Floor at 1ms. */
static long long tickBudgetUs(void) {
    int hz = server.hz > 0 ? server.hz : 10;
    int pct = server.compression_sweep_max_cpu_pct;
    if (pct <= 0) pct = 1;
    if (pct > 100) pct = 100;
    long long tick_us = 1000000LL / hz;
    long long budget_us = tick_us * pct / 100;
    if (budget_us < 1000) budget_us = 1000;
    return budget_us;
}

/* Run kvstoreScan until the budget expires or the pass completes.
 * Updates sweep.cursor / sweep.current_db in place. Returns 1 if the
 * pass completed during this tick. */
static int runSweepBudget(int master) {
    long long budget_us = tickBudgetUs();
    monotime start;
    elapsedStart(&start);

    while (sweep.current_db < server.dbnum) {
        serverDb *db = server.db[sweep.current_db];
        if (db == NULL) {
            sweep.current_db++;
            sweep.cursor = 0;
            continue;
        }

        sweepCbCtx ctx = {.master = master, .dbid = sweep.current_db};
        sweep.cursor = kvstoreScan(db->keys, sweep.cursor, -1,
                                   sweepScanCallback, NULL, &ctx);

        if (sweep.cursor == 0) {
            sweep.current_db++;
        }

        if ((long long)elapsedUs(start) >= budget_us) break;
    }

    if (sweep.current_db >= server.dbnum) {
        sweep.current_db = 0;
        sweep.cursor = 0;
        return 1;
    }
    return 0;
}

/* Rate-limited warning for master=compression + threads=0. Logged at
 * most once per minute. */
static void maybeWarnNoThreads(void) {
    mstime_t now = mstime();
    if (now - sweep.last_warning_ms < 60000) return;
    serverLog(LL_WARNING,
              "Compression sweeper: master=compression but compression-threads=0; "
              "skipping pass (every enqueue would be dropped). "
              "Raise compression-threads to enable productive sweeps.");
    sweep.last_warning_ms = now;
}

void compressionSweepCron(void) {
    int master = server.compression_master_switch;

    /* No direction; nothing to do. The apply hook for master-switch
     * already cleared in-flight state on the off transition. */
    if (master == COMPRESSION_MASTER_OFF) return;

    /* Determine whether to scan this tick. The trigger sources are:
     *   - enable_once: armed by an apply hook or FORCE.
     *   - scan_in_progress: a pass is mid-flight; keep going.
     *   - nextIntervalElapsed(): periodic re-run is due. */
    int interval_due = nextIntervalElapsed();
    if (!sweep.enable_once && !sweep.scan_in_progress && !interval_due) return;

    /* Consume the one-shot trigger. */
    sweep.enable_once = 0;

    /* Begin a fresh pass if we weren't mid-flight. */
    if (!sweep.scan_in_progress) {
        /* cursor + current_db should already be 0 (set by
         * resetScanStateAndEnableOnce() or by the previous pass's
         * completion). Defensive reset matches that invariant. */
        sweep.cursor = 0;
        sweep.current_db = 0;
        sweep.scan_in_progress = 1;
        serverLog(LL_NOTICE,
                  "Compression sweeper: %s pass starting%s.",
                  master == COMPRESSION_MASTER_COMPRESSION ? "compression" : "decompression",
                  interval_due ? " (interval expired)" : "");
    }

    /* Non-functional state: master=compression + threads=0. The
     * worker pool refuses every enqueue, so iterating the keyspace
     * just burns CPU. Skip the scan with a rate-limited warning;
     * scan_in_progress stays set so a future threads=N CONFIG SET
     * picks up where we left off. */
    if (master == COMPRESSION_MASTER_COMPRESSION && server.compression_threads == 0) {
        maybeWarnNoThreads();
        return;
    }

    /* Do work. */
    int completed = runSweepBudget(master);
    if (completed) {
        sweep.scan_in_progress = 0;
        sweep.last_completion_ms = mstime();
        sweep.passes_completed++;
        serverLog(LL_NOTICE,
                  "Compression sweeper: %s pass complete (%llu keys processed).",
                  master == COMPRESSION_MASTER_COMPRESSION ? "compression" : "decompression",
                  (unsigned long long)sweep.keys_processed);
    }
}

/* ========================================================================
 * Test-only entry points
 * ======================================================================== */

void testOnlyCompressionSweepReset(void) {
    compressionSweepInit();
}

void testOnlyCompressionSweepRunOneTick(void) {
    compressionSweepCron();
}
