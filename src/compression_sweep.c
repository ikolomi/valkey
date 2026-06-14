/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_sweep.c — sweeper engine.
 *
 * One file-static state struct. All entry points run on the main
 * thread (compressionCron, command handler, gtest hooks). The sweeper
 * interacts with the worker pool only through the producer-side API
 * compressionEnqueueCandidate, which itself is main-thread-only.
 *
 * Per-tick design:
 *   1. Read master switch and active-sweeper config (fresh every tick;
 *      the design says direction changes resolve at the next tick).
 *   2. Detect direction change vs. last_master_seen; if changed,
 *      reset cursor.
 *   3. Compute next state from (config, master, force_pending,
 *      current_state, interval).
 *   4. If SCANNING, do up to budget_us of work via kvstoreScan.
 *   5. If we wrapped through all dbs, mark pass complete; transition
 *      to IDLE (interval=0) or SLEEPING (interval>0).
 *
 * Per-key callback dispatch:
 *   master == compression  -> compressionEnqueueCandidate (filtered
 *                             by the existing eligibility predicate;
 *                             non-RAW values fall through cheaply).
 *   master == decompression -> compressionPermanentlyDecompress for
 *                              compressed values; skip everything else.
 *
 * Non-functional state warning (R2.1.6): master=compression +
 * compression-threads=0 is allowed but pointless — every enqueue
 * drops at the producer side. The cron tick logs a rate-limited
 * warning (1/min) and skips the pass.
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

/* All sweeper state lives in this single file-static struct. Touching
 * it requires the main thread (no locking). Init zero-fills; shutdown
 * resets to the same shape. */
typedef struct compressionSweepState {
    int state;                     /* COMPRESSION_SWEEPER_STATE_* */
    int last_master;               /* tracked to detect master-switch changes */
    int force_pending;             /* COMPRESSION SWEEP FORCE was requested */
    int current_db;                /* 0..server.dbnum-1 during SCANNING */
    unsigned long long cursor;     /* persisted across ticks within a pass */
    mstime_t pass_started_ms;
    mstime_t pass_completed_ms;    /* base for SLEEPING-state interval timer */
    mstime_t last_warning_ms;      /* rate-limit for the threads=0 warning */
    /* Counters surfaced via INFO. Reset only on init/shutdown — they
     * are cumulative since process start. */
    uint64_t passes_completed;
    uint64_t keys_processed;
} compressionSweepState;

static compressionSweepState sweep;

/* Set by compressionSweepNotifyMasterSwitchChanged() (called from the
 * master-switch apply hook) on every transition. The cron tick consumes
 * the flag — clearing it — to detect direction changes that happen
 * faster than cron polling could observe (e.g., compression→off→
 * compression within a single 100ms tick window). The cron alone
 * can't see those edges because it only ever sees the current value,
 * not the history.
 *
 * Main-thread only: apply hooks and cron both run on the main thread,
 * so a plain int suffices. */
static int sweep_master_switch_changed = 0;

void compressionSweepNotifyMasterSwitchChanged(void) {
    sweep_master_switch_changed = 1;
}

/* Map state values to user-visible names for log lines + INFO. Stays
 * in sync with the COMPRESSION_SWEEPER_STATE_* block in the header. */
static const char *kStateNames[] = {
    [COMPRESSION_SWEEPER_STATE_DISABLED] = "disabled",
    [COMPRESSION_SWEEPER_STATE_IDLE] = "idle",
    [COMPRESSION_SWEEPER_STATE_SCANNING] = "scanning",
    [COMPRESSION_SWEEPER_STATE_SLEEPING] = "sleeping",
};

const char *compressionSweepStateName(int state) {
    if (state < 0 || state > COMPRESSION_SWEEPER_STATE_SLEEPING) return "?";
    return kStateNames[state];
}

int compressionSweepGetState(void) {
    return sweep.state;
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
    sweep.state = COMPRESSION_SWEEPER_STATE_DISABLED;
    sweep.last_master = COMPRESSION_MASTER_OFF;
}

void compressionSweepShutdown(void) {
    /* Same shape as init — nothing to free. The sweeper holds no
     * heap-allocated state; it's a pure cron-driven state machine. */
    memset(&sweep, 0, sizeof(sweep));
    sweep.state = COMPRESSION_SWEEPER_STATE_DISABLED;
}

/* ========================================================================
 * Force entry point
 * ======================================================================== */

int compressionSweepForce(void) {
    if (server.compression_master_switch == COMPRESSION_MASTER_OFF) {
        return COMPRESSION_SWEEP_FORCE_REJECTED;
    }
    /* Idempotent: if force is already pending or a pass is already
     * scanning, this is a no-op. The cron tick handles transition. */
    sweep.force_pending = 1;
    return COMPRESSION_SWEEP_FORCE_OK;
}

/* ========================================================================
 * Per-key callback
 * ======================================================================== */

/* Privdata threaded through kvstoreScan to the per-key callback. */
typedef struct sweepCbCtx {
    int master; /* current master-switch value at tick start */
    int dbid;   /* index into server.db[] for this scan call */
} sweepCbCtx;

static void sweepScanCallback(void *privdata, void *entry, int didx) {
    UNUSED(didx);
    sweepCbCtx *ctx = privdata;
    robj *val = entry;
    sweep.keys_processed++;

    if (ctx->master == COMPRESSION_MASTER_COMPRESSION) {
        /* Eligibility predicate inside compressionEnqueueCandidate
         * filters non-string values, non-RAW encodings, hot keys,
         * etc. Sweeper just hands every entry to the producer; the
         * existing R2.2 logic decides whether to actually enqueue. */
        compressionEnqueueCandidate(NULL, val, ctx->dbid);
    } else if (ctx->master == COMPRESSION_MASTER_DECOMPRESSION) {
        /* Drain mode. Permanently decompress every compressed value.
         * Non-compressed values are a no-op (the helper checks the
         * encoding and returns 0 if not COMPRESSED). */
        if (val->encoding == OBJ_ENCODING_COMPRESSED) {
            compressionPermanentlyDecompress(val);
        }
    }
    /* master == off cannot reach here — sweepCron returns early. */
}

/* ========================================================================
 * Per-tick budget + state machine
 * ======================================================================== */

/* Compute the wall-clock budget for this tick in microseconds, derived
 * from the cron tick interval (1/hz seconds) and the sweep-pacing
 * percent. Floor at 1ms — useful work needs at least one kvstoreScan
 * call's worth of time. */
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

/* Run the sweep until the wall-clock budget expires or until the pass
 * completes (current_db wraps past dbnum). Updates sweep.cursor and
 * sweep.current_db in place. Returns 1 if the pass completed during
 * this tick, 0 otherwise. */
static int runSweepBudget(int master) {
    long long budget_us = tickBudgetUs();
    monotime start;
    elapsedStart(&start);

    while (sweep.current_db < server.dbnum) {
        serverDb *db = server.db[sweep.current_db];
        if (db == NULL) {
            /* Unmaterialized DB slot. Skip. */
            sweep.current_db++;
            sweep.cursor = 0;
            continue;
        }

        sweepCbCtx ctx = {.master = master, .dbid = sweep.current_db};
        sweep.cursor = kvstoreScan(db->keys, sweep.cursor, -1,
                                   sweepScanCallback, NULL, &ctx);

        if (sweep.cursor == 0) {
            /* This DB done; move on. */
            sweep.current_db++;
        }

        if ((long long)elapsedUs(start) >= budget_us) break;
    }

    if (sweep.current_db >= server.dbnum) {
        /* Pass complete. */
        sweep.current_db = 0;
        sweep.cursor = 0;
        return 1;
    }
    return 0;
}

/* Rate-limited warning for master=compression + threads=0. Logged
 * at most once per minute so a misconfigured operator gets a clear
 * signal but doesn't drown the log. */
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
    int sweeper_cfg = server.compression_active_sweeper;

    /* master=off: sweeper has no direction. Abort any in-flight scan
     * (cursor lost) and reflect the config in state. FORCE is rejected
     * at the command level when master=off, so we shouldn't see
     * force_pending here, but defend anyway. */
    if (master == COMPRESSION_MASTER_OFF) {
        sweep.force_pending = 0;
        sweep.cursor = 0;
        sweep.current_db = 0;
        sweep.state = (sweeper_cfg == COMPRESSION_ACTIVE_SWEEPER_ENABLED)
                          ? COMPRESSION_SWEEPER_STATE_IDLE
                          : COMPRESSION_SWEEPER_STATE_DISABLED;
        sweep.last_master = master;
        sweep_master_switch_changed = 0; /* consumed; nothing to scan */
        return;
    }

    /* Direction change detection. We can't rely on
     * (master != sweep.last_master) alone because the cron polls every
     * ~100ms and master can flip compression→off→compression faster
     * than that — leaving last_master==master at every observed tick.
     * The apply hook sets sweep_master_switch_changed on every
     * transition; we OR that with the polled comparison so neither
     * source of edge is missed.
     *
     * Reset cursor on direction change AND trigger a fresh pass (if
     * config enabled). Even mid-pass: per design, the sweeper takes
     * its direction from the current master switch, and "any direction
     * change with compression-active-sweeper=enabled wakes the
     * sweeper, resets cursor, restarts pass with the new direction"
     * (R2.1.5). */
    int direction_changed = sweep_master_switch_changed ||
                            (master != sweep.last_master);
    sweep_master_switch_changed = 0;
    sweep.last_master = master;
    if (direction_changed && sweep.state == COMPRESSION_SWEEPER_STATE_SCANNING) {
        serverLog(LL_NOTICE,
                  "Compression sweeper: direction changed mid-pass to %s, "
                  "restarting from cursor 0.",
                  master == COMPRESSION_MASTER_COMPRESSION ? "compression" : "decompression");
        sweep.cursor = 0;
        sweep.current_db = 0;
    }

    /* Decide whether to scan this tick. The triggers are:
     *
     *   - Already in SCANNING (committed to the pass — keep going).
     *   - FORCE pending (operator-driven; wins over everything except
     *     master=off, which is handled above).
     *   - config=enabled AND (direction changed | config just
     *     transitioned to enabled | SLEEPING with interval expired).
     *
     * If none of those: stay in current non-scanning state, possibly
     * adjusting it to reflect config. */
    int should_scan = (sweep.state == COMPRESSION_SWEEPER_STATE_SCANNING);

    if (sweep.force_pending) {
        if (!should_scan) {
            serverLog(LL_NOTICE,
                      "Compression sweeper: force-pass starting (master=%s).",
                      master == COMPRESSION_MASTER_COMPRESSION ? "compression" : "decompression");
        }
        should_scan = 1;
        sweep.force_pending = 0;
    } else if (sweeper_cfg == COMPRESSION_ACTIVE_SWEEPER_ENABLED && !should_scan) {
        if (direction_changed) {
            should_scan = 1;
            serverLog(LL_NOTICE,
                      "Compression sweeper: direction changed to %s, starting pass.",
                      master == COMPRESSION_MASTER_COMPRESSION ? "compression" : "decompression");
        } else if (sweep.state == COMPRESSION_SWEEPER_STATE_DISABLED) {
            should_scan = 1;
            serverLog(LL_NOTICE,
                      "Compression sweeper: enabled, starting pass (master=%s).",
                      master == COMPRESSION_MASTER_COMPRESSION ? "compression" : "decompression");
        } else if (sweep.state == COMPRESSION_SWEEPER_STATE_SLEEPING) {
            int interval = server.compression_active_sweeper_interval;
            if (interval > 0) {
                mstime_t elapsed = mstime() - sweep.pass_completed_ms;
                if (elapsed >= (mstime_t)interval * 1000) {
                    should_scan = 1;
                    serverLog(LL_NOTICE,
                              "Compression sweeper: interval expired, starting pass.");
                }
            }
            /* interval==0 in SLEEPING is a transitional anomaly (interval
             * was changed to 0 while sleeping). Stay sleeping forever
             * until direction change or FORCE; matches the
             * "interval=0 = no periodic re-runs" semantic. */
        }
        /* IDLE with no direction-change/force/interval: stay idle. */
    }

    if (!should_scan) {
        /* Reflect config in state without starting a pass. */
        if (sweeper_cfg == COMPRESSION_ACTIVE_SWEEPER_DISABLED) {
            sweep.state = COMPRESSION_SWEEPER_STATE_DISABLED;
        } else if (sweep.state == COMPRESSION_SWEEPER_STATE_DISABLED) {
            /* Config is enabled but we didn't trigger; transitional. */
            sweep.state = COMPRESSION_SWEEPER_STATE_IDLE;
        }
        return;
    }

    /* SCANNING. Reset state if we're transitioning into SCANNING this
     * tick (vs. continuing an in-flight pass). */
    if (sweep.state != COMPRESSION_SWEEPER_STATE_SCANNING) {
        sweep.cursor = 0;
        sweep.current_db = 0;
        sweep.state = COMPRESSION_SWEEPER_STATE_SCANNING;
        sweep.pass_started_ms = mstime();
    }

    /* Non-functional state: master=compression + threads=0. The
     * worker pool refuses every enqueue, so iterating the keyspace
     * just burns CPU. Skip the pass (state stays SCANNING; a future
     * threads=N CONFIG SET picks up where we left off). */
    if (master == COMPRESSION_MASTER_COMPRESSION && server.compression_threads == 0) {
        maybeWarnNoThreads();
        return;
    }

    /* Do work. Returns 1 if the pass completed during this tick. */
    int completed = runSweepBudget(master);
    if (completed) {
        sweep.passes_completed++;
        sweep.pass_completed_ms = mstime();
        int interval = server.compression_active_sweeper_interval;
        if (sweeper_cfg == COMPRESSION_ACTIVE_SWEEPER_DISABLED) {
            /* Config flipped off during the pass (force-pass-style
             * commitment kept us going to completion). Go DISABLED. */
            sweep.state = COMPRESSION_SWEEPER_STATE_DISABLED;
        } else {
            sweep.state = (interval > 0)
                              ? COMPRESSION_SWEEPER_STATE_SLEEPING
                              : COMPRESSION_SWEEPER_STATE_IDLE;
        }
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
