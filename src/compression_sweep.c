/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_sweep.c — keyspace sweep state machine.
 *
 * See compression_sweep.h for the contract. Implementation pattern
 * mirrors compression_train.c's training-tick state machine: a file-
 * scoped state struct, a kvstoreScan callback that does the per-key
 * work, and a tick function that splices iteration across cron ticks
 * under a wall-time budget.
 *
 * Per-tick budget:
 *   target_ms_per_sec = 1000 × compression_sweep_max_cpu_pct / 100
 *   per_tick_us       = target_ms_per_sec × 1000 / max(server.hz, 1)
 *
 * At default 25% / hz=10 → 25 ms per tick. Operators raise the pct
 * (up to 100) for faster keyspace coverage at the cost of main-thread
 * latency.
 *
 * Single-flight model: only one sweep at a time. Overlapping requests
 * during an in-flight sweep are dropped; the operator can re-issue
 * after the current run completes. This matches R2.1.4's "operator
 * owns the timing" property.
 */

#include "server.h"
#include "compression.h"
#include "compression_sweep.h"
#include "compression_registry.h"
#include "monotonic.h"
#include "kvstore.h"

#include <string.h>

/* ========================================================================
 * Constants
 * ======================================================================== */

/* Floor for the per-tick budget. Even at 1% CPU and hz=10 we want at
 * least ~10 µs of work per tick so the sweep eventually completes. */
#define SWEEP_MIN_TICK_BUDGET_US 10

/* ========================================================================
 * Sweep state
 * ======================================================================== */

typedef enum {
    SWEEP_IDLE = 0,
    SWEEP_SCANNING,
} sweepStateEnum;

typedef struct compressionSweepState {
    sweepStateEnum state;
    int direction;                               /* COMPRESSION_SWEEP_DIR_{COMPRESS,DECOMPRESS} */
    int requested;                               /* 1 = a sweep was asked for; cleared when scan starts */
    int requested_direction;                     /* direction of the queued request */
    int current_db;                              /* 0..dbnum-1 — DB being scanned */
    unsigned long long cursor;                   /* kvstore scan cursor within current_db */
    unsigned long long visited;                  /* keys touched this run, for the completion log */
    unsigned long long enqueued_or_decompressed; /* successful work this run */
    monotime started_at;
} compressionSweepState;

/* File-scoped state — single instance. */
static compressionSweepState sweep_state;

/* ========================================================================
 * Per-tick budget
 * ======================================================================== */

/* Compute the per-tick wall-time budget in microseconds, derived from
 * compression-sweep-max-cpu-pct and server.hz. Bounded below by
 * SWEEP_MIN_TICK_BUDGET_US so a misconfigured (very low) percent
 * doesn't make the sweep effectively never advance. */
static long long sweepTickBudgetUs(void) {
    int hz = server.hz > 0 ? server.hz : 10;
    int pct = server.compression_sweep_max_cpu_pct;
    if (pct < 1) pct = 1;
    if (pct > 100) pct = 100;
    long long us = (long long)pct * 1000000LL / 100LL / hz;
    if (us < SWEEP_MIN_TICK_BUDGET_US) us = SWEEP_MIN_TICK_BUDGET_US;
    return us;
}

/* ========================================================================
 * Scan callback
 * ========================================================================
 *
 * Invoked by kvstoreScan for every entry in a single hash table bucket.
 * Cannot stop mid-bucket — the outer tick loop terminates the scan
 * when the time budget is exhausted (or all DBs are exhausted), AFTER
 * the current bucket completes. This is the same pattern as
 * compression_train.c.
 *
 * The callback does the per-key work: enqueue (compress direction) or
 * permanent-decompress (decompress direction). Both operations are
 * cheap from the iterator's perspective — they don't free `entry`,
 * don't restructure the kvstore, don't call signalModifiedKey (R2.9.2).
 */
static void sweepScanCallback(void *privdata, void *entry, int didx) {
    UNUSED(didx);
    compressionSweepState *ss = privdata;
    robj *value = entry;
    ss->visited++;

    if (ss->direction == COMPRESSION_SWEEP_DIR_COMPRESS) {
        /* Compress direction: only string values that pass the R2.2
         * eligibility predicate get queued. compressionEnqueueCandidate
         * does the predicate check internally + the active-dict guard
         * (R2.1.5) + the pin (R2.4.4). For non-eligible objects this is
         * a cheap branch + return; for eligible objects we get a queued
         * compression job. The `key` argument is ignored by the function
         * (the embedded key in `value` is authoritative). */
        compressionEnqueueCandidate(NULL, value, ss->current_db);
        ss->enqueued_or_decompressed++;
        return;
    }

    /* Decompress direction (R2.1.4 explicit decompress sweep): only
     * already-compressed string values need work. Everything else is
     * a no-op. compressionPermanentlyDecompress is a no-op on RAW
     * encoding (returns 0); on COMPRESSED encoding it frees the
     * compressed buffer, decRef's the dict, installs a fresh sds, and
     * flips encoding to RAW. Synchronous — the time budget is what
     * keeps the sweep from monopolizing the main thread. */
    if (value->type != OBJ_STRING) return;
    if (value->encoding != OBJ_ENCODING_COMPRESSED) return;
    if (compressionPermanentlyDecompress(value) == 0) {
        ss->enqueued_or_decompressed++;
    }
}

/* ========================================================================
 * State transitions
 * ======================================================================== */

static void enterScanning(compressionSweepState *ss, int direction) {
    ss->state = SWEEP_SCANNING;
    ss->direction = direction;
    ss->current_db = 0;
    ss->cursor = 0;
    ss->visited = 0;
    ss->enqueued_or_decompressed = 0;
    elapsedStart(&ss->started_at);
    serverLog(LL_NOTICE,
              "Compression sweep: started (direction=%s).",
              direction == COMPRESSION_SWEEP_DIR_COMPRESS ? "compress" : "decompress");
}

static void enterIdle(compressionSweepState *ss) {
    long long elapsed_ms = elapsedMs(ss->started_at);
    serverLog(LL_NOTICE,
              "Compression sweep: completed (direction=%s, visited=%llu, "
              "%s=%llu, duration=%lldms).",
              ss->direction == COMPRESSION_SWEEP_DIR_COMPRESS ? "compress" : "decompress",
              ss->visited,
              ss->direction == COMPRESSION_SWEEP_DIR_COMPRESS ? "enqueued" : "decompressed",
              ss->enqueued_or_decompressed,
              elapsed_ms);
    ss->state = SWEEP_IDLE;
    ss->visited = 0;
    ss->enqueued_or_decompressed = 0;
    ss->current_db = 0;
    ss->cursor = 0;
}

/* ========================================================================
 * Scan advancement
 * ========================================================================
 *
 * Iterates kvstoreScan against the current DB until either the time
 * budget is exhausted (return; resume next tick) or all DBs are
 * exhausted (transition to IDLE). Mirrors compression_train.c's
 * advanceScan structure.
 */
static void advanceScan(compressionSweepState *ss) {
    long long budget_us = sweepTickBudgetUs();
    monotime start;
    elapsedStart(&start);

    while (1) {
        serverDb *db = server.db[ss->current_db];
        if (db == NULL) {
            /* Sparse DB slot (never accessed via selectDb / SWAPDB).
             * Treated as empty — advance to the next slot. Mirrors the
             * NULL guard in compression_train.c's advanceScan and the
             * canonical pattern in server.c (e.g. the blocking/watched-
             * keys aggregator at server.c:6105). */
            ss->current_db++;
            if (ss->current_db >= server.dbnum) {
                enterIdle(ss);
                return;
            }
            continue;
        }
        ss->cursor = kvstoreScan(db->keys, ss->cursor, -1,
                                 sweepScanCallback, NULL, ss);

        /* Current DB exhausted — advance to next DB. kvstoreScan
         * returns 0 when the scan completes a full cycle. */
        if (ss->cursor == 0) {
            ss->current_db++;
            if (ss->current_db >= server.dbnum) {
                /* All DBs scanned — sweep done. */
                enterIdle(ss);
                return;
            }
            /* Continue with next DB on the same tick if budget allows. */
        }

        /* Time budget exhausted — resume next tick. The cursor (and
         * current_db) is preserved in ss; next tick picks up where
         * we left off. */
        if (elapsedUs(start) >= (uint64_t)budget_us) {
            return;
        }
    }
}

/* ========================================================================
 * Public API
 * ======================================================================== */

void compressionSweepInit(void) {
    memset(&sweep_state, 0, sizeof(sweep_state));
    sweep_state.state = SWEEP_IDLE;
}

void compressionSweepRelease(void) {
    /* No heap state to free — the sweep state machine holds only
     * scalar fields. State is reset to IDLE in case the registry is
     * reinitialized during the same process lifetime (unit tests). */
    memset(&sweep_state, 0, sizeof(sweep_state));
}

int compressionSweepRequest(int direction) {
    if (direction != COMPRESSION_SWEEP_DIR_COMPRESS &&
        direction != COMPRESSION_SWEEP_DIR_DECOMPRESS) {
        /* Caller bug — assert in debug, return failure in release. */
        serverAssertWithInfo(NULL, NULL,
                             direction == COMPRESSION_SWEEP_DIR_COMPRESS ||
                                 direction == COMPRESSION_SWEEP_DIR_DECOMPRESS);
        return 0;
    }

    /* If a sweep is already in flight, drop the request. Operators
     * can re-issue once the current run completes. The `requested`
     * flag governs the IDLE→SCANNING transition; an in-flight
     * SCANNING sweep is unaffected. */
    if (sweep_state.state == SWEEP_SCANNING) return 0;

    sweep_state.requested = 1;
    sweep_state.requested_direction = direction;
    return 1;
}

void compressionSweepCron(void) {
    /* Pick up a queued request. The state-machine table in §3.4
     * specifies the master-switch interaction explicitly:
     *   - request(COMPRESS) when disabled is rejected at the command
     *     handler; reaching the cron with such a request would be a
     *     defensive corner case (config race during startup or a
     *     future caller bypassing the command handler). Drop it.
     *   - request(DECOMPRESS) is always allowed — it's the canonical
     *     drain path for R2.1.4's yes→no transition. */
    if (sweep_state.state == SWEEP_IDLE && sweep_state.requested) {
        sweep_state.requested = 0;
        if (sweep_state.requested_direction == COMPRESSION_SWEEP_DIR_COMPRESS &&
            !server.compression_enabled) {
            return;
        }
        enterScanning(&sweep_state, sweep_state.requested_direction);
    }

    if (sweep_state.state == SWEEP_SCANNING) {
        /* Master switch toggled off mid-sweep. A compress-direction
         * sweep now violates R2.1.4's "new writes stop being
         * compressed" guarantee (background work is "new
         * compression" too), so abort. A decompress-direction sweep
         * continues — it's exactly the drain path R2.1.4 calls out.
         *
         * Worker-pool jobs already enqueued by sweepScanCallback
         * still complete and install — workers don't observe the
         * master switch (R2.4 / R2.11.4). The sweep state machine
         * is the authoritative lever; further work just stops being
         * produced. */
        if (sweep_state.direction == COMPRESSION_SWEEP_DIR_COMPRESS &&
            !server.compression_enabled) {
            serverLog(LL_NOTICE,
                      "Compression sweep aborted: master switch turned off "
                      "(direction=compress, visited=%llu, work=%llu).",
                      sweep_state.visited, sweep_state.enqueued_or_decompressed);
            enterIdle(&sweep_state);
            return;
        }
        advanceScan(&sweep_state);
    }
}

int compressionSweepIsScanning(void) {
    return sweep_state.state == SWEEP_SCANNING;
}

int compressionSweepCurrentDirection(void) {
    if (sweep_state.state != SWEEP_SCANNING) return 0;
    return sweep_state.direction;
}

int compressionSweepDriveForTesting(int max_iterations) {
    int n = 0;
    while (n < max_iterations) {
        compressionSweepCron();
        n++;
        if (sweep_state.state == SWEEP_IDLE && !sweep_state.requested) {
            break;
        }
    }
    return n;
}
