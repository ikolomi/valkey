/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_train.c — Dictionary training sampler and bio coordination.
 *
 * Implements:
 *   - Main-thread incremental kvstore scan (time-budgeted ~100µs per
 *     cron tick) that copies eligible value bytes into a pre-allocated
 *     contiguous buffer with a parallel sizes[] array.
 *   - Trigger evaluation (first-training, drift, time-based, manual).
 *   - BIO_COMPRESSION_TRAIN job submission when caps are met.
 *   - Completion polling via atomic result pointer.
 *   - Registry promotion on success.
 *
 * All state is file-scoped static — only one training at a time.
 *
 * Threading model (R2.3.6):
 *   Main thread: trigger evaluation, kvstore scan, promotion.
 *   Bio thread:  ZDICT_trainFromBuffer + CDict/DDict creation.
 *   Bio never touches robj/kvstore/refcounts (R2.11.4).
 */

#include "server.h"
#include "compression_train.h"
#include "compression_registry.h"
#include "bio.h"
#include "monotonic.h"
#include "kvstore.h"

#include <stdatomic.h>
#include <string.h>
#ifdef USE_ZSTD
#include <zstd.h>
#include <zdict.h>
#endif

/* ========================================================================
 * Constants
 * ======================================================================== */

/* Time budget per cron tick in microseconds. */
#define TRAIN_SCAN_BUDGET_US 100

/* Cooldown after a failed training attempt (not enough samples). */
#define TRAIN_COOLDOWN_MS 30000

/* ========================================================================
 * Training state
 * ======================================================================== */

typedef enum {
    TRAIN_IDLE = 0,
    TRAIN_SCANNING,
    TRAIN_SUBMITTED,
    TRAIN_COOLDOWN,
} trainState;

typedef struct compressionTrainState {
    trainState state;
    int training_requested;    /* Unified trigger flag. */
    int current_db;            /* Which DB we're scanning (0..dbnum-1). */
    unsigned long long cursor; /* kvstore scan cursor within current_db. */
    char *buffer;              /* Pre-allocated sample buffer. */
    size_t *sizes;             /* Per-sample sizes array. */
    int sample_count;          /* Eligible samples collected. */
    size_t buffer_used;        /* Bytes written into buffer. */
    mstime_t cooldown_until;   /* Timestamp when cooldown expires. */
} compressionTrainState;

/* File-scoped training state — single instance. */
static compressionTrainState train_state;

/* ========================================================================
 * Bio result
 * ======================================================================== */

typedef struct compressionTrainResult {
    ZSTD_CDict *cdict;
    ZSTD_DDict *ddict;
    void *dict_bytes;      /* Raw dict for RDB persistence. */
    size_t dict_bytes_len; /* Actual trained dict size. */
    int success;           /* 1 = ok, 0 = failed. */
    size_t error_code;     /* ZSTD error code if failed. */
} compressionTrainResult;

/* Atomic pointer for bio→main signaling. NULL = no result pending. */
static _Atomic(compressionTrainResult *) train_result = NULL;

/* ========================================================================
 * Scan callback
 * ========================================================================
 *
 * Invoked by kvstoreScan for every entry in a single hash table bucket.
 * Cannot stop mid-bucket — cap checks here guard against overflow, and
 * the outer loop terminates the scan after the bucket completes.
 */
static void trainingScanCallback(void *privdata, void *entry, int didx) {
    UNUSED(didx);
    compressionTrainState *ts = privdata;
    robj *o = entry;

    /* 1. Must be a string object. */
    if (o->type != OBJ_STRING) return;

    /* 2. Must be RAW encoding (skip INT, EMBSTR, already-compressed). */
    if (o->encoding != OBJ_ENCODING_RAW) return;

    /* 3. Size bounds. */
    size_t len = sdslen((sds)objectGetVal(o));
    if (len < server.compression_min_value_size) return;
    if (server.compression_max_value_size > 0 &&
        len > server.compression_max_value_size) return;

    /* 4. Check caps before copying. */
    if (ts->sample_count >= server.compression_dict_max_training_keys) return;
    if (ts->buffer_used + len > server.compression_training_buffer_size) return;

    /* 5. Copy value bytes into buffer. */
    memcpy(ts->buffer + ts->buffer_used, objectGetVal(o), len);
    ts->sizes[ts->sample_count] = len;
    ts->buffer_used += len;
    ts->sample_count++;
}

/* ========================================================================
 * Internal helpers
 * ======================================================================== */

/* Total keys across all DBs. O(1) per DB via kvstoreSize. */
static unsigned long long totalDbKeys(void) {
    unsigned long long total = 0;
    for (int j = 0; j < server.dbnum; j++) {
        /* server.db is sparse: created on first use of each DB. Slots
         * 1..dbnum-1 are NULL until something accesses them via
         * createDatabaseIfNeeded() (called from selectDb / SWAPDB).
         * On a fresh server with default `databases 16`, only db[0]
         * is materialized; iterating through 1..15 dereferences NULL.
         * A NULL DB trivially has 0 keys; skip it. Same guard pattern
         * is used by the equivalent loop in server.c (e.g. the
         * blocking/watched-keys aggregator). */
        if (server.db[j] == NULL) continue;
        total += kvstoreSize(server.db[j]->keys);
    }
    return total;
}

/* Returns 1 if remaining buffer can't fit any eligible value. */
static int bufferFull(compressionTrainState *ts) {
    size_t remaining = server.compression_training_buffer_size - ts->buffer_used;
    return remaining < server.compression_min_value_size;
}

/* Allocate training buffer and sizes array. */
static void allocTrainingBuffers(compressionTrainState *ts) {
    ts->buffer = zmalloc(server.compression_training_buffer_size);
    ts->sizes = zmalloc(sizeof(size_t) * (size_t)server.compression_dict_max_training_keys);
    ts->buffer_used = 0;
    ts->sample_count = 0;
}

/* Free training buffer and sizes (used on abort). */
static void freeTrainingBuffers(compressionTrainState *ts) {
    if (ts->buffer) {
        zfree(ts->buffer);
        ts->buffer = NULL;
    }
    if (ts->sizes) {
        zfree(ts->sizes);
        ts->sizes = NULL;
    }
    ts->buffer_used = 0;
    ts->sample_count = 0;
}

/* Enter cooldown state. */
static void enterCooldown(compressionTrainState *ts) {
    freeTrainingBuffers(ts);
    ts->state = TRAIN_COOLDOWN;
    ts->cooldown_until = mstime() + TRAIN_COOLDOWN_MS;
}

/* ========================================================================
 * Trigger evaluation (Phase 1 of compressionCron)
 * ======================================================================== */

static void evaluateTriggers(compressionTrainState *ts) {
    if (ts->state != TRAIN_IDLE || ts->training_requested) return;

    compressionDictPair *active = compressionRegistryActive();

    if ((!active && totalDbKeys() >= (unsigned long long)server.compression_dict_min_training_keys) ||
        (active && 0 /* TODO: drift detection */) ||
        (active && 0 /* TODO: refresh interval */)) {
        ts->training_requested = 1;
    }
}

/* ========================================================================
 * Scan advancement (Phase 2 of compressionCron)
 * ======================================================================== */

static void advanceScan(compressionTrainState *ts) {
    monotime start;
    elapsedStart(&start);

    while (1) {
        serverDb *db = server.db[ts->current_db];
        if (db == NULL) {
            /* Sparse DB slot (never accessed). Treated as empty —
             * advance to the next slot. Same NULL guard as totalDbKeys
             * above. */
            ts->current_db++;
            if (ts->current_db >= server.dbnum) {
                break; /* All DBs scanned. */
            }
            continue;
        }
        ts->cursor = kvstoreScan(db->keys, ts->cursor, -1,
                                 trainingScanCallback, NULL, ts);

        /* Check if caps reached or buffer full. */
        if (ts->sample_count >= server.compression_dict_max_training_keys ||
            bufferFull(ts)) {
            break;
        }

        /* Current DB exhausted — advance to next. */
        if (ts->cursor == 0) {
            ts->current_db++;
            if (ts->current_db >= server.dbnum) {
                break; /* All DBs scanned. */
            }
        }

        /* Time budget exhausted — resume next tick. */
        if (elapsedUs(start) >= TRAIN_SCAN_BUDGET_US) {
            return; /* Stay in SCANNING state. */
        }
    }

    /* Scan complete — submit or abort. */
    if (ts->sample_count >= server.compression_dict_min_training_keys) {
        /* Submit to bio. Ownership of buffer+sizes transfers. */
        bioCreateCompTrainJob(ts->buffer, ts->sizes,
                              ts->sample_count, ts->buffer_used);
        ts->buffer = NULL;
        ts->sizes = NULL;
        ts->state = TRAIN_SUBMITTED;
    } else {
        serverLog(LL_WARNING,
                  "Compression training: insufficient eligible samples (%d < %d). "
                  "Entering %d ms cooldown.",
                  ts->sample_count,
                  server.compression_dict_min_training_keys,
                  TRAIN_COOLDOWN_MS);
        enterCooldown(ts);
    }
}

/* ========================================================================
 * Public API
 * ======================================================================== */

void compressionTrainInit(void) {
    memset(&train_state, 0, sizeof(train_state));
    /* Free any stale result from a previous lifecycle (e.g., unit tests). */
    compressionTrainResult *stale =
        atomic_exchange_explicit(&train_result, NULL, memory_order_acquire);
    if (stale) {
#ifdef USE_ZSTD
        if (stale->cdict) ZSTD_freeCDict(stale->cdict);
        if (stale->ddict) ZSTD_freeDDict(stale->ddict);
#endif
        if (stale->dict_bytes) zfree(stale->dict_bytes);
        zfree(stale);
    }
}

int compressionTrainMaybeTrigger(int reason) {
    UNUSED(reason);
    if (train_state.state != TRAIN_IDLE) return 0;
    train_state.training_requested = 1;
    return 1;
}

int compressionTrainAdvanceSampling(int budget) {
    UNUSED(budget);
    /* Legacy API — scanning is now driven internally by compressionTrainCron. */
    return 0;
}

/* Called from compressionCron in compression.c. */
void compressionTrainCron(void) {
    /* Training only runs when the operator's intent is productive
     * compression. In master=decompression we are draining (no point
     * training a new dict that would never be used); in master=off we
     * are paused. (R2.1.5) */
    if (server.compression_master_switch != COMPRESSION_MASTER_COMPRESSION) return;

    compressionTrainState *ts = &train_state;

    /* Handle cooldown expiry. */
    if (ts->state == TRAIN_COOLDOWN) {
        if (mstime() >= ts->cooldown_until) {
            ts->state = TRAIN_IDLE;
        } else {
            return;
        }
    }

    /* Poll for bio completion. */
    if (ts->state == TRAIN_SUBMITTED) {
        compressionTrainResult *result =
            atomic_load_explicit(&train_result, memory_order_acquire);
        if (!result) return; /* Still running. */

        /* Clear the atomic slot. */
        atomic_store_explicit(&train_result, NULL, memory_order_relaxed);

        if (result->success) {
            /* Build a compressionDictPair and promote. */
            compressionDictPair *pair = zcalloc(sizeof(*pair));
            pair->cdict = result->cdict;
            pair->ddict = result->ddict;
            pair->bytes = result->dict_bytes;
            pair->bytes_len = result->dict_bytes_len;
            pair->dict_id = 0; /* Registry assigns ID on add. */

            if (compressionRegistryAdd(pair, 1) == COMPRESSION_DICT_ID_NONE) {
                serverLog(LL_WARNING,
                          "Compression training: promotion failed (registry cap?).");
                /* Free the pair — registry rejected it. */
#ifdef USE_ZSTD
                ZSTD_freeCDict(pair->cdict);
                ZSTD_freeDDict(pair->ddict);
#endif
                zfree(pair->bytes);
                zfree(pair);
            } else {
                serverLog(LL_NOTICE,
                          "Compression training: new dictionary promoted "
                          "(samples=%d, dict_size=%zu).",
                          ts->sample_count, result->dict_bytes_len);
            }
        } else {
            serverLog(LL_WARNING,
                      "Compression training: ZSTD training failed "
                      "(error=%zu).",
                      result->error_code);
        }

        zfree(result);
        ts->state = TRAIN_IDLE;
        ts->sample_count = 0;
        ts->buffer_used = 0;
        return;
    }

    /* Phase 1: evaluate triggers. */
    evaluateTriggers(ts);

    /* Phase 2: start scan if triggered. */
    if (ts->state == TRAIN_IDLE && ts->training_requested) {
        ts->training_requested = 0;
        ts->current_db = 0;
        ts->cursor = 0;
        allocTrainingBuffers(ts);
        ts->state = TRAIN_SCANNING;
    }

    /* Phase 3: advance scan. */
    if (ts->state == TRAIN_SCANNING) {
        advanceScan(ts);
    }
}

/* Called from bio thread after training completes.
 * Sets the atomic result pointer for main-thread pickup. */
void compressionTrainCompleteFromBio(compressionDictPair *new_pair, sds err) {
    compressionTrainResult *result = zmalloc(sizeof(*result));
    if (new_pair) {
        result->success = 1;
        result->cdict = new_pair->cdict;
        result->ddict = new_pair->ddict;
        result->dict_bytes = new_pair->bytes;
        result->dict_bytes_len = new_pair->bytes_len;
        result->error_code = 0;
        zfree(new_pair);
    } else {
        result->success = 0;
        result->cdict = NULL;
        result->ddict = NULL;
        result->dict_bytes = NULL;
        result->dict_bytes_len = 0;
        result->error_code = 0;
        if (err) sdsfree(err);
    }
    atomic_store_explicit(&train_result, result, memory_order_release);
}
