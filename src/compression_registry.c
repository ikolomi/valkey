/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_registry.c — QSBR-based dictionary lifecycle registry.
 *
 * Implements the dictionary registry described in design §4.4. Uses
 * Quiescent-State-Based Reclamation (QSBR) for safe dictionary retirement
 * without per-job refcounting across thread boundaries.
 *
 * Ownership model:
 *   - Main thread: all registry mutations, frame-ref accounting, GC, free.
 *   - Workers: atomic load of active pointer, report quiescent after use.
 *
 * See .agents/planning/realtime-data-compression/design/detailed-design.md §4.4.
 */

#include "server.h"
#include "compression_registry.h"
#include "adlist.h"

#ifdef USE_ZSTD
#include <zstd.h>
#endif

/* ========================================================================
 * Registry state — file-scoped.
 * ======================================================================== */

static struct {
    compressionDict *dicts[COMPRESSION_DICT_MAX];
    int count;
    _Atomic(compressionDict *) active;
    list *retiring;
    uint32_t next_id;
} registry;

/* Per-worker quiescent generation counters. Workers write their own slot
 * (atomic increment); main thread reads all slots during GC. Separate
 * from the registry struct to avoid false sharing. */
static _Atomic(uint64_t) worker_quiescent_gen[COMPRESSION_WORKERS_MAX];

/* ========================================================================
 * Internal helpers
 * ======================================================================== */

/* Remove a dict from the dicts[] array, shifting remaining entries down. */
static void removeFromDicts(compressionDict *dict) {
    for (int i = 0; i < registry.count; i++) {
        if (registry.dicts[i] == dict) {
            for (int j = i; j < registry.count - 1; j++) {
                registry.dicts[j] = registry.dicts[j + 1];
            }
            registry.dicts[registry.count - 1] = NULL;
            registry.count--;
            return;
        }
    }
    serverPanic("compressionRegistry: dict %u not found in dicts[]", dict->dict_id);
}

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

void compressionRegistryInit(void) {
    memset(&registry, 0, sizeof(registry));
    registry.next_id = 1;
    atomic_store(&registry.active, NULL);
    registry.retiring = listCreate();
    memset(worker_quiescent_gen, 0, sizeof(worker_quiescent_gen));
}

/* Release all registry resources. Called at shutdown AFTER compression
 * workers have been joined. No QSBR checks — assumes no worker is
 * running or holding a dict pointer. */
void compressionRegistryRelease(void) {
    for (int i = 0; i < registry.count; i++) {
        registry.dicts[i]->state = DICT_STATE_RETIRED;
        compressionDictFree(registry.dicts[i]);
    }
    registry.count = 0;
    atomic_store(&registry.active, NULL);
    listRelease(registry.retiring);
    registry.retiring = NULL;
}

/* ========================================================================
 * Dict creation and promotion
 * ======================================================================== */

/* Creates a new dict from raw bytes and adds it to the registry.
 * If promote=1: publishes as active, retires previous active.
 * If promote=0: adds as RETIRING (decompress-only, for RDB load).
 * Returns dict_id on success, 0 on failure (cap reached).
 * Takes ownership of bytes. Main-thread only. */
uint32_t compressionDictAdd(unsigned char *bytes, size_t len, int promote) {
    serverAssert(bytes != NULL);

    /* Cap check (R2.3.3): try GC first to make room. */
    if (registry.count >= server.compression_dict_max_versions) {
        compressionDictTryGc();
    }
    if (registry.count >= server.compression_dict_max_versions) {
        serverLog(LL_WARNING,
                  "Compression: dictionary registry cap reached (%d). "
                  "Run COMPRESSION SWEEP or raise compression-dict-max-versions.",
                  server.compression_dict_max_versions);
        return COMPRESSION_DICT_ID_NONE;
    }

#ifdef USE_ZSTD
    ZSTD_CDict *cdict = ZSTD_createCDict(bytes, len, 3); /* level 3 */
    ZSTD_DDict *ddict = ZSTD_createDDict(bytes, len);
    if (!cdict || !ddict) {
        serverLog(LL_WARNING, "Compression: failed to create CDict/DDict from %zu bytes", len);
        if (cdict) ZSTD_freeCDict(cdict);
        if (ddict) ZSTD_freeDDict(ddict);
        return COMPRESSION_DICT_ID_NONE;
    }
#else
    void *cdict = NULL;
    void *ddict = NULL;
#endif

    compressionDict *d = zcalloc(sizeof(*d));
    d->dict_id = registry.next_id++;
    d->bytes = bytes;
    d->bytes_len = len;
    d->cdict = cdict;
    d->ddict = ddict;
    d->frame_refs = 0;
    d->promoted_at_ms = mstime();

    if (promote) {
        d->state = DICT_STATE_ACTIVE;

        /* Retire previous active. */
        compressionDict *prev = atomic_load(&registry.active);
        if (prev) compressionDictStartRetirement(prev);

        /* Publish new dict — atomic store visible to workers. */
        atomic_store(&registry.active, d);
    } else {
        d->state = DICT_STATE_RETIRING;
        listAddNodeTail(registry.retiring, d);
    }

    registry.dicts[registry.count++] = d;

    serverLog(LL_NOTICE, "Compression: dictionary %u added (%s, registry: %d/%d).",
              d->dict_id, promote ? "active" : "retiring",
              registry.count, server.compression_dict_max_versions);

    return d->dict_id;
}

/* ========================================================================
 * Retirement
 * ======================================================================== */

/* Moves a dict from ACTIVE to RETIRING. Snapshots worker generations.
 * Adds to the retiring list. Main-thread only. */
void compressionDictStartRetirement(compressionDict *dict) {
    serverAssert(dict->state == DICT_STATE_ACTIVE);
    dict->state = DICT_STATE_RETIRING;

    /* Snapshot each worker's current generation. */
    for (int i = 0; i < server.compression_threads; i++) {
        dict->retire_worker_gen[i] = atomic_load(&worker_quiescent_gen[i]);
    }

    listAddNodeTail(registry.retiring, dict);
}

/* ========================================================================
 * GC — grace-period reclamation
 * ======================================================================== */

/* Returns 1 if a retiring dict is safe to free. Main-thread only.
 * Safe iff:
 *   - state is RETIRING
 *   - frame_refs == 0 (no compressed frames need the DDict)
 *   - every worker has advanced past its retirement snapshot */
int compressionDictCanFree(compressionDict *dict) {
    if (dict->state != DICT_STATE_RETIRING) return 0;
    if (dict->frame_refs > 0) return 0;

    for (int i = 0; i < server.compression_threads; i++) {
        uint64_t gen = atomic_load(&worker_quiescent_gen[i]);
        if (gen <= dict->retire_worker_gen[i]) return 0;
    }
    return 1;
}

/* Scan the retiring list, free dicts that are safe to reclaim.
 * Called from compressionCron, after result drain, after SWEEP.
 * Main-thread only. */
void compressionDictTryGc(void) {
    if (!registry.retiring || listLength(registry.retiring) == 0) return;

    listIter li;
    listNode *ln;
    listRewind(registry.retiring, &li);

    while ((ln = listNext(&li)) != NULL) {
        compressionDict *dict = listNodeValue(ln);
        if (compressionDictCanFree(dict)) {
            dict->state = DICT_STATE_RETIRED;
            listDelNode(registry.retiring, ln);
            removeFromDicts(dict);
            compressionDictFree(dict);
        }
    }
}

/* ========================================================================
 * Free
 * ======================================================================== */

/* Free a dict and all its owned resources.
 * Must only be called after compressionDictCanFree() returned true
 * (or during shutdown via compressionRegistryRelease). */
void compressionDictFree(compressionDict *dict) {
    serverAssert(dict->state == DICT_STATE_RETIRED);
#ifdef USE_ZSTD
    if (dict->cdict) ZSTD_freeCDict(dict->cdict);
    if (dict->ddict) ZSTD_freeDDict(dict->ddict);
#endif
    zfree(dict->bytes);
    zfree(dict);
}

/* ========================================================================
 * Accessors
 * ======================================================================== */

/* Returns the currently active dict. Thread-safe (atomic load).
 * Workers call this to get the CDict for compression.
 * Main thread calls this for validation or INFO.
 * Returns NULL if no active dict exists.
 *
 * TODO(S2.7): discuss with @ikolomi whether the main thread should
 * discard compressed results whose dict_id != active dict_id at install
 * time (avoids adding frame_refs to retiring dicts, helping them drain
 * faster). */
compressionDict *compressionDictGetActive(void) {
    return atomic_load(&registry.active);
}

/* Find a dict by ID. Returns NULL if not found.
 * Used by decompression path and RDB loader.
 * Main-thread only. */
compressionDict *compressionDictLookup(uint32_t dict_id) {
    if (dict_id == COMPRESSION_DICT_ID_NONE) return NULL;
    for (int i = 0; i < registry.count; i++) {
        if (registry.dicts[i]->dict_id == dict_id) return registry.dicts[i];
    }
    return NULL;
}

/* ========================================================================
 * Frame reference counting (main-thread only)
 * ======================================================================== */

/* Increment frame reference count for a dict.
 * Called after installing a compressed robj.
 * Silently skips if dict_id is the no-dict sentinel or not found
 * (handles RDB load ordering where frames may precede dict AUX). */
void compressionDictIncrFrameRef(uint32_t dict_id) {
    if (dict_id == COMPRESSION_DICT_ID_NONE) return;
    compressionDict *d = compressionDictLookup(dict_id);
    if (!d) return;
    d->frame_refs++;
}

/* Decrement frame reference count for a dict.
 * Called when a compressed robj is freed/overwritten/expired.
 * Pokes GC if it drops to zero on a retiring dict.
 * Silently skips if dict not found (dict may have been freed already
 * during shutdown or error recovery). */
void compressionDictDecrFrameRef(uint32_t dict_id) {
    if (dict_id == COMPRESSION_DICT_ID_NONE) return;
    compressionDict *d = compressionDictLookup(dict_id);
    if (!d) return;
    serverAssert(d->frame_refs > 0);
    d->frame_refs--;
    if (d->frame_refs == 0 && d->state == DICT_STATE_RETIRING) {
        compressionDictTryGc();
    }
}

/* ========================================================================
 * Iterator
 * ======================================================================== */

/* Iterate all non-retired dicts. Callback must not modify the registry.
 * Used by INFO compression, RDB save (emit AUX entries), DICT LIST.
 * Note: retired dicts are never in dicts[] — TryGc removes them from
 * the array before freeing.
 * Main-thread only. */
void compressionRegistryForEach(void (*cb)(const compressionDict *, void *), void *ctx) {
    for (int i = 0; i < registry.count; i++) {
        serverAssert(registry.dicts[i]->state != DICT_STATE_RETIRED);
        cb(registry.dicts[i], ctx);
    }
}

/* ========================================================================
 * Worker-side QSBR API
 * ======================================================================== */

/* Called by a worker after finishing a job (real or barrier).
 * Advances the worker's generation counter, signaling it no longer
 * holds any dict pointer obtained before this point. */
void compressionWorkerReportQuiescent(int worker_id) {
    atomic_fetch_add(&worker_quiescent_gen[worker_id], 1);
}

/* Returns the current quiescent generation for a worker.
 * Called by main thread in compressionDictCanFree.
 * Also useful for debug/observability. */
uint64_t compressionWorkerGetGen(int worker_id) {
    return atomic_load(&worker_quiescent_gen[worker_id]);
}
