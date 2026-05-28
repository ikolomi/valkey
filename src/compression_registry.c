/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_registry.c — QSBR-based dictionary lifecycle registry.
 *
 * Implements design §4.4. Workers load the active dict atomically,
 * use the immutable CDict, then report quiescent. The main thread
 * retires dicts by snapshotting worker generations; a dict is freed
 * only after all workers have advanced past the snapshot AND
 * frame_refs == 0.
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
    compressionDictPair *dicts[COMPRESSION_DICT_MAX];
    int count;
    _Atomic(compressionDictPair *) active;
    list *retiring;
    uint32_t next_id;
} registry;

static _Atomic(uint64_t) worker_quiescent_gen[COMPRESSION_DICT_MAX];

/* ========================================================================
 * Internal helpers
 * ======================================================================== */

static void removeFromDicts(compressionDictPair *dict) {
    for (int i = 0; i < registry.count; i++) {
        if (registry.dicts[i] == dict) {
            int remaining = registry.count - 1 - i;
            if (remaining > 0) {
                memmove(&registry.dicts[i], &registry.dicts[i + 1],
                        (size_t)remaining * sizeof(compressionDictPair *));
            }
            registry.dicts[registry.count - 1] = NULL;
            registry.count--;
            return;
        }
    }
    serverPanic("compressionRegistry: dict %u not found in dicts[]", dict->dict_id);
}

static void dictPairFree(compressionDictPair *p) {
    serverAssert(p->state == COMPRESSION_DICT_STATE_RETIRED);
#ifdef USE_ZSTD
    if (p->cdict) ZSTD_freeCDict(p->cdict);
    if (p->ddict) ZSTD_freeDDict(p->ddict);
#endif
    zfree(p->bytes);
    zfree(p);
}

static int canFree(compressionDictPair *dict) {
    if (dict->state != COMPRESSION_DICT_STATE_RETIRING) return 0;
    if (dict->frame_refs > 0) return 0;
    for (int i = 0; i < server.compression_threads; i++) {
        uint64_t gen = atomic_load(&worker_quiescent_gen[i]);
        if (gen <= dict->retire_worker_gen[i]) return 0;
    }
    return 1;
}

static void startRetirement(compressionDictPair *dict) {
    dict->state = COMPRESSION_DICT_STATE_RETIRING;
    for (int i = 0; i < server.compression_threads; i++) {
        dict->retire_worker_gen[i] = atomic_load(&worker_quiescent_gen[i]);
    }
    listAddNodeTail(registry.retiring, dict);
}

/* ========================================================================
 * Public API — same order as Phase 0.
 * ======================================================================== */

void compressionRegistryInit(void) {
    memset(&registry, 0, sizeof(registry));
    registry.next_id = 1;
    atomic_store(&registry.active, NULL);
    registry.retiring = listCreate();
    memset(worker_quiescent_gen, 0, sizeof(worker_quiescent_gen));
}

/* Called at shutdown AFTER compression workers have been joined. */
void compressionRegistryRelease(void) {
    for (int i = 0; i < registry.count; i++) {
        registry.dicts[i]->state = COMPRESSION_DICT_STATE_RETIRED;
        dictPairFree(registry.dicts[i]);
    }
    registry.count = 0;
    atomic_store(&registry.active, NULL);
    listRelease(registry.retiring);
    registry.retiring = NULL;
}

compressionDictPair *compressionRegistryActive(void) {
    return atomic_load(&registry.active);
}

compressionDictPair *compressionRegistryLookup(uint32_t dict_id) {
    if (dict_id == COMPRESSION_DICT_ID_NONE) return NULL;
    for (int i = 0; i < registry.count; i++) {
        if (registry.dicts[i]->dict_id == dict_id) return registry.dicts[i];
    }
    return NULL;
}

uint32_t compressionRegistryAdd(compressionDictPair *p, int promote) {
    serverAssert(p != NULL);

    /* Cap check (R2.3.3): try GC first to make room. */
    if (registry.count >= server.compression_dict_max_versions) {
        compressionRegistryTryGc();
    }
    if (registry.count >= server.compression_dict_max_versions) {
        serverLog(LL_WARNING,
                  "Compression: dictionary registry cap reached (%d). "
                  "Run COMPRESSION SWEEP or raise compression-dict-max-versions.",
                  server.compression_dict_max_versions);
        return COMPRESSION_DICT_ID_NONE;
    }

    p->dict_id = registry.next_id++;
    p->promoted_at_ms = mstime();
    p->frame_refs = 0;

    if (promote) {
        p->state = COMPRESSION_DICT_STATE_ACTIVE;
        compressionDictPair *prev = atomic_load(&registry.active);
        if (prev) startRetirement(prev);
        atomic_store(&registry.active, p);
    } else {
        p->state = COMPRESSION_DICT_STATE_RETIRING;
        listAddNodeTail(registry.retiring, p);
    }

    registry.dicts[registry.count++] = p;

    serverLog(LL_NOTICE, "Compression: dictionary %u added (%s, registry: %d/%d).",
              p->dict_id, promote ? "active" : "retiring",
              registry.count, server.compression_dict_max_versions);

    return p->dict_id;
}

int compressionRegistryRetire(uint32_t dict_id) {
    compressionDictPair *d = compressionRegistryLookup(dict_id);
    if (!d) return -1;
    if (d->state == COMPRESSION_DICT_STATE_RETIRING) return 0;

    if (atomic_load(&registry.active) == d) {
        atomic_store(&registry.active, NULL);
    }
    startRetirement(d);
    return 0;
}

void compressionRegistryIncRef(uint32_t dict_id) {
    if (dict_id == COMPRESSION_DICT_ID_NONE) return;
    compressionDictPair *d = compressionRegistryLookup(dict_id);
    if (!d) return;
    d->frame_refs++;
}

void compressionRegistryDecRef(uint32_t dict_id) {
    if (dict_id == COMPRESSION_DICT_ID_NONE) return;
    compressionDictPair *d = compressionRegistryLookup(dict_id);
    if (!d) return;
    serverAssert(d->frame_refs > 0);
    d->frame_refs--;
    if (d->frame_refs == 0 && d->state == COMPRESSION_DICT_STATE_RETIRING) {
        compressionRegistryTryGc();
    }
}

void compressionRegistryForEach(void (*cb)(const compressionDictPair *, void *), void *ctx) {
    for (int i = 0; i < registry.count; i++) {
        serverAssert(registry.dicts[i]->state != COMPRESSION_DICT_STATE_RETIRED);
        cb(registry.dicts[i], ctx);
    }
}

/* ========================================================================
 * QSBR grace-period GC
 * ======================================================================== */

void compressionRegistryTryGc(void) {
    if (!registry.retiring || listLength(registry.retiring) == 0) return;

    listIter li;
    listNode *ln;
    listRewind(registry.retiring, &li);

    while ((ln = listNext(&li)) != NULL) {
        compressionDictPair *dict = listNodeValue(ln);
        if (canFree(dict)) {
            dict->state = COMPRESSION_DICT_STATE_RETIRED;
            listDelNode(registry.retiring, ln);
            removeFromDicts(dict);
            dictPairFree(dict);
        }
    }
}

/* ========================================================================
 * Worker-side QSBR API
 * ======================================================================== */

void compressionWorkerReportQuiescent(int worker_id) {
    atomic_fetch_add(&worker_quiescent_gen[worker_id], 1);
}

uint64_t compressionWorkerGetGen(int worker_id) {
    return atomic_load(&worker_quiescent_gen[worker_id]);
}
