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
#include "compression_workers.h"

#ifdef USE_ZSTD
#include <zstd.h>
#endif

/* ========================================================================
 * Registry state — file-scoped.
 * ======================================================================== */

static struct {
    compressionDictPair *dicts[COMPRESSION_DICT_MAX]; /* all known dicts (active + retiring) */
    int count;                                        /* number of valid entries in dicts[] */
    _Atomic(compressionDictPair *) active;            /* current dict for new compressions; atomic for worker reads */
    uint32_t next_id;                                 /* next dict_id to assign (monotonic, starts at 1) */
    long long dicts_retired;                          /* total dicts that completed lifecycle (freed by GC) */
} registry;

/* QSBR per-worker quiescent generation counters. Sized by the worker
 * pool's compile-time max (compression_workers.h) — the registry is
 * indexed by worker_id which the pool assigns in [0, n_threads). */
static _Atomic(uint64_t) worker_quiescent_gen[COMPRESSION_WORKERS_MAX];

/* ========================================================================
 * Internal helpers
 * ======================================================================== */

static void removeFromDicts(compressionDictPair *dict) {
    for (int i = 0; i < registry.count; i++) {
        if (registry.dicts[i] == dict) {
            registry.dicts[i] = registry.dicts[registry.count - 1];
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

/* Returns 1 iff every worker that could have observed `dict` before it
 * retired has since advanced its quiescent generation past the
 * retirement snapshot — i.e. the QSBR grace period has elapsed.
 *
 * Only checks slots that BOTH had a worker at retire time AND have a
 * worker now. See dict->retire_n_workers in compression_registry.h for
 * the rationale. The min() handles both resize directions cleanly:
 *   - resize-up after retire: new slot didn't observe this dict, so
 *     its gen doesn't constrain us.
 *   - resize-down after retire: dead-slot worker is joined, so it
 *     can't hold a pointer.
 */
static int workersQuiescedPastRetire(const compressionDictPair *dict) {
    int n = dict->retire_n_workers;
    if (server.compression_threads < n) n = server.compression_threads;
    for (int i = 0; i < n; i++) {
        uint64_t gen = atomic_load(&worker_quiescent_gen[i]);
        if (gen <= dict->retire_worker_gen[i]) return 0;
    }
    return 1;
}

static int canFree(compressionDictPair *dict) {
    if (dict->state != COMPRESSION_DICT_STATE_RETIRING) return 0;
    if (dict->frame_refs > 0) return 0;
    return workersQuiescedPastRetire(dict);
}

/* Returns 1 iff `dict` is retiring, has no remaining frame references,
 * and is held back from reclamation ONLY because the QSBR grace period
 * has not yet elapsed (a worker hasn't advanced its quiescent
 * generation past the retirement snapshot). This is exactly the
 * complement of workersQuiescedPastRetire() for a frame-free retiring
 * dict — the condition the cron GC nudges below. It is distinct from
 * "blocked because frames still reference the dict" (frame_refs > 0),
 * which a nudge cannot help. */
static int blockedOnWorkerGen(compressionDictPair *dict) {
    if (dict->state != COMPRESSION_DICT_STATE_RETIRING) return 0;
    if (dict->frame_refs > 0) return 0;
    return !workersQuiescedPastRetire(dict);
}

static void startRetirement(compressionDictPair *dict) {
    dict->state = COMPRESSION_DICT_STATE_RETIRING;
    dict->retire_n_workers = server.compression_threads;
    for (int i = 0; i < server.compression_threads; i++) {
        dict->retire_worker_gen[i] = atomic_load(&worker_quiescent_gen[i]);
    }
    /* QSBR grace barrier: wake every parked worker so they advance
     * their quiescent_gen counter past the just-snapshotted value.
     * Without this, an idle worker's gen could remain at the snapshot
     * indefinitely and compressionRegistryTryGc() would never reclaim
     * this dict. No-op when the pool is uninitialized or has zero
     * workers (e.g. during shutdown teardown). */
    compressionWorkersWakeAll();
}

/* ========================================================================
 * Public API — same order as Phase 0.
 * ======================================================================== */

void compressionRegistryInit(void) {
    memset(&registry, 0, sizeof(registry));
    registry.next_id = 1;
    atomic_store(&registry.active, NULL);
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

    /* Cap check (R2.3.3): the dicts[] array holds both active and retiring
     * entries. When full, both training and promotion are refused — this
     * implicitly bounds the retiring population (design §4.4 step 7). */
    if (registry.count >= server.compression_dict_max_versions) {
        compressionRegistryTryGc();
    }
    if (registry.count >= server.compression_dict_max_versions) {
        serverLog(LL_WARNING,
                  "Compression: dictionary registry cap reached (%d). "
                  "Set compression-master-switch=decompression and compression-automatic-sweeper=enabled "
                  "to drain compressed frames, COMPRESSION DICT DROP a specific dict, or raise "
                  "compression-dict-max-versions.",
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

int compressionRegistryGetKnownCount(void) {
    return registry.count;
}

long long compressionRegistryGetDictsRetired(void) {
    return registry.dicts_retired;
}

/* ========================================================================
 * QSBR grace-period GC
 * ======================================================================== */

void compressionRegistryTryGc(void) {
    int gen_blocked = 0;
    for (int i = registry.count - 1; i >= 0; i--) {
        compressionDictPair *dict = registry.dicts[i];
        if (canFree(dict)) {
            dict->state = COMPRESSION_DICT_STATE_RETIRED;
            removeFromDicts(dict);
            dictPairFree(dict);
            registry.dicts_retired++;
        } else if (blockedOnWorkerGen(dict)) {
            gen_blocked = 1;
        }
    }

    /* Self-healing QSBR nudge. A retiring, frame-ref-free dict that is
     * held up only because a worker hasn't advanced its quiescent
     * generation past the retirement snapshot needs the worker to wake
     * and report quiescent. startRetirement() issues a one-shot
     * wake-all, but on a fully-idle worker pool that broadcast can be
     * lost — a worker that is not parked in pthread_cond_wait at the
     * broadcast instant never sees it, and with no compression jobs
     * arriving there is nothing to advance its generation naturally.
     * The dict would then never be reclaimed (known_dicts and
     * dict_cap_reached would stay pinned forever, and an operator who
     * drains via master=decompression could never reclaim dict memory
     * or get back under the version cap).
     *
     * Re-broadcasting here, from the compression cron, closes that
     * hole: every tick that a gen-blocked dict remains, we wake the
     * pool again, so the broadcast eventually lands while the worker
     * is parked. The worker advances its generation and the dict is
     * reclaimed on a subsequent tick. The nudge stops automatically
     * once no gen-blocked dict remains, so it is a no-op in steady
     * state. */
    if (gen_blocked) compressionWorkersWakeAll();
}

/* ========================================================================
 * Worker-side QSBR API
 * ======================================================================== */

void compressionWorkerReportQuiescent(int worker_id) {
    /* Bounded by COMPRESSION_WORKERS_MAX (the worker pool's compile-
     * time upper bound, == runtime upper bound on `compression-threads`). */
    serverAssert(worker_id >= 0 && worker_id < COMPRESSION_WORKERS_MAX);
    atomic_fetch_add(&worker_quiescent_gen[worker_id], 1);
}

uint64_t compressionWorkerGetGen(int worker_id) {
    /* Bounded by compression-threads config (max 16).
     * TODO: Replace 16 with static var.
     */
    serverAssert(worker_id >= 0 && worker_id < 16);
    return atomic_load(&worker_quiescent_gen[worker_id]);
}
