/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_workers.c — pool plumbing for background compression.
 *
 * Implements design §2.11 (concurrency invariants) and §4.6 (queue
 * primitives + worker contract). The per-job payload is a placeholder
 * pass-through in S2.4 — the worker accepts a job, does NO compression
 * yet, and posts an empty result back to the outbox so the
 * end-to-end plumbing can be exercised. ZSTD_compress_usingCDict
 * integration lands in S2.5; the pool plumbing in this file is stable
 * across S2.4 → S2.5 → S2.7.
 *
 * Threading model:
 *   - One inbox (mutexQueue): main thread is the producer, N workers
 *     are consumers. mutexQueue gives us blocking-pop with cond-var
 *     parking when idle (the bio pattern), which is the right fit for
 *     a feature whose default state is enabled-but-quiet.
 *   - One outbox (mpscQueue from src/queues.h): workers produce, main
 *     thread polls non-blocking from compressionAfterSleep. Lock-free
 *     because the main thread should not block on dequeue.
 *
 * Lifecycle invariant for the registry's QSBR contract (§4.4):
 *   - Workers call compressionWorkerReportQuiescent(worker_id) after
 *     every job, regardless of success / error / placeholder. The
 *     registry's per-worker quiescent_gen counter is monotonic; missed
 *     reports delay reclamation but do not cause use-after-free.
 *   - On Stop, all workers are joined BEFORE compressionRegistryRelease
 *     runs (server.c shutdown ordering).
 */

#include "server.h"
#include "compression_workers.h"
#include "compression_header.h"
#include "compression_registry.h"
#include "mutexqueue.h"

#ifdef USE_ZSTD
#include <zstd.h>
#endif
#include "queues.h"

#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <unistd.h>

/* ========================================================================
 * Per-job payload — file-private. Callers see only the public Enqueue
 * API; the job struct is filled in by Enqueue and consumed by the
 * worker / outbox drain.
 * ======================================================================== */

typedef struct compressionJob {
    /* Set by Enqueue (immutable for the rest of the job lifetime).
     *
     * `value` is the robj the caller pinned via incrRefCount(value).
     * NULL is permitted only via testOnlyCompressionWorkersEnqueueRaw
     * — sentinel for "test mode; production drain skips install".
     *
     * `src` aliases objectGetVal(value) (when value != NULL) or the
     * caller-supplied sds (test path). The worker reads `src` only;
     * never the robj.
     *
     * `dbid` is captured for the drain-time kvstore lookup. */
    robj *value;
    sds src;
    int dbid;

    /* Filled by worker on the worker thread. The worker loads the
     * active dict via compressionRegistryActive() at compress time per
     * the QSBR contract (§4.6) — Enqueue does NOT capture the dict_id
     * itself. The dict_id field exists on the job struct so the worker
     * can carry the snapshot it actually used into the compressed-
     * frame header on the outbox side. */
    uint32_t dict_id; /* dict the worker actually used; 0 == none */
    void *dst;        /* zmalloc'd compressedHeader + frame */
    size_t dst_len;   /* total bytes in dst */
    int err;          /* 0 = ok, !=0 = ZSTD error code */
} compressionJob;

/* ========================================================================
 * Pool state — file-private singleton.
 *
 * We deliberately do NOT use server-global state for this. The pool is
 * an internal implementation detail of compression_workers.c; exposing
 * it via server.h would invite ad-hoc reads from elsewhere and break
 * the encapsulation contract.
 * ======================================================================== */

static struct {
    int initialized; /* 1 between Start and Stop */
    int n_threads;   /* current pool size; 0 = disabled */

    mutexQueue *inbox; /* main → workers (blocking pop) */
    mpscQueue outbox;  /* workers → main (lock-free) */

    pthread_t worker_tids[COMPRESSION_WORKERS_MAX];
    int worker_ids[COMPRESSION_WORKERS_MAX]; /* pthread_create arg slot — must outlive create */

    /* Set by Stop, read by workers each loop iteration. Atomic because
     * worker may be parked on the inbox cond_var when we set it. */
    _Atomic(int) shutdown_requested;
} pool;

/* ========================================================================
 * Inbox sentinel for shutdown
 * ========================================================================
 *
 * We can't rely on mutexQueueWakeAll alone for shutdown: there is a
 * tiny race where a worker reads `shutdown_requested == 0` at top of
 * the loop, then is preempted before entering mutexQueuePop's
 * cond_wait. If Stop fires its broadcast in that window, the broadcast
 * has no waiters and is lost; the worker subsequently parks and
 * deadlocks against pthread_join.
 *
 * Sentinels avoid this because they put DATA in the queue. Once Stop
 * has pushed N sentinels, ANY subsequent mutexQueuePop sees length>0
 * under the mutex and pops without entering cond_wait. Race-free.
 *
 * Address-of-static gives us a unique pointer that no zmalloc'd
 * compressionJob can collide with — distinguishable via pointer
 * equality (IS_SHUTDOWN_SENTINEL) without runtime cost beyond a
 * single comparison per pop.
 *
 * For QSBR grace barriers (from compressionRegistryRetire) we still
 * use mutexQueueWakeAll, NOT sentinels. A missed barrier wake is
 * benign — the next event (real job, another retire) catches the
 * worker. Shutdown is correctness-critical (must not hang); barriers
 * are liveness-only.
 */
static int kShutdownSentinel;
#define IS_SHUTDOWN_SENTINEL(p) ((p) == &kShutdownSentinel)

/* ========================================================================
 * Worker thread main loop
 * ======================================================================== */

static void *workerThreadMain(void *arg) {
    int worker_id = *(int *)arg;
    char thd_name[32];

    snprintf(thd_name, sizeof(thd_name), "compress_thd_%d", worker_id);
    valkey_set_thread_title(thd_name);
    serverSetCpuAffinity(server.compression_cpulist);

    /* Per-worker ZSTD compression context. Not thread-safe, so each
     * worker owns its own. Allocated once, reused for every job —
     * keeps allocator pressure off the per-job hot path. Freed at
     * thread exit (along the same exit_thread label as the rest of
     * the worker's owned state).
     *
     * Only present when USE_ZSTD is compiled in. Without it, the
     * encoder body below short-circuits to the "not compressed"
     * branch identically to the no-active-dict case, and no CCtx is
     * needed. */
#ifdef USE_ZSTD
    ZSTD_CCtx *cctx = ZSTD_createCCtx();
    if (cctx == NULL) {
        serverLog(LL_WARNING,
                  "Compression worker %d: ZSTD_createCCtx() failed; "
                  "worker exiting. Compression effectively disabled "
                  "until the pool is restarted.",
                  worker_id);
        return NULL;
    }
#endif

    while (!atomic_load(&pool.shutdown_requested)) {
        /* Block until a job arrives or someone calls
         * compressionWorkersWakeAll() (which broadcasts on the inbox
         * cond var via mutexQueueWakeAll). We use the *Wakable*
         * variant of pop, which returns NULL on wake-all rather than
         * re-parking. Three possible items from the pop:
         *   - NULL: QSBR grace barrier from compressionRegistryRetire
         *     via compressionWorkersWakeAll. Advance our gen and
         *     continue.
         *   - &kShutdownSentinel: shutdown signal from Stop. Advance
         *     gen (harmless) and exit the loop. The sentinel is data-
         *     in-queue; this path cannot be missed even if we entered
         *     the pop after Stop's broadcast (see the sentinel rationale
         *     above).
         *   - compressionJob*: a real job to process. */
        void *item = mutexQueuePopWakable(pool.inbox, /*blocking=*/true);

        if (item == NULL) {
            /* QSBR grace barrier wake. */
            compressionWorkerReportQuiescent(worker_id);
            continue;
        }

        if (IS_SHUTDOWN_SENTINEL(item)) {
            /* Shutdown signal. Advance gen for QSBR consistency
             * (matches the barrier-wake path; lets registry GC see
             * one final advance from this worker before the slot
             * goes inactive). Exit the loop. The sentinel itself
             * isn't a heap allocation; nothing to free. */
            compressionWorkerReportQuiescent(worker_id);
            break;
        }

        compressionJob *job = (compressionJob *)item;

        /* Load the active dict atomically. The QSBR contract (§4.4)
         * guarantees the pointer is valid until we report quiescent
         * at end-of-iteration. The pointer is stable for our use here
         * because retirement is always preceded by an atomic publish
         * of a new active dict, and the registry never frees a dict
         * while any worker may still be holding it. */
        compressionDictPair *active = compressionRegistryActive();

        if (active == NULL) {
            /* "compression-enabled yes but no active dict yet" state
             * documented in R2.1.5. Mark the job as not-compressed so
             * the drain handler can dispose without touching dst.
             * err=1 is the not-an-actual-ZSTD-error sentinel meaning
             * "worker chose not to compress"; ZSTD error codes are
             * always negative when wrapped in size_t (they fit in the
             * sign bit), so positive `err` values are reserved for
             * worker-policy decisions like this.
             *
             * Same branch is taken when USE_ZSTD is not compiled in
             * (the BUILD_ZSTD=no build mode): the registry is empty
             * and the worker simply marks every job not-compressed. */
            job->dict_id = 0;
            job->dst = NULL;
            job->dst_len = 0;
            job->err = 1;
        }
#ifdef USE_ZSTD
        else {
            size_t src_len = sdslen(job->src);
            size_t bound = ZSTD_compressBound(src_len);
            size_t alloc = COMPRESSION_HEADER_SIZE + bound;
            void *buf = zmalloc(alloc);

            /* Compress directly into buf at offset HEADER_SIZE. The
             * header is written in-place after we know the actual
             * compressed size. */
            size_t got = ZSTD_compress_usingCDict(
                cctx,
                (char *)buf + COMPRESSION_HEADER_SIZE, /* dst */
                bound,                                 /* dst capacity */
                job->src, src_len,                     /* src + len */
                active->cdict);                        /* dict */

            if (ZSTD_isError(got)) {
                zfree(buf);
                job->dict_id = 0;
                job->dst = NULL;
                job->dst_len = 0;
                /* Preserve the error code. ZSTD error codes are
                 * negative-when-cast-to-int, so a downcast keeps
                 * sign. */
                job->err = (int)(ssize_t)got;
            } else {
                /* Shrink to actual size to avoid leaking
                 * ZSTD_compressBound slack into used_memory. May copy
                 * if the shrink crosses a jemalloc size class —
                 * acceptable per design §5.2 (paid on worker thread,
                 * bytes are small). zrealloc preserves the existing
                 * contents up to min(old, new) size. */
                size_t total = COMPRESSION_HEADER_SIZE + got;
                void *shrunk = zrealloc(buf, total);
                if (shrunk == NULL) {
                    /* zrealloc shrink failure is allocator-specific.
                     * Defensive: keep the original buffer (which is
                     * still valid; zrealloc on shrink only releases
                     * the old block when it returns a different
                     * pointer or NULL on success). On NULL we leave
                     * `buf` allocated and use it, paying the slack
                     * cost rather than failing the job. */
                    shrunk = buf;
                }

                /* Encode the header into the first 16 bytes of the
                 * (now-shrunk) buffer. dict_id from the active dict
                 * goes into alg_meta. */
                compressionHeaderEncode((unsigned char *)shrunk,
                                        COMPRESSION_ALG_ZSTD_MAGIC,
                                        active->dict_id,
                                        (uint32_t)src_len,
                                        (uint32_t)got);

                job->dict_id = active->dict_id;
                job->dst = shrunk;
                job->dst_len = total;
                job->err = 0;
            }
        }
#endif /* USE_ZSTD */

        /* Post the result. Outbox is bounded; on full, retry until it
         * drains (the main thread's compressionAfterSleep is
         * guaranteed to run). Discarding a completed job would
         * orphan the caller's incrRefCount, so we must not drop. */
        mpscTicket ticket = {0};
        while (!mpscEnqueue(&pool.outbox, job, &ticket)) {
            /* Back-pressure: outbox is full. Brief yield, then retry.
             * The `ticket` reservation persists across retries (per
             * mpscEnqueue contract in queues.h). */
            usleep(100); /* 100 µs */
            atomic_thread_fence(memory_order_acquire);
            if (atomic_load(&pool.shutdown_requested)) {
                /* Shutdown asked while we were stuck in back-pressure.
                 * The job is leaked (the main thread won't drain it),
                 * but we're shutting down so it's accepted as part of
                 * shutdown cleanup. The dst payload (NULL here) has
                 * no allocations to free. */
                zfree(job);
                goto exit_thread;
            }
        }

        /* QSBR: advance our generation counter. After this point we
         * hold no registry pointers — the registry's GC pass can
         * reclaim a retired dict whose retire-time snapshot of our
         * gen we've now exceeded. */
        compressionWorkerReportQuiescent(worker_id);
    }

exit_thread:
    /* Free the per-worker CCtx allocated at thread start. ZSTD_freeCCtx
     * is documented as accepting NULL safely, but the cctx allocation
     * above bails out before reaching this label if it returned NULL,
     * so the pointer is non-NULL whenever we get here.
     *
     * Only present when USE_ZSTD is compiled in (matches the gating
     * on the allocation site above). */
#ifdef USE_ZSTD
    ZSTD_freeCCtx(cctx);
#endif
    return NULL;
}

/* ========================================================================
 * Public API
 * ======================================================================== */

int compressionWorkersStart(int n_threads) {
    serverAssert(!pool.initialized);
    serverAssert(n_threads >= 0 && n_threads <= COMPRESSION_WORKERS_MAX);

    pool.inbox = mutexQueueCreate();
    if (!pool.inbox) return -1;
    mpscInit(&pool.outbox);
    atomic_store(&pool.shutdown_requested, 0);
    pool.n_threads = n_threads;

    for (int i = 0; i < n_threads; i++) {
        pool.worker_ids[i] = i;
        int err = pthread_create(&pool.worker_tids[i], NULL,
                                 workerThreadMain, &pool.worker_ids[i]);
        if (err) {
            serverLog(LL_WARNING,
                      "Compression: failed to create worker %d (pthread_create: %s). "
                      "Stopping pool, feature will be inert.",
                      i, strerror(err));

            /* Roll back: signal shutdown, push one sentinel per
             * created worker so each one can pop one and exit, join
             * them, free the queues. */
            atomic_store(&pool.shutdown_requested, 1);
            for (int j = 0; j < i; j++) {
                mutexQueueAdd(pool.inbox, &kShutdownSentinel);
            }
            for (int j = 0; j < i; j++) {
                pthread_join(pool.worker_tids[j], NULL);
            }
            mutexQueueRelease(pool.inbox);
            mpscFree(&pool.outbox);
            pool.inbox = NULL;
            pool.n_threads = 0;
            return -1;
        }
    }

    pool.initialized = 1;
    if (n_threads > 0) {
        serverLog(LL_NOTICE,
                  "Compression: worker pool started with %d thread(s).",
                  n_threads);
    }
    return 0;
}

void compressionWorkersStop(void) {
    if (!pool.initialized) return;

    /* Signal shutdown FIRST (so workers exiting the cond_wait critical
     * section see it on their next top-of-loop check), then push N
     * sentinels — one per worker — into the inbox.
     *
     * Why sentinels (data-in-queue) rather than mutexQueueWakeAll:
     * with wake-all alone there is a small race window where a worker
     * has read shutdown_requested == 0 and is about to enter
     * mutexQueuePop. If Stop's broadcast fires in that window, the
     * broadcast hits no waiters (worker hasn't called cond_wait yet),
     * is lost, and the worker then enters cond_wait waiting for a
     * signal that will never come. Sentinels eliminate this: the
     * worker's mutex_lock + length check sees length>0 and pops a
     * sentinel without entering cond_wait. */
    atomic_store(&pool.shutdown_requested, 1);
    for (int i = 0; i < pool.n_threads; i++) {
        mutexQueueAdd(pool.inbox, &kShutdownSentinel);
    }

    /* Join all worker threads. This is the synchronization point that
     * lets compressionRegistryRelease run safely afterwards (registry
     * comment: "Called at shutdown AFTER compression workers have been
     * joined"). */
    for (int i = 0; i < pool.n_threads; i++) {
        pthread_join(pool.worker_tids[i], NULL);
    }

    /* Drain any leftover inbox items. If the pool was stopped while
     * jobs were in flight, the workers may have left some unconsumed.
     * Free their job structs; the caller's incrRefCount on the value
     * robj is leaked at shutdown — accepted as shutdown cleanup, since
     * the entire process is exiting. Skip any leftover sentinels —
     * they're addresses of a static, not heap allocations. */
    void *leftover;
    while ((leftover = mutexQueuePop(pool.inbox, /*blocking=*/false)) != NULL) {
        if (IS_SHUTDOWN_SENTINEL(leftover)) continue;
        compressionJob *job = (compressionJob *)leftover;
        zfree(job->dst);
        zfree(job);
    }

    /* Drain any outbox results similarly. */
    void *jobs_out[64];
    size_t got;
    while ((got = mpscDequeueBatch(&pool.outbox, jobs_out, 64)) > 0) {
        for (size_t i = 0; i < got; i++) {
            compressionJob *job = jobs_out[i];
            zfree(job->dst);
            zfree(job);
        }
    }

    mutexQueueRelease(pool.inbox);
    mpscFree(&pool.outbox);
    pool.inbox = NULL;
    pool.n_threads = 0;
    pool.initialized = 0;

    serverLog(LL_NOTICE, "Compression: worker pool stopped.");
}

int compressionWorkersResize(int n_threads) {
    if (n_threads < 0 || n_threads > COMPRESSION_WORKERS_MAX) return -1;
    if (pool.initialized && pool.n_threads == n_threads) return 0;
    if (pool.initialized) compressionWorkersStop();
    return compressionWorkersStart(n_threads);
}

int compressionWorkersGetThreadCount(void) {
    /* Test/introspection accessor. Returns 0 if uninitialized — the
     * pool's `n_threads` is set by Start and zeroed by Stop, so this
     * always reflects the live thread count. */
    return pool.initialized ? pool.n_threads : 0;
}

int compressionWorkersEnqueue(robj *value, int dbid) {
    if (!pool.initialized || pool.n_threads == 0) return -1;
    serverAssert(value != NULL);

    compressionJob *job = zmalloc(sizeof(*job));
    job->value = value;
    job->src = (sds)objectGetVal(value);
    job->dbid = dbid;
    job->dict_id = 0;
    job->dst = NULL;
    job->dst_len = 0;
    job->err = 0;

    mutexQueueAdd(pool.inbox, job);
    return 0;
}

/* Test-only. Lets gtest enqueue a job from a raw sds without a real
 * robj-owned value (no kvstore, no refcount).  job->value = NULL is
 * the sentinel that tells the production drain to skip the install
 * path; tests using this MUST extract the job via
 * testOnlyCompressionWorkersDrainOutbox before the production
 * compressionWorkersDrainOutbox runs (otherwise the drain's
 * value!=NULL serverAssert would fire). */
int testOnlyCompressionWorkersEnqueueRaw(sds src, int dbid) {
    if (!pool.initialized || pool.n_threads == 0) return -1;

    compressionJob *job = zmalloc(sizeof(*job));
    job->value = NULL; /* test sentinel */
    job->src = src;
    job->dbid = dbid;
    job->dict_id = 0;
    job->dst = NULL;
    job->dst_len = 0;
    job->err = 0;

    mutexQueueAdd(pool.inbox, job);
    return 0;
}

/* Install a worker-produced compressed buffer into the kvstore on the
 * main thread. Called from compressionWorkersDrainOutbox after the
 * net-savings guard accepts the result.
 *
 * Lifetime / staleness:
 *   - Caller holds the pin (incrRefCount(job->value)) from enqueue.
 *     This both protects the bytes from in-place mutation (R2.4.4) and
 *     reserves the robj address so the pointer-equality stale-check
 *     below is ABA-safe.
 *   - If the value at (dbid, key) was overwritten / expired between
 *     enqueue and now, the kvstore slot points to a different robj
 *     (or no robj at all). We detect via pointer equality and discard
 *     the compression result.
 *
 * Notifications:
 *   - dbReplaceValue → dbSetValue(..., overwrite=0, ...) does NOT call
 *     signalModifiedKey, moduleNotifyKeyUnlink, or
 *     signalDeletedKeyAsReady. Background compression is a
 *     storage-only change (§2.9 R2.9.2).
 *
 * Returns 1 if installed, 0 if discarded due to staleness. Either way
 * caller still owns the pin (decrRefCount happens in the drain loop). */
static int compressionInstall(compressionJob *job) {
    serverDb *db = &server.db[job->dbid];
    sds key_sds = (sds)objectGetKey(job->value);
    int dict_index = getKVStoreIndexForKey(key_sds);
    void **slot = kvstoreHashtableFindRef(db->keys, dict_index, key_sds);

    if (slot == NULL || *slot != job->value) {
        /* Stale: overwrite, expire, or COW happened between enqueue
         * and now. Discard the compressed buffer. */
        zfree(job->dst);
        return 0;
    }

    /* Build the compressed robj. createCompressedObject takes ownership
     * of job->dst. */
    robj *compressed = createCompressedObject(OBJ_STRING, job->dst, job->dst_len);

    /* Replace via dbReplaceValue. We need a temporary key robj wrapping
     * the sds — initStaticStringObject is the standard pattern. */
    robj key_obj;
    initStaticStringObject(key_obj, key_sds);

    /* dbReplaceValue may reallocate `compressed` (objectSetKeyAndExpire
     * embeds the key into the new robj). It also decrRefs the old value
     * (job->value), dropping its kvstore reference; our pin still
     * holds via the caller's refcount. */
    dbReplaceValue(db, &key_obj, &compressed);

    /* Bump the registry ref for the dict this frame is built with.
     * Decrement happens when the compressed robj is freed (S2.x will
     * wire freeStringObject → compressionRegistryDecRef). */
    compressionRegistryIncRef(job->dict_id);

    /* TODO(S4.1): compression_compressions_per_sec++ rate update; fold
     * (dst_len / sdslen(src)) into compression_live_ratio_10m EMA per
     * R2.3.5; compression_compressed_objects++. */

    return 1;
}

int compressionWorkersDrainOutbox(int budget) {
    if (!pool.initialized) return 0;

    void *jobs_out[64];
    int total = 0;
    while (total < budget) {
        size_t batch = budget - total;
        if (batch > 64) batch = 64;
        size_t got = mpscDequeueBatch(&pool.outbox, jobs_out, batch);
        if (got == 0) break;

        for (size_t i = 0; i < got; i++) {
            compressionJob *job = jobs_out[i];

            /* S2.5 drain handler: post-compression net-savings guard,
             * then dispose. Real install (createCompressedObject +
             * dbOverwrite + compressionRegistryIncRef + decrRefCount on
             * the caller's value pin) lands with the write-path hook
             * in S2.7; that PR will introduce a real production caller
             * (dbAdd / dbOverwrite). For now the worker-pool plumbing
             * runs end-to-end against test fixtures only, so we
             * exercise the encoder + guard but not the install. The
             * S2.5 round-trip tests use a peeking variant of drain
             * (testOnlyCompressionWorkersDrainOutbox) that does NOT
             * free the buffer, letting the test verify decompression. */
            if (job->err != 0 || job->dst == NULL) {
                /* Worker chose not to compress (no active dict yet) or
                 * ZSTD reported an error.
                 *
                 * TODO(S4.1): two distinct counter contributions feed
                 * here in S4.1:
                 *   - job->err > 0 (worker policy, e.g. no-dict): no
                 *     INFO counter — this is a benign expected state
                 *     (R2.1.5), tracked indirectly via
                 *     compression_state == "active" || "idle".
                 *   - job->err < 0 (real ZSTD error): increment
                 *     compression_errors_total per R2.10.1 and emit a
                 *     rate-limited LL_WARNING per R6.1.
                 * No live_ratio contribution — no compression actually
                 * ran, so there's no measured ratio to fold in. */
                if (job->dst != NULL) zfree(job->dst);
            } else {
                /* Net-savings guard (R2.4.3 / R2.2 second block):
                 *
                 *   compressed_size + header_size >=
                 *       uncompressed_size * (1 - savings_ratio_pct/100)
                 *
                 * Equivalent integer form below avoids floating point.
                 * Note that `job->dst_len` already includes the
                 * header (HEADER_SIZE + frame_len), so it IS the
                 * full on-heap footprint we want to compare against. */
                size_t uncompressed_len = sdslen(job->src);
                int pct = server.compression_min_savings_ratio;
                size_t threshold =
                    uncompressed_len - (uncompressed_len * (size_t)pct / 100u);

                if (job->dst_len >= threshold) {
                    /* No useful saving — discard the compressed form,
                     * leave the value uncompressed.
                     *
                     * TODO(S4.1): two contributions here in S4.1:
                     *   - compression_skipped_incompressible++ per
                     *     R2.10.1.
                     *   - Fold the actual measured ratio
                     *     (job->dst_len / uncompressed_len) into the
                     *     EMA compression_live_ratio_10m per R2.3.5
                     *     ("rejections contribute their actual
                     *     measured ratio, typically in [0.9, 1.05]").
                     *     Sustained high rejection rate inflates the
                     *     metric and naturally trips the drift
                     *     threshold → drives retraining. */
                    zfree(job->dst);
                }
                /* Net-savings guard accepted the compressed form. */
                else if (job->value == NULL) {
                    /* Test path (testOnlyCompressionWorkersEnqueueRaw):
                     * no real kvstore install. Tests that need the
                     * compressed buffer extract via
                     * testOnlyCompressionWorkersDrainOutbox before the
                     * production drain runs; if a job with value==NULL
                     * reaches here it means the test forgot to extract
                     * — dispose to avoid leaking, but the test is
                     * structurally broken. */
                    zfree(job->dst);
                } else {
                    /* Production install. compressionInstall handles
                     * staleness check, dbReplaceValue, registry ref
                     * bump. Always consumes job->dst (either installed
                     * via createCompressedObject or zfree'd on stale). */
                    compressionInstall(job);
                }
            }

            /* Release the caller's pin. For test-mode jobs (value==NULL)
             * there's nothing to release. */
            if (job->value != NULL) decrRefCount(job->value);
            zfree(job);

            total++;
        }
    }
    return total;
}

/* ============================================================
 * Test-only entry points (gtest)
 * ============================================================
 *
 * Convention follows quicklist.c / intset.c: define here, do NOT
 * declare in compression_workers.h. The gtest unit test declares
 * what it needs locally in its own extern "C" block. This keeps
 * the production-callable surface (the public header) free of
 * test-only symbols.
 *
 * The S2.5 encoder-path tests need to inspect the worker's
 * compressed output before the production drain handler frees it.
 * The peek-and-extract entry point below pulls completed jobs off
 * the outbox without freeing; the read entry point projects the
 * file-private compressionJob shape into a flat result struct
 * (defined alongside the gtest test code that uses it) so the
 * production code carries no record of the private job shape;
 * the free entry point frees a job + its dst buffer.
 */

/* Pop up to `budget` completed jobs into `jobs_out` without freeing
 * them. Caller takes ownership and MUST free each job via
 * testOnlyCompressionWorkersFreeJob. */
int testOnlyCompressionWorkersDrainOutbox(void **jobs_out, int budget) {
    if (!pool.initialized || jobs_out == NULL || budget <= 0) return 0;

    int total = 0;
    while (total < budget) {
        size_t batch = (size_t)(budget - total);
        if (batch > 64) batch = 64;
        size_t got = mpscDequeueBatch(&pool.outbox,
                                      jobs_out + total,
                                      batch);
        if (got == 0) break;
        total += (int)got;
    }
    return total;
}

/* Free a job + its dst buffer, mirroring what the production drain
 * handler does after install. */
void testOnlyCompressionWorkersFreeJob(void *job_ptr) {
    if (job_ptr == NULL) return;
    compressionJob *job = (compressionJob *)job_ptr;
    if (job->dst != NULL) zfree(job->dst);
    zfree(job);
}

/* Project the file-private compressionJob into the caller-provided
 * fields. The caller's struct shape is defined in the gtest test code;
 * we pass field pointers individually here so this function does not
 * have to know about that struct's layout. NULL-tolerant: any
 * out-pointer may be NULL to skip projecting that field. */
void testOnlyCompressionWorkersJobRead(void *job_ptr,
                                       const char **out_src,
                                       void **out_dst,
                                       size_t *out_dst_len,
                                       uint32_t *out_dict_id,
                                       int *out_err) {
    compressionJob *job = (compressionJob *)job_ptr;
    if (out_src) *out_src = job->src;
    if (out_dst) *out_dst = job->dst;
    if (out_dst_len) *out_dst_len = job->dst_len;
    if (out_dict_id) *out_dict_id = job->dict_id;
    if (out_err) *out_err = job->err;
}

void compressionWorkersWakeAll(void) {
    /* No-op when the pool is not initialized or the worker count is
     * zero — the caller (compressionRegistryRetire and other registry
     * state-change sites) is allowed to invoke this unconditionally
     * without checking pool state. With zero workers there is nothing
     * to wake; the registry's GC pass observes an empty
     * worker_quiescent_gen[] range and reclaims dicts immediately. */
    if (!pool.initialized || pool.n_threads == 0) return;
    mutexQueueWakeAll(pool.inbox);
}
