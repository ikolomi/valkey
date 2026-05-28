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
#include "compression_registry.h"
#include "mutexqueue.h"
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
    /* Set by Enqueue (immutable for the rest of the job lifetime): */
    sds key; /* key name; main thread re-resolves the robj on drain */
    int dbid;
    uint64_t version; /* robj version counter; detects concurrent rewrites */
    sds src;          /* value sds at enqueue time; pinned by caller's incrRefCount */

    /* Filled by worker on the worker thread (S2.5 will populate the
     * dst/dst_len/dict_id with real compressed output; S2.4 leaves
     * them as zero-initialised below). The worker loads the active
     * dict via compressionRegistryActive() at compress time per the
     * QSBR contract (§4.6) — Enqueue does NOT capture the dict_id
     * itself. The dict_id field exists on the job struct so the
     * worker can carry the snapshot it actually used into the
     * compressed-frame header on the outbox side. */
    uint32_t dict_id; /* dict the worker actually used; 0 == none / placeholder */
    void *dst;        /* zmalloc'd compressedHeader + frame; owned by main thread post-drain */
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

        /* Phase 1 placeholder: pass-through. S2.5 replaces the body
         * with `ZSTD_compress_usingCDict(...)` against the dict
         * loaded from `job->dict_id` (or the active dict if 0).
         *
         * The placeholder semantics — `dst = NULL`, `dst_len = 0`,
         * `err = 0` — tell the outbox drain "nothing to install,
         * just clean up the job and release the caller's refcount."
         * This lets the rest of the plumbing (write-path enqueue,
         * outbox drain, refcount lifecycle) be exercised end-to-end
         * before the encoder lands. */
        job->dst = NULL;
        job->dst_len = 0;
        job->err = 0;

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

int compressionWorkersEnqueue(const sds key,
                              int dbid,
                              uint64_t version,
                              sds src) {
    if (!pool.initialized || pool.n_threads == 0) return -1;

    compressionJob *job = zmalloc(sizeof(*job));
    job->key = key;
    job->dbid = dbid;
    job->version = version;
    job->src = src;
    /* dict_id is NOT captured by Enqueue. It is filled by the worker
     * after compression (S2.5), reflecting the dict the worker
     * actually used. See §4.6 of the design doc. The placeholder
     * worker leaves it zero-initialised. */
    job->dict_id = 0;
    job->dst = NULL;
    job->dst_len = 0;
    job->err = 0;

    /* mutexQueueAdd is fire-and-forget on cond_broadcast; if the inbox
     * has unbounded capacity we'd never drop. mutexQueue is unbounded
     * (it's a fifo + mutex), so enqueue always succeeds. The bounded-
     * inbox concern from §4.6 will be revisited in S2.x if operational
     * experience shows we need an explicit cap. For v1 we accept the
     * unbounded inbox as a simplification — the natural backpressure
     * comes from the sweep-pacing config (§2.11 R2.11.2) which limits
     * how fast the producer side can submit. */
    mutexQueueAdd(pool.inbox, job);
    return 0;
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

            /* Phase 1 placeholder: nothing to install. S2.5 replaces
             * this with the real net-savings guard + robj install
             * via createCompressedObject(job->dst, job->dst_len),
             * plus compressionRegistryIncRef(job->dict_id) and the
             * decrRefCount that releases the caller's pin from
             * compressionWorkersEnqueue.
             *
             * For S2.4 the placeholder worker leaves dst=NULL/dst_len=0,
             * so there's nothing to install and nothing to free in
             * dst. We still free the job struct itself. */
            if (job->dst != NULL) zfree(job->dst);
            zfree(job);

            total++;
        }
    }
    return total;
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
