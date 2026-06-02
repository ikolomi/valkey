/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef __COMPRESSION_WORKERS_H
#define __COMPRESSION_WORKERS_H

/*
 * Compression worker pool — background compression only.
 *
 * Design of record:
 *   .agents/planning/realtime-data-compression/design/detailed-design.md §2.11
 *   .agents/planning/realtime-data-compression/design/detailed-design.md §3.3
 *   .agents/planning/realtime-data-compression/design/detailed-design.md §4.6
 *
 * Hard invariants (§2.11 R2.11.4):
 *   - Workers NEVER touch `robj`. They consume and produce flat byte
 *     buffers via the inbox / outbox queues.
 *   - Workers never mutate the dictionary registry; they only call
 *     compressionWorkerReportQuiescent() per-job to advance the QSBR
 *     generation counter (registry-side GC observes it).
 *   - Main thread owns all `robj` mutation: enqueue a candidate from
 *     dbAdd/dbOverwrite, poll the outbox on afterSleep, install the
 *     compressed buffer into the robj, update the registry refcount.
 *   - Pool is sized by `compression-threads` (independent of
 *     `io-threads` — §2.11 R2.11.1).
 *   - Sweep pacing is governed by `compression-sweep-max-cpu-pct`.
 *     On-demand jobs (training promotion, multi-key compress) are not
 *     paced; they are arrival-bounded.
 *
 * Phase 1 status (S2.4):
 *   The pool, queues, and worker thread loop are real. The per-job
 *   payload is a placeholder pass-through — the worker accepts a job,
 *   does no compression yet, and posts the (empty) result back.
 *   `ZSTD_compress_usingCDict` integration lands in S2.5 (encoder
 *   path); robj installation lands with it. The pool plumbing in this
 *   file is stable across S2.4 → S2.5 → S2.7; only the worker's per-
 *   job body and the outbox drain's install-into-robj logic change.
 */

#include "server.h"

#include <stddef.h>
#include <stdint.h>

/* ========================================================================
 * Compile-time bounds
 * ========================================================================
 *
 * Maximum number of concurrent worker threads supported by the pool.
 * Matches the runtime upper bound on `compression-threads` (range
 * 0..16 per R2.11.1). Sized independently of COMPRESSION_DICT_MAX
 * (compression_registry.h) — they happen to share the value 16 today
 * but mean different things. The registry-side QSBR generation array
 * (`worker_quiescent_gen[]`) MUST size by COMPRESSION_WORKERS_MAX, not
 * COMPRESSION_DICT_MAX.
 */
#define COMPRESSION_WORKERS_MAX 16

/* ========================================================================
 * Pool lifecycle
 * ========================================================================
 *
 * Called from compressionInit (startup) and from the
 * `compression-threads` config apply path (runtime resize). `n_threads`
 * == 0 disables the pool entirely — eligible candidates still enqueue
 * but no work happens (this is the "enable without auto-sweep" pattern
 * from §2.1 R2.1.3).
 *
 * Start/stop are NOT reentrant. Calling Start when already running, or
 * Stop when not running, is a programmer error and trips an assertion.
 * Resize is the safe re-entrant entry point and dispatches to start /
 * stop / restart internally.
 */

/* Initialize queues and spawn `n_threads` worker threads. Returns 0 on
 * success, -1 on pthread_create failure (caller should fail server
 * startup). After return: pool is hot, enqueue/drain are safe to call. */
int compressionWorkersStart(int n_threads);

/* Signal shutdown, wake all workers, join their threads, drain any
 * remaining outbox results, and free the queues. Idempotent — calling
 * on an already-stopped pool is a no-op. After return: enqueue/drain
 * become no-ops; safe to call Start again afterwards. */
void compressionWorkersStop(void);

/* Resize to `n_threads`. v1 implementation: graceful stop + restart
 * (no live add/remove). Returns 0 on success, -1 on failure. Same
 * range as Start (0..COMPRESSION_WORKERS_MAX). Called from the
 * `compression-threads` CONFIG SET apply hook. */
int compressionWorkersResize(int n_threads);

/* Wake every idle worker thread without enqueuing a job. Called by
 * compressionRegistryRetire (and other registry-side state changes)
 * to force workers parked on the inbox cond var to advance their QSBR
 * generation counter, so the registry GC pass can reclaim retired
 * dicts whose retire-time gen-snapshot would otherwise never be
 * crossed by a permanently-idle worker.
 *
 * No-op if the pool is not initialized or has zero workers — the
 * registry can call this unconditionally on every retire transition.
 *
 * Cross-module: this is the registry's only entry point into the
 * worker pool. The worker pool's queue primitives are otherwise
 * file-private to compression_workers.c. */
void compressionWorkersWakeAll(void);

/* ========================================================================
 * Worker identity contract — for §4.4 QSBR
 * ========================================================================
 *
 * Workers in the pool are identified by a stable integer in the range
 * [0, n_threads). The pool passes worker_id to each thread at startup
 * and the worker calls `compressionWorkerReportQuiescent(worker_id)`
 * after every job. This identity is the registry's reference into
 * `worker_quiescent_gen[]`.
 *
 * After a graceful Stop + Start (e.g. via Resize), worker_ids are
 * re-assigned starting from 0. The registry's quiescent_gen array is
 * NOT reset — generations only ever advance. This is safe because any
 * dict that captured an old worker's gen at retire time has been
 * either freed (registry-side GC fired between Stop and Start) or
 * promoted past it during the Stop's drain.
 */

/* ========================================================================
 * Candidate inbox (main thread → workers)
 * ========================================================================
 *
 * Enqueue a compression job for an already-eligible candidate.
 *
 * Ownership contract:
 *   - `key` (sds) and `src` (sds) are borrowed pointers. The pool
 *     reads them on the worker thread and the outbox drain on the
 *     main thread, but does NOT take ownership and does NOT free
 *     them. The caller MUST keep both pointers valid until the job
 *     surfaces on the outbox and the drain completes.
 *
 *   - In the production path (S2.7 write-path hook), the caller will
 *     hold `incrRefCount(val)` on the value's owning robj before
 *     calling Enqueue. The drain handler (S2.5) will call
 *     decrRefCount(val) once it has either installed or discarded
 *     the compressed result. The bumped refcount also enforces the
 *     immutable-snapshot invariant (§2.4 R2.4.4): any concurrent
 *     mutating command sees refcount >= 2 and goes through
 *     `dbUnshareStringValue`, COW-ing rather than mutating in place.
 *
 *   - In the S2.4 unit-test path the caller (test fixture) owns the
 *     sds pointers directly and frees them after the drain. There is
 *     no robj and no refcount to manage; the pool is exercised purely
 *     for its plumbing semantics.
 *
 * Active dict: the worker loads `compressionRegistryActive()` at
 * compress time per the QSBR contract (§4.6: "the worker loads the
 * active dict pointer atomically at compress time"). Enqueue does
 * NOT capture the dict_id — the field in the job struct is filled by
 * the worker after compression, so it can be carried into the
 * compressed-frame header on the outbox side.
 *
 * `version` is the robj version counter at enqueue time. The drain
 * compares it with the current version to detect concurrent rewrites
 * — if mismatched, the compressed result is discarded.
 *
 * Returns 0 on success, -1 if the pool is uninitialized, has zero
 * workers, or the inbox is full. On -1 the caller retains ownership
 * of `key`/`src` (and any incrRefCount the caller did) — the pool
 * never partially-acquires.
 */
int compressionWorkersEnqueue(const sds key,
                              int dbid,
                              uint64_t version,
                              sds src);

/* Test-only / introspection accessor: returns the current pool size
 * (0 if uninitialized). Used by unit tests to verify Resize actually
 * created or destroyed worker threads, since the pool's internal
 * state is otherwise file-private. NOT a stable runtime-config API —
 * production code should read `server.compression_threads` instead. */
int compressionWorkersGetThreadCount(void);

/* ========================================================================
 * Result outbox (workers → main thread)
 * ========================================================================
 *
 * Polled from compressionAfterSleep. Processes up to `budget` results,
 * installing compressed buffers into the owning robjs (via
 * createCompressedObject — which takes ownership of the flat buffer
 * produced by the worker; see compression_header.h for the zero-copy
 * contract) and running the net-savings guard (§2.4 R2.4.3). Returns
 * the number of results processed.
 *
 * Phase 1 status: drain currently frees jobs without installing — the
 * S2.5 encoder PR replaces the placeholder body with the real install
 * path.
 */
int compressionWorkersDrainOutbox(int budget);

/* ========================================================================
 * Test-only accessors (NOT a stable API)
 * ========================================================================
 *
 * The S2.5 encoder-path tests need to inspect the worker's compressed
 * output before the production drain handler frees it. The accessors
 * below extract a job pointer from the outbox without freeing, expose
 * its fields by value, and let the test reclaim the job.
 *
 * compressionJob is intentionally file-private to compression_workers.c
 * (only the public Enqueue API is part of the worker-pool contract);
 * these accessors are the only way a test can peek at the worker's
 * intermediate state. They MUST NOT be called from production code.
 *
 * Real production callers (S2.7 write-path hook) consume jobs via the
 * regular compressionWorkersDrainOutbox path, which installs the
 * buffer into a robj via createCompressedObject and frees the job.
 */

/* Pop up to `budget` completed jobs into `jobs_out` without freeing
 * them. Caller takes ownership and MUST free each job via
 * compressionWorkersFreeJobForTesting. */
int compressionWorkersDrainOutboxForTesting(void **jobs_out, int budget);

/* Free a job + its dst buffer, mirroring what the production drain
 * handler would do after install. */
void compressionWorkersFreeJobForTesting(void *job_ptr);

/* Field accessors for compressionJob. Tests verify the worker's output
 * by reading these. */
void       *compressionWorkersJobDstForTesting(void *job_ptr);
size_t      compressionWorkersJobDstLenForTesting(void *job_ptr);
uint32_t    compressionWorkersJobDictIdForTesting(void *job_ptr);
int         compressionWorkersJobErrForTesting(void *job_ptr);
const char *compressionWorkersJobSrcForTesting(void *job_ptr);

#endif /* __COMPRESSION_WORKERS_H */
