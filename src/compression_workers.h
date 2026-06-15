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
 *   - `value` MUST be a pinned robj — the caller holds `incrRefCount(value)`
 *     before calling. The drain handler releases the pin via
 *     `decrRefCount(value)` after install (or discard).
 *
 *   - The bumped refcount also enforces the immutable-snapshot invariant
 *     (§2.4 R2.4.4): any concurrent mutating command sees `refcount >= 2`
 *     and goes through `dbUnshareStringValue`, COW-ing rather than
 *     mutating in place. AND it reserves the robj's memory address so
 *     the drain handler's pointer-equality check (`*kvstore_slot ==
 *     job->value`) is ABA-safe — the allocator cannot reuse the
 *     address for a different robj while the pin holds.
 *
 *   - The worker reads `objectGetVal(value)` once at enqueue time
 *     (captured into `job->src`) and never touches the robj
 *     afterwards. R2.11.4 stays intact: workers consume flat byte
 *     buffers, not robjs.
 *
 *   - For unit tests that don't have a real robj-owned value (no
 *     server.db, no kvstore), use `testOnlyCompressionWorkersEnqueueRaw`
 *     below — it stores `job->value = NULL` so the production drain's
 *     install path is skipped. Tests that need raw enqueue extract
 *     jobs via `testOnlyCompressionWorkersDrainOutbox` BEFORE the
 *     production drain runs.
 *
 * Active dict: the worker loads `compressionRegistryActive()` at
 * compress time per the QSBR contract (§4.6: "the worker loads the
 * active dict pointer atomically at compress time"). Enqueue does
 * NOT capture the dict_id — the field in the job struct is filled by
 * the worker after compression, so it can be carried into the
 * compressed-frame header on the outbox side.
 *
 * Returns COMPRESSION_ENQUEUE_OK (0) on success.
 * Returns COMPRESSION_ENQUEUE_DISABLED if the pool is uninitialized
 *   or has zero workers (configuration state, not back-pressure).
 * Returns COMPRESSION_ENQUEUE_FULL if the bounded inbox is at
 *   capacity (`max(256, 128 * compression-threads)`); this is
 *   back-pressure — the caller is expected to drop the candidate
 *   AND increment the appropriate per-caller counter
 *   (`compression_candidates_dropped_total` for write-path drops,
 *   `compression_sweep_backpressure_total` for sweeper pauses).
 *   The pool itself does NOT increment any counter on full — the
 *   distinction matters to operators (different remediations per
 *   §2.10 R2.10.4).
 *
 * On any non-zero return the caller retains the pin and is
 * responsible for releasing it.
 */
#define COMPRESSION_ENQUEUE_OK 0
#define COMPRESSION_ENQUEUE_DISABLED -1
#define COMPRESSION_ENQUEUE_FULL -2
int compressionWorkersEnqueue(robj *value, int dbid);

/* Returns 1 iff the inbox is at or above its soft cap (used by the
 * sweeper for pre-check before kvstoreScan, so the sweeper can pause
 * cleanly at a bucket boundary instead of dropping mid-callback).
 * Returns 0 if pool is uninitialized (no enqueue path active). */
int compressionWorkersInboxIsFull(void);

/* INFO accessors. All four counters are zeroed at pool start; reset
 * on a fresh pool start (resize via stop+start does NOT reset, since
 * §2.10 R2.10.4 says "cumulative since process start" semantically;
 * resize is rare and operators reading these expect monotonic). */
uint64_t compressionWorkersGetCandidatesDropped(void);
uint64_t compressionWorkersGetOutboxBackpressure(void);

/* Increment the candidates-dropped counter. Called from the write
 * path (compression.c) when compressionWorkersEnqueue returns
 * COMPRESSION_ENQUEUE_FULL. Main-thread only; not atomic. */
void compressionWorkersIncrCandidatesDropped(void);

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

#endif /* __COMPRESSION_WORKERS_H */
