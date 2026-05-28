/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef __COMPRESSION_TRAIN_H
#define __COMPRESSION_TRAIN_H

/*
 * Dictionary training job coordination — main-thread sampler, bio-thread trainer.
 *
 * Design of record:
 *   .agents/planning/realtime-data-compression/design/detailed-design.md §2.3
 *   .agents/planning/realtime-data-compression/implementation/plan.md §4.4
 *
 * Threading model (R2.3.6 corrected per walkthrough T-3168235257):
 *
 *   Main thread   : incremental kvstore iteration, splices ~hz ticks of
 *                   sample collection. Selects eligible samples per the
 *                   R2.2 predicate. Copies sample bytes into a single
 *                   contiguous `samples_buffer` with a parallel
 *                   `sample_sizes[]` array. Submits the buffer to bio.
 *
 *   bio thread    : receives the immutable (buffer, sizes, count)
 *                   triple. Calls `ZDICT_trainFromBuffer` directly on
 *                   the flat buffer. Never touches robj / kvstore /
 *                   refcounts.
 *
 *   Main thread   : on bio completion, creates ZSTD_CDict/ZSTD_DDict
 *                   handles from the returned dictionary bytes and
 *                   installs via compressionDictAdd (promotion,
 *                   §2.3 R2.3.9).
 *
 * Why a new BIO_COMPRESSION_TRAIN job type and not the worker pool:
 * training is heavy, infrequent, and one-at-a-time. `bio` matches that
 * shape. The compression worker pool is optimized for per-value
 * fan-out and should not be blocked by seconds-long training runs.
 */

#include "server.h"
#include "compression_registry.h"

#include <stddef.h>
#include <stdint.h>

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

/* Called from compressionInit. Registers the BIO_COMPRESSION_TRAIN bio
 * job type and wires up the completion event fd that bio uses to signal
 * the main thread. */
void compressionTrainInit(void);

/* ========================================================================
 * Trigger — called from compressionCron and from `COMPRESSION TRAIN`
 * ========================================================================
 *
 * Reasons (R2.3.5):
 *   0 = first-training    : eligible-key counter reached
 *   1 = drift-retrain     : live ratio regressed below drift threshold
 *   2 = time-based        : periodic cadence elapsed
 *   3 = manual            : operator-triggered via `COMPRESSION TRAIN`
 *
 * Returns 1 if training was scheduled, 0 if a training is already
 * in-flight (single-inflight invariant — §2.3).
 */
int compressionTrainMaybeTrigger(int reason);

/* ========================================================================
 * Main-thread sample collection (spliced across cron ticks)
 * ========================================================================
 *
 * Called from compressionCron when a training is in the "collecting
 * samples" phase. Walks up to `budget` kvstore entries this tick; when
 * the target sample count is reached, hands the contiguous buffer off
 * to bio via bioSubmit. Returns the number of samples appended this
 * tick (0 when collection is complete or paused).
 */
int compressionTrainAdvanceSampling(int budget);

/* ========================================================================
 * Bio completion — back on main thread via bio's completion path
 * ========================================================================
 *
 * `new_pair`: on success, a fully-populated compressionDict ready
 *             for registry install. Ownership transfers to the callee
 *             (main-thread promoter).
 * `err`     : on failure, the caller passes an sds error message and
 *             new_pair must be NULL. Callee takes ownership of the sds
 *             (will sdsfree after logging).
 */
void compressionTrainCompleteFromBio(compressionDict *new_pair,
                                     sds err);

#endif /* __COMPRESSION_TRAIN_H */
