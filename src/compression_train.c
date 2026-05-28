/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_train.c — Phase 0 stub.
 *
 * Phase 1 (plan.md §5 — dictionary lifecycle subsystem) implements:
 *   - Main-thread incremental kvstore iteration (splices over
 *     serverCron ticks, reuses active-expiry/defrag pattern)
 *   - Contiguous samples_buffer + sample_sizes[] assembly on main
 *   - BIO_COMPRESSION_TRAIN job submission
 *   - Bio-thread ZDICT_trainFromBuffer call
 *   - Main-thread promotion of the trained dict via the registry
 *
 * The corrected threading split (walkthrough T-3168235257) keeps bio
 * off robj/kvstore entirely, which preserves the §2.11 R2.11.4
 * invariant.
 */

#include "server.h"
#include "compression_train.h"
#include "compression_registry.h"

void compressionTrainInit(void) {
    /* Phase 0: no BIO_COMPRESSION_TRAIN registration yet. */
}

int compressionTrainMaybeTrigger(int reason) {
    UNUSED(reason);
    /* Phase 0: never schedule training — feature is disabled. */
    return 0;
}

int compressionTrainAdvanceSampling(int budget) {
    UNUSED(budget);
    return 0;
}

void compressionTrainCompleteFromBio(compressionDictPair *new_pair,
                                     sds err) {
    /* Phase 0: if somehow called (should not be until the bio training
     * job is wired), we must honor the ownership contract: free both
     * the pair and the error sds. Since Phase 0 never submits a job,
     * this path is unreachable in practice, but the defensive free
     * keeps later wiring honest. */
    if (new_pair) {
        /* In Phase 1 this will go through a compressionDictPair free
         * helper that also decrements the registry's cap counter. */
        if (new_pair->bytes) zfree(new_pair->bytes);
        zfree(new_pair);
    }
    if (err) sdsfree(err);
}
