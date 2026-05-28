/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Tests for the compression worker pool — start/stop, runtime resize,
 * enqueue/drain end-to-end, shutdown drain, identity contract for QSBR.
 *
 * The pool's per-job body is a placeholder pass-through in S2.4
 * (compression_workers.c — workers leave dst=NULL/dst_len=0/err=0).
 * These tests verify the plumbing end-to-end:
 *   - threads spawn (verified via compressionWorkersGetThreadCount)
 *   - enqueue → worker → outbox → drain delivers the job
 *   - shutdown joins cleanly even if items are in flight
 *   - Resize across all transitions (0→N, N→M, N→0) is safe
 *
 * The encoder (S2.5) replaces the placeholder body and adds tests for
 * compressed-frame correctness; THIS test file is intentionally
 * encoder-agnostic so it remains stable across S2.4 → S2.5 → S2.7.
 *
 * Note on out-of-range testing for Start: compressionWorkersStart
 * validates n_threads via serverAssert (caller-bug; should never
 * happen in production where the config layer enforces 0..MAX before
 * calling). Out-of-range public surface is exercised via
 * compressionWorkersResize (see ResizeRejectsOutOfRange below). We do
 * not test Start with out-of-range directly because it would abort
 * the test process via the assertion path.
 */

#include "generated_wrappers.hpp"

#include <unistd.h>

#include <chrono>
#include <thread>

extern "C" {
#include "compression_registry.h"
#include "compression_workers.h"
#include "sds.h"
#include "server.h"
}

class CompressionWorkersTest : public ::testing::Test {
  protected:
    void SetUp() override {
        /* The pool's lifecycle calls serverLog at LL_NOTICE on
         * Start/Stop. Match the existing test_compression_header /
         * test_compression_registry pattern: initialize the minimum
         * server state needed for serverLogRaw not to dereference a
         * NULL logfile pointer. Verbosity at LL_WARNING suppresses the
         * NOTICE-level lifecycle logs to keep test output clean while
         * still letting any LL_WARNING through (so future warning
         * paths get test coverage). */
        server.logfile = (char *)"";
        server.verbosity = LL_WARNING;

        /* Registry must be initialized — the worker calls
         * compressionWorkerReportQuiescent which writes into the
         * registry's worker_quiescent_gen[] array. */
        compressionRegistryInit();

        /* Workers consult these on startup. The unit test environment
         * does not have a real config layer; make the values
         * deterministic. */
        server.compression_threads = 0;
        if (server.compression_cpulist) {
            zfree(server.compression_cpulist);
            server.compression_cpulist = nullptr;
        }
    }

    void TearDown() override {
        compressionWorkersStop(); /* idempotent */
        compressionRegistryRelease();
    }

    /* Wait until the outbox drains exactly `expected` jobs OR a deadline
     * elapses. Returns the number drained. */
    int drainUntil(int expected, int deadline_ms = 1000) {
        int drained = 0;
        auto start = std::chrono::steady_clock::now();
        while (drained < expected) {
            drained += compressionWorkersDrainOutbox(expected - drained);
            if (drained >= expected) break;
            auto elapsed = std::chrono::steady_clock::now() - start;
            if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count() >= deadline_ms) {
                break;
            }
            std::this_thread::sleep_for(std::chrono::microseconds(200));
        }
        return drained;
    }
};

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

TEST_F(CompressionWorkersTest, ThreadCountIsZeroBeforeStart) {
    /* Sanity: accessor returns 0 when pool is uninitialized. */
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
}

TEST_F(CompressionWorkersTest, StartZeroThreadsIsValid) {
    /* compression-threads=0 disables the pool but leaves it initialized
     * (queues exist, enqueue is rejected, drain is a no-op). */
    ASSERT_EQ(0, compressionWorkersStart(0));
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
    EXPECT_EQ(0, compressionWorkersDrainOutbox(10));
    /* enqueue rejected: pool is initialized but n_threads == 0. */
    sds key = sdsnew("k1");
    sds val = sdsnew("v1");
    EXPECT_EQ(-1, compressionWorkersEnqueue(key, 0, 1, val));
    sdsfree(key);
    sdsfree(val);
    compressionWorkersStop();
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
}

TEST_F(CompressionWorkersTest, StartOneThread) {
    ASSERT_EQ(0, compressionWorkersStart(1));
    EXPECT_EQ(1, compressionWorkersGetThreadCount());
    compressionWorkersStop();
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
}

TEST_F(CompressionWorkersTest, StartFourThreads) {
    ASSERT_EQ(0, compressionWorkersStart(4));
    EXPECT_EQ(4, compressionWorkersGetThreadCount());
    compressionWorkersStop();
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
}

TEST_F(CompressionWorkersTest, StartMaxThreads) {
    ASSERT_EQ(0, compressionWorkersStart(COMPRESSION_WORKERS_MAX));
    EXPECT_EQ(COMPRESSION_WORKERS_MAX, compressionWorkersGetThreadCount());
    compressionWorkersStop();
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
}

TEST_F(CompressionWorkersTest, StopIsIdempotent) {
    ASSERT_EQ(0, compressionWorkersStart(2));
    compressionWorkersStop();
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
    /* Second call must not crash or assert. */
    compressionWorkersStop();
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
    /* Third call after no Start must also be safe. */
    compressionWorkersStop();
    EXPECT_EQ(0, compressionWorkersGetThreadCount());
}

TEST_F(CompressionWorkersTest, EnqueueRejectedBeforeStart) {
    sds key = sdsnew("k1");
    sds val = sdsnew("v1");
    EXPECT_EQ(-1, compressionWorkersEnqueue(key, 0, 1, val));
    sdsfree(key);
    sdsfree(val);
}

TEST_F(CompressionWorkersTest, DrainBeforeStartReturnsZero) {
    EXPECT_EQ(0, compressionWorkersDrainOutbox(10));
}

/* ========================================================================
 * Enqueue → worker → drain end-to-end
 * ======================================================================== */

TEST_F(CompressionWorkersTest, SingleJobRoundTrip) {
    ASSERT_EQ(0, compressionWorkersStart(1));

    sds key = sdsnew("mykey");
    sds val = sdsnew("a moderately compressible value goes here");

    EXPECT_EQ(0, compressionWorkersEnqueue(key, 0, 42, val));

    /* The placeholder worker just passes through; we should see it
     * surface on the outbox very quickly. */
    EXPECT_EQ(1, drainUntil(1));

    /* Post-condition: outbox is empty after the expected count
     * surfaced. Asserting nothing was double-posted or leaked. */
    EXPECT_EQ(0, compressionWorkersDrainOutbox(10));

    sdsfree(key);
    sdsfree(val);
    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, BurstOf256JobsOneWorker) {
    /* Stress the inbox a bit; mutexQueue is unbounded so enqueue
     * always succeeds, but the worker is single-threaded. */
    ASSERT_EQ(0, compressionWorkersStart(1));

    constexpr int kCount = 256;
    sds keys[kCount];
    sds vals[kCount];
    for (int i = 0; i < kCount; i++) {
        keys[i] = sdsnew("k");
        vals[i] = sdsnew("v");
        EXPECT_EQ(0, compressionWorkersEnqueue(keys[i], 0, (uint64_t)i, vals[i]));
    }

    EXPECT_EQ(kCount, drainUntil(kCount, /*deadline_ms=*/2000));
    EXPECT_EQ(0, compressionWorkersDrainOutbox(10)); /* nothing extra */

    for (int i = 0; i < kCount; i++) {
        sdsfree(keys[i]);
        sdsfree(vals[i]);
    }
    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, BurstOf1024JobsFourWorkers) {
    /* With four workers, the same burst should drain faster but we
     * only test that it drains, not absolute timing. */
    ASSERT_EQ(0, compressionWorkersStart(4));
    EXPECT_EQ(4, compressionWorkersGetThreadCount());

    constexpr int kCount = 1024;
    sds keys[kCount];
    sds vals[kCount];
    for (int i = 0; i < kCount; i++) {
        keys[i] = sdsnew("k");
        vals[i] = sdsnew("v");
        EXPECT_EQ(0, compressionWorkersEnqueue(keys[i], 0, (uint64_t)i, vals[i]));
    }

    EXPECT_EQ(kCount, drainUntil(kCount, /*deadline_ms=*/3000));
    EXPECT_EQ(0, compressionWorkersDrainOutbox(10)); /* nothing extra */

    for (int i = 0; i < kCount; i++) {
        sdsfree(keys[i]);
        sdsfree(vals[i]);
    }
    compressionWorkersStop();
}

/* ========================================================================
 * Resize
 * ======================================================================== */

TEST_F(CompressionWorkersTest, ResizeStartsPoolWhenNotInitialized) {
    EXPECT_EQ(0, compressionWorkersResize(2));
    EXPECT_EQ(2, compressionWorkersGetThreadCount());
    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, ResizeIsNoOpWhenSameCount) {
    ASSERT_EQ(0, compressionWorkersStart(2));
    EXPECT_EQ(2, compressionWorkersGetThreadCount());
    EXPECT_EQ(0, compressionWorkersResize(2));
    EXPECT_EQ(2, compressionWorkersGetThreadCount());
    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, ResizeUpAndDown) {
    /* Verify each transition actually changes the live thread count. */
    ASSERT_EQ(0, compressionWorkersStart(1));
    EXPECT_EQ(1, compressionWorkersGetThreadCount());

    EXPECT_EQ(0, compressionWorkersResize(4));
    EXPECT_EQ(4, compressionWorkersGetThreadCount());

    EXPECT_EQ(0, compressionWorkersResize(8));
    EXPECT_EQ(8, compressionWorkersGetThreadCount());

    EXPECT_EQ(0, compressionWorkersResize(2));
    EXPECT_EQ(2, compressionWorkersGetThreadCount());

    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, ResizeToZeroDisablesPool) {
    ASSERT_EQ(0, compressionWorkersStart(2));
    EXPECT_EQ(2, compressionWorkersGetThreadCount());

    EXPECT_EQ(0, compressionWorkersResize(0));
    EXPECT_EQ(0, compressionWorkersGetThreadCount());

    /* Pool initialized but no workers — enqueue must reject. */
    sds key = sdsnew("k");
    sds val = sdsnew("v");
    EXPECT_EQ(-1, compressionWorkersEnqueue(key, 0, 1, val));
    sdsfree(key);
    sdsfree(val);

    /* Bring it back. */
    EXPECT_EQ(0, compressionWorkersResize(1));
    EXPECT_EQ(1, compressionWorkersGetThreadCount());
    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, ResizeRejectsOutOfRange) {
    /* Public dynamic surface (Resize) returns -1 on out-of-range.
     * Start asserts on out-of-range as a caller-bug — see file
     * header comment for the rationale. */
    EXPECT_EQ(-1, compressionWorkersResize(-1));
    EXPECT_EQ(-1, compressionWorkersResize(COMPRESSION_WORKERS_MAX + 1));
    EXPECT_EQ(0, compressionWorkersGetThreadCount()); /* unchanged */
}

TEST_F(CompressionWorkersTest, ResizeAcrossEnqueuedJobs) {
    /* Mid-flight resize: enqueue a burst, then resize while jobs are
     * still in flight. Resize is implemented as Stop+Start (graceful
     * shutdown of the old pool, start of a new pool with the new
     * size). The drop contract is:
     *
     *   - Jobs already POPPED by a worker BEFORE the resize: they
     *     proceed to completion, get posted to the outbox, and are
     *     drained by the next compressionWorkersDrainOutbox() call.
     *
     *   - Jobs STILL IN THE INBOX when Stop runs (workers haven't
     *     popped them yet): they are reclaimed by Stop's leftover
     *     drain — `zfree`d, never processed.
     *
     * This is acceptable because in production the dropped candidates
     * would be re-discovered by the next sweep tick (R2.4.1 sweeper),
     * and resize is an operator-driven event (CONFIG SET
     * compression-threads), not a data-path event. The test asserts
     * the contract: drained_before + drained_after <= kCount, and
     * kCount - drained may be > 0 (= the dropped fraction). */
    ASSERT_EQ(0, compressionWorkersStart(2));
    EXPECT_EQ(2, compressionWorkersGetThreadCount());

    constexpr int kCount = 64;
    sds keys[kCount];
    sds vals[kCount];
    for (int i = 0; i < kCount; i++) {
        keys[i] = sdsnew("k");
        vals[i] = sdsnew("v");
        EXPECT_EQ(0, compressionWorkersEnqueue(keys[i], 0, (uint64_t)i, vals[i]));
    }

    /* Some unknown number have surfaced by now. Drain them. */
    int drained_before = compressionWorkersDrainOutbox(kCount);

    /* Resize: Stop+Start. Stop reclaims any in-flight inbox items. */
    EXPECT_EQ(0, compressionWorkersResize(4));
    EXPECT_EQ(4, compressionWorkersGetThreadCount());

    /* After resize, drain anything else that surfaced from work that
     * had already been popped before Stop. */
    int drained_after = compressionWorkersDrainOutbox(kCount);

    /* Contract: total drained never exceeds enqueue count, and may be
     * less (dropped fraction). The new pool is functional regardless. */
    EXPECT_LE(drained_before + drained_after, kCount);
    EXPECT_GE(drained_before + drained_after, 0);

    for (int i = 0; i < kCount; i++) {
        sdsfree(keys[i]);
        sdsfree(vals[i]);
    }
    compressionWorkersStop();
}
