/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Tests for the compression sweeper state machine.
 *
 * The sweeper is a cron-driven state machine; we drive it from tests
 * by manipulating server.compression_master_switch /
 * compression_active_sweeper / compression_active_sweeper_interval +
 * compression_threads, then calling testOnlyCompressionSweepRunOneTick
 * once per "tick". testOnlyCompressionSweepReset resets the state
 * struct between tests.
 *
 * These tests deliberately do NOT exercise the per-key callback path
 * (kvstoreScan over a populated keyspace) — the unit-test environment
 * doesn't have a real serverDb / kvstore. State-machine coverage is
 * what matters; the per-key dispatch is a one-line if/else and is
 * exercised at the integration-test level (Tcl) and in production
 * smoke runs.
 */

#include "generated_wrappers.hpp"

extern "C" {
#include "compression.h"
#include "compression_sweep.h"
#include "monotonic.h"
#include "server.h"
}

class CompressionSweepTest : public ::testing::Test {
  protected:
    int saved_master_;
    int saved_sweeper_;
    int saved_interval_;
    int saved_threads_;
    int saved_pct_;
    int saved_dbnum_;

    void SetUp() override {
        /* serverLog requires a non-NULL logfile pointer. Suppress
         * NOTICE/log spam from the sweeper by raising verbosity to
         * LL_WARNING; the tests don't inspect the log. */
        server.logfile = (char *)"";
        server.verbosity = LL_WARNING;

        /* Sweeper uses elapsedStart()/elapsedUs() for the per-tick
         * budget; those resolve to getMonotonicUs() which is a
         * function-pointer initialized lazily in production via
         * monotonicInit(). Unit tests need to call it explicitly. */
        monotonicInit();

        /* Snapshot mutable server state. */
        saved_master_ = server.compression_master_switch;
        saved_sweeper_ = server.compression_active_sweeper;
        saved_interval_ = server.compression_active_sweeper_interval;
        saved_threads_ = server.compression_threads;
        saved_pct_ = server.compression_sweep_max_cpu_pct;
        saved_dbnum_ = server.dbnum;

        /* Defaults consistent with src/config.c registration. */
        server.compression_master_switch = COMPRESSION_MASTER_OFF;
        server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_DISABLED;
        server.compression_active_sweeper_interval = 0;
        server.compression_threads = 1;
        server.compression_sweep_max_cpu_pct = 25;
        /* Pretend we have zero databases; runSweepBudget then sees no
         * server.db slots to scan and the pass completes instantly.
         * Avoids requiring real kvstore initialization in the unit
         * test environment. */
        server.dbnum = 0;

        testOnlyCompressionSweepReset();
    }

    void TearDown() override {
        testOnlyCompressionSweepReset();
        server.compression_master_switch = saved_master_;
        server.compression_active_sweeper = saved_sweeper_;
        server.compression_active_sweeper_interval = saved_interval_;
        server.compression_threads = saved_threads_;
        server.compression_sweep_max_cpu_pct = saved_pct_;
        server.dbnum = saved_dbnum_;
    }
};

/* ============================================================
 * Initial state
 * ============================================================ */

TEST_F(CompressionSweepTest, InitStateIsDisabled) {
    /* After init, master=off + sweeper=disabled => DISABLED. */
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_DISABLED, compressionSweepGetState());
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(0u, compressionSweepGetKeysProcessed());
}

TEST_F(CompressionSweepTest, StateNamesAreStable) {
    /* INFO / dashboard scrapers depend on these strings. */
    EXPECT_STREQ("disabled", compressionSweepStateName(COMPRESSION_SWEEPER_STATE_DISABLED));
    EXPECT_STREQ("idle", compressionSweepStateName(COMPRESSION_SWEEPER_STATE_IDLE));
    EXPECT_STREQ("scanning", compressionSweepStateName(COMPRESSION_SWEEPER_STATE_SCANNING));
    EXPECT_STREQ("sleeping", compressionSweepStateName(COMPRESSION_SWEEPER_STATE_SLEEPING));
    EXPECT_STREQ("?", compressionSweepStateName(99));
}

/* ============================================================
 * Master=off semantics
 * ============================================================ */

TEST_F(CompressionSweepTest, MasterOffWithSweeperEnabledIsIdle) {
    /* R2.1.6: master=off + sweeper=enabled is allowed but no-op.
     * State should be IDLE (not DISABLED — the engine is enabled
     * but has no direction). */
    server.compression_master_switch = COMPRESSION_MASTER_OFF;
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_IDLE, compressionSweepGetState());
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, MasterOffRejectsForce) {
    server.compression_master_switch = COMPRESSION_MASTER_OFF;
    int rc = compressionSweepForce();
    EXPECT_EQ(COMPRESSION_SWEEP_FORCE_REJECTED, rc);
    /* No pass should run on the next tick. */
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
}

/* ============================================================
 * Direction-change trigger
 * ============================================================ */

TEST_F(CompressionSweepTest, DirectionChangeTriggersOnePass) {
    /* Boot with sweeper=enabled, master=off => IDLE. */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_IDLE, compressionSweepGetState());

    /* Flip master to compression => triggers pass; with dbnum=0 it
     * completes in this single tick. interval=0 => state goes IDLE. */
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_IDLE, compressionSweepGetState());
}

TEST_F(CompressionSweepTest, IdleWithoutDirectionChangeStaysIdle) {
    /* The cardinal correctness property: with interval=0 and no
     * direction change / FORCE / config flip, IDLE stays IDLE. The
     * sweeper must NOT loop infinitely after a pass completes. */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    server.compression_active_sweeper_interval = 0;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());

    /* 10 more ticks without any config change — pass count must
     * not increase. */
    for (int i = 0; i < 10; i++) {
        testOnlyCompressionSweepRunOneTick();
    }
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_IDLE, compressionSweepGetState());
}

TEST_F(CompressionSweepTest, EachDirectionChangeTriggersExactlyOnePass) {
    /* off -> compression -> decompression -> compression -> off. Each
     * non-off transition triggers exactly one pass. */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;

    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());

    server.compression_master_switch = COMPRESSION_MASTER_DECOMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(2u, compressionSweepGetPassesCompleted());

    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(3u, compressionSweepGetPassesCompleted());

    server.compression_master_switch = COMPRESSION_MASTER_OFF;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(3u, compressionSweepGetPassesCompleted()); /* off doesn't trigger */
}

/* ============================================================
 * FORCE semantics
 * ============================================================ */

TEST_F(CompressionSweepTest, ForceWithSweeperDisabledRunsOnePass) {
    /* R2.1.4: COMPRESSION SWEEP FORCE is allowed even with
     * compression-active-sweeper=disabled (the operator's catch-up
     * affordance). */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_DISABLED;
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted()); /* baseline: no pass */

    int rc = compressionSweepForce();
    EXPECT_EQ(COMPRESSION_SWEEP_FORCE_OK, rc);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());

    /* After the FORCE pass completes, with sweeper=disabled, state
     * should return to DISABLED (not IDLE — config wins). */
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_DISABLED, compressionSweepGetState());

    /* Subsequent ticks shouldn't run more passes. */
    for (int i = 0; i < 5; i++) {
        testOnlyCompressionSweepRunOneTick();
    }
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, ForceIsIdempotentWithinOneTickWindow) {
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    EXPECT_EQ(COMPRESSION_SWEEP_FORCE_OK, compressionSweepForce());
    EXPECT_EQ(COMPRESSION_SWEEP_FORCE_OK, compressionSweepForce());
    EXPECT_EQ(COMPRESSION_SWEEP_FORCE_OK, compressionSweepForce());
    /* Three FORCE calls before any tick collapse to one pass. */
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

/* ============================================================
 * Interval semantics
 * ============================================================ */

TEST_F(CompressionSweepTest, IntervalGreaterThanZeroEntersSleeping) {
    /* With interval > 0, after pass completion state goes SLEEPING
     * (not IDLE). The actual interval-expiry transition is timing-
     * sensitive and harder to test without injecting a clock — the
     * Tcl integration test covers that. */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    server.compression_active_sweeper_interval = 60; /* 60 seconds */
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_SLEEPING, compressionSweepGetState());

    /* Sub-interval ticks should not trigger another pass. */
    for (int i = 0; i < 5; i++) {
        testOnlyCompressionSweepRunOneTick();
    }
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_SLEEPING, compressionSweepGetState());
}

TEST_F(CompressionSweepTest, IntervalZeroEntersIdleNotSleeping) {
    /* With interval=0, after pass completion state goes IDLE. */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    server.compression_active_sweeper_interval = 0;
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_IDLE, compressionSweepGetState());
}

/* ============================================================
 * Non-functional state warning
 * ============================================================ */

TEST_F(CompressionSweepTest, MasterCompressionThreadsZeroSkipsPass) {
    /* R2.1.6: master=compression + threads=0 is allowed but the
     * sweeper skips the pass (every enqueue would drop). */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    server.compression_threads = 0;
    testOnlyCompressionSweepRunOneTick();
    /* No pass completion (we returned early before runSweepBudget). */
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
    /* State should be SCANNING — we transitioned in but skipped the
     * actual scan; the cursor is left where it was so a future
     * threads>0 setting picks up where we left off. */
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_SCANNING, compressionSweepGetState());
}

TEST_F(CompressionSweepTest, MasterDecompressionThreadsZeroIsFunctional) {
    /* In decompression mode the worker pool is unused, so threads=0
     * is fine. Sweeper still runs and completes. */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    server.compression_master_switch = COMPRESSION_MASTER_DECOMPRESSION;
    server.compression_threads = 0;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_IDLE, compressionSweepGetState());
}

/* ============================================================
 * Config-toggle semantics
 * ============================================================ */

TEST_F(CompressionSweepTest, ConfigEnabledTransitionStartsPass) {
    /* Boot with sweeper=disabled, master=compression => DISABLED
     * (no work). Toggle sweeper to enabled => one pass on the next
     * tick. */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_DISABLED;
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_DISABLED, compressionSweepGetState());

    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, ConfigDisabledMidPassCompletesThenGoesDisabled) {
    /* Start a pass with sweeper=enabled. With dbnum=0 the pass
     * completes in the same tick where it started. Then flip to
     * disabled → state should be DISABLED (pass already complete). */
    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_ENABLED;
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());

    server.compression_active_sweeper = COMPRESSION_ACTIVE_SWEEPER_DISABLED;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(COMPRESSION_SWEEPER_STATE_DISABLED, compressionSweepGetState());
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted()); /* no extra pass */
}
