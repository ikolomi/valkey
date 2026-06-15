/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Tests for the compression sweeper.
 *
 * Internal model: 2 booleans (scan_in_progress, enable_once) +
 * timestamp + cursor. Apply hooks set state synchronously; the cron
 * tick consumes it. We drive the sweeper by manipulating
 * server.compression_master_switch / compression_automatic_sweeper /
 * compression_automatic_sweeper_interval / compression_threads,
 * calling the corresponding notify* callback explicitly (mimicking
 * what the apply hooks do in production), then calling
 * testOnlyCompressionSweepRunOneTick once per "tick".
 *
 * testOnlyCompressionSweepReset resets all state between tests.
 *
 * These tests deliberately do NOT exercise the per-key callback path
 * (kvstoreScan over a populated keyspace) — the unit-test environment
 * doesn't have a real serverDb / kvstore. State coverage is what
 * matters here; the per-key dispatch is a one-line if/else exercised
 * at the integration-test level (Tcl) and in production smoke runs.
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

        saved_master_ = server.compression_master_switch;
        saved_sweeper_ = server.compression_automatic_sweeper;
        saved_interval_ = server.compression_automatic_sweeper_interval;
        saved_threads_ = server.compression_threads;
        saved_pct_ = server.compression_sweep_max_cpu_pct;
        saved_dbnum_ = server.dbnum;

        /* Defaults consistent with src/config.c registration. */
        server.compression_master_switch = COMPRESSION_MASTER_OFF;
        server.compression_automatic_sweeper = COMPRESSION_AUTOMATIC_SWEEPER_DISABLED;
        server.compression_automatic_sweeper_interval = 0;
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
        server.compression_automatic_sweeper = saved_sweeper_;
        server.compression_automatic_sweeper_interval = saved_interval_;
        server.compression_threads = saved_threads_;
        server.compression_sweep_max_cpu_pct = saved_pct_;
        server.dbnum = saved_dbnum_;
    }

    /* Helpers that mimic what production apply hooks do: set the
     * server-global config field, then call the notify hook. */
    void setMaster(int v) {
        server.compression_master_switch = v;
        compressionSweepNotifyMasterSwitchChanged();
    }
    void setSweeper(int v) {
        server.compression_automatic_sweeper = v;
        compressionSweepNotifyAutomaticSweeperChanged();
    }
};

/* ============================================================
 * Initial state
 * ============================================================ */

TEST_F(CompressionSweepTest, InitNotRunningNoPasses) {
    EXPECT_EQ(0, compressionSweepIsRunning());
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(0u, compressionSweepGetKeysProcessed());
}

TEST_F(CompressionSweepTest, InitArmsEnableOnceWhenSweeperOnAndMasterSet) {
    /* If boot config has sweeper=enabled and master ∈ {compression,
     * decompression}, init arms enable_once so the first cron tick
     * after boot runs a pass — same as the disabled→enabled apply
     * hook at runtime. We simulate this by setting the server fields
     * directly (no notify), then re-invoking init. */
    server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
    server.compression_automatic_sweeper = COMPRESSION_AUTOMATIC_SWEEPER_ENABLED;
    testOnlyCompressionSweepReset(); /* re-init with new boot values */
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, InitDoesNotArmWhenMasterOff) {
    server.compression_master_switch = COMPRESSION_MASTER_OFF;
    server.compression_automatic_sweeper = COMPRESSION_AUTOMATIC_SWEEPER_ENABLED;
    testOnlyCompressionSweepReset();
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
}

/* ============================================================
 * Master=off semantics
 * ============================================================ */

TEST_F(CompressionSweepTest, MasterOffCronIsNoOp) {
    server.compression_master_switch = COMPRESSION_MASTER_OFF;
    server.compression_automatic_sweeper = COMPRESSION_AUTOMATIC_SWEEPER_ENABLED;
    for (int i = 0; i < 5; i++) {
        testOnlyCompressionSweepRunOneTick();
    }
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(0, compressionSweepIsRunning());
}

TEST_F(CompressionSweepTest, MasterOffRejectsForce) {
    server.compression_master_switch = COMPRESSION_MASTER_OFF;
    EXPECT_EQ(COMPRESSION_SWEEP_FORCE_REJECTED, compressionSweepForce());
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, MasterToOffAbortsInFlightScan) {
    /* Set up: pass just started, but mid-flight (we'd need a
     * real kvstore to actually be mid-flight; instead we rely on
     * the fact that with dbnum=0 the pass starts and completes in
     * the same tick, so we can't easily test "mid-flight" abort
     * without scaffolding. Instead test the simpler invariant:
     * setMaster(off) clears enable_once and any in-progress flag. */
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    /* enable_once is now armed. setMaster(off) should abort. */
    setMaster(COMPRESSION_MASTER_OFF);
    /* Cron tick: should NOT run a pass because master is off. */
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
}

/* ============================================================
 * Master-switch trigger
 * ============================================================ */

TEST_F(CompressionSweepTest, DirectionChangeTriggersOnePass) {
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(0, compressionSweepIsRunning());
}

TEST_F(CompressionSweepTest, NoChangeNoExtraPass) {
    /* With interval=0 and no triggers, repeated cron ticks must NOT
     * loop. This is the cardinal correctness property of the
     * simplified model. */
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    for (int i = 0; i < 10; i++) testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, EachDirectionChangeTriggersOnePass) {
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);

    setMaster(COMPRESSION_MASTER_COMPRESSION);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());

    setMaster(COMPRESSION_MASTER_DECOMPRESSION);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(2u, compressionSweepGetPassesCompleted());

    setMaster(COMPRESSION_MASTER_COMPRESSION);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(3u, compressionSweepGetPassesCompleted());

    setMaster(COMPRESSION_MASTER_OFF);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(3u, compressionSweepGetPassesCompleted()); /* off doesn't trigger */
}

TEST_F(CompressionSweepTest, MasterChangeWithSweeperDisabledNoTrigger) {
    /* Sweeper config disabled: master changes do NOT auto-schedule. */
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_DISABLED);
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, ForcePassInFlightDirectionChangeRestarts) {
    /* Bug fix: sweeper=disabled + FORCE pass in flight + master
     * changes to a different productive direction. The in-flight
     * scan must be aborted and restarted from cursor 0 with the new
     * direction; otherwise the keyspace ends up in a mixed state
     * (DBs scanned before the change got the old direction's work,
     * DBs after the change got the new direction's work).
     *
     * To leave scan_in_progress=1 across cron ticks (needed to
     * simulate "force pass in flight"), we use the master=compression
     * + threads=0 path: the cron sets scan_in_progress=1 when entering
     * the pass, then short-circuits at the threads=0 check without
     * completing. The flag persists. */
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_DISABLED);
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    server.compression_threads = 0;
    EXPECT_EQ(COMPRESSION_SWEEP_FORCE_OK, compressionSweepForce());
    testOnlyCompressionSweepRunOneTick();
    /* Force pass armed but threads=0 short-circuited completion;
     * scan_in_progress is set, no pass yet. */
    EXPECT_EQ(1, compressionSweepIsRunning());
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());

    /* Direction change while the force pass is in flight. The notify
     * hook must reset cursor + clear scan_in_progress + arm
     * enable_once, even though sweeper=disabled. */
    setMaster(COMPRESSION_MASTER_DECOMPRESSION);
    EXPECT_EQ(0, compressionSweepIsRunning());

    /* With threads restored, the next cron tick should run a fresh
     * pass with direction=decompression. Decompression doesn't need
     * the worker pool, so threads=0 is OK in that direction — but
     * restore for clarity. */
    server.compression_threads = 1;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

/* ============================================================
 * Sweeper-config trigger
 * ============================================================ */

TEST_F(CompressionSweepTest, SweeperEnabledKicksScan) {
    setMaster(COMPRESSION_MASTER_COMPRESSION); /* no trigger; sweeper=disabled */
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());

    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, SweeperEnabledMasterOffNoTrigger) {
    setMaster(COMPRESSION_MASTER_OFF);
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, SweeperDisabledAbortsAndDoesNotResume) {
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    /* Now disable BEFORE the cron runs the pass. enable_once was
     * armed by setSweeper/setMaster, but setSweeper(disabled) calls
     * abortScan() which clears it. */
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_DISABLED);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
}

/* ============================================================
 * FORCE semantics
 * ============================================================ */

TEST_F(CompressionSweepTest, ForceWithSweeperDisabledRunsOnePass) {
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_DISABLED);
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted()); /* no automatic */

    EXPECT_EQ(COMPRESSION_SWEEP_FORCE_OK, compressionSweepForce());
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());

    /* No follow-up triggers; subsequent ticks don't add passes. */
    for (int i = 0; i < 5; i++) testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

TEST_F(CompressionSweepTest, ForceCollapsesWithinOneTick) {
    setMaster(COMPRESSION_MASTER_COMPRESSION);
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

TEST_F(CompressionSweepTest, IntervalZeroNoPeriodicReruns) {
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    server.compression_automatic_sweeper_interval = 0;
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    /* No periodic re-runs. */
    for (int i = 0; i < 10; i++) testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
}

/* (Interval-expiry test deferred to Tcl integration test — needs
 * real wall-clock progression that gtest can't easily inject.) */

/* ============================================================
 * Non-functional state
 * ============================================================ */

TEST_F(CompressionSweepTest, MasterCompressionThreadsZeroSkipsScanWork) {
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    setMaster(COMPRESSION_MASTER_COMPRESSION);
    server.compression_threads = 0;
    testOnlyCompressionSweepRunOneTick();
    /* Pass NOT completed — we returned early from the threads check.
     * scan_in_progress stays set so a future threads>0 setting
     * resumes the pass from cursor 0. */
    EXPECT_EQ(0u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(1, compressionSweepIsRunning());
}

TEST_F(CompressionSweepTest, MasterDecompressionThreadsZeroIsFunctional) {
    /* Decompression mode doesn't use the worker pool. threads=0 OK. */
    setSweeper(COMPRESSION_AUTOMATIC_SWEEPER_ENABLED);
    setMaster(COMPRESSION_MASTER_DECOMPRESSION);
    server.compression_threads = 0;
    testOnlyCompressionSweepRunOneTick();
    EXPECT_EQ(1u, compressionSweepGetPassesCompleted());
    EXPECT_EQ(0, compressionSweepIsRunning());
}
