/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Tests for compression_incompressible.c — the dict-ID scoped retry
 * guard for keys whose post-compression net-savings check rejected
 * them (R2.4 / Q6b / PR #1 Thread #20).
 *
 * Behavior under test:
 *   - A key never marked is retry-eligible.
 *   - A key marked under dict_id D stays ineligible while the active
 *     dict_id stays D.
 *   - A key marked under dict_id D becomes eligible when the active
 *     dict_id changes to D' != D (primary signal: dict promoted).
 *   - A key marked under dict_id D becomes eligible after
 *     `compression-retry-interval` seconds, even if dict_id is
 *     unchanged (fallback signal: catches content-driven changes).
 *   - Mark on an existing entry overwrites the dict_id and timestamp.
 *   - Clear removes the entry; subsequent retry-eligible returns 1.
 *   - Pre-init / post-release: every operation is a safe no-op or
 *     "always eligible" read.
 */

#include "generated_wrappers.hpp"

#include <cstdint>

extern "C" {
#include "compression_incompressible.h"
#include "server.h"
}

class CompressionIncompressibleTest : public ::testing::Test {
  protected:
    /* Snapshot of the only server-state field this module reads:
     * `compression_retry_interval` (drives the time-fallback branch). */
    int saved_retry_interval;
    long long saved_mstime;

    void SetUp() override {
        saved_retry_interval = server.compression_retry_interval;
        saved_mstime = server.mstime;
        server.compression_retry_interval = 3600; /* default: 1 h */
        server.mstime = 1000000LL;                /* stable anchor */
        compressionIncompressibleInit();
    }

    void TearDown() override {
        compressionIncompressibleRelease();
        server.compression_retry_interval = saved_retry_interval;
        server.mstime = saved_mstime;
    }

    /* Build a fresh sds key. Tests are responsible for sdsfree-ing on
     * the way out (the table dups on insert; callers always own
     * their original sds). */
    sds makeKey(const char *s) {
        return sdsnew(s);
    }
};

/* ============================================================
 * Pre-init / post-release safety
 * ============================================================ */

TEST_F(CompressionIncompressibleTest, RetryEligibleBeforeInitReturnsTrue) {
    /* The fixture's SetUp already initialized; release first to simulate
     * the pre-init state explicitly. */
    compressionIncompressibleRelease();

    sds key = makeKey("foo");
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 42u));
    /* Mark and Clear must also be safe no-ops. */
    compressionIncompressibleMark(key, 42u);
    compressionIncompressibleClear(key);
    EXPECT_EQ(0u, compressionIncompressibleSize());

    sdsfree(key);
    /* Restore for TearDown's Release (it tolerates double-release). */
    compressionIncompressibleInit();
}

TEST_F(CompressionIncompressibleTest, SizeIsZeroAfterInit) {
    EXPECT_EQ(0u, compressionIncompressibleSize());
}

/* ============================================================
 * Basic mark/lookup/clear
 * ============================================================ */

TEST_F(CompressionIncompressibleTest, NeverMarkedIsEligible) {
    sds key = makeKey("foo");
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 42u));
    sdsfree(key);
}

TEST_F(CompressionIncompressibleTest, MarkedUnderSameDictIsNotEligible) {
    sds key = makeKey("foo");
    compressionIncompressibleMark(key, 42u);
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(key, 42u));
    EXPECT_EQ(1u, compressionIncompressibleSize());
    sdsfree(key);
}

TEST_F(CompressionIncompressibleTest, MarkedKeyClearedBecomesEligible) {
    sds key = makeKey("foo");
    compressionIncompressibleMark(key, 42u);
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(key, 42u));

    compressionIncompressibleClear(key);
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 42u));
    EXPECT_EQ(0u, compressionIncompressibleSize());
    sdsfree(key);
}

TEST_F(CompressionIncompressibleTest, ClearOnUnmarkedKeyIsHarmless) {
    sds key = makeKey("foo");
    compressionIncompressibleClear(key); /* not in table */
    EXPECT_EQ(0u, compressionIncompressibleSize());
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 42u));
    sdsfree(key);
}

/* ============================================================
 * Primary retry signal — dict_id changed
 * ============================================================ */

TEST_F(CompressionIncompressibleTest, DifferentDictIdMakesKeyEligible) {
    sds key = makeKey("foo");
    compressionIncompressibleMark(key, 42u);

    /* Same dict → still ineligible. */
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(key, 42u));
    /* New dict → eligible (dict promotion is the primary retry signal). */
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 43u));
    /* And again with another new id. */
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 100u));
    sdsfree(key);
}

TEST_F(CompressionIncompressibleTest, DictIdNoneZeroSentinelStillScopes) {
    /* The sentinel "no active dict" (dict_id 0) is a legitimate value;
     * a key marked under it stays ineligible while active is also 0,
     * eligible when active changes to anything else. */
    sds key = makeKey("foo");
    compressionIncompressibleMark(key, 0u);

    EXPECT_EQ(0, compressionIncompressibleRetryEligible(key, 0u));
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 1u));
    sdsfree(key);
}

/* ============================================================
 * Mark on existing entry overwrites
 * ============================================================ */

TEST_F(CompressionIncompressibleTest, ReMarkUpdatesDictIdAndTimestamp) {
    sds key = makeKey("foo");

    /* First mark under dict 42 at t=1_000_000. */
    compressionIncompressibleMark(key, 42u);
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(key, 42u));
    EXPECT_EQ(1u, compressionIncompressibleSize());

    /* Advance virtual time and re-mark under a new dict. */
    server.mstime = 1000000LL + 7200LL * 1000LL; /* +2 h */
    compressionIncompressibleMark(key, 99u);

    /* Still one entry — re-mark must overwrite, not append. */
    EXPECT_EQ(1u, compressionIncompressibleSize());
    /* Old dict id is no longer recorded; query against 42 is now eligible. */
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 42u));
    /* New dict id is the active scope. */
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(key, 99u));
    sdsfree(key);
}

/* ============================================================
 * Time-fallback signal
 * ============================================================ */

TEST_F(CompressionIncompressibleTest, TimeFallbackTriggersAfterRetryInterval) {
    server.compression_retry_interval = 60; /* 60 s */
    sds key = makeKey("foo");
    compressionIncompressibleMark(key, 42u);

    /* Same dict, age == 0: ineligible. */
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(key, 42u));

    /* Same dict, age = 30 s: ineligible. */
    server.mstime = 1000000LL + 30LL * 1000LL;
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(key, 42u));

    /* Same dict, age = 60 s: eligible (boundary is `>=`). */
    server.mstime = 1000000LL + 60LL * 1000LL;
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 42u));

    /* Same dict, age = 600 s: eligible. */
    server.mstime = 1000000LL + 600LL * 1000LL;
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 42u));
    sdsfree(key);
}

TEST_F(CompressionIncompressibleTest, RetryIntervalZeroAlwaysEligible) {
    /* `compression-retry-interval == 0` disables the time-based scope —
     * matches §7.1 transparency-mode harness config. */
    server.compression_retry_interval = 0;
    sds key = makeKey("foo");
    compressionIncompressibleMark(key, 42u);

    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 42u));
    sdsfree(key);
}

TEST_F(CompressionIncompressibleTest, DictChangeTrumpsTimeFallback) {
    /* If both signals would fire (age >= retry_interval AND dict !=
     * failed_dict_id), the function returns eligible regardless of
     * which path is taken first. */
    server.compression_retry_interval = 60;
    sds key = makeKey("foo");
    compressionIncompressibleMark(key, 42u);

    /* Advance time past the interval and change the dict. */
    server.mstime = 1000000LL + 120LL * 1000LL;
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(key, 99u));
    sdsfree(key);
}

/* ============================================================
 * Multi-key independence
 * ============================================================ */

TEST_F(CompressionIncompressibleTest, MultipleKeysAreIndependent) {
    sds k1 = makeKey("alpha");
    sds k2 = makeKey("beta");
    sds k3 = makeKey("gamma");

    compressionIncompressibleMark(k1, 1u);
    compressionIncompressibleMark(k2, 2u);
    /* k3 never marked. */

    EXPECT_EQ(2u, compressionIncompressibleSize());

    EXPECT_EQ(0, compressionIncompressibleRetryEligible(k1, 1u)); /* same dict */
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(k1, 2u)); /* different dict */
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(k2, 2u)); /* same dict */
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(k2, 1u)); /* different dict */
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(k3, 1u)); /* never marked */

    /* Clearing one doesn't affect the other. */
    compressionIncompressibleClear(k1);
    EXPECT_EQ(1u, compressionIncompressibleSize());
    EXPECT_EQ(1, compressionIncompressibleRetryEligible(k1, 1u));
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(k2, 2u));

    sdsfree(k1);
    sdsfree(k2);
    sdsfree(k3);
}

TEST_F(CompressionIncompressibleTest, KeyOwnershipSurvivesCallerFree) {
    /* The table dups on insert — caller can free the original sds and
     * subsequent operations on a fresh dup-key still find the entry. */
    sds k1 = makeKey("foo");
    compressionIncompressibleMark(k1, 42u);
    sdsfree(k1); /* original gone */

    sds k2 = makeKey("foo"); /* equal-bytes lookup key */
    EXPECT_EQ(0, compressionIncompressibleRetryEligible(k2, 42u));
    EXPECT_EQ(1u, compressionIncompressibleSize());

    /* Clear via the lookup-key dup; entry's owned key is freed by the
     * destructor. */
    compressionIncompressibleClear(k2);
    EXPECT_EQ(0u, compressionIncompressibleSize());
    sdsfree(k2);
}
