/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Tests for compressionIsEligible() — the R2.2 / Q6 eligibility
 * predicate. The predicate is policy-aware: in LRU and noeviction modes
 * it applies time-based settle/idle thresholds; in LFU mode it applies a
 * frequency threshold.
 *
 * Each test sets up a robj, configures the relevant `server.compression_*`
 * fields, sets `maxmemory_policy` (and the lrulfu clock), and asserts
 * the predicate's verdict.
 *
 * The S2.3 incompressible-keys hashtable lookup branch is not exercised
 * here — the implementation currently treats every key as
 * "always retry-eligible", which we verify only indirectly (a key that
 * passes every other gate is reported eligible). S2.3 will add coverage
 * for that branch when the side hashtable lands.
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstring>

extern "C" {
#include "compression.h"
#include "lrulfu.h"
#include "server.h"
}

class CompressionEligibilityTest : public ::testing::Test {
  protected:
    /* Saved server state so we can restore in TearDown. */
    int saved_compression_master_switch;
    size_t saved_min_value_size;
    size_t saved_max_value_size;
    int saved_min_idle_seconds;
    int saved_lfu_threshold;
    int saved_maxmemory_policy;

    void SetUp() override {
        /* Snapshot. */
        saved_compression_master_switch = server.compression_master_switch;
        saved_min_value_size = server.compression_min_value_size;
        saved_max_value_size = server.compression_max_value_size;
        saved_min_idle_seconds = server.compression_min_idle_seconds;
        saved_lfu_threshold = server.compression_lfu_threshold;
        saved_maxmemory_policy = server.maxmemory_policy;

        /* Defaults consistent with src/config.c registration. */
        server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
        server.compression_min_value_size = 256;
        server.compression_max_value_size = 131072;
        server.compression_min_idle_seconds = 60;
        server.compression_lfu_threshold = 5;

        /* Default to LRU semantics for time-based test cases. */
        useLruPolicy();
    }

    void TearDown() override {
        server.compression_master_switch = saved_compression_master_switch;
        server.compression_min_value_size = saved_min_value_size;
        server.compression_max_value_size = saved_max_value_size;
        server.compression_min_idle_seconds = saved_min_idle_seconds;
        server.compression_lfu_threshold = saved_lfu_threshold;
        server.maxmemory_policy = saved_maxmemory_policy;
        /* Re-sync the lrulfu module's cached policy-flag boolean. */
        lrulfu_updateClockAndPolicy(server.mstime,
                                    (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) != 0);
    }

    /* Switch the test to LRU semantics (non-LFU policy) and bump the LRU
     * clock so we have known idle-time anchors. */
    void useLruPolicy() {
        server.maxmemory_policy = MAXMEMORY_ALLKEYS_LRU;
        /* Pick an mstime that gives a stable lru_clock base. */
        server.mstime = 1000000LL;
        lrulfu_updateClockAndPolicy(server.mstime, false);
    }

    /* Switch the test to LFU semantics. */
    void useLfuPolicy() {
        server.maxmemory_policy = MAXMEMORY_ALLKEYS_LFU;
        server.mstime = 1000000LL;
        lrulfu_updateClockAndPolicy(server.mstime, true);
    }

    /* Build a RAW-encoded string of the requested length. The default
     * createStringObject auto-encodes short strings as EMBSTR; we force
     * RAW for predicate tests by going via createRawStringObject. */
    robj *makeRawString(size_t len) {
        sds s = sdsnewlen(NULL, len);
        memset(s, 'x', len);
        return createObject(OBJ_STRING, s);
    }

    /* Convenience: stamp the robj's lru field to advertise a specific
     * LRU idle (seconds since "now"). */
    void setLruIdleSecs(robj *o, uint32_t idle_secs) {
        o->lru = lru_import(idle_secs);
    }

    /* Convenience: stamp the robj's lru field to advertise a specific
     * LFU frequency counter. */
    void setLfuFreq(robj *o, uint8_t freq) {
        o->lru = lfu_import(freq);
    }
};

/* ============================================================
 * Master switch
 * ============================================================ */

TEST_F(CompressionEligibilityTest, MasterSwitchOff) {
    server.compression_master_switch = COMPRESSION_MASTER_OFF;
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 600); /* very cold */
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, MasterSwitchDecompressionBlocksEligibility) {
    /* In master=decompression we are draining; new compressions would
     * defeat the purpose. (R2.1.5) */
    server.compression_master_switch = COMPRESSION_MASTER_DECOMPRESSION;
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 600);
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, AllGatesPassWhenColdAndRaw) {
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 600);
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}

/* ============================================================
 * Type + encoding gate
 * ============================================================ */

TEST_F(CompressionEligibilityTest, RejectsNonStringType) {
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 600);
    o->type = OBJ_LIST; /* artificially mislabel — tests the gate */
    EXPECT_EQ(0, compressionIsEligible(o));
    o->type = OBJ_STRING; /* restore for clean free */
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, RejectsIntEncoding) {
    /* INT-encoded strings hold their value in val_ptr; already memory-
     * optimal. */
    robj *o = createStringObjectFromLongLong(12345);
    ASSERT_EQ((unsigned)OBJ_ENCODING_INT, o->encoding);
    setLruIdleSecs(o, 600);
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, RejectsEmbstrEncoding) {
    /* createStringObject auto-encodes short strings as EMBSTR. */
    robj *o = createStringObject("short", 5);
    ASSERT_EQ((unsigned)OBJ_ENCODING_EMBSTR, o->encoding);
    setLruIdleSecs(o, 600);
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, RejectsAlreadyCompressedEncoding) {
    /* Defense in depth — an already-compressed value isn't re-eligible. */
    robj *o = makeRawString(1024);
    o->encoding = OBJ_ENCODING_COMPRESSED;
    setLruIdleSecs(o, 600);
    EXPECT_EQ(0, compressionIsEligible(o));
    o->encoding = OBJ_ENCODING_RAW; /* restore for clean free */
    decrRefCount(o);
}

/* ============================================================
 * Refcount gate (shared RESP constants)
 * ============================================================ */

TEST_F(CompressionEligibilityTest, RejectsSharedRefcount) {
    robj *o = makeRawString(1024);
    int saved_refcount = o->refcount;
    o->refcount = OBJ_SHARED_REFCOUNT;
    setLruIdleSecs(o, 600);
    EXPECT_EQ(0, compressionIsEligible(o));
    o->refcount = saved_refcount; /* restore so decrRefCount frees normally */
    decrRefCount(o);
}

/* ============================================================
 * Size bounds
 * ============================================================ */

TEST_F(CompressionEligibilityTest, RejectsBelowMinSize) {
    server.compression_min_value_size = 256;
    robj *o = makeRawString(255);
    setLruIdleSecs(o, 600);
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, AcceptsAtMinSize) {
    server.compression_min_value_size = 256;
    robj *o = makeRawString(256);
    setLruIdleSecs(o, 600);
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, RejectsAboveMaxSize) {
    server.compression_max_value_size = 1024;
    robj *o = makeRawString(1025);
    setLruIdleSecs(o, 600);
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, AcceptsAtMaxSize) {
    server.compression_max_value_size = 1024;
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 600);
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, MaxSizeZeroDisablesUpperBound) {
    server.compression_max_value_size = 0;
    robj *o = makeRawString(1 << 20); /* 1 MiB */
    setLruIdleSecs(o, 600);
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}

/* ============================================================
 * Hot-key skip — LRU branch
 * ============================================================ */

TEST_F(CompressionEligibilityTest, LruRejectsRecentTouch) {
    /* Idle below threshold → not eligible. */
    server.compression_min_idle_seconds = 60;
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 30);
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, LruAcceptsBeyondThreshold) {
    server.compression_min_idle_seconds = 60;
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 120);
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, LruAtThresholdAcceptsBoundary) {
    /* Exact-equal idle is "old enough" — boundary is `>=`. */
    server.compression_min_idle_seconds = 60;
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 60);
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, LruZeroThresholdAcceptsImmediately) {
    server.compression_min_idle_seconds = 0;
    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 0);
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}

/* ============================================================
 * noeviction — same code path as LRU per design (R2.2)
 * ============================================================ */

TEST_F(CompressionEligibilityTest, NoevictionUsesLruTimeBasedCheck) {
    /* noeviction has neither LFU nor LRU flags; lrulfu falls back to
     * LRU semantics. The eligibility predicate should follow the LRU
     * branch (time-based skip). */
    server.maxmemory_policy = MAXMEMORY_NO_EVICTION;
    lrulfu_updateClockAndPolicy(server.mstime,
                                (server.maxmemory_policy & MAXMEMORY_FLAG_LFU) != 0);
    server.compression_min_idle_seconds = 60;

    robj *o = makeRawString(1024);
    setLruIdleSecs(o, 30); /* still hot */
    EXPECT_EQ(0, compressionIsEligible(o));

    setLruIdleSecs(o, 120); /* cold */
    EXPECT_EQ(1, compressionIsEligible(o));

    decrRefCount(o);
}

/* ============================================================
 * Hot-key skip — LFU branch
 * ============================================================ */

TEST_F(CompressionEligibilityTest, LfuRejectsAtOrAboveThreshold) {
    useLfuPolicy();
    server.compression_lfu_threshold = 5;
    robj *o = makeRawString(1024);
    setLfuFreq(o, 5); /* exactly threshold */
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);

    o = makeRawString(1024);
    setLfuFreq(o, 10); /* above threshold */
    EXPECT_EQ(0, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, LfuAcceptsBelowThreshold) {
    useLfuPolicy();
    server.compression_lfu_threshold = 5;
    robj *o = makeRawString(1024);
    setLfuFreq(o, 4);
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}

TEST_F(CompressionEligibilityTest, LfuTimeKnobIsInactive) {
    /* Setting the time-based knob to an extreme value must NOT affect
     * the LFU branch — the time check is policy-skipped. */
    useLfuPolicy();
    server.compression_lfu_threshold = 255; /* always pass freq guard */
    server.compression_min_idle_seconds = INT_MAX;

    robj *o = makeRawString(1024);
    setLfuFreq(o, 0); /* well below freq threshold */
    /* Despite min_idle being INT_MAX, the LFU branch ignores it. */
    EXPECT_EQ(1, compressionIsEligible(o));
    decrRefCount(o);
}
