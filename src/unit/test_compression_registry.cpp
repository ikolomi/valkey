/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * test_compression_registry.cpp — unit tests for src/compression_registry.c.
 *
 * Covers the QSBR-based dictionary lifecycle: add/promote, retirement,
 * grace-period GC, frame-ref accounting, worker quiescent reporting,
 * cap enforcement, and iteration.
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstring>
#include <vector>

extern "C" {
#include "compression_registry.h"
#include "server.h"
#include "zmalloc.h"
}

/* Helper: allocate fake dict bytes (not real ZSTD dict — registry doesn't
 * validate content, only ZSTD APIs do, which are stubbed in tests). */
static unsigned char *fakeDictBytes(size_t len) {
    unsigned char *buf = (unsigned char *)zmalloc(len);
    memset(buf, 0xAB, len);
    return buf;
}

class CompressionRegistryTest : public ::testing::Test {
protected:
    void SetUp() override {
        /* Set server config defaults needed by the registry. */
        server.compression_dict_max_versions = 4;
        server.compression_threads = 2;
        server.logfile = (char *)"";  /* Prevent crash in serverLog */
        server.verbosity = LL_WARNING; /* Suppress NOTICE logs in tests */
        compressionRegistryInit();
    }

    void TearDown() override {
        compressionRegistryRelease();
    }
};

/* ========================================================================
 * Basic add and promote
 * ======================================================================== */

TEST_F(CompressionRegistryTest, AddWithPromoteMakesActive) {
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);
    ASSERT_NE(id, (uint32_t)COMPRESSION_DICT_ID_NONE);

    compressionDict *active = compressionDictGetActive();
    ASSERT_NE(active, nullptr);
    ASSERT_EQ(active->dict_id, id);
    ASSERT_EQ(active->state, DICT_STATE_ACTIVE);
}

TEST_F(CompressionRegistryTest, AddWithoutPromoteIsRetiring) {
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 0);
    ASSERT_NE(id, (uint32_t)COMPRESSION_DICT_ID_NONE);

    /* Active should still be NULL. */
    compressionDict *active = compressionDictGetActive();
    ASSERT_EQ(active, nullptr);

    /* But lookup should find it. */
    compressionDict *d = compressionDictLookup(id);
    ASSERT_NE(d, nullptr);
    ASSERT_EQ(d->state, DICT_STATE_RETIRING);
}

TEST_F(CompressionRegistryTest, SecondPromoteRetiresFirst) {
    uint32_t id1 = compressionDictAdd(fakeDictBytes(100), 100, 1);
    uint32_t id2 = compressionDictAdd(fakeDictBytes(100), 100, 1);

    /* Active should be the second one. */
    compressionDict *active = compressionDictGetActive();
    ASSERT_EQ(active->dict_id, id2);

    /* First should be RETIRING. */
    compressionDict *d1 = compressionDictLookup(id1);
    ASSERT_NE(d1, nullptr);
    ASSERT_EQ(d1->state, DICT_STATE_RETIRING);
}

/* ========================================================================
 * Cap enforcement
 * ======================================================================== */

TEST_F(CompressionRegistryTest, CapEnforcementRejectsWhenFull) {
    /* Fill to cap (4). */
    for (int i = 0; i < 4; i++) {
        uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);
        ASSERT_NE(id, (uint32_t)COMPRESSION_DICT_ID_NONE);
    }

    /* Next add should fail. Workers haven't advanced so GC can't free. */
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);
    ASSERT_EQ(id, (uint32_t)COMPRESSION_DICT_ID_NONE);
}

/* ========================================================================
 * Retirement and QSBR
 * ======================================================================== */

TEST_F(CompressionRegistryTest, StartRetirementSnapshotsWorkerGens) {
    /* Advance worker 0 to gen 5, worker 1 to gen 3. */
    for (int i = 0; i < 5; i++) compressionWorkerReportQuiescent(0);
    for (int i = 0; i < 3; i++) compressionWorkerReportQuiescent(1);

    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDict *d = compressionDictLookup(id);

    /* Promote a new one to retire this one. */
    compressionDictAdd(fakeDictBytes(100), 100, 1);

    ASSERT_EQ(d->state, DICT_STATE_RETIRING);
    ASSERT_EQ(d->retire_worker_gen[0], (uint64_t)5);
    ASSERT_EQ(d->retire_worker_gen[1], (uint64_t)3);
}

TEST_F(CompressionRegistryTest, CanFreeReturnsFalseWhenFrameRefsPositive) {
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDictIncrFrameRef(id);

    /* Promote new to retire old. */
    compressionDictAdd(fakeDictBytes(100), 100, 1);

    /* Advance workers past snapshot. */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);

    compressionDict *d = compressionDictLookup(id);
    ASSERT_EQ(compressionDictCanFree(d), 0); /* frame_refs > 0 */
}

TEST_F(CompressionRegistryTest, CanFreeReturnsFalseWhenWorkerBehind) {
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);

    /* Promote new to retire old. */
    compressionDictAdd(fakeDictBytes(100), 100, 1);

    /* Only advance worker 0, not worker 1. */
    compressionWorkerReportQuiescent(0);

    compressionDict *d = compressionDictLookup(id);
    ASSERT_EQ(compressionDictCanFree(d), 0); /* worker 1 behind */
}

TEST_F(CompressionRegistryTest, CanFreeReturnsTrueWhenBothConditionsMet) {
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);

    /* Promote new to retire old. */
    compressionDictAdd(fakeDictBytes(100), 100, 1);

    /* Advance both workers past snapshot. */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);

    compressionDict *d = compressionDictLookup(id);
    /* frame_refs == 0 (never incremented) and workers advanced. */
    ASSERT_EQ(compressionDictCanFree(d), 1);
}

TEST_F(CompressionRegistryTest, CanFreeWithMultipleWorkersOneBehind) {
    server.compression_threads = 4;

    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDictAdd(fakeDictBytes(100), 100, 1); /* retire first */

    /* Advance workers 0, 1, 2 but not 3. */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);
    compressionWorkerReportQuiescent(2);

    compressionDict *d = compressionDictLookup(id);
    ASSERT_EQ(compressionDictCanFree(d), 0); /* worker 3 behind */

    /* Now advance worker 3. */
    compressionWorkerReportQuiescent(3);
    ASSERT_EQ(compressionDictCanFree(d), 1);
}

/* ========================================================================
 * GC
 * ======================================================================== */

TEST_F(CompressionRegistryTest, TryGcFreesSafeDicts) {
    uint32_t id1 = compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDictAdd(fakeDictBytes(100), 100, 1); /* retires id1 */

    /* Advance workers. */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);

    compressionDictTryGc();

    /* id1 should be gone. */
    ASSERT_EQ(compressionDictLookup(id1), nullptr);
}

TEST_F(CompressionRegistryTest, TryGcKeepsUnsafeDicts) {
    uint32_t id1 = compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDictIncrFrameRef(id1);
    compressionDictAdd(fakeDictBytes(100), 100, 1); /* retires id1 */

    /* Advance workers but frame_refs still > 0. */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);

    compressionDictTryGc();

    /* id1 should still exist. */
    ASSERT_NE(compressionDictLookup(id1), nullptr);
}

/* ========================================================================
 * Frame reference counting
 * ======================================================================== */

TEST_F(CompressionRegistryTest, FrameRefIncrDecr) {
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);

    compressionDictIncrFrameRef(id);
    compressionDictIncrFrameRef(id);

    compressionDict *d = compressionDictLookup(id);
    ASSERT_EQ(d->frame_refs, (size_t)2);

    compressionDictDecrFrameRef(id);
    ASSERT_EQ(d->frame_refs, (size_t)1);
}

TEST_F(CompressionRegistryTest, DecrFrameRefTriggersGcOnRetiringDict) {
    uint32_t id1 = compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDictIncrFrameRef(id1);
    compressionDictAdd(fakeDictBytes(100), 100, 1); /* retires id1 */

    /* Advance workers past snapshot. */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);

    /* Dict still alive because frame_refs == 1. */
    ASSERT_NE(compressionDictLookup(id1), nullptr);

    /* Decrement to 0 — should trigger GC and free. */
    compressionDictDecrFrameRef(id1);
    ASSERT_EQ(compressionDictLookup(id1), nullptr);
}

/* ========================================================================
 * Lookup
 * ======================================================================== */

TEST_F(CompressionRegistryTest, LookupFindsById) {
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDict *d = compressionDictLookup(id);
    ASSERT_NE(d, nullptr);
    ASSERT_EQ(d->dict_id, id);
}

TEST_F(CompressionRegistryTest, LookupReturnsNullOnMiss) {
    ASSERT_EQ(compressionDictLookup(999), nullptr);
    ASSERT_EQ(compressionDictLookup(COMPRESSION_DICT_ID_NONE), nullptr);
}

/* ========================================================================
 * ForEach
 * ======================================================================== */

static void countCallback(const compressionDict *d, void *ctx) {
    (void)d;
    int *count = (int *)ctx;
    (*count)++;
}

TEST_F(CompressionRegistryTest, ForEachIteratesAllNonRetired) {
    compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDictAdd(fakeDictBytes(100), 100, 0);
    compressionDictAdd(fakeDictBytes(100), 100, 1);

    int count = 0;
    compressionRegistryForEach(countCallback, &count);
    /* 3 dicts total (1 active + 2 retiring). None retired. */
    ASSERT_EQ(count, 3);
}

/* ========================================================================
 * Worker quiescent reporting
 * ======================================================================== */

TEST_F(CompressionRegistryTest, WorkerReportQuiescentAdvancesGen) {
    ASSERT_EQ(compressionWorkerGetGen(0), (uint64_t)0);
    compressionWorkerReportQuiescent(0);
    ASSERT_EQ(compressionWorkerGetGen(0), (uint64_t)1);
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(0);
    ASSERT_EQ(compressionWorkerGetGen(0), (uint64_t)3);
}

/* ========================================================================
 * End-to-end: promote → retire → workers advance → decr → freed
 * ======================================================================== */

TEST_F(CompressionRegistryTest, EndToEndLifecycle) {
    /* 1. Create and promote dict. */
    uint32_t id = compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDictIncrFrameRef(id);
    compressionDictIncrFrameRef(id);

    /* 2. Promote a new dict — old one retires. */
    compressionDictAdd(fakeDictBytes(100), 100, 1);
    compressionDict *d = compressionDictLookup(id);
    ASSERT_EQ(d->state, DICT_STATE_RETIRING);

    /* 3. Workers advance past snapshot. */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);

    /* Still alive — frame_refs == 2. */
    ASSERT_EQ(compressionDictCanFree(d), 0);

    /* 4. Drain frame refs. */
    compressionDictDecrFrameRef(id);
    ASSERT_NE(compressionDictLookup(id), nullptr); /* still 1 ref */

    compressionDictDecrFrameRef(id); /* triggers GC */

    /* 5. Dict should be freed. */
    ASSERT_EQ(compressionDictLookup(id), nullptr);
}
