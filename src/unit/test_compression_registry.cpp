/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstring>

extern "C" {
#include "compression_registry.h"
#include "server.h"
#include "zmalloc.h"
}

static compressionDictPair *makeFakeDictPair(void) {
    compressionDictPair *p = (compressionDictPair *)zcalloc(sizeof(*p));
    p->bytes = (unsigned char *)zmalloc(100);
    memset(p->bytes, 0xAB, 100);
    p->bytes_len = 100;
    return p;
}

class CompressionRegistryTest : public ::testing::Test {
  protected:
    void SetUp() override {
        server.compression_dict_max_versions = 4;
        server.compression_threads = 2;
        server.logfile = (char *)"";
        /* Suppress all logging — serverLog accesses server fields that
         * aren't fully initialized in the unit test context, which
         * triggers ASAN false positives. */
        server.verbosity = LL_NOTHING;
        compressionRegistryInit();
    }
    void TearDown() override {
        compressionRegistryRelease();
    }
};

TEST_F(CompressionRegistryTest, AddWithPromoteMakesActive) {
    compressionDictPair *p = makeFakeDictPair();
    uint32_t id = compressionRegistryAdd(p, 1);
    ASSERT_NE(id, (uint32_t)COMPRESSION_DICT_ID_NONE);
    compressionDictPair *active = compressionRegistryActive();
    ASSERT_NE(active, nullptr);
    ASSERT_EQ(active->dict_id, id);
    ASSERT_EQ(active->state, COMPRESSION_DICT_STATE_ACTIVE);
}

TEST_F(CompressionRegistryTest, AddWithoutPromoteIsRetiring) {
    compressionDictPair *p = makeFakeDictPair();
    uint32_t id = compressionRegistryAdd(p, 0);
    ASSERT_NE(id, (uint32_t)COMPRESSION_DICT_ID_NONE);
    ASSERT_EQ(compressionRegistryActive(), nullptr);
    compressionDictPair *d = compressionRegistryLookup(id);
    ASSERT_NE(d, nullptr);
    ASSERT_EQ(d->state, COMPRESSION_DICT_STATE_RETIRING);
}

TEST_F(CompressionRegistryTest, SecondPromoteRetiresFirst) {
    uint32_t id1 = compressionRegistryAdd(makeFakeDictPair(), 1);
    uint32_t id2 = compressionRegistryAdd(makeFakeDictPair(), 1);
    ASSERT_EQ(compressionRegistryActive()->dict_id, id2);
    compressionDictPair *d1 = compressionRegistryLookup(id1);
    ASSERT_EQ(d1->state, COMPRESSION_DICT_STATE_RETIRING);
}

TEST_F(CompressionRegistryTest, CapEnforcementRejectsWhenFull) {
    for (int i = 0; i < 4; i++) {
        ASSERT_NE(compressionRegistryAdd(makeFakeDictPair(), 1), (uint32_t)COMPRESSION_DICT_ID_NONE);
    }
    compressionDictPair *rejected = makeFakeDictPair();
    uint32_t id = compressionRegistryAdd(rejected, 1);
    ASSERT_EQ(id, (uint32_t)COMPRESSION_DICT_ID_NONE);
    /* Caller still owns on rejection — free it. */
    zfree(rejected->bytes);
    zfree(rejected);
}

TEST_F(CompressionRegistryTest, StartRetirementSnapshotsWorkerGens) {
    for (int i = 0; i < 5; i++) compressionWorkerReportQuiescent(0);
    for (int i = 0; i < 3; i++) compressionWorkerReportQuiescent(1);
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryAdd(makeFakeDictPair(), 1); /* retires first */
    compressionDictPair *d = compressionRegistryLookup(id);
    ASSERT_EQ(d->state, COMPRESSION_DICT_STATE_RETIRING);
    ASSERT_EQ(d->retire_worker_gen[0], (uint64_t)5);
    ASSERT_EQ(d->retire_worker_gen[1], (uint64_t)3);
}

TEST_F(CompressionRegistryTest, CannotFreeWhenFrameRefsPositive) {
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryIncRef(id);
    compressionRegistryAdd(makeFakeDictPair(), 1); /* retires first */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);
    compressionRegistryTryGc();
    ASSERT_NE(compressionRegistryLookup(id), nullptr); /* still alive */
}

TEST_F(CompressionRegistryTest, CannotFreeWhenWorkerBehind) {
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionWorkerReportQuiescent(0); /* only worker 0 advances */
    compressionRegistryTryGc();
    ASSERT_NE(compressionRegistryLookup(id), nullptr);
}

TEST_F(CompressionRegistryTest, GcFreesWhenBothConditionsMet) {
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);
    compressionRegistryTryGc();
    ASSERT_EQ(compressionRegistryLookup(id), nullptr); /* freed */
}

TEST_F(CompressionRegistryTest, MultipleWorkersOneBehindBlocksFree) {
    server.compression_threads = 4;
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);
    compressionWorkerReportQuiescent(2);
    compressionRegistryTryGc();
    ASSERT_NE(compressionRegistryLookup(id), nullptr); /* worker 3 behind */
    compressionWorkerReportQuiescent(3);
    compressionRegistryTryGc();
    ASSERT_EQ(compressionRegistryLookup(id), nullptr); /* now freed */
}

TEST_F(CompressionRegistryTest, FrameRefIncrDecr) {
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryIncRef(id);
    compressionRegistryIncRef(id);
    ASSERT_EQ(compressionRegistryLookup(id)->frame_refs, (size_t)2);
    compressionRegistryDecRef(id);
    ASSERT_EQ(compressionRegistryLookup(id)->frame_refs, (size_t)1);
}

TEST_F(CompressionRegistryTest, DecRefTriggersGcOnRetiringDict) {
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryIncRef(id);
    compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);
    ASSERT_NE(compressionRegistryLookup(id), nullptr);
    compressionRegistryDecRef(id); /* triggers GC */
    ASSERT_EQ(compressionRegistryLookup(id), nullptr);
}

TEST_F(CompressionRegistryTest, LookupFindsById) {
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    ASSERT_EQ(compressionRegistryLookup(id)->dict_id, id);
}

TEST_F(CompressionRegistryTest, LookupReturnsNullOnMiss) {
    ASSERT_EQ(compressionRegistryLookup(999), nullptr);
    ASSERT_EQ(compressionRegistryLookup(COMPRESSION_DICT_ID_NONE), nullptr);
}

static void countCb(const compressionDictPair *d, void *ctx) {
    (void)d;
    (*(int *)ctx)++;
}

TEST_F(CompressionRegistryTest, ForEachIteratesAll) {
    compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryAdd(makeFakeDictPair(), 0);
    compressionRegistryAdd(makeFakeDictPair(), 1);
    int count = 0;
    compressionRegistryForEach(countCb, &count);
    ASSERT_EQ(count, 3);
}

TEST_F(CompressionRegistryTest, WorkerReportQuiescentAdvancesGen) {
    ASSERT_EQ(compressionWorkerGetGen(0), (uint64_t)0);
    compressionWorkerReportQuiescent(0);
    ASSERT_EQ(compressionWorkerGetGen(0), (uint64_t)1);
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(0);
    ASSERT_EQ(compressionWorkerGetGen(0), (uint64_t)3);
}

TEST_F(CompressionRegistryTest, EndToEndLifecycle) {
    uint32_t id = compressionRegistryAdd(makeFakeDictPair(), 1);
    compressionRegistryIncRef(id);
    compressionRegistryIncRef(id);
    compressionRegistryAdd(makeFakeDictPair(), 1); /* retires first */
    compressionWorkerReportQuiescent(0);
    compressionWorkerReportQuiescent(1);
    ASSERT_NE(compressionRegistryLookup(id), nullptr); /* frame_refs=2 */
    compressionRegistryDecRef(id);
    ASSERT_NE(compressionRegistryLookup(id), nullptr); /* frame_refs=1 */
    compressionRegistryDecRef(id);                     /* triggers GC, freed */
    ASSERT_EQ(compressionRegistryLookup(id), nullptr);
}
