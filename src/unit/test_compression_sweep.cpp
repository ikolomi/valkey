/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * End-to-end test for the keyspace sweep state machine.
 *
 * Drives the full `enable feature → sweep → values get compressed →
 * decompress sweep → values back to RAW` round-trip in a single test,
 * inspecting `robj->encoding` directly rather than going through the
 * (not-yet-implemented) `OBJECT ENCODING` / `INFO compression` surfaces.
 *
 * Bypasses real training via the `installSyntheticDict()` helper —
 * mirrors test_compression_workers.cpp's pattern. The synthetic dict
 * goes through the same `compressionRegistryAdd` path production
 * training uses; only the corpus source differs.
 *
 * Test fixture mirrors test_compression_train.cpp: fake `server.db`
 * with one populated kvstore, registry + sweep + workers initialized,
 * bio threads spawned (in case any path tries to use them).
 *
 * Single-shard DB is used for most tests (server.dbnum = 1) — the
 * sparse-DB iteration is exercised by a dedicated test that bumps
 * server.dbnum to 16 with NULL slots between populated ones.
 */

#include "generated_wrappers.hpp"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include "bio.h"
#include "compression.h"
#include "compression_registry.h"
#include "compression_sweep.h"
#include "compression_train.h"
#include "compression_workers.h"
#include "hashtable.h"
#include "kvstore.h"
#include "monotonic.h"
#include "sds.h"
#include "server.h"
#include "zmalloc.h"
#ifdef USE_ZSTD
#include <zdict.h>
#include <zstd.h>
#endif
}

#ifdef USE_ZSTD

namespace {

/* ========================================================================
 * Synthetic dictionary
 * ========================================================================
 *
 * Identical pattern to test_compression_workers.cpp's installSyntheticDict.
 * We replicate (not extract to a shared helper) because the function is
 * tiny and the alternative — a shared test header — adds build-system
 * scaffolding for marginal gain.
 */

constexpr const char *kCorpusSamples[] = {
    "{\"event\":\"order.created\",\"region\":\"us-east-1\",\"customer_id\":1001}",
    "{\"event\":\"order.shipped\",\"region\":\"us-east-1\",\"customer_id\":1002}",
    "{\"event\":\"order.delivered\",\"region\":\"us-east-1\",\"customer_id\":1003}",
    "{\"event\":\"order.canceled\",\"region\":\"us-east-1\",\"customer_id\":1004}",
    "{\"event\":\"order.created\",\"region\":\"us-west-2\",\"customer_id\":2001}",
    "{\"event\":\"order.shipped\",\"region\":\"us-west-2\",\"customer_id\":2002}",
    "{\"event\":\"order.delivered\",\"region\":\"us-west-2\",\"customer_id\":2003}",
    "{\"event\":\"order.canceled\",\"region\":\"us-west-2\",\"customer_id\":2004}",
    "{\"event\":\"order.created\",\"region\":\"eu-west-1\",\"customer_id\":3001}",
    "{\"event\":\"order.shipped\",\"region\":\"eu-west-1\",\"customer_id\":3002}",
    "{\"event\":\"order.delivered\",\"region\":\"eu-west-1\",\"customer_id\":3003}",
    "{\"event\":\"order.canceled\",\"region\":\"eu-west-1\",\"customer_id\":3004}",
    "{\"event\":\"order.created\",\"region\":\"ap-south-1\",\"customer_id\":4001}",
    "{\"event\":\"order.shipped\",\"region\":\"ap-south-1\",\"customer_id\":4002}",
    "{\"event\":\"order.delivered\",\"region\":\"ap-south-1\",\"customer_id\":4003}",
    "{\"event\":\"order.canceled\",\"region\":\"ap-south-1\",\"customer_id\":4004}",
};
constexpr size_t kCorpusSampleCount = sizeof(kCorpusSamples) / sizeof(kCorpusSamples[0]);

uint32_t installSyntheticDict() {
    constexpr int kSampleCount = 1500;
    std::vector<std::string> samples;
    samples.reserve(kSampleCount);
    for (int i = 0; i < kSampleCount; i++) {
        std::string s = kCorpusSamples[i % kCorpusSampleCount];
        s += ",\"seq\":" + std::to_string(i) + "}";
        samples.push_back(std::move(s));
    }

    size_t total_bytes = 0;
    for (auto &s : samples) total_bytes += s.size();
    std::vector<unsigned char> sample_buf(total_bytes);
    std::vector<size_t> sample_sizes(samples.size());
    size_t off = 0;
    for (size_t i = 0; i < samples.size(); i++) {
        memcpy(sample_buf.data() + off, samples[i].data(), samples[i].size());
        sample_sizes[i] = samples[i].size();
        off += samples[i].size();
    }

    constexpr size_t kDictCapacity = 1024;
    std::vector<unsigned char> dict_bytes(kDictCapacity);
    size_t got = ZDICT_trainFromBuffer(dict_bytes.data(), kDictCapacity,
                                       sample_buf.data(), sample_sizes.data(),
                                       (unsigned)samples.size());
    if (ZDICT_isError(got)) return 0;
    dict_bytes.resize(got);

    auto *pair = (compressionDictPair *)zcalloc(sizeof(compressionDictPair));
    pair->bytes = (unsigned char *)zmalloc(dict_bytes.size());
    memcpy(pair->bytes, dict_bytes.data(), dict_bytes.size());
    pair->bytes_len = dict_bytes.size();
    pair->cdict = ZSTD_createCDict(pair->bytes, pair->bytes_len, /*level=*/3);
    pair->ddict = ZSTD_createDDict(pair->bytes, pair->bytes_len);
    if (pair->cdict == NULL || pair->ddict == NULL) {
        if (pair->cdict) ZSTD_freeCDict(pair->cdict);
        if (pair->ddict) ZSTD_freeDDict(pair->ddict);
        zfree(pair->bytes);
        zfree(pair);
        return 0;
    }
    return compressionRegistryAdd(pair, /*promote=*/1);
}

/* ========================================================================
 * Compressible robj factory
 * ========================================================================
 *
 * Builds a string robj whose value is large + repetitive enough to
 * pass the post-compression net-savings guard with the synthetic dict
 * installed above. Matches the test_compression_train.cpp factory's
 * shape (embedded key, RAW encoding, cold lru) so kvstoreHashtableAdd
 * + drain-time pointer-equality checks both work.
 */
robj *makeKeyedRawString(const char *key, size_t value_len, int seed,
                         bool compressible) {
    sds val = sdsnewlen(NULL, value_len);
    if (compressible) {
        /* JSON-shaped — same patterns as the corpus → high compression. */
        const char *base = kCorpusSamples[seed % kCorpusSampleCount];
        size_t base_len = strlen(base);
        for (size_t i = 0; i < value_len; i++) {
            val[i] = base[i % base_len];
        }
    } else {
        /* Random-looking content — won't compress. */
        for (size_t i = 0; i < value_len; i++) {
            val[i] = (char)((seed * 31 + i * 17) & 0xff);
        }
    }
    robj *o = createRawStringObject(val, sdslen(val));
    sdsfree(val);
    sds k = sdsnew(key);
    o = objectSetKeyAndExpire(o, k, -1);
    sdsfree(k);
    o->lru = 0; /* idle = 0 today; the cold-key check uses a config knob set in SetUp */
    return o;
}

/* Counts robjs in db[0]'s kvstore by encoding. We iterate the kvstore
 * directly since OBJECT ENCODING is not yet wired (R2.7.1). */
struct EncodingCounts {
    int compressed = 0;
    int raw = 0;
    int embstr = 0;
    int int_enc = 0;
    int other = 0;
    int total = 0;
};

EncodingCounts countEncodings(int dbid) {
    EncodingCounts c;
    if (server.db[dbid] == NULL) return c;
    kvstoreIterator *it = kvstoreIteratorInit(server.db[dbid]->keys, 0);
    void *raw;
    while (kvstoreIteratorNext(it, &raw)) {
        robj *o = (robj *)raw;
        c.total++;
        switch (o->encoding) {
        case OBJ_ENCODING_COMPRESSED: c.compressed++; break;
        case OBJ_ENCODING_RAW:        c.raw++; break;
        case OBJ_ENCODING_EMBSTR:     c.embstr++; break;
        case OBJ_ENCODING_INT:        c.int_enc++; break;
        default:                      c.other++; break;
        }
    }
    kvstoreIteratorRelease(it);
    return c;
}

/* Wait until the worker outbox has been fully drained AND the sweep
 * is back to IDLE. The sweep enqueues asynchronously to the worker
 * pool; the workers compress concurrently; the drain handler installs
 * results back into the kvstore. We can't predict the exact number
 * of cron ticks because worker timing is non-deterministic. So poll
 * with a deadline.
 *
 * Returns the number of cron iterations executed. */
int driveAndDrain(int max_iterations_ms) {
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(max_iterations_ms);
    int iterations = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        iterations++;
        compressionSweepDriveForTesting(1);
        compressionWorkersDrainOutbox(/*budget=*/256);
        if (!compressionSweepIsScanning()) {
            /* Sweep done. Drain any final outbox items the workers
             * are still finishing. */
            for (int i = 0; i < 5; i++) {
                int got = compressionWorkersDrainOutbox(256);
                if (got == 0) break;
                std::this_thread::sleep_for(std::chrono::microseconds(500));
            }
            break;
        }
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }
    return iterations;
}

} /* anonymous namespace */

/* ========================================================================
 * Fixture
 * ======================================================================== */

class CompressionSweepTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        uint8_t seed[16] = {0};
        hashtableSetHashFunctionSeed(seed);
        monotonicInit();
        bioInit();
    }

    void SetUp() override {
        server.logfile = (char *)"";
        server.verbosity = LL_NOTHING;

        /* Eligibility predicate values (R2.2). All defaults except a
         * smaller min_value_size so a 256-byte test value is
         * comfortably above the threshold. */
        server.compression_enabled = 1;
        server.compression_threads = 2;
        server.compression_sweep_max_cpu_pct = 100; /* faster tests */
        server.compression_min_value_size = 64;
        server.compression_max_value_size = 131072;
        server.compression_min_savings_ratio = 10;
        server.compression_lfu_threshold = 255; /* don't filter on LFU */
        server.compression_min_idle_seconds = 0; /* don't filter on LRU */
        server.compression_dict_max_versions = 4;
        server.hz = 10;
        server.maxmemory_policy = 0; /* noeviction — uses lru_idle path */
        server.dbnum = 1;

        /* Allocate db[0] with a real kvstore. */
        server.db = (serverDb **)zcalloc(sizeof(serverDb *) * server.dbnum);
        server.db[0] = (serverDb *)zcalloc(sizeof(serverDb));
        server.db[0]->keys = kvstoreCreate(&kvstoreKeysHashtableType, 0, 0);

        compressionRegistryInit();
        compressionTrainInit();
        compressionSweepInit();
        ASSERT_EQ(0, compressionWorkersStart(server.compression_threads));
    }

    void TearDown() override {
        compressionWorkersStop();
        compressionSweepRelease();
        for (int i = 0; i < server.dbnum; i++) {
            if (server.db[i] == NULL) continue;
            kvstoreRelease(server.db[i]->keys);
            zfree(server.db[i]);
        }
        zfree(server.db);
        server.db = NULL;
        compressionRegistryRelease();
    }
};

/* ========================================================================
 * Tests
 * ======================================================================== */

/* The headline test: half the keys are eligible-and-compressible, half
 * are too small to be eligible. Compress sweep flips half to
 * OBJ_ENCODING_COMPRESSED. Decompress sweep flips them all back. */
TEST_F(CompressionSweepTest, CompressDecompressRoundTrip) {
    ASSERT_NE(0u, installSyntheticDict());

    constexpr int kEligible = 100;   /* large + compressible */
    constexpr int kIneligible = 100; /* small — fails compression-min-value-size */

    /* Eligible: 1024-byte JSON-shaped values (well above min_value_size=64). */
    for (int i = 0; i < kEligible; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "elig:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, /*compressible=*/true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
    /* Ineligible: 32-byte values — below min_value_size=64.
     * createRawStringObject still produces RAW encoding (we explicitly
     * use the raw-string factory, not createStringObject), so they
     * appear in the kvstore as RAW but the eligibility predicate
     * rejects them on size. */
    for (int i = 0; i < kIneligible; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "skip:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 32, i + 10000, /*compressible=*/true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }

    /* Sanity: pre-sweep, all eligible keys are RAW. */
    EncodingCounts pre = countEncodings(0);
    EXPECT_EQ(kEligible + kIneligible, pre.total);
    EXPECT_EQ(kEligible + kIneligible, pre.raw);
    EXPECT_EQ(0, pre.compressed);

    /* Compress sweep. */
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    int iters = driveAndDrain(/*max_iterations_ms=*/2000);
    EXPECT_LE(iters, 200) << "sweep should converge in <200 iterations";

    /* Eligible keys → COMPRESSED. Ineligible keys → still RAW. */
    EncodingCounts post_compress = countEncodings(0);
    EXPECT_EQ(kEligible + kIneligible, post_compress.total);
    EXPECT_EQ(kEligible, post_compress.compressed);
    EXPECT_EQ(kIneligible, post_compress.raw);

    /* Counter sanity (these are atomic, updated by createCompressedObject
     * via compressionAccountInstall). */
    EXPECT_GT(compressionGetTotalUncompressedBytes(), 0u);
    EXPECT_GT(compressionGetTotalCompressedBytes(), 0u);
    /* Compressed < uncompressed — net savings positive. */
    EXPECT_LT(compressionGetTotalCompressedBytes(),
              compressionGetTotalUncompressedBytes());

    /* Decompress sweep. */
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_DECOMPRESS));
    iters = driveAndDrain(/*max_iterations_ms=*/2000);
    EXPECT_LE(iters, 200) << "decompress sweep should converge in <200 iterations";

    /* All values back to RAW. */
    EncodingCounts post_decompress = countEncodings(0);
    EXPECT_EQ(kEligible + kIneligible, post_decompress.total);
    EXPECT_EQ(0, post_decompress.compressed);
    EXPECT_EQ(kEligible + kIneligible, post_decompress.raw);

    /* Counters drained. */
    EXPECT_EQ(0u, compressionGetTotalUncompressedBytes());
    EXPECT_EQ(0u, compressionGetTotalCompressedBytes());
}

/* Sweep on an empty keyspace: starts and completes in one tick;
 * no values exist so no enqueues happen. */
TEST_F(CompressionSweepTest, EmptyKeyspaceCompletes) {
    ASSERT_NE(0u, installSyntheticDict());
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    int iters = driveAndDrain(500);
    EXPECT_LE(iters, 5) << "empty keyspace should finish almost immediately";
    EXPECT_FALSE(compressionSweepIsScanning());
    EncodingCounts c = countEncodings(0);
    EXPECT_EQ(0, c.total);
    EXPECT_EQ(0, c.compressed);
}

/* No active dict: sweep runs through the keyspace but
 * compressionEnqueueCandidate's active-dict guard short-circuits
 * every value. No values get compressed; no crash. */
TEST_F(CompressionSweepTest, NoActiveDictGracefulNoOp) {
    /* Skip installSyntheticDict — registry is empty.
     * Validate that eligibility predicate doesn't even look up the
     * registry, so this path is fine even on a fresh server. */
    ASSERT_EQ(NULL, compressionRegistryActive());

    constexpr int n = 50;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, /*compressible=*/true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }

    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    driveAndDrain(500);

    EncodingCounts c = countEncodings(0);
    EXPECT_EQ(n, c.total);
    EXPECT_EQ(0, c.compressed) << "no dict → nothing should compress";
    EXPECT_EQ(n, c.raw);
}

/* Master switch off: cron tick is a no-op even with a queued request. */
TEST_F(CompressionSweepTest, MasterSwitchOffMakesCronNoOp) {
    ASSERT_NE(0u, installSyntheticDict());
    constexpr int n = 50;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }

    server.compression_enabled = 0; /* master switch OFF */
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));

    /* Drive a few ticks — sweep should NOT advance because the cron
     * function returns early on !server.compression_enabled. */
    for (int i = 0; i < 10; i++) compressionSweepDriveForTesting(1);

    /* State machine still IDLE (request flag set but never picked up). */
    EXPECT_FALSE(compressionSweepIsScanning());
    EXPECT_EQ(0, countEncodings(0).compressed);

    /* Re-enable: pending request gets picked up immediately. */
    server.compression_enabled = 1;
    int iters = driveAndDrain(2000);
    EXPECT_LE(iters, 200);
    EXPECT_GT(countEncodings(0).compressed, 0);
}

/* Sparse server.db: only db[0] and db[5] populated; db[1..4], db[6..15]
 * are NULL. Sweep must skip the NULL slots without crashing (regression
 * for the same NULL-deref class as PR #25 fixed in totalDbKeys). */
TEST_F(CompressionSweepTest, SparseDbIteratesPastNullSlots) {
    /* Tear down the single-DB setup and re-install a sparse one. */
    kvstoreRelease(server.db[0]->keys);
    zfree(server.db[0]);
    zfree(server.db);

    server.dbnum = 16;
    server.db = (serverDb **)zcalloc(sizeof(serverDb *) * server.dbnum);
    /* Populate only slots 0 and 5; the rest stay NULL — same shape
     * as a fresh server before any SELECT operation. */
    for (int idx : {0, 5}) {
        server.db[idx] = (serverDb *)zcalloc(sizeof(serverDb));
        server.db[idx]->keys = kvstoreCreate(&kvstoreKeysHashtableType, 0, 0);
    }

    ASSERT_NE(0u, installSyntheticDict());

    constexpr int per_db = 30;
    for (int idx : {0, 5}) {
        for (int i = 0; i < per_db; i++) {
            char keybuf[64];
            snprintf(keybuf, sizeof(keybuf), "db%d:%06d", idx, i);
            robj *o = makeKeyedRawString(keybuf, 1024, i, true);
            kvstoreHashtableAdd(server.db[idx]->keys, 0, o);
        }
    }

    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    int iters = driveAndDrain(2000);
    EXPECT_LE(iters, 200);

    /* Both populated DBs should have all values compressed. */
    EncodingCounts db0 = countEncodings(0);
    EncodingCounts db5 = countEncodings(5);
    EXPECT_EQ(per_db, db0.compressed);
    EXPECT_EQ(per_db, db5.compressed);
}

/* Single-flight: a second request while the sweep is in flight is
 * rejected (returns 0). The first sweep continues unaffected. */
TEST_F(CompressionSweepTest, SingleFlightRejectsConcurrentRequest) {
    ASSERT_NE(0u, installSyntheticDict());
    /* Populate a lot of values so the sweep takes >1 cron tick. */
    constexpr int n = 500;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 4096, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
    /* Constrain sweep budget so it definitely takes multiple ticks. */
    server.compression_sweep_max_cpu_pct = 1;

    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    /* Drive one tick — sweep enters SCANNING. */
    compressionSweepDriveForTesting(1);
    EXPECT_TRUE(compressionSweepIsScanning());

    /* Second request: refused. */
    EXPECT_EQ(0, compressionSweepRequest(COMPRESSION_SWEEP_DIR_DECOMPRESS));

    /* Restore pacing and drive to completion. */
    server.compression_sweep_max_cpu_pct = 100;
    driveAndDrain(2000);
    EXPECT_FALSE(compressionSweepIsScanning());
    EXPECT_EQ(n, countEncodings(0).compressed);
}

#endif /* USE_ZSTD */
