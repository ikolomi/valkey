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
robj *makeKeyedRawString(const char *key, size_t value_len, int seed, bool compressible) {
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
        case OBJ_ENCODING_RAW: c.raw++; break;
        case OBJ_ENCODING_EMBSTR: c.embstr++; break;
        case OBJ_ENCODING_INT: c.int_enc++; break;
        default: c.other++; break;
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

    /* Phase 1: drive the sweep state machine until it hits IDLE.
     * Each loop iteration: one cron tick + one drain of any outbox
     * results that surfaced during this tick. */
    while (std::chrono::steady_clock::now() < deadline) {
        iterations++;
        compressionSweepDriveForTesting(1);
        compressionWorkersDrainOutbox(/*budget=*/256);
        if (!compressionSweepIsScanning()) break;
        std::this_thread::sleep_for(std::chrono::microseconds(500));
    }

    /* Phase 2: sweep is IDLE but workers may still be processing jobs
     * the sweep enqueued. Drain until we see a sustained stretch of
     * zero results (workers truly idle). 32-bit and slow CI runners
     * are noticeably slower than amd64 here — keep the budget
     * generous. We exit early if the deadline expires too. */
    int consecutive_zero_drains = 0;
    while (std::chrono::steady_clock::now() < deadline) {
        int got = compressionWorkersDrainOutbox(/*budget=*/256);
        if (got == 0) {
            consecutive_zero_drains++;
            /* 10 consecutive empty drains spaced ~500 µs apart = 5 ms
             * of quiescence, which is well past the worst-case
             * worker-to-drain handoff latency on a slow CI runner. */
            if (consecutive_zero_drains >= 10) break;
        } else {
            consecutive_zero_drains = 0;
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
        server.compression_lfu_threshold = 255;  /* don't filter on LFU */
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

/* P1 (R2.1.6): toggling master switch yes does NOT auto-compress
 * existing values. The applyCompressionEnabled apply hook was removed;
 * the master switch now governs only future write eligibility (and
 * gates compress-direction sweep requests). Operator must run an
 * explicit `COMPRESSION SWEEP direction=compress` to retroactively
 * compress the keyspace. */
TEST_F(CompressionSweepTest, NoAutoTriggerOnEnable) {
    ASSERT_NE(0u, installSyntheticDict());
    constexpr int n = 50;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }

    /* Simulate `CONFIG SET compression-enabled no` then `... yes`. The
     * test fixture's SetUp set compression_enabled = 1; flip to 0
     * then back to 1. With the apply-hook gone, this is just a flag
     * write — no sweep should be queued. */
    server.compression_enabled = 0;
    server.compression_enabled = 1;

    /* Drive cron ticks. State machine must stay IDLE (no requested
     * flag set; nothing for the cron to pick up). */
    for (int i = 0; i < 10; i++) compressionSweepDriveForTesting(1);
    EXPECT_FALSE(compressionSweepIsScanning());

    /* No values compressed — eligibility is enforced at the write path
     * (compressionEnqueueModified, called from signalModifiedKey),
     * which the test bypasses by using kvstoreHashtableAdd directly. */
    EncodingCounts c = countEncodings(0);
    EXPECT_EQ(n, c.total);
    EXPECT_EQ(n, c.raw);
    EXPECT_EQ(0, c.compressed);

    /* Operator's explicit compress sweep DOES compress them. */
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    int iters = driveAndDrain(2000);
    EXPECT_LE(iters, 200);
    EXPECT_EQ(n, countEncodings(0).compressed);
}

/* P1 + §3.4 state-machine table: when an `IDLE.requested(COMPRESS)`
 * meets a master-switch-off state at cron entry, the cron defensively
 * clears the request without entering SCANNING. This path is only
 * reachable if a caller bypasses the command handler (which itself
 * rejects compress-while-disabled), e.g. internal future auto-trigger
 * code racing with a CONFIG SET. */
TEST_F(CompressionSweepTest, CompressRequestClearedAtCronWhenDisabled) {
    ASSERT_NE(0u, installSyntheticDict());
    /* Bypass the command handler by calling compressionSweepRequest
     * directly, then disable BEFORE the cron picks up the request. */
    server.compression_enabled = 1;
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    server.compression_enabled = 0; /* now disabled with COMPRESS queued */

    compressionSweepDriveForTesting(1);

    /* State stays IDLE; request was cleared without entering SCANNING. */
    EXPECT_FALSE(compressionSweepIsScanning());
}

/* P1 + R2.1.4: `COMPRESSION SWEEP direction=decompress` is the
 * canonical drain path for the yes→no transition. It MUST work when
 * the master switch is off — that's the operator's only way to
 * convert the keyspace back to RAW after disabling. */
TEST_F(CompressionSweepTest, DecompressSweepRunsWhenDisabled) {
    ASSERT_NE(0u, installSyntheticDict());

    /* First populate + compress with master switch on. */
    constexpr int n = 100;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    driveAndDrain(2000);
    EXPECT_EQ(n, countEncodings(0).compressed);

    /* Disable the feature. Existing compressed values stay compressed. */
    server.compression_enabled = 0;
    EXPECT_EQ(n, countEncodings(0).compressed);

    /* Run the operator-initiated decompress sweep. This is the
     * scenario the bug fix targets: pre-fix, this command would be
     * rejected at the handler and the cron would refuse to advance
     * even if accepted. */
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_DECOMPRESS));
    int iters = driveAndDrain(2000);
    EXPECT_LE(iters, 200);

    /* All values back to RAW. */
    EncodingCounts post = countEncodings(0);
    EXPECT_EQ(0, post.compressed);
    EXPECT_EQ(n, post.raw);
}

/* P1 + §3.4: a SCANNING(COMPRESS) sweep aborts when the master switch
 * flips off. R2.1.4's "new writes stop being compressed" extends to
 * background work — a still-running compress sweep would violate that. */
TEST_F(CompressionSweepTest, InFlightCompressSweepAbortsOnDisable) {
    ASSERT_NE(0u, installSyntheticDict());
    /* Populate enough that the sweep can't finish in one tick under
     * tight pacing (same trick as SingleFlightRejectsConcurrentRequest). */
    constexpr int n = 500;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
    server.compression_sweep_max_cpu_pct = 1;
    server.hz = 500; /* 20 µs/tick */

    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    compressionSweepDriveForTesting(1);
    ASSERT_TRUE(compressionSweepIsScanning());

    /* Toggle off mid-sweep. */
    server.compression_enabled = 0;

    /* Next cron tick observes master-switch-off + direction=COMPRESS
     * and aborts; state returns to IDLE. */
    compressionSweepDriveForTesting(1);
    EXPECT_FALSE(compressionSweepIsScanning());

    /* Some keys may have been processed before the abort (the first
     * tick enqueued some). After the abort, no further compress work
     * happens. We don't assert an exact count — just that the abort
     * happened cleanly (state IDLE) and the sweep didn't continue
     * working through the remaining keys. */
    server.compression_sweep_max_cpu_pct = 100;
    server.hz = 10;
    /* Drain whatever was already in-flight when we aborted. After the
     * abort no new jobs enqueue; this just lets the workers finish
     * any compression jobs they had picked up before the abort, and
     * the drain handler installs the resulting compressed buffers
     * into the kvstore. Without this, leftover jobs would be cleaned
     * up at TearDown by compressionWorkersStop, but we want the test
     * to verify state after a clean drain. */
    driveAndDrain(2000);
    EncodingCounts after_abort = countEncodings(0);
    EXPECT_LT(after_abort.compressed, n)
        << "abort should have stopped before completing the keyspace";
}

/* P1 + §3.4: a SCANNING(DECOMPRESS) sweep is unaffected by master-
 * switch state. It runs regardless — it's the canonical drain path. */
TEST_F(CompressionSweepTest, InFlightDecompressSweepContinuesOnDisable) {
    ASSERT_NE(0u, installSyntheticDict());

    /* First populate + compress everything. */
    constexpr int n = 500;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    driveAndDrain(2000);
    EXPECT_EQ(n, countEncodings(0).compressed);

    /* Now run a paced decompress sweep. */
    server.compression_sweep_max_cpu_pct = 1;
    server.hz = 500;
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_DECOMPRESS));
    compressionSweepDriveForTesting(1);
    ASSERT_TRUE(compressionSweepIsScanning());

    /* Toggle off mid-decompress-sweep. The sweep should NOT abort —
     * it's the drain path that the operator presumably wants to
     * complete (and disabling and then explicitly draining is the
     * R2.1.4 happy path). */
    server.compression_enabled = 0;

    /* Restore pacing and drive to completion. */
    server.compression_sweep_max_cpu_pct = 100;
    server.hz = 10;
    int iters = driveAndDrain(2000);
    EXPECT_LE(iters, 200);
    EXPECT_FALSE(compressionSweepIsScanning());
    EXPECT_EQ(0, countEncodings(0).compressed)
        << "decompress sweep should have completed despite master-switch toggle";
    EXPECT_EQ(n, countEncodings(0).raw);
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
    /* Populate enough values so the sweep can't possibly finish in
     * one cron tick under tight pacing. */
    constexpr int n = 500;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
    /* Force a tiny per-tick budget. The budget formula is
     * `pct * 1000000 / 100 / hz`; with pct=1 and hz=500 we get
     * 20 µs/tick — well below the few-ms it takes to enqueue 500
     * keys, so the first cron tick is guaranteed to leave the
     * sweep in SCANNING state. */
    server.compression_sweep_max_cpu_pct = 1;
    server.hz = 500;

    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    /* Drive one tick — sweep enters SCANNING. */
    compressionSweepDriveForTesting(1);
    ASSERT_TRUE(compressionSweepIsScanning());

    /* Second request: refused. */
    EXPECT_EQ(0, compressionSweepRequest(COMPRESSION_SWEEP_DIR_DECOMPRESS));

    /* Restore pacing and drive to completion. */
    server.compression_sweep_max_cpu_pct = 100;
    server.hz = 10;
    driveAndDrain(2000);
    EXPECT_FALSE(compressionSweepIsScanning());
    EXPECT_EQ(n, countEncodings(0).compressed);
}

/* P1 + R2.5.7 two-mode drain: when a decompress sweep is in flight,
 * `compressionBeforeSleep` permanent-decompresses side-map entries
 * instead of restoring them. This cooperation lets read-touched values
 * help drain the keyspace.
 *
 * Test pattern:
 *   1. Compress N values via an explicit compress sweep.
 *   2. Pick one, materialize a transient view → encoding flips RAW,
 *      pin established (refcount=2), side-map size 1.
 *   3. Tight pacing + request DECOMPRESS sweep + drive 1 tick to
 *      enter SCANNING(DECOMPRESS).
 *   4. Verify compressionSweepCurrentDirection() returns DECOMPRESS.
 *   5. Call compressionBeforeSleep manually.
 *   6. Verify the materialized robj is now permanently RAW (val_ptr
 *      remains the temp sds — NOT swapped back to compressed_buffer)
 *      and the side-map is empty.
 *   7. Drive sweep to completion.
 */
TEST_F(CompressionSweepTest, BeforeSleepCooperatesWithDecompressSweep) {
    ASSERT_NE(0u, installSyntheticDict());

    /* Step 1: populate + compress. */
    constexpr int n = 200;
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    driveAndDrain(2000);
    ASSERT_EQ(n, countEncodings(0).compressed);

    /* Step 2: materialize one. Look up a known-key compressed robj. */
    sds target_key = sdsnew("k:000050");
    void *found = NULL;
    ASSERT_TRUE(kvstoreHashtableFind(server.db[0]->keys, 0, target_key, &found));
    sdsfree(target_key);
    robj *target = (robj *)found;
    ASSERT_EQ(OBJ_ENCODING_COMPRESSED, (int)target->encoding);

    /* The savings counter is already non-zero from step 1's
     * compress sweep — savings = total_unc - total_comp > 0 — so the
     * materialize cap won't fall back to permanent decompress. */
    int rc = compressionMaterializeTransientView(target, 0);
    ASSERT_EQ(0, rc);
    EXPECT_EQ(OBJ_ENCODING_RAW, (int)target->encoding);
    EXPECT_EQ(2, (int)target->refcount); /* pinned */
    EXPECT_EQ(1, transientViewActive(target));

    /* Snapshot val_ptr. After permanent-decompress drain, val_ptr
     * MUST still point at the temp sds (the materialized bytes).
     * After restore drain, val_ptr would be swapped back to the
     * original compressed_buffer. The two are distinguishable. */
    void *temp_sds_ptr = target->val_ptr;

    /* Step 3: tight pacing + decompress sweep request. */
    server.compression_sweep_max_cpu_pct = 1;
    server.hz = 500; /* 20 µs/tick */
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_DECOMPRESS));
    compressionSweepDriveForTesting(1);
    ASSERT_TRUE(compressionSweepIsScanning());

    /* Step 4: direction helper reports DECOMPRESS. */
    EXPECT_EQ(COMPRESSION_SWEEP_DIR_DECOMPRESS, compressionSweepCurrentDirection());

    /* Step 5: drain. Mode should be permanent-decompress. */
    compressionBeforeSleep();

    /* Step 6: verify permanent-decompress, NOT restoration. */
    EXPECT_EQ(OBJ_ENCODING_RAW, (int)target->encoding);
    EXPECT_EQ(temp_sds_ptr, target->val_ptr) << "val_ptr should be unchanged "
                                             << "(permanent-decompress mode keeps the temp sds installed; "
                                             << "restore mode would have swapped back to compressed_buffer)";
    EXPECT_EQ(0, transientViewActive(target));

    /* Step 7: drive sweep to completion + final cleanup. */
    server.compression_sweep_max_cpu_pct = 100;
    server.hz = 10;
    driveAndDrain(2000);
    EXPECT_FALSE(compressionSweepIsScanning());
    EXPECT_EQ(0, countEncodings(0).compressed);
}

/* Sanity: compressionSweepCurrentDirection() returns 0 when IDLE,
 * the queued direction when SCANNING. Used by compressionBeforeSleep
 * to select restore vs permanent-decompress mode (R2.5.7). */
TEST_F(CompressionSweepTest, SweepCurrentDirectionReportsState) {
    ASSERT_NE(0u, installSyntheticDict());

    /* IDLE → 0. */
    EXPECT_EQ(0, compressionSweepCurrentDirection());

    /* Populate enough that the sweep can't finish in 1 tick under
     * tight pacing. */
    for (int i = 0; i < 500; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        robj *o = makeKeyedRawString(keybuf, 1024, i, true);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
    server.compression_sweep_max_cpu_pct = 1;
    server.hz = 500;

    /* Request COMPRESS, drive 1 tick → SCANNING(COMPRESS). */
    EXPECT_EQ(1, compressionSweepRequest(COMPRESSION_SWEEP_DIR_COMPRESS));
    compressionSweepDriveForTesting(1);
    ASSERT_TRUE(compressionSweepIsScanning());
    EXPECT_EQ(COMPRESSION_SWEEP_DIR_COMPRESS, compressionSweepCurrentDirection());

    /* Restore pacing; drive to completion. */
    server.compression_sweep_max_cpu_pct = 100;
    server.hz = 10;
    driveAndDrain(2000);
    EXPECT_FALSE(compressionSweepIsScanning());

    /* Back to IDLE → 0. */
    EXPECT_EQ(0, compressionSweepCurrentDirection());
}

#endif /* USE_ZSTD */
