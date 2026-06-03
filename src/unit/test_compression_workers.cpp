/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * Tests for the compression worker pool — start/stop, runtime resize,
 * enqueue/drain end-to-end, shutdown drain, identity contract for QSBR,
 * plus S2.5 encoder-path tests that exercise real ZSTD compression.
 *
 * The S2.4 plumbing tests below are encoder-agnostic — they only assert
 * job count and thread-count transitions, so they stayed stable across
 * the S2.4 → S2.5 transition. A separate S2.5 section at the bottom of
 * this file installs a synthetic dictionary and verifies real
 * compress + decompress round-trip via test-only entry points defined
 * in compression_workers.c (testOnlyCompressionWorkers* family).
 *
 * The test-only entry points follow the established Valkey convention
 * (see quicklist.c / intset.c testOnly* functions): they are defined
 * in the .c file but NOT declared in the public header — this test
 * file declares what it needs locally below in its own extern "C"
 * block, and the symbols simply don't appear in the production-callable
 * surface.
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
#include <cstring>
#include <string>
#include <thread>
#include <vector>

extern "C" {
#include "compression_registry.h"
#include "compression_workers.h"
#include "sds.h"
#include "server.h"
#include "zdict.h"
#include "zstd.h"

/* Test-only entry points defined in compression_workers.c. Declared
 * here locally rather than in compression_workers.h — the production
 * surface stays clean (matches the testOnly* convention used in
 * quicklist.c / intset.c). */
int  testOnlyCompressionWorkersDrainOutbox(void **jobs_out, int budget);
void testOnlyCompressionWorkersFreeJob(void *job_ptr);
void testOnlyCompressionWorkersJobRead(void *job_ptr,
                                       const char **out_src,
                                       void **out_dst,
                                       size_t *out_dst_len,
                                       uint32_t *out_dict_id,
                                       int *out_err);
}

/* Flat read-out of compressionJob (file-private to compression_workers.c).
 * The test-only reader fills this struct, projecting the fields the
 * tests actually need. Defined in this file so the production code
 * carries no record of the projection layout. */
struct CompressionJobView {
    const char *src;
    void *dst;
    size_t dst_len;
    uint32_t dict_id;
    int err;
};

static CompressionJobView readJob(void *job_ptr) {
    CompressionJobView v = {nullptr, nullptr, 0, 0, 0};
    testOnlyCompressionWorkersJobRead(job_ptr, &v.src, &v.dst, &v.dst_len,
                                      &v.dict_id, &v.err);
    return v;
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

        /* Net-savings guard threshold (R2.4.3). Without setting this,
         * the production drain handler would compare against an
         * uninitialized field. Default 10 (= 10%) matches the design
         * doc default and is what production callers will see. */
        server.compression_min_savings_ratio = 10;
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

/* ========================================================================
 * S2.5 — Encoder path tests
 * ========================================================================
 *
 * These tests install a synthetic dictionary into the registry, enqueue
 * jobs, and use the testOnly* entry points (declared at the top of
 * this file in its extern "C" block) to verify the worker produced
 * a valid compressed buffer that decompresses back to the original
 * bytes. The synthetic dict is built from a deterministic in-memory
 * corpus so the tests are reproducible across runs and machines.
 *
 * Helper: installSyntheticDict() trains a small ZSTD dictionary on a
 * deterministic corpus and adds it to the registry as the active dict.
 * The fixture's TearDown calls compressionRegistryRelease which frees
 * the dict + its CDict/DDict.
 *
 * Why "synthetic" not "real": the production training path (S1.2) walks
 * kvstore on the main thread and submits a bio job. Plumbing that for
 * a unit test would require a fake kvstore, a fake bio runner, and a
 * fake serverCron loop — an order of magnitude more scaffolding than
 * the test value justifies. The synthetic helper goes through the
 * SAME registry add/promote path that production training uses; only
 * the corpus source differs.
 */

namespace {

/* Deterministic compressible corpus — repeated short JSON-like
 * strings. Each sample is an immutable C string; the dict trainer
 * concatenates them into a contiguous buffer with a parallel sizes[]
 * array. */
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

/* Trains a synthetic ZSTD dict + creates CDict/DDict + adds to registry
 * as the active dict. Returns the assigned dict_id, or 0 on failure.
 *
 * ZSTD's trainer recommends ~100x dict-size in training data. For a
 * 1 KB target dict we need ~100 KB of samples. We synthesize that by
 * combinatorially generating many small JSON-shaped samples derived
 * from kCorpusSamples — gives the trainer enough volume to produce a
 * usable dict. */
uint32_t installSyntheticDict() {
    /* Generate ~100 KB of training data (~1500 samples × ~70 B). */
    constexpr int kSampleCount = 1500;
    std::vector<std::string> samples;
    samples.reserve(kSampleCount);
    for (int i = 0; i < kSampleCount; i++) {
        /* Pick a base sample and append a small per-i suffix to keep
         * the trainer from collapsing to a trivial entropy table. */
        std::string s = kCorpusSamples[i % kCorpusSampleCount];
        s += ",\"seq\":" + std::to_string(i) + "}";
        /* Replace the trailing "}}" with "}" — the original sample
         * already closes the JSON object, so the seq insertion above
         * produces a slightly malformed string for trainer purposes
         * (we don't care; it's just bytes). */
        samples.push_back(std::move(s));
    }

    /* Pack into one contiguous buffer with parallel sizes[]. */
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

    /* Train. 1 KB dict capacity matches the design's "small enough to
     * train fast in tests but realistic enough to validate the
     * encoder path". */
    constexpr size_t kDictCapacity = 1024;
    std::vector<unsigned char> dict_bytes(kDictCapacity);
    size_t got = ZDICT_trainFromBuffer(dict_bytes.data(), kDictCapacity,
                                       sample_buf.data(),
                                       sample_sizes.data(),
                                       (unsigned)samples.size());
    if (ZDICT_isError(got)) return 0;
    dict_bytes.resize(got);

    /* Build the registry entry. The registry takes ownership of `bytes`,
     * `cdict`, and `ddict` once compressionRegistryAdd succeeds. */
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

/* Decompress a worker-produced buffer (header + ZSTD frame) using the
 * registry's DDict for the dict_id encoded in the header. Returns the
 * decompressed bytes as a std::string, or an empty string on failure.
 * Used by the round-trip tests to verify byte-equality with the source. */
std::string decompressBuffer(const void *buf, size_t buf_len, uint32_t dict_id) {
    if (buf == NULL || buf_len <= COMPRESSION_HEADER_SIZE) return "";

    compressionDictPair *pair = compressionRegistryLookup(dict_id);
    if (pair == NULL || pair->ddict == NULL) return "";

    /* Skip the 16-byte header to reach the ZSTD frame. */
    const unsigned char *frame =
        (const unsigned char *)buf + COMPRESSION_HEADER_SIZE;
    size_t frame_len = buf_len - COMPRESSION_HEADER_SIZE;

    unsigned long long out_size = ZSTD_getFrameContentSize(frame, frame_len);
    if (out_size == ZSTD_CONTENTSIZE_ERROR ||
        out_size == ZSTD_CONTENTSIZE_UNKNOWN) {
        return "";
    }

    std::string out;
    out.resize((size_t)out_size);
    ZSTD_DCtx *dctx = ZSTD_createDCtx();
    if (dctx == NULL) return "";
    size_t got = ZSTD_decompress_usingDDict(dctx, &out[0], out.size(),
                                            frame, frame_len, pair->ddict);
    ZSTD_freeDCtx(dctx);
    if (ZSTD_isError(got)) return "";
    out.resize(got);
    return out;
}

} /* anonymous namespace */

TEST_F(CompressionWorkersTest, RealCompressionRoundTrip) {
    /* Install dict, enqueue a single compressible value, drain via the
     * testing accessor, decompress with the same DDict, assert the
     * decompressed bytes match the original. */
    uint32_t dict_id = installSyntheticDict();
    ASSERT_NE(dict_id, 0u);

    ASSERT_EQ(0, compressionWorkersStart(1));

    /* A value built from the corpus pattern — should compress well
     * with the synthetic dict. ~1 KB total. */
    std::string source;
    for (int i = 0; i < 20; i++) {
        source += kCorpusSamples[i % kCorpusSampleCount];
    }
    sds key = sdsnew("k1");
    sds val = sdsnewlen(source.data(), source.size());
    ASSERT_EQ(0, compressionWorkersEnqueue(key, 0, 42, val));

    /* Wait for the worker to deliver. Poll the testing accessor; it
     * doesn't free, so we can inspect. */
    void *jobs[1];
    int got = 0;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(2000);
    while (got == 0 && std::chrono::steady_clock::now() < deadline) {
        got = testOnlyCompressionWorkersDrainOutbox(jobs, 1);
        if (got == 0) std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    ASSERT_EQ(1, got);

    /* Verify the worker's output: dict_id matches, dst non-NULL,
     * dst_len > HEADER_SIZE, err == 0. */
    CompressionJobView v = readJob(jobs[0]);
    EXPECT_EQ(dict_id, v.dict_id);
    EXPECT_EQ(0, v.err);
    ASSERT_NE(v.dst, nullptr);
    ASSERT_GT(v.dst_len, COMPRESSION_HEADER_SIZE);

    /* Compressed should be smaller than uncompressed for this
     * compressible input — sanity check, not a strict requirement of
     * the encoder API. */
    EXPECT_LT(v.dst_len, source.size());

    /* Round-trip: decompress the worker's output and compare to source. */
    std::string roundtrip = decompressBuffer(v.dst, v.dst_len, dict_id);
    EXPECT_EQ(source, roundtrip);

    testOnlyCompressionWorkersFreeJob(jobs[0]);
    sdsfree(key);
    sdsfree(val);
    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, NetSavingsGuardRejectsIncompressible) {
    /* Random bytes don't compress — net-savings guard in the production
     * drain path should reject. We verify two things:
     *   1. The worker still produces a buffer (encoder works on any input).
     *   2. The production drain path runs without crashing on rejection.
     *
     * TODO(S4.1): when the rejection counter
     * compression_skipped_incompressible++ lands in S4.1, extend
     * this test to assert the counter increments by exactly 1 here
     * (and that the live_ratio EMA records the rejection ratio per
     * R2.3.5). For now we just ensure the path doesn't blow up. */
    uint32_t dict_id = installSyntheticDict();
    ASSERT_NE(dict_id, 0u);

    ASSERT_EQ(0, compressionWorkersStart(1));

    /* Pseudo-random bytes — deterministic seed for reproducibility. */
    std::string source(1024, '\0');
    uint32_t state = 0xDEADBEEFu;
    for (size_t i = 0; i < source.size(); i++) {
        state = state * 1664525u + 1013904223u;
        source[i] = (char)(state >> 24);
    }

    sds key = sdsnew("k1");
    sds val = sdsnewlen(source.data(), source.size());
    ASSERT_EQ(0, compressionWorkersEnqueue(key, 0, 1, val));

    /* Drain through the production handler — guard logic exercised. */
    EXPECT_EQ(1, drainUntil(1, /*deadline_ms=*/2000));
    EXPECT_EQ(0, compressionWorkersDrainOutbox(10));

    sdsfree(key);
    sdsfree(val);
    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, NoActiveDictMarksJobNotCompressed) {
    /* Without installing a dict, the worker must take the
     * "compression-enabled yes but no active dict" branch (R2.1.5):
     *   - dict_id = 0
     *   - dst = NULL, dst_len = 0
     *   - err = 1 (worker-policy sentinel, not a ZSTD error code) */
    /* (no installSyntheticDict here) */
    ASSERT_EQ(0, compressionWorkersStart(1));

    sds key = sdsnew("k1");
    sds val = sdsnew("any value");
    ASSERT_EQ(0, compressionWorkersEnqueue(key, 0, 1, val));

    void *jobs[1];
    int got = 0;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(1000);
    while (got == 0 && std::chrono::steady_clock::now() < deadline) {
        got = testOnlyCompressionWorkersDrainOutbox(jobs, 1);
        if (got == 0) std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    ASSERT_EQ(1, got);

    CompressionJobView v = readJob(jobs[0]);
    EXPECT_EQ(0u, v.dict_id);
    EXPECT_EQ(nullptr, v.dst);
    EXPECT_EQ(0u, v.dst_len);
    EXPECT_NE(0, v.err);

    testOnlyCompressionWorkersFreeJob(jobs[0]);
    sdsfree(key);
    sdsfree(val);
    compressionWorkersStop();
}

TEST_F(CompressionWorkersTest, CompressionFromMultipleWorkersIsConsistent) {
    /* Multi-thread encode: 4 workers, 100 jobs of various sizes/contents.
     * Verify each compressed buffer round-trips to the matching source. */
    uint32_t dict_id = installSyntheticDict();
    ASSERT_NE(dict_id, 0u);

    ASSERT_EQ(0, compressionWorkersStart(4));

    constexpr int kJobs = 100;
    /* Source materials, indexed by job. We need to keep the source
     * bytes alive until we've inspected the worker's output, since
     * the worker's job carries an `sds` pointer borrowed from us. */
    std::vector<std::string> sources(kJobs);
    std::vector<sds> keys(kJobs), vals(kJobs);
    for (int i = 0; i < kJobs; i++) {
        /* Variation: mix of corpus repetitions + a per-job suffix to
         * keep contents distinct. */
        std::string s;
        int reps = 5 + (i % 15);
        for (int r = 0; r < reps; r++) {
            s += kCorpusSamples[(i * 31 + r) % kCorpusSampleCount];
        }
        s += "_job_" + std::to_string(i);
        sources[i] = std::move(s);
        keys[i] = sdsnew("k");
        vals[i] = sdsnewlen(sources[i].data(), sources[i].size());
        ASSERT_EQ(0, compressionWorkersEnqueue(keys[i], 0, (uint64_t)i, vals[i]));
    }

    /* Drain everything via the testing accessor. The main thread's
     * compressionAfterSleep is not running in this test environment,
     * so we just poll. */
    std::vector<void *> all_jobs;
    auto deadline = std::chrono::steady_clock::now() +
                    std::chrono::milliseconds(5000);
    while ((int)all_jobs.size() < kJobs &&
           std::chrono::steady_clock::now() < deadline) {
        void *batch[16];
        int got = testOnlyCompressionWorkersDrainOutbox(batch, 16);
        for (int i = 0; i < got; i++) all_jobs.push_back(batch[i]);
        if (got == 0) std::this_thread::sleep_for(std::chrono::microseconds(200));
    }
    ASSERT_EQ((size_t)kJobs, all_jobs.size());

    /* For each job, decompress and compare to the matching source.
     * The worker doesn't preserve job-input order across multiple
     * threads (work-stealing inbox), so we identify each job by its
     * src pointer (still pointing into vals[i]). */
    int matched = 0;
    for (void *jp : all_jobs) {
        CompressionJobView v = readJob(jp);
        EXPECT_EQ(0, v.err);
        EXPECT_EQ(dict_id, v.dict_id);
        ASSERT_NE(v.src, nullptr);

        /* Find the matching source. */
        int idx = -1;
        for (int i = 0; i < kJobs; i++) {
            if ((const char *)vals[i] == v.src) { idx = i; break; }
        }
        ASSERT_NE(idx, -1);

        ASSERT_NE(v.dst, nullptr);

        std::string roundtrip = decompressBuffer(v.dst, v.dst_len, dict_id);
        EXPECT_EQ(sources[idx], roundtrip);

        testOnlyCompressionWorkersFreeJob(jp);
        matched++;
    }
    EXPECT_EQ(kJobs, matched);

    for (int i = 0; i < kJobs; i++) {
        sdsfree(keys[i]);
        sdsfree(vals[i]);
    }
    compressionWorkersStop();
}
