/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * test_compression_header.cpp — unit tests for src/compression_header.c.
 *
 * Covers:
 *   - compressionHeaderEncode / Decode round-trip across all four
 *     fields, including boundary values.
 *   - compressionHeaderDecode rejects unknown alg_magic values
 *     (R2.5.3 — corrupted/foreign data treated as invalid).
 *   - createCompressedObject success path: takes ownership of the
 *     buffer, sets type / encoding=OBJ_ENCODING_COMPRESSED, and the
 *     buffer is reachable via objectGetVal.
 *   - createCompressedObject validation rejection paths return NULL
 *     without consuming the caller's buffer.
 *   - freeCompressedObject releases the buffer via decrRefCount → no
 *     leak under valgrind/asan.
 *
 * The Phase 0 dictionary registry is no-op stubs, so IncRef/DecRef
 * calls inside createCompressedObject / freeCompressedObject are
 * verified only by absence of crashes here. Refcount accounting will
 * be tested end-to-end once S1 lands a real registry.
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstring>

extern "C" {
#include "compression_header.h"
#include "compression_registry.h"
#include "server.h"
#include "zmalloc.h"
}

/* Sentinel value for "no dictionary" — mirrors COMPRESSION_DICT_ID_NONE
 * in src/compression_registry.h. We don't include compression_registry.h
 * from C++ because the Phase 0 stub uses `<stdatomic.h>` and
 * `atomic_size_t`, which are C-only pre-C++23. The pattern that DOES
 * work for C++ (see src/queues.h + tests/unit/test_queues.cpp) is the
 * `_Atomic(T)` keyword syntax without including `<stdatomic.h>` in the
 * public header. Refactoring compression_registry.h to follow that
 * pattern is tracked as a follow-up for S1's owner; until then, this
 * literal mirrors the constant. If COMPRESSION_DICT_ID_NONE ever
 * changes from 0, this file must update with it. */
static constexpr uint32_t kNoDictId = 0u;

class CompressionHeaderTest : public ::testing::Test {
  protected:
    void SetUp() override {
        server.compression_dict_max_versions = 4;
        server.compression_threads = 1;
        server.logfile = (char *)"";
        server.verbosity = LL_WARNING;
        compressionRegistryInit();
    }
    void TearDown() override {
        compressionRegistryRelease();
    }
};

/* ========================================================================
 * Encode / decode round-trip
 * ========================================================================
 *
 * One round-trip test exercises typical, zero-edge, and max-edge values
 * in a single test body. Splitting boundary values into a separate test
 * adds no signal — the codec is straight memcpy-over-four-uint32, with
 * no value-dependent code paths to exercise individually.
 */

TEST_F(CompressionHeaderTest, EncodeDecodeRoundTrip) {
    unsigned char buf[COMPRESSION_HEADER_SIZE];
    compressedHeader out{};

    /* Typical values. */
    compressionHeaderEncode(buf,
                            COMPRESSION_ALG_ZSTD_MAGIC,
                            /*alg_meta=*/0xCAFEBABEu,
                            /*uncompressed_len=*/4096u,
                            /*compressed_len=*/1234u);
    ASSERT_EQ(0, compressionHeaderDecode(buf, &out));
    ASSERT_EQ(COMPRESSION_ALG_ZSTD_MAGIC, out.alg_magic);
    ASSERT_EQ(0xCAFEBABEu, out.alg_meta);
    ASSERT_EQ(4096u, out.uncompressed_len);
    ASSERT_EQ(1234u, out.compressed_len);

    /* Zero edge — the eligibility predicate prevents zero-length values
     * in practice, but the codec must not special-case them. */
    compressionHeaderEncode(buf, COMPRESSION_ALG_ZSTD_MAGIC, 0u, 0u, 0u);
    ASSERT_EQ(0, compressionHeaderDecode(buf, &out));
    ASSERT_EQ(0u, out.alg_meta);
    ASSERT_EQ(0u, out.uncompressed_len);
    ASSERT_EQ(0u, out.compressed_len);

    /* Max edge — uint32 saturation. */
    compressionHeaderEncode(buf,
                            COMPRESSION_ALG_ZSTD_MAGIC,
                            UINT32_MAX, UINT32_MAX, UINT32_MAX);
    ASSERT_EQ(0, compressionHeaderDecode(buf, &out));
    ASSERT_EQ(UINT32_MAX, out.alg_meta);
    ASSERT_EQ(UINT32_MAX, out.uncompressed_len);
    ASSERT_EQ(UINT32_MAX, out.compressed_len);
}

/* ========================================================================
 * Decode rejects unknown algorithm magic
 * ======================================================================== */

TEST_F(CompressionHeaderTest, DecodeRejectsUnknownAlgMagic) {
    /* Build a syntactically valid header but with an algorithm tag
     * that is not implemented (or has been corrupted in storage).
     * Decode must return -1. */
    unsigned char buf[COMPRESSION_HEADER_SIZE];
    constexpr uint32_t kReservedMagic = 0xDEADBEEFu;
    compressionHeaderEncode(buf, kReservedMagic, 0, 100, 50);

    compressedHeader out{};
    ASSERT_EQ(-1, compressionHeaderDecode(buf, &out));
}

TEST_F(CompressionHeaderTest, DecodeRejectsZeroMagic) {
    /* All-zero buffer is the most common corruption pattern (an
     * uninitialized region). It must be rejected. */
    unsigned char buf[COMPRESSION_HEADER_SIZE];
    memset(buf, 0, sizeof(buf));
    compressedHeader out{};
    ASSERT_EQ(-1, compressionHeaderDecode(buf, &out));
}

/* ========================================================================
 * createCompressedObject — success and rejection paths
 * ========================================================================
 *
 * Tests use a payload of `compressed_len` bytes filled with a
 * recognizable pattern. createCompressedObject takes ownership on
 * success, so we release via decrRefCount; on rejection it returns
 * NULL and we zfree the buffer ourselves.
 */

namespace {

/* Allocate a buffer with a valid header followed by `compressed_len`
 * payload bytes (filled with `payload_byte`). Caller owns the buffer. */
void *makeCompressedBuffer(uint32_t alg_magic,
                           uint32_t alg_meta,
                           uint32_t uncompressed_len,
                           uint32_t compressed_len,
                           unsigned char payload_byte = 0x5Au) {
    size_t total = (size_t)COMPRESSION_HEADER_SIZE + compressed_len;
    void *buf = zmalloc(total);
    compressionHeaderEncode((unsigned char *)buf, alg_magic, alg_meta,
                            uncompressed_len, compressed_len);
    memset((unsigned char *)buf + COMPRESSION_HEADER_SIZE, payload_byte,
           compressed_len);
    return buf;
}

} // namespace

TEST_F(CompressionHeaderTest, CreateCompressedObjectSuccess) {
    constexpr uint32_t kCompressedLen = 32u;
    constexpr uint32_t kUncompressedLen = 64u;
    void *buf = makeCompressedBuffer(COMPRESSION_ALG_ZSTD_MAGIC,
                                     /*dict_id=*/42u,
                                     kUncompressedLen,
                                     kCompressedLen);
    size_t total = (size_t)COMPRESSION_HEADER_SIZE + kCompressedLen;

    robj *o = createCompressedObject(OBJ_STRING, buf, total);
    ASSERT_NE(nullptr, o);
    ASSERT_EQ((unsigned)OBJ_STRING, o->type);
    ASSERT_EQ((unsigned)OBJ_ENCODING_COMPRESSED, o->encoding);
    ASSERT_EQ(buf, objectGetVal(o)); /* zero-copy contract */

    /* Header is reachable through the value pointer. */
    compressedHeader hdr{};
    ASSERT_EQ(0, compressionHeaderDecode(
                     (const unsigned char *)objectGetVal(o), &hdr));
    ASSERT_EQ(COMPRESSION_ALG_ZSTD_MAGIC, hdr.alg_magic);
    ASSERT_EQ(42u, hdr.alg_meta);
    ASSERT_EQ(kUncompressedLen, hdr.uncompressed_len);
    ASSERT_EQ(kCompressedLen, hdr.compressed_len);

    /* Releases the buffer via freeStringObject → freeCompressedObject. */
    decrRefCount(o);
}

TEST_F(CompressionHeaderTest, CreateCompressedObjectRejectsBadMagic) {
    /* Header decodes structurally but has an unknown alg_magic. This is
     * the only legitimate "rejection" path: bytes from disk can present
     * any 4-byte sequence, so we return NULL (caller — typically RDB
     * load — wraps with rdbReportCorruptRDB). The other rejection-style
     * conditions (NULL buffer, undersized buffer, size mismatch vs.
     * header) are programmer errors and raise serverAssert; tests for
     * those would crash the gtest process and aren't included here.
     * See research/error-handling-conventions.md for the convention. */
    constexpr uint32_t kCompressedLen = 8u;
    void *buf = makeCompressedBuffer(/*alg_magic=*/0xBAADF00Du,
                                     /*alg_meta=*/0u,
                                     /*uncompressed_len=*/16u,
                                     kCompressedLen);
    size_t total = (size_t)COMPRESSION_HEADER_SIZE + kCompressedLen;

    robj *o = createCompressedObject(OBJ_STRING, buf, total);
    ASSERT_EQ(nullptr, o);

    /* Caller retains ownership on rejection. */
    zfree(buf);
}

TEST_F(CompressionHeaderTest, CreateCompressedObjectAcceptsZeroDictId) {
    /* alg_meta == 0 (the COMPRESSION_DICT_ID_NONE sentinel) is
     * structurally valid — it means "ZSTD without a dictionary." v1
     * doesn't emit this (the eligibility filter requires a trained
     * dict) but the codec must accept it; the IncRef call is
     * conditional on dict_id != 0 so this also exercises that branch. */
    constexpr uint32_t kCompressedLen = 4u;
    void *buf = makeCompressedBuffer(COMPRESSION_ALG_ZSTD_MAGIC,
                                     /*dict_id=*/kNoDictId,
                                     /*uncompressed_len=*/8u,
                                     kCompressedLen);
    size_t total = (size_t)COMPRESSION_HEADER_SIZE + kCompressedLen;

    robj *o = createCompressedObject(OBJ_STRING, buf, total);
    ASSERT_NE(nullptr, o);
    ASSERT_EQ((unsigned)OBJ_ENCODING_COMPRESSED, o->encoding);

    decrRefCount(o);
}
