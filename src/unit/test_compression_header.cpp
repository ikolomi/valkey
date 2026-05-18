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
 *     buffer, sets type=OBJ_STRING + encoding=OBJ_ENCODING_COMPRESSED,
 *     and the buffer is reachable via objectGetVal.
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
#include <vector>

extern "C" {
#include "compression_header.h"
#include "server.h"
#include "zmalloc.h"
}

class CompressionHeaderTest : public ::testing::Test {};

/* ========================================================================
 * Encode / decode round-trip
 * ======================================================================== */

TEST_F(CompressionHeaderTest, EncodeDecodeRoundTrip) {
    unsigned char buf[COMPRESSION_HEADER_SIZE];

    compressionHeaderEncode(buf,
                            COMPRESSION_ALG_ZSTD_MAGIC,
                            /*alg_meta=*/0xCAFEBABEu,
                            /*uncompressed_len=*/4096u,
                            /*compressed_len=*/1234u);

    compressedHeader out{};
    ASSERT_EQ(0, compressionHeaderDecode(buf, &out));
    ASSERT_EQ(COMPRESSION_ALG_ZSTD_MAGIC, out.alg_magic);
    ASSERT_EQ(0xCAFEBABEu, out.alg_meta);
    ASSERT_EQ(4096u, out.uncompressed_len);
    ASSERT_EQ(1234u, out.compressed_len);
}

TEST_F(CompressionHeaderTest, EncodeDecodeBoundaryValues) {
    /* uncompressed_len = 0 is unusual but legal at the format level
     * (the eligibility predicate prevents it in practice). compressed_len
     * == 0 is similarly legal at the format level. UINT32_MAX is the
     * upper bound of the field type. */
    unsigned char buf[COMPRESSION_HEADER_SIZE];

    compressionHeaderEncode(buf,
                            COMPRESSION_ALG_ZSTD_MAGIC,
                            /*alg_meta=*/0u,
                            /*uncompressed_len=*/0u,
                            /*compressed_len=*/0u);
    compressedHeader out{};
    ASSERT_EQ(0, compressionHeaderDecode(buf, &out));
    ASSERT_EQ(0u, out.alg_meta);
    ASSERT_EQ(0u, out.uncompressed_len);
    ASSERT_EQ(0u, out.compressed_len);

    compressionHeaderEncode(buf,
                            COMPRESSION_ALG_ZSTD_MAGIC,
                            /*alg_meta=*/UINT32_MAX,
                            /*uncompressed_len=*/UINT32_MAX,
                            /*compressed_len=*/UINT32_MAX);
    ASSERT_EQ(0, compressionHeaderDecode(buf, &out));
    ASSERT_EQ(UINT32_MAX, out.alg_meta);
    ASSERT_EQ(UINT32_MAX, out.uncompressed_len);
    ASSERT_EQ(UINT32_MAX, out.compressed_len);
}

TEST_F(CompressionHeaderTest, DecodeAcceptsNullOutPointer) {
    /* The decode helper must tolerate a NULL out pointer for callers
     * that only want the validation result (e.g. a quick header
     * sanity check before passing the buffer along). */
    unsigned char buf[COMPRESSION_HEADER_SIZE];
    compressionHeaderEncode(buf, COMPRESSION_ALG_ZSTD_MAGIC, 1, 100, 50);
    ASSERT_EQ(0, compressionHeaderDecode(buf, nullptr));
}

/* ========================================================================
 * Decode rejects unknown algorithm magic
 * ======================================================================== */

TEST_F(CompressionHeaderTest, DecodeRejectsUnknownAlgMagic) {
    /* Build a syntactically valid header but with an algorithm tag
     * that is not yet implemented (or has been corrupted in storage).
     * Decode must return -1 and must not write to `out`. */
    unsigned char buf[COMPRESSION_HEADER_SIZE];

    /* Reserved future ZSTD-without-dict magic, not yet emitted by v1. */
    constexpr uint32_t kReservedMagic = 0xDEADBEEFu;
    compressionHeaderEncode(buf, kReservedMagic, 0, 100, 50);

    compressedHeader out{};
    out.alg_magic = 0xAAAAAAAAu;  /* sentinel to detect overwrite */
    ASSERT_EQ(-1, compressionHeaderDecode(buf, &out));
    ASSERT_EQ(0xAAAAAAAAu, out.alg_magic);  /* unchanged */
}

TEST_F(CompressionHeaderTest, DecodeRejectsZeroMagic) {
    /* All-zero buffer is the most common corruption pattern (an
     * uninitialized region). It must be rejected. */
    unsigned char buf[COMPRESSION_HEADER_SIZE];
    memset(buf, 0, sizeof(buf));
    ASSERT_EQ(-1, compressionHeaderDecode(buf, nullptr));
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

}  // namespace

TEST_F(CompressionHeaderTest, CreateCompressedObjectSuccess) {
    constexpr uint32_t kCompressedLen = 32u;
    constexpr uint32_t kUncompressedLen = 64u;
    void *buf = makeCompressedBuffer(COMPRESSION_ALG_ZSTD_MAGIC,
                                     /*dict_id=*/42u,
                                     kUncompressedLen,
                                     kCompressedLen);
    size_t total = (size_t)COMPRESSION_HEADER_SIZE + kCompressedLen;

    robj *o = createCompressedObject(buf, total);
    ASSERT_NE(nullptr, o);
    ASSERT_EQ(OBJ_STRING, o->type);
    ASSERT_EQ(OBJ_ENCODING_COMPRESSED, o->encoding);
    ASSERT_EQ(buf, objectGetVal(o));  /* zero-copy contract */

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

TEST_F(CompressionHeaderTest, CreateCompressedObjectRejectsNull) {
    ASSERT_EQ(nullptr, createCompressedObject(nullptr, 100));
}

TEST_F(CompressionHeaderTest, CreateCompressedObjectRejectsTooSmall) {
    /* Buffer smaller than the 16-byte header cannot possibly be valid. */
    unsigned char tiny[COMPRESSION_HEADER_SIZE - 1] = {0};
    ASSERT_EQ(nullptr, createCompressedObject(tiny, sizeof(tiny)));
}

TEST_F(CompressionHeaderTest, CreateCompressedObjectRejectsBadMagic) {
    /* Header decodes structurally but has an unknown alg_magic. */
    constexpr uint32_t kCompressedLen = 8u;
    void *buf = makeCompressedBuffer(/*alg_magic=*/0xBAADF00Du,
                                     /*alg_meta=*/0u,
                                     /*uncompressed_len=*/16u,
                                     kCompressedLen);
    size_t total = (size_t)COMPRESSION_HEADER_SIZE + kCompressedLen;

    robj *o = createCompressedObject(buf, total);
    ASSERT_EQ(nullptr, o);

    /* Caller retains ownership on rejection. */
    zfree(buf);
}

TEST_F(CompressionHeaderTest, CreateCompressedObjectRejectsSizeMismatch) {
    /* Header advertises a 32-byte frame but we hand it a buffer sized
     * for a 16-byte frame. createCompressedObject must reject. */
    void *buf = makeCompressedBuffer(COMPRESSION_ALG_ZSTD_MAGIC,
                                     /*dict_id=*/1u,
                                     /*uncompressed_len=*/64u,
                                     /*compressed_len=*/32u);
    /* Lie about the buffer length: only header + 16 bytes. */
    size_t too_small = (size_t)COMPRESSION_HEADER_SIZE + 16u;

    robj *o = createCompressedObject(buf, too_small);
    ASSERT_EQ(nullptr, o);

    zfree(buf);
}

TEST_F(CompressionHeaderTest, CreateCompressedObjectAcceptsZeroDictId) {
    /* alg_meta == COMPRESSION_DICT_ID_NONE (0) is structurally valid
     * — it means "ZSTD without a dictionary." v1 doesn't emit this
     * (the eligibility filter requires a trained dict) but the
     * codec must accept it; the IncRef call is conditional on
     * dict_id != 0 so this also exercises that branch. */
    constexpr uint32_t kCompressedLen = 4u;
    void *buf = makeCompressedBuffer(COMPRESSION_ALG_ZSTD_MAGIC,
                                     /*dict_id=*/COMPRESSION_DICT_ID_NONE,
                                     /*uncompressed_len=*/8u,
                                     kCompressedLen);
    size_t total = (size_t)COMPRESSION_HEADER_SIZE + kCompressedLen;

    robj *o = createCompressedObject(buf, total);
    ASSERT_NE(nullptr, o);
    ASSERT_EQ(OBJ_ENCODING_COMPRESSED, o->encoding);

    decrRefCount(o);
}

/* ========================================================================
 * freeCompressedObject — defensive paths
 * ======================================================================== */

TEST_F(CompressionHeaderTest, FreeCompressedObjectIgnoresNull) {
    /* Should not crash; matches the pattern of freeStringObject etc. */
    freeCompressedObject(nullptr);
}

TEST_F(CompressionHeaderTest, FreeCompressedObjectIgnoresWrongEncoding) {
    /* If freeStringObject is called against a non-compressed robj for
     * any reason (defensive double-call guard), it must be a no-op. */
    sds s = sdsnew("hello");
    robj *o = createObject(OBJ_STRING, s);
    ASSERT_EQ(OBJ_ENCODING_RAW, o->encoding);

    freeCompressedObject(o);  /* should be a no-op for RAW */

    /* Still reachable; release normally. */
    decrRefCount(o);
}
