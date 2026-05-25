/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef __COMPRESSION_HEADER_H
#define __COMPRESSION_HEADER_H

/*
 * Per-value compressed-buffer header + allocation helpers.
 *
 * Design of record:
 *   .agents/planning/realtime-data-compression/design/detailed-design.md §5.2
 *
 * Layout (16 B header + compressed frame):
 *
 *   +-----------------------+---------------------------+
 *   | compressedHeader (16) | compressed frame bytes    |
 *   +-----------------------+---------------------------+
 *
 *   compressedHeader {
 *       uint32_t alg_magic;           // algorithm tag, doubles as magic:
 *                                     //   'Z','S','T','D' = ZSTD + trained dict
 *                                     //   (reserved: 'L','Z','4',' ' = LZ4)
 *                                     //   unknown value → corrupt, reject
 *       uint32_t alg_meta;            // per-algorithm metadata
 *                                     //   ZSTD: dict_id of the registry entry
 *                                     //   LZ4 : 0 (reserved for future flags)
 *       uint32_t uncompressed_len;    // original payload length
 *       uint32_t compressed_len;      // frame bytes, excluding this header
 *   }
 *
 * Why alg_magic + alg_meta instead of a bare dict_id: the feature is
 * structurally designed around a single algorithm (ZSTD+dict) in v1 but
 * the RDB on-disk layout (§2.6 R2.6.1) and this in-memory header are
 * write-once formats. Reserving an explicit algorithm tag in Phase 0
 * lets us add LZ4 / snappy / hardware backends in v2 without another
 * encoding-byte migration. The tag also doubles as corruption magic
 * (wrong pattern → reject), so we pay no size cost for the generality.
 *
 * The struct is native-byte-order in memory. RDB and DUMP/RESTORE
 * re-encode the fields through rdb's length-encoding (§2.6 R2.6.1), so
 * the in-memory layout does not leak onto disk or the wire.
 *
 * Boundary: this header is owned by the compression hot path. The
 * dictionary registry does not touch it. The RDB load/save path reads
 * the four fields but writes the on-disk variant separately.
 */

#include "server.h"

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

/* ========================================================================
 * Algorithm-magic constants
 * ======================================================================== */

/* ZSTD + trained dict — the only algorithm implemented in v1. ASCII
 * "ZSTD" as four little-endian bytes. */
#define COMPRESSION_ALG_ZSTD_MAGIC 0x4454535Au

/* Reserved values for future algorithms (not emitted by v1):
 *     COMPRESSION_ALG_LZ4_MAGIC     = 'L','Z','4',' ' = 0x20345A4Cu
 *     COMPRESSION_ALG_SNAPPY_MAGIC  = 'S','N','A','P' = 0x50414E53u
 */

#define COMPRESSION_HEADER_SIZE 16u /* sizeof(compressedHeader) */

typedef struct compressedHeader {
    uint32_t alg_magic;
    uint32_t alg_meta;
    uint32_t uncompressed_len;
    uint32_t compressed_len;
} compressedHeader;

/* Compile-time size check. `static_assert` is C11 (via <assert.h>) and C++11
 * (keyword), so this header is includable from both C sources and the gtest
 * unit tests under src/unit/. */
static_assert(sizeof(compressedHeader) == COMPRESSION_HEADER_SIZE,
              "compressedHeader must be exactly 16 bytes");

/* ========================================================================
 * Encoding / decoding
 * ======================================================================== */

/* Writes a header into `dst` in native byte order. `dst` must point at
 * COMPRESSION_HEADER_SIZE bytes of writable storage. `alg_magic` is one
 * of the COMPRESSION_ALG_*_MAGIC constants; `alg_meta` is interpreted
 * per algorithm (ZSTD uses it as dict_id). */
void compressionHeaderEncode(unsigned char *dst,
                             uint32_t alg_magic,
                             uint32_t alg_meta,
                             uint32_t uncompressed_len,
                             uint32_t compressed_len);

/* Reads a header from `src` and validates that alg_magic is one of the
 * supported algorithms. `out` MUST be non-NULL. Returns 0 on success,
 * -1 if the magic is not recognized (caller treats as corrupt value). */
int compressionHeaderDecode(const unsigned char *src,
                            compressedHeader *out);

/* ========================================================================
 * robj allocation / free helpers
 * ========================================================================
 *
 * Called by the compression main-thread install path (§2.4 R2.4.3) and
 * the complementary free path (driven by `freeStringObject` /
 * `decrRefCount` in object.c once the hot path is wired in Phase 1).
 *
 * The API is type-polymorphic by design: the per-value compressed
 * buffer (header + ZSTD frame) is type-agnostic, and the create/free
 * pair operates the same way regardless of which object type the
 * compressed value came from. v1 only emits compressed values for
 * `OBJ_STRING` (the eligibility predicate enforces this); v2+ may
 * emit compressed values for HASH / ZSET / etc. without API changes
 * to this layer — they only need to extend the eligibility filter.
 */

/* Creates an robj with `type` and encoding=OBJ_ENCODING_COMPRESSED that
 * TAKES OWNERSHIP of `buffer`.
 *
 * `type` is one of OBJ_STRING / OBJ_HASH / etc. v1 only ever passes
 * OBJ_STRING; future types extend without changing this signature.
 *
 * Contract (zero-copy by construction):
 *   - `buffer` MUST be non-NULL and MUST have been allocated with
 *     zmalloc (or zrealloc'd down from a zmalloc'd allocation).
 *     `freeCompressedObject` will eventually reclaim it via `zfree`.
 *   - `buffer` MUST start with a valid `compressedHeader` followed by
 *     `compressed_len` bytes of compressed frame payload. Producers
 *     (compression workers) write the header + frame directly into the
 *     buffer before handing it to this function.
 *   - `buffer_len` MUST equal `sizeof(compressedHeader) + compressed_len`.
 *   - After this call returns, the caller MUST NOT use, free, or
 *     mutate `buffer`. Ownership transfers to the returned robj.
 *
 * Contract violations (NULL buffer, undersized buffer, size mismatch
 * vs. header) are treated as programmer errors and raise serverAssert.
 * The convention is documented under
 *   .agents/planning/realtime-data-compression/research/error-handling-conventions.md
 *
 * Returns NULL only when the header's `alg_magic` is unrecognized — the
 * one input byte that can legitimately be corrupt (e.g. when called
 * from an RDB load path with disk corruption). On NULL return the
 * caller retains ownership of `buffer` and must reclaim it via zfree;
 * RDB-load callers additionally invoke rdbReportCorruptRDB(). */
robj *createCompressedObject(int type, void *buffer, size_t buffer_len);

/* Frees the compressed buffer owned by a robj with
 * encoding=OBJ_ENCODING_COMPRESSED. Called from freeStringObject (and,
 * in v2, from analogous free helpers for other types). The free path
 * is type-agnostic — it operates on the compressed buffer's header,
 * not on the source object's type — so this single function services
 * every type that uses OBJ_ENCODING_COMPRESSED. The caller guarantees
 * `o->encoding == OBJ_ENCODING_COMPRESSED`. */
void freeCompressedObject(robj *o);

#endif /* __COMPRESSION_HEADER_H */
