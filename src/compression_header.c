/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression_header.c — per-value header codec + robj alloc/free helpers.
 *
 * Phase 1 / S2.1 (plan §5):
 *   - compressionHeaderEncode / Decode: native-byte-order codec for the
 *     four-uint32 header described in detailed-design.md §5.2.
 *   - createCompressedObject / freeCompressedObject: zero-copy install
 *     and matching teardown for OBJ_ENCODING_COMPRESSED robjs. Type-
 *     polymorphic: createCompressedObject takes the object type as a
 *     parameter; freeCompressedObject is type-agnostic because the
 *     compressed buffer's layout does not depend on the source type.
 *
 * Concurrency: all entry points run on the main thread. Workers produce
 * a flat buffer (header + frame) and hand it to createCompressedObject;
 * the worker side never touches a robj (R2.11.4).
 */

#include "server.h"
#include "compression.h"
#include "compression_header.h"
#include "compression_registry.h"
#include "serverassert.h"

#include <string.h>

/* Returns 1 if `alg_magic` is a known algorithm tag. v1 only accepts
 * the ZSTD magic; additional entries can be added without changing
 * the on-disk/in-memory layout. */
static int compressionAlgMagicRecognized(uint32_t alg_magic) {
    switch (alg_magic) {
    case COMPRESSION_ALG_ZSTD_MAGIC:
        return 1;
    default:
        return 0;
    }
}

void compressionHeaderEncode(unsigned char *dst,
                             uint32_t alg_magic,
                             uint32_t alg_meta,
                             uint32_t uncompressed_len,
                             uint32_t compressed_len) {
    compressedHeader h = {
        .alg_magic = alg_magic,
        .alg_meta = alg_meta,
        .uncompressed_len = uncompressed_len,
        .compressed_len = compressed_len,
    };
    memcpy(dst, &h, sizeof(h));
}

int compressionHeaderDecode(const unsigned char *src, compressedHeader *out) {
    serverAssert(out != NULL);
    memcpy(out, src, sizeof(*out));
    if (!compressionAlgMagicRecognized(out->alg_magic)) return -1;
    return 0;
}

robj *createCompressedObject(int type, void *buffer, size_t buffer_len) {
    /* Most input checks here are programmer-error guards, not corruption
     * checks. The convention is documented in
     *   .agents/planning/realtime-data-compression/research/error-handling-conventions.md
     *
     * - `buffer != NULL`: callers (worker outbox, RDB load) never pass
     *   NULL legitimately. Bug.
     * - `buffer_len >= COMPRESSION_HEADER_SIZE`: callers allocate the
     *   buffer as HEADER + compressed_len. Smaller is impossible by
     *   construction. Bug.
     * - `buffer_len == HEADER + h.compressed_len`: same — the caller
     *   wrote the header value matching the allocation. Mismatch is
     *   impossible by construction. Bug.
     *
     * Only the alg_magic check is true corruption handling: bytes from
     * disk can present any 4-byte sequence, so we return NULL and let
     * the caller (typically RDB load) report via rdbReportCorruptRDB. */
    serverAssert(buffer != NULL);
    serverAssert(buffer_len >= COMPRESSION_HEADER_SIZE);

    compressedHeader h;
    if (compressionHeaderDecode((const unsigned char *)buffer, &h) != 0) {
        /* Unknown alg_magic — treat as corruption. Per the contract in
         * compression_header.h, the caller retains ownership of
         * `buffer` and must reclaim it. */
        return NULL;
    }

    serverAssert(buffer_len == (size_t)COMPRESSION_HEADER_SIZE + h.compressed_len);

    /* Allocate the robj with the buffer as its value pointer. The
     * caller already wrote the header + frame into `buffer`, so
     * ownership transfers without a memcpy. createObject defaults
     * encoding to OBJ_ENCODING_RAW; we override after construction. */
    robj *o = createObject(type, buffer);
    o->encoding = OBJ_ENCODING_COMPRESSED;

    /* Reference the dictionary that the frame was encoded with so the
     * registry cannot retire it while this frame still exists
     * (R2.3.4 — frame_refs gates GC under the QSBR model in §4.4).
     * For ZSTD, alg_meta is the dict_id; the COMPRESSION_DICT_ID_NONE
     * sentinel (0) means "no dictionary" and is skipped. Other
     * algorithms (none in v1) may use alg_meta for non-dictionary
     * metadata; we deliberately gate the IncRef on the algorithm. */
    if (h.alg_magic == COMPRESSION_ALG_ZSTD_MAGIC &&
        h.alg_meta != COMPRESSION_DICT_ID_NONE) {
        compressionRegistryIncRef(h.alg_meta);
    }

    /* Account the install in the design §5.6 counters: total
     * uncompressed payload bytes and total on-heap compressed bytes
     * (frame + 16-byte header). Atomic because the matching free path
     * can run on bio (lazyfree). The savings counter that the
     * transient-view memory cap (R2.5.7) consults is derived from
     * these two via compressionGetSavingsBytes(); we deliberately
     * track both totals rather than just savings so S4.1 can surface
     * them under their canonical INFO field names. */
    compressionAccountInstall((int64_t)h.uncompressed_len,
                              (int64_t)h.compressed_len + COMPRESSION_HEADER_SIZE);

    return o;
}

void freeCompressedObject(robj *o) {
    /* Caller (freeStringObject in object.c, and analogous free helpers
     * for other types in v2) guarantees encoding == OBJ_ENCODING_COMPRESSED.
     * The buffer pointer is the robj's value; the header was validated
     * on install in createCompressedObject, so a corrupt header here
     * would mean memory corruption — fail loud via serverAssert rather
     * than leaking the dictionary frame_ref. */
    void *buffer = objectGetVal(o);
    compressedHeader h;
    int rc = compressionHeaderDecode((const unsigned char *)buffer, &h);
    serverAssert(rc == 0);

    if (h.alg_magic == COMPRESSION_ALG_ZSTD_MAGIC &&
        h.alg_meta != COMPRESSION_DICT_ID_NONE) {
        compressionRegistryDecRef(h.alg_meta);
    }

    /* Reverse the install accounting. May run on a bio thread
     * (lazyfree); compressionAccountInstall is atomic. */
    compressionAccountInstall(-(int64_t)h.uncompressed_len,
                              -((int64_t)h.compressed_len + COMPRESSION_HEADER_SIZE));

    zfree(buffer);
}
