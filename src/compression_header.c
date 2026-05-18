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
 *     and matching teardown for OBJ_ENCODING_COMPRESSED robjs.
 *
 * Concurrency: all entry points run on the main thread. Workers produce
 * a flat buffer (header + frame) and hand it to createCompressedObject;
 * the worker side never touches a robj (R2.11.4).
 */

#include "server.h"
#include "compression_header.h"
#include "compression_registry.h"

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
    compressedHeader h;
    memcpy(&h, src, sizeof(h));
    if (!compressionAlgMagicRecognized(h.alg_magic)) return -1;
    if (out) *out = h;
    return 0;
}

robj *createCompressedObject(void *buffer, size_t buffer_len) {
    if (buffer == NULL) return NULL;
    if (buffer_len < COMPRESSION_HEADER_SIZE) return NULL;

    compressedHeader h;
    if (compressionHeaderDecode((const unsigned char *)buffer, &h) != 0) {
        /* Unknown alg_magic — treat as corrupt. Per the contract in
         * compression_header.h, the caller retains ownership of
         * `buffer` and must reclaim it. */
        return NULL;
    }

    /* Validate that the buffer's size matches what the header claims.
     * `compressed_len` is the frame size only; the buffer holds the
     * 16-byte header in front of it. */
    if (buffer_len != (size_t)COMPRESSION_HEADER_SIZE + h.compressed_len) {
        return NULL;
    }

    /* Allocate the robj with the buffer as its value pointer. The
     * caller already wrote the header + frame into `buffer`, so
     * ownership transfers without a memcpy. createObject defaults
     * encoding to OBJ_ENCODING_RAW; we override after construction. */
    robj *o = createObject(OBJ_STRING, buffer);
    o->encoding = OBJ_ENCODING_COMPRESSED;

    /* Reference the dictionary that the frame was encoded with so the
     * registry cannot retire it while this frame still exists
     * (R2.3.4). For ZSTD, alg_meta is the dict_id; the
     * COMPRESSION_DICT_ID_NONE sentinel (0) means "no dictionary"
     * and is skipped. Other algorithms (none in v1) may use alg_meta
     * for non-dictionary metadata; we deliberately gate the IncRef
     * on the algorithm. */
    if (h.alg_magic == COMPRESSION_ALG_ZSTD_MAGIC &&
        h.alg_meta != COMPRESSION_DICT_ID_NONE) {
        compressionRegistryIncRef(h.alg_meta);
    }

    return o;
}

void freeCompressedObject(robj *o) {
    if (o == NULL) return;
    if (o->encoding != OBJ_ENCODING_COMPRESSED) return;

    void *buffer = objectGetVal(o);
    if (buffer == NULL) return;

    /* Drop the dictionary reference taken in createCompressedObject.
     * If the buffer's header is corrupt for any reason we still
     * release the storage; we just cannot identify which dict_id to
     * DecRef. This path is unreachable in normal operation because
     * the header is validated on install. */
    compressedHeader h;
    if (compressionHeaderDecode((const unsigned char *)buffer, &h) == 0) {
        if (h.alg_magic == COMPRESSION_ALG_ZSTD_MAGIC &&
            h.alg_meta != COMPRESSION_DICT_ID_NONE) {
            compressionRegistryDecRef(h.alg_meta);
        }
    }

    zfree(buffer);
}
