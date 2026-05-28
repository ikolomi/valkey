/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef __COMPRESSION_REGISTRY_H
#define __COMPRESSION_REGISTRY_H

/*
 * Dictionary registry — QSBR-based lifecycle management.
 *
 * Design of record:
 *   .agents/planning/realtime-data-compression/design/detailed-design.md §4.4
 *
 * Ownership model:
 *   - Main thread: all registry mutations, frame-ref accounting, GC, free.
 *   - Workers: atomic load of active pointer, report quiescent after use.
 *
 * Dictionary lifetime is protected by QSBR (Quiescent-State-Based
 * Reclamation). Workers load the active dict atomically, use the
 * immutable CDict for compression, then report quiescent. The main
 * thread retires dicts by snapshotting worker generations; a dict is
 * freed only after all workers have advanced past the snapshot AND
 * frame_refs == 0.
 */

#include "server.h"

#include <stddef.h>
#include <stdint.h>

/* Forward declarations of ZSTD handle types. */
typedef struct ZSTD_CDict_s ZSTD_CDict;
typedef struct ZSTD_DDict_s ZSTD_DDict;

/* Registry cap (R2.3.3). Bounded by compile-time max. */
#define COMPRESSION_DICT_MAX 16

/* Max compression worker threads. */
#define COMPRESSION_WORKERS_MAX 16

/* Sentinel dict_id meaning "no dictionary". */
#define COMPRESSION_DICT_ID_NONE 0u

typedef enum compressionDictState {
    DICT_STATE_ACTIVE = 0, /* current dict for new compressions */
    DICT_STATE_RETIRING,   /* decompress-only, pending GC */
    DICT_STATE_RETIRED,    /* safe to free */
} compressionDictState;

typedef struct compressionDictPair {
    uint32_t dict_id;     /* monotonic, never reused; 0 = no-dict */
    unsigned char *bytes; /* raw training output (persisted to RDB AUX) */
    size_t bytes_len;
    ZSTD_CDict *cdict; /* immutable after publication; used by workers */
    ZSTD_DDict *ddict; /* used by main thread for decompression */
    size_t frame_refs; /* installed compressed frames referencing this dict
                          (main-thread only) */
    compressionDictState state;
    mstime_t promoted_at_ms;
    uint64_t retire_worker_gen[COMPRESSION_WORKERS_MAX];
    /* per-worker gen snapshot at retirement time */
} compressionDictPair;

/* ========================================================================
 * Registry lifecycle — main-thread only unless noted.
 * ======================================================================== */

void compressionRegistryInit(void);
void compressionRegistryRelease(void);

/* ========================================================================
 * Dict creation
 * ======================================================================== */

/* Creates a new dict from raw bytes and adds it to the registry.
 * If promote=1: publishes as active, retires previous active.
 * If promote=0: adds as RETIRING (decompress-only, for RDB load).
 * Returns dict_id on success, 0 on failure (cap reached).
 * Takes ownership of bytes. Main-thread only. */
uint32_t compressionRegistryAdd(unsigned char *bytes, size_t len, int promote);

/* ========================================================================
 * Retirement and GC
 * ======================================================================== */

/* Moves a dict from ACTIVE to RETIRING. Snapshots worker generations.
 * Main-thread only. */
void compressionDictStartRetirement(compressionDictPair *dict);

/* Returns 1 if a retiring dict is safe to free (frame_refs == 0 AND
 * all workers advanced past retirement snapshot). Main-thread only. */
int compressionDictCanFree(compressionDictPair *dict);

/* Scan the retiring list, free dicts that are safe to reclaim.
 * Main-thread only. */
void compressionDictTryGc(void);

/* Free a dict and all owned resources. Must only be called when
 * state == DICT_STATE_RETIRED. Main-thread only. */
void compressionDictFree(compressionDictPair *dict);

/* ========================================================================
 * Accessors
 * ======================================================================== */

/* Returns the currently active dict. Thread-safe (atomic load).
 * Workers call this to get the CDict for compression.
 * Returns NULL if no active dict exists. */
compressionDictPair *compressionDictGetActive(void);

/* Find a dict by ID. Returns NULL if not found.
 * Main-thread only. */
compressionDictPair *compressionDictLookup(uint32_t dict_id);

/* ========================================================================
 * Frame reference counting — main-thread only
 * ======================================================================== */

void compressionDictIncrFrameRef(uint32_t dict_id);
void compressionDictDecrFrameRef(uint32_t dict_id);

/* ========================================================================
 * Iterator — main-thread only
 * ======================================================================== */

/* Iterate all non-retired dicts. Callback must not modify the registry. */
void compressionRegistryForEach(void (*cb)(const compressionDictPair *, void *), void *ctx);

/* ========================================================================
 * Worker-side QSBR API
 * ======================================================================== */

/* Called by a worker after finishing a job. Advances the worker's
 * generation counter. */
void compressionWorkerReportQuiescent(int worker_id);

/* Returns the current quiescent generation for a worker.
 * Main-thread only (used in GC and observability). */
uint64_t compressionWorkerGetGen(int worker_id);

#endif /* __COMPRESSION_REGISTRY_H */
