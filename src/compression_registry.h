/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef __COMPRESSION_REGISTRY_H
#define __COMPRESSION_REGISTRY_H

/*
 * Dictionary registry — main-thread writer, hot-path + RDB loader readers.
 *
 * Design of record:
 *   .agents/planning/realtime-data-compression/design/detailed-design.md §4.4
 *   .agents/planning/realtime-data-compression/implementation/plan.md §4.2
 *
 * Invariants (see §2.3 of the design):
 *   - Single-writer on the main thread. Atomic pointer swap on promote.
 *   - At most one dict is "active" (used for new compressions).
 *   - Zero or more "retiring" dicts are decompress-only; they outlive the
 *     frames that reference them via refcounting (R2.3.4).
 *   - Retired dicts are freed when refcount hits zero.
 *   - Registry capped at `compression-dict-max-versions` entries (R2.3.3).
 *
 * Opaque ZSTD types: we forward-declare here so this header does not
 * require <zstd.h>. The actual zstd type definitions are introduced in
 * compression_registry.c, gated by USE_ZSTD.
 */

#include "server.h"

#include <stddef.h>
#include <stdint.h>

/* Forward declarations of ZSTD handle types. Matches upstream ZSTD's own
 * typedef shape (zstd.h uses `typedef struct ZSTD_CDict_s ZSTD_CDict;`). */
typedef struct ZSTD_CDict_s ZSTD_CDict;
typedef struct ZSTD_DDict_s ZSTD_DDict;

/* Registry cap (R2.3.3). Actual effective cap is
 * `compression-dict-max-versions`, bounded by this compile-time max. */
#define COMPRESSION_DICT_MAX 16

/* Sentinel dict_id meaning "no dictionary". */
#define COMPRESSION_DICT_ID_NONE 0u

typedef enum compressionDictState {
    COMPRESSION_DICT_STATE_ACTIVE = 0, /* current dict for new compressions */
    COMPRESSION_DICT_STATE_RETIRING,   /* decompress-only, CDict may be NULL */
    COMPRESSION_DICT_STATE_RETIRED,    /* scheduled for free; refcount == 0 */
} compressionDictState;

/*
 * A dictionary + its digested handles. Named "pair" because it bundles
 * CDict and DDict, which the registry treats as one lifecycle unit.
 *
 * TODO: discuss with @ikolomi whether to encapsulate this struct behind
 * accessor functions (getDictId, getCDict, etc.) for consistency with
 * the registry being private. Follow-up PR.
 *
 * Field conventions:
 *   - dict_id: monotonically-increasing, never reused. 0 = no-dict.
 *   - bytes / bytes_len: the raw dictionary bytes (persisted to RDB AUX
 *     entries, see §2.6 R2.6.1).
 *   - cdict: immutable after publication; workers read it without locks.
 *   - ddict: used by main thread for decompression.
 *   - frame_refs: number of installed compressed frames referencing this
 *     dict (main-thread only, replaces the old atomic refcount).
 *   - retire_worker_gen: per-worker quiescent-gen snapshot taken at
 *     retirement time (QSBR — see design §4.4).
 */
typedef struct compressionDictPair {
    uint32_t dict_id;
    unsigned char *bytes;
    size_t bytes_len;
    ZSTD_CDict *cdict;
    ZSTD_DDict *ddict;
    size_t frame_refs;
    compressionDictState state;
    mstime_t promoted_at_ms;
    uint64_t retire_worker_gen[COMPRESSION_DICT_MAX];
} compressionDictPair;

/* ========================================================================
 * Lifecycle — all operations run on the main thread.
 * ======================================================================== */

void compressionRegistryInit(void);
void compressionRegistryRelease(void);

/* Returns the currently-active dict, or NULL if no active dict exists.
 * The returned pointer is stable for the duration of the current
 * single-threaded section; callers that hand off to a worker MUST take
 * a refcount via compressionRegistryIncRef. */
compressionDictPair *compressionRegistryActive(void);

/* Returns the dict entry for `dict_id`, or NULL if not found. Used by the
 * decompression helper (compression.h) and by the RDB loader. */
compressionDictPair *compressionRegistryLookup(uint32_t dict_id);

/* Adds a new dict to the registry. If promote=1, publishes as active
 * and retires the previous active. If promote=0, adds as RETIRING
 * (decompress-only, for RDB load). Returns the newly-assigned dict_id
 * (0 on failure — cap reached per R2.3.3). Takes ownership of `p`. */
uint32_t compressionRegistryAdd(compressionDictPair *p, int promote);

/* Marks the dict as RETIRING. A dict is freed only when its refcount
 * reaches zero, either through frame rewrites or via
 * `COMPRESSION SWEEP direction=decompress`. Returns 0 on success, -1 if
 * `dict_id` is unknown. */
int compressionRegistryRetire(uint32_t dict_id);

/* Reference counting. Callers MUST pair inc/dec calls with care —
 * compressed frames hold an implicit reference from encode time to
 * free time. */
void compressionRegistryIncRef(uint32_t dict_id);
void compressionRegistryDecRef(uint32_t dict_id);

/* Iterator. Callback is invoked once per non-retired entry. Used by
 * `COMPRESSION DICT LIST` and by the RDB writer to emit AUX
 * entries. Must not modify the registry from inside the callback. */
void compressionRegistryForEach(void (*cb)(const compressionDictPair *, void *), void *ctx);

/* ========================================================================
 * QSBR grace-period GC
 * ======================================================================== */

/* Scan the retiring list, free dicts that are safe to reclaim.
 * Called from compressionCron, after result drain, after SWEEP. */
void compressionRegistryTryGc(void);

/* ========================================================================
 * Worker-side QSBR API
 * ======================================================================== */

/* Called by a worker after finishing a job. Advances the worker's
 * generation counter. */
void compressionWorkerReportQuiescent(int worker_id);

/* Returns the current quiescent generation for a worker. */
uint64_t compressionWorkerGetGen(int worker_id);

#endif /* __COMPRESSION_REGISTRY_H */
