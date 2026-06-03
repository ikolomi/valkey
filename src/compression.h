/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef __COMPRESSION_H
#define __COMPRESSION_H

/*
 * Real-time in-memory value compression for Valkey — public API.
 *
 * Design of record:
 *   .agents/planning/realtime-data-compression/design/detailed-design.md §4.3
 *   .agents/planning/realtime-data-compression/implementation/plan.md §4.1
 *
 * Scope reminder (v1):
 *   - OBJ_STRING values only, RAW encoding, size in [min..max].
 *   - Background compression on a dedicated worker pool.
 *   - Synchronous decompression on the main thread.
 *   - One trained ZSTD dictionary "active"; zero or more "retiring" for
 *     existing frames. Lifecycle in compression_registry.h.
 *
 * Phase 0 status: every function below is a feature-disabled stub. Real
 * implementations land in Phase 1 (dictionary lifecycle + compression
 * hot path).
 */

#include "server.h"

/* ========================================================================
 * Lifecycle
 * ======================================================================== */

/* Called once at startup from InitServerLast(), after bio + io_threads. */
void compressionInit(void);

/* Called from serverCron. Sweep pacing, drift-retrain triggers, etc. */
void compressionCron(void);

/* Called from the event-loop afterSleep hook. Drains the worker outbox. */
void compressionAfterSleep(void);

/* Called once from finishShutdown(). Stops the worker pool (joins all
 * worker threads), then releases the dictionary registry. The order
 * matters — workers must be joined before the registry is freed; this
 * function preserves that contract internally. */
void compressionShutdown(void);

/* ========================================================================
 * Master-switch toggle
 * ========================================================================
 *
 * Called by the config apply hook for `compression-enabled` and by the
 * `COMPRESSION ENABLE`/`DISABLE` convenience subcommands (§2.1 R2.1.2).
 *
 * Returns 1 on success, 0 on error (with *err set to an sds caller must
 * sdsfree()). The caller owns the sds; NULL if no error.
 */
int compressionToggle(int enabled, sds *err);

/* ========================================================================
 * Hot path — read
 * ========================================================================
 *
 * Single entry point for every client-facing read of a compressed value
 * (§2.5 R2.5.2). Always synchronous on the main thread (R2.5.1). Never
 * calls signalModifiedKey (§2.9 R2.9.2).
 *
 *   - If `o->encoding != OBJ_ENCODING_COMPRESSED`, returns `o` unchanged
 *     and does not touch `*scratch` or `*view_out`. Zero cost — typical
 *     uncompressed-value path pays only one branch.
 *
 *   - Otherwise decompresses into `*scratch` (caller-owned sds buffer;
 *     may be NULL on first call — the helper allocates and grows as
 *     needed via sds APIs), populates `*view_out` to wrap the
 *     decompressed bytes (refcount = OBJ_STATIC_REFCOUNT — caller MUST
 *     NOT decrRefCount), and returns `view_out`.
 *
 *   - Returns NULL on any decompression failure (corrupt header, missing
 *     dict, ZSTD error, OOM). Logs at LL_WARNING (rate-limited per §6.1)
 *     and increments `compression_errors_total`. Caller is responsible
 *     for translating NULL into a client-facing error reply (the helper
 *     deliberately doesn't know about clients — see §6.2). Callers MUST
 *     still sdsfree(*scratch) on the NULL path; it may have been grown.
 *
 * The view_out parameter (vs returning a heap-allocated robj) keeps every
 * read of every compressed value off the allocator hot path. Typical use:
 *
 *     robj  view;
 *     sds   scratch = NULL;
 *     robj *u = objectGetUncompressedView(o, &scratch, &view);
 *     if (u == NULL) { addReplyError(c, "..."); sdsfree(scratch); return; }
 *     addReplyBulk(c, u);
 *     if (u != o) sdsfree(scratch);
 *
 * Both `scratch` and `view_out` MUST be non-NULL pointers. Helper does
 * not free either — caller's lifetime owns both. Stack-allocating
 * `view_out` is the intended pattern; `OBJ_STATIC_REFCOUNT` matches the
 * existing `initStaticStringObject` convention (see server.h).
 *
 * Concurrency: scratch and view_out are caller-owned, so concurrent
 * callers (e.g. multiple commands in a pipeline burst) each pass their
 * own. The helper itself uses a file-static main-thread DCtx; that is
 * safe because v1 decompression is sync-on-main-thread (R2.5.1).
 */
robj *objectGetUncompressedView(robj *o, sds *scratch, robj *view_out);

/* ========================================================================
 * Hot path — write / eligibility
 * ========================================================================
 *
 * Called by dbAddInternal / dbSetValue / dbOverwrite on the write path.
 * Evaluates the R2.2 eligibility predicate; eligible values get queued
 * onto the worker inbox for background compression.
 *
 * Runs on the main thread. Cheap; designed to be inlined at the call site
 * once the Phase 1 eligibility predicate lands.
 *
 * The signature takes `robj *` (not `const robj *`) because the predicate's
 * LFU branch reads the freq counter via `lfu_getFrequency()`, which decays
 * the counter in place — matching the standard Valkey "decay-on-read"
 * pattern (see objectGetIdleness in src/object.c). For LRU/noeviction
 * modes there is no mutation.
 */
int compressionIsEligible(robj *o);
void compressionEnqueueCandidate(const sds key, robj *o);

/* ========================================================================
 * COMPRESSION command — subcommand dispatch
 * ========================================================================
 *
 * The single command handler. Reads c->argv[1] to dispatch to STATUS /
 * DICT LIST / DICT DROP / SWEEP / TRAIN / ENABLE / DISABLE / DEBUG / HELP.
 *
 * Subcommand JSON metadata lives under src/commands/compression-*.json;
 * `utils/generate-command-code.py` regenerates src/commands.def.
 */
void compressionCommand(client *c);

/* Individual subcommand entry points, exposed for unit tests. */
int compressionStatus(client *c);
int compressionForceTrain(client *c);
int compressionSweep(client *c, int direction /* 1 = compress, -1 = decompress */);
int compressionDictList(client *c);
int compressionDictExport(client *c, uint32_t dict_id);
int compressionDictImport(client *c, const unsigned char *bytes, size_t len);
int compressionDictDrop(client *c, uint32_t dict_id);

/* ========================================================================
 * INFO
 * ======================================================================== */

/* Appends an `# Compression` section to `info`. See §2.10 R2.10.1 for
 * the authoritative field list. */
void infoCompression(sds *info);

#endif /* __COMPRESSION_H */
