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

/* Called from the event-loop beforeSleep hook. Restores the compressed view
 * for any robjs that were transiently decompressed during the previous
 * iteration's lookupKey* calls (LOOKUP_READ_BYTES). Restoration is a free
 * pointer-swap when the kvstore slot still points at the pinned robj;
 * mutated/overwritten/expired entries are detected via pointer-equality
 * staleness check and discarded. See design §2.5.7 + Appendix E. */
void compressionBeforeSleep(void);

/* Called once from finishShutdown(). Stops the worker pool (joins all
 * worker threads), then releases the dictionary registry. The order
 * matters — workers must be joined before the registry is freed; this
 * function preserves that contract internally. */
void compressionShutdown(void);

/* ========================================================================
 * Master-switch + active-sweeper apply hooks
 * ========================================================================
 *
 * Called by the config layer when `compression-master-switch` /
 * `compression-active-sweeper` is set (R2.1.1 / R2.1.2). The field
 * `server.compression_master_switch` (resp. `..._active_sweeper`) has
 * already been written to the new value; the hook detects the
 * transition relative to its own static prior value and applies side
 * effects (most importantly: auto-retire the active dict on any
 * transition INTO `decompression`, per R2.1.5).
 *
 * Convenience aliases `COMPRESSION ENABLE` / `COMPRESSION DISABLE` are
 * NOT part of v1's surface — operators set the master switch via
 * `CONFIG SET compression-master-switch …` directly (the 3-state enum
 * doesn't map cleanly to enable/disable verbs).
 *
 * Returns 1 on success, 0 on error (with *err set to a static C string
 * the caller does not free).
 */
int applyCompressionMasterSwitch(const char **err);
int applyCompressionActiveSweeper(const char **err);

/* Hook called by applyCompressionThreads (in config.c) after a
 * compression-threads change has been applied. Maintains the warning
 * for the non-functional `master=compression + threads=0` state (R2.1.6). */
void compressionAfterThreadsApplied(void);

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
 * Read path — transient-view model (R2.5.7, Appendix E)
 * ========================================================================
 *
 * `compressionMaterializeTransientView` is called from inside lookupKey()
 * when (a) the caller did NOT pass LOOKUP_NO_BYTES and (b) the value is
 * compressed. It decompresses into a freshly-allocated temp sds, registers
 * the robj in the per-server transient-view side-map (saving the original
 * compressed buffer for restoration), `incrRefCount`s the robj to pin it,
 * replaces `o->val_ptr` with the temp sds, and flips `o->encoding` to
 * `OBJ_ENCODING_RAW`.
 *
 * Returns 0 on success (object is now in transient-view state, encoding
 * is RAW), -1 on failure (decompression error, OOM). On failure the
 * function logs at LL_WARNING and increments compression_errors_total;
 * the object is left in its original COMPRESSED state. The caller (i.e.
 * lookupKey) treats failure as a corruption event — see R6.2.
 *
 * If `o` is already in transient-view state (encoding == RAW because a
 * previous lookup already materialized it), this function is a no-op.
 * In practice lookupKey only calls this when encoding == COMPRESSED, so
 * the guard is belt-and-suspenders.
 *
 * Restoration happens at compressionBeforeSleep(); see §4.2. */
int compressionMaterializeTransientView(robj *o, int dbid);

/* O(1) presence check used by isCopyAvoidPreferred() in networking.c.
 * Returns 1 if `o` is currently in the transient-view side-map (its
 * val_ptr is a temp sds that will be freed at the next beforeSleep);
 * returns 0 otherwise.
 *
 * The bulk-reply zero-copy path captures `bulkStrRef = {.obj, .str =
 * objectGetVal(obj)}` for the IO thread to dereference at write time.
 * Under transient view, .str points to a temp sds that is freed at
 * beforeSleep — a use-after-free if the IO thread runs after restoration.
 * isCopyAvoidPreferred returns 0 when transientViewActive(obj) is true,
 * forcing the memcpy reply path so bytes land in c->reply independent
 * of val_ptr. See design Appendix E.7 for the audit and rationale.
 *
 * This function MUST be cheap (called once per bulk reply on compressed
 * values). The side-map is a pointer-keyed hashtable; lookup is O(1)
 * amortized. When the side-map is empty (the common case — no compressed
 * values touched in this iteration), an early-out short-circuits the
 * hashtable lookup entirely. */
int transientViewActive(const robj *o);

/* Permanently decompresses an OBJ_ENCODING_COMPRESSED string in place.
 * Used by lookupKey() on LOOKUP_WRITE callers — they will mutate the
 * value, so we don't preserve the compressed form across the write.
 *
 *   - decompresses into a fresh sds (via objectGetUncompressedView)
 *   - releases the dict frame-ref (compressionRegistryDecRef per R2.3.4)
 *   - frees the original compressed buffer
 *   - replaces val_ptr with the decompressed sds
 *   - flips encoding to OBJ_ENCODING_RAW
 *   - refcount unchanged (no pin; not registered in any side-map)
 *
 * After permanent decompress, dbUnshareStringValue sees refcount==1 RAW
 * → no COW → mutation in place. The post-mutation value is re-enqueued
 * for compression via compressionEnqueueModified() called from
 * signalModifiedKey at the end of the write command (NOT from
 * dbReplaceValue / dbSetValue, which would never fire for in-place
 * mutations like APPEND/SETRANGE/BITOP). The signalModifiedKey hook is
 * the canonical "logical value at this key changed" signal in Valkey,
 * which is exactly when compression should re-evaluate.
 *
 * Returns 0 on success (encoding is now RAW), -1 on decoder failure
 * (corruption, missing dict). On failure the robj is left in its
 * original COMPRESSED state and the caller (lookupKey) treats it as
 * a corruption-class read error per R6.2.
 *
 * If `o` is already RAW (e.g., a previous lookup in the same iteration
 * permanent-decompressed it), this function is a no-op. */
int compressionPermanentlyDecompress(robj *o);

/* Called from signalModifiedKey() to enqueue the (possibly mutated)
 * value at `key` for background compression. No-op when:
 *   - The feature is disabled.
 *   - The key was deleted (dbFind returns NULL).
 *   - The value is ineligible (e.g., wrong type, recently written —
 *     compressionIsEligible filters internally).
 *
 * Why hook on signalModifiedKey: it's the canonical "the logical value
 * at this key changed" signal in Valkey, called by every byte-mutating
 * command after mutation completes. This is exactly the right moment
 * to re-evaluate compressibility.
 *
 * R2.9.2 invariant: this is signalModifiedKey → compression-enqueue,
 * NOT the reverse. Compression infrastructure (worker drain, this
 * function's caller chain via permanent-decompress, etc.) must NOT
 * call signalModifiedKey. */
void compressionEnqueueModified(serverDb *db, robj *key);

/* ========================================================================
 * Memory-bytes accounting (design §5.6 counters + the transient-view cap)
 * ========================================================================
 *
 * Two counters, per design §5.6:
 *   - compression_total_uncompressed_bytes — sum of uncompressed
 *     payload bytes across all currently-installed compressed frames.
 *   - compression_total_compressed_bytes — sum of on-heap compressed
 *     bytes (16-byte header + ZSTD frame).
 *
 * Updated at the 3 frame-lifecycle transitions, both deltas in one
 * call so the derived savings counter (uncompressed - compressed) is
 * consistent at all observation points:
 *   - createCompressedObject   → += (uncompressed_len, compressed_len + HEADER)
 *   - freeCompressedObject     → -= (uncompressed_len, compressed_len + HEADER)
 *   - compressionPermanentlyDecompress → -= ...
 *
 * The savings counter (uncompressed - compressed) caps the transient-
 * view side-map's uncompressed footprint via R2.5.7. The invariant
 * `transient_view_uncompressed_bytes ≤ savings` ensures peak memory
 * never exceeds the no-compression baseline — independent of any
 * operator-tunable knob.
 *
 * S4.1 will expose both counters under their canonical INFO field
 * names (`compression_total_uncompressed_bytes`,
 * `compression_total_compressed_bytes`) and derive
 * `compression_net_saved_bytes` from them. PR #23 wires only the
 * counters; the INFO surface comes with S4.1.
 *
 * Atomic because freeCompressedObject can run on a bio thread via
 * lazyfree; the create + permanent-decompress paths are main-thread
 * only. Tests can call with one delta zero (e.g. `compressionAccountInstall(1<<20, 0)`)
 * to bump only the uncompressed side, raising the derived savings
 * budget without faking a real install. */
void compressionAccountInstall(int64_t delta_unc, int64_t delta_comp);
size_t compressionGetTotalUncompressedBytes(void);
size_t compressionGetTotalCompressedBytes(void);
size_t compressionGetSavingsBytes(void); /* derived: unc - comp */
size_t compressionGetTransientViewCappedTotal(void);

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

/* Enqueue an eligible value for background compression. Called from the
 * write-path seams in db.c (dbAddInternal, dbSetValue) AFTER the value
 * has been installed in the kvstore (so it has its embedded key).
 *
 *   - `key` is advisory only (the embedded key in `value` is the
 *     authoritative lookup key at drain time). Kept in the signature
 *     for future v2 non-string types where embedded keys may not apply.
 *   - `value` is the just-installed robj. Refcount is whatever the
 *     installer left it at (typically 1: the kvstore's reference).
 *   - `dbid` is the database index — captured so the drain handler can
 *     re-resolve the kvstore slot.
 *
 * The function is a no-op when:
 *   - The value is not eligible (R2.2 predicate).
 *   - No active dictionary exists (R2.1.5 — encoder would skip anyway,
 *     but checking here saves the allocator round-trip).
 *
 * On enqueue the function bumps `incrRefCount(value)` to pin the bytes
 * (R2.4.4 immutable-snapshot invariant + ABA safety for the drain
 * handler's pointer-equality stale check). The drain handler releases
 * the pin via `decrRefCount` after install or discard.
 *
 * If the worker pool refuses the job (pool not started or, future,
 * inbox full per S2.11), the pin is released immediately. */
void compressionEnqueueCandidate(robj *key, robj *value, int dbid);

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
