/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

/*
 * compression.c — Phase 0 stub.
 *
 * All public entry points currently return feature-disabled defaults.
 * Real implementations land in Phase 1 (see plan.md §5).
 *
 * Order of operations for future work:
 *   - compressionInit wires worker pool + registry + training hooks
 *   - objectGetUncompressedView is the hot-path decompress seam
 *   - compressionEnqueueCandidate wires into dbAdd/dbSetValue
 *   - compressionCron runs the sweep tick and drift-retrain trigger
 *   - compressionAfterSleep drains the worker outbox
 *
 * DO NOT call any ZSTD API from this file directly until BUILD_ZSTD
 * linkage lands (see plan §6 milestone M0 exit criteria).
 */

#include "server.h"
#include "compression.h"
#include "compression_header.h"
#include "compression_registry.h"
#include "compression_sweep.h"
#include "compression_workers.h"
#include "compression_train.h"
#include "lrulfu.h"

#include <stdatomic.h>

#ifdef USE_ZSTD
#include "zstd.h"
#endif

/* ========================================================================
 * Decompression — main-thread DCtx
 * ========================================================================
 *
 * One ZSTD_DCtx for the whole server, lazily allocated on first
 * decompression and freed in compressionShutdown. ZSTD_DCtx is not
 * thread-safe, but v1 decompression is synchronous on the main thread
 * (R2.5.1), so a single instance is sufficient. Reusing the DCtx
 * across calls amortizes the (small) per-context setup cost — same
 * pattern the per-worker CCtx uses on the encoder side.
 *
 * Lifecycle invariant for compressionShutdown ordering: the DCtx is
 * freed before compressionRegistryRelease so any final decompression
 * in shutdown sequence (none today, but future RDB save / DUMP paths
 * may call) can still see a valid context.
 */

#ifdef USE_ZSTD
static ZSTD_DCtx *server_dctx;

/* Lazy accessor. Returns the singleton DCtx, allocating on first use.
 * Returns NULL on allocation failure (rare; logged by caller). */
static ZSTD_DCtx *compressionGetDCtx(void) {
    if (server_dctx == NULL) {
        server_dctx = ZSTD_createDCtx();
    }
    return server_dctx;
}
#endif

/* ========================================================================
 * Read path — transient-view side-map
 * ========================================================================
 *
 * Implements R2.5.7 + Appendix E. When a caller invokes lookupKey* without
 * LOOKUP_NO_BYTES on a compressed value, we decompress into a temp sds,
 * remember the original compressed buffer here, pin the robj, and flip
 * encoding to RAW for the duration of the event-loop iteration. At the
 * next compressionBeforeSleep() boundary we restore — by pointer-swap if
 * the kvstore slot still holds our pinned robj, by discard otherwise.
 *
 * Pointer-equality stale check (the same primitive as the write-path
 * drain — see PR #19) detects mutation/overwrite/expiry without any
 * mutation-time hook in the keyspace mutation paths. The pin guarantees
 * ABA safety: while we hold a refcount, the allocator cannot reclaim
 * the robj's address for a future zmalloc — so a "same pointer at the
 * slot" outcome at restore time is unambiguous.
 *
 * Memory bound (savings-based cap, R2.5.7): a naive transient view lets
 * compressed and uncompressed bytes coexist for every touched key for
 * the duration of an event-loop iteration. Peak inflation could reach
 * `(1 + ratio)` × the no-compression baseline (e.g. 10000 × 128 KiB
 * compressed values touched in one iteration → ~1.5 GB OOM headroom
 * needed even though the dataset is normally compressed).
 *
 * To keep the feature from making memory WORSE than the no-compression
 * baseline, materialize is capped against the running compression
 * savings: `transient_view_uncompressed_bytes ≤ savings` where
 * `savings = compression_total_uncompressed_bytes -
 * compression_total_compressed_bytes` across all currently-installed
 * compressed frames. The two underlying counters are per design §5.6
 * (S4.1 surfaces them via `INFO compression`); the savings used here
 * is derived on demand via compressionGetSavingsBytes(). When the next
 * materialize would exceed the cap, the path falls back to
 * compressionPermanentlyDecompress
 * for that value (releasing the compressed form and freeing memory the
 * side-map would otherwise have held). The cap is a derived signal —
 * no operator-tunable knob — so its semantic ("transient memory ≤
 * what compression saved") is correct by construction.
 *
 * Cap exhaustion is observable via compression_transient_view_capped_total
 * (S4.1 surfaces it through INFO compression).
 *
 * Side-map structure: hashtable.h primitive keyed by `robj *`. Default
 * pointer-bits hash and pointer-equality compare are exactly right for
 * an integer-cast pointer key — no custom hash/compare needed. No
 * entryDestructor either: the restore branch needs to do different
 * cleanup from the discard branch (restore preserves the compressed
 * buffer; discard frees it), and a single destructor cannot distinguish
 * them. Both branches free the entry struct via the
 * discardTransientEntry helper or inline, then bulk-clear the bucket
 * pointers via hashtableEmpty(map, NULL).
 */

typedef struct compressionTransientEntry {
    robj *obj;               /* hashtable key (default: pointer-bits hash) */
    void *compressed_buffer; /* saved original val_ptr (the zmalloc'd compressed frame) */
    int dbid;                /* for kvstore re-fetch on restore */
} compressionTransientEntry;

static const void *transientEntryGetKey(const void *entry) {
    const compressionTransientEntry *e = entry;
    return e->obj;
}

static hashtableType transientViewMapType = {
    .entryGetKey = transientEntryGetKey,
    /* hashFunction NULL → default pointer-bits hash. */
    /* keyCompare NULL → default pointer-equality compare. */
    /* entryDestructor NULL → explicit cleanup in the restore loop. */
};

/* The side-map. Lazily allocated on first use (the first time a
 * compressed value is read with LOOKUP_NO_BYTES not set). NULL until
 * then; freed in compressionShutdown. The lazy allocation keeps the
 * "master=off, no compressed values exist" state at zero cost, and
 * keeps `transientViewActive()` cheap when the map is empty: an early
 * NULL check short-circuits the hashtable lookup entirely. */
static hashtable *transient_view_map = NULL;

/* Total uncompressed bytes accounted for in the side-map currently.
 * Reset to 0 at the end of compressionBeforeSleep when the side-map
 * is fully drained. The cap check in compressionMaterializeTransientView
 * uses this against the savings derived from the design-spec counters
 * (`compression_total_uncompressed_bytes - compression_total_compressed_bytes`)
 * to decide whether to fall back to permanent decompress. */
static size_t transient_view_uncompressed_bytes = 0;

/* Design §5.6 counters: total uncompressed payload bytes and total
 * on-heap compressed bytes (header + frame) across all currently-
 * installed compressed frames. Atomic because freeCompressedObject
 * can run on a bio thread (lazyfree); the create / permanent-
 * decompress paths run on the main thread.
 *
 * S4.1 will surface these via `INFO compression` as
 * `compression_total_uncompressed_bytes` and
 * `compression_total_compressed_bytes`. They also serve the
 * savings-based cap (R2.5.7): savings = uncompressed - compressed,
 * derived on demand without a third dedicated counter.
 *
 * Initial value is 0 (no compressed frames at startup). They rise
 * together as the worker drain installs compressed values and fall
 * together when values are freed or permanent-decompressed. Both
 * invariants are non-negative; a debug-build assert in the accounting
 * function guards against drift. */
static _Atomic(size_t) compression_total_uncompressed_bytes = 0;
static _Atomic(size_t) compression_total_compressed_bytes = 0;

/* Cumulative count of compressionMaterializeTransientView calls that
 * fell back to permanent decompress because the cap was hit. Climbing
 * counter signals "your workload is touching too many compressed keys
 * within a single event-loop iteration; consider why."
 *
 * Not atomic: incremented only inside compressionMaterializeTransientView
 * (main-thread only) and read only via compressionGetTransientViewCappedTotal
 * (called from S4.1's infoCompression on the main thread). No cross-thread
 * access. */
static size_t compression_transient_view_capped_total = 0;

/* Account a compressed-object install / free / permanent-decompress.
 * `delta_unc` is the uncompressed-payload delta (positive on install,
 * negative on free / permanent-decompress); `delta_comp` is the
 * on-heap compressed delta (HEADER + frame bytes), same sign.
 *
 * Both updates are atomic. Production callers always pass both deltas
 * in the same call so that the derived savings counter
 * (uncompressed - compressed) is consistent at all observation points.
 * Tests can pass `delta_comp == 0` to bump only one side (e.g. raise
 * the savings budget for a transient-view test without faking a
 * compressed install). */
void compressionAccountInstall(int64_t delta_unc, int64_t delta_comp) {
    if (delta_unc != 0) {
        if (delta_unc > 0) {
            atomic_fetch_add_explicit(&compression_total_uncompressed_bytes,
                                      (size_t)delta_unc, memory_order_relaxed);
        } else {
            size_t magnitude = (size_t)(-delta_unc);
            size_t before = atomic_fetch_sub_explicit(&compression_total_uncompressed_bytes,
                                                      magnitude, memory_order_relaxed);
            serverAssert(before >= magnitude);
        }
    }
    if (delta_comp != 0) {
        if (delta_comp > 0) {
            atomic_fetch_add_explicit(&compression_total_compressed_bytes,
                                      (size_t)delta_comp, memory_order_relaxed);
        } else {
            size_t magnitude = (size_t)(-delta_comp);
            size_t before = atomic_fetch_sub_explicit(&compression_total_compressed_bytes,
                                                      magnitude, memory_order_relaxed);
            serverAssert(before >= magnitude);
        }
    }
}

size_t compressionGetTotalUncompressedBytes(void) {
    return atomic_load_explicit(&compression_total_uncompressed_bytes,
                                memory_order_relaxed);
}

size_t compressionGetTotalCompressedBytes(void) {
    return atomic_load_explicit(&compression_total_compressed_bytes,
                                memory_order_relaxed);
}

/* Derived savings: uncompressed - compressed across all currently-
 * installed compressed frames. Used by the materialize-cap check; S4.1
 * will also use it directly for the `compression_net_saved_bytes` INFO
 * field (= savings - fixed_overhead). Two atomic loads; cheap. The two
 * counters are updated atomically per object install/free/permanent-
 * decompress, so a transient view at install/free time can briefly see
 * a not-yet-symmetric (unc, comp) pair; the derived savings can never
 * go negative because we always increment uncompressed first on install
 * and decrement compressed first on free. */
size_t compressionGetSavingsBytes(void) {
    size_t unc = compressionGetTotalUncompressedBytes();
    size_t comp = compressionGetTotalCompressedBytes();
    return (unc > comp) ? (unc - comp) : 0;
}

size_t compressionGetTransientViewCappedTotal(void) {
    return compression_transient_view_capped_total;
}

static inline void transientViewMapEnsure(void) {
    if (transient_view_map == NULL) {
        transient_view_map = hashtableCreate(&transientViewMapType);
    }
}

/* Release ownership of a compressed buffer: decode the per-value
 * header to extract the dictID and byte counts, decrement the dict
 * frame-ref (R2.3.4), reverse the compression accounting (R2.10.1's
 * compression_total_*_bytes counters), and free the underlying
 * memory.
 *
 * Used by every code path that disposes of a compressed buffer
 * outside `freeStringObject` (which handles the steady-state
 * decRef→free path). Specifically:
 *   - `compressionPermanentlyDecompress` after installing the
 *     decompressed sds and flipping encoding to RAW (the robj is
 *     not freed here, so we can't defer to freeStringObject).
 *   - `discardTransientEntry` orphan-branch (the original robj was
 *     overwritten/expired/COW'd while the compressed bytes were
 *     squirreled away in the side-map; freeStringObject on the
 *     dropped robj sees encoding=RAW and never gets to decRef the
 *     dict for us — without this helper, the dict frame-ref leaked
 *     and `compression_total_*_bytes` drifted whenever the side-map
 *     discarded an orphaned entry).
 *
 * Caller is responsible for ensuring the buffer is no longer
 * referenced by any robj's `val_ptr` before calling — this function
 * does not touch any robj.
 *
 * Header-decode failure is treated as memory corruption (serverPanic).
 * The buffer that reaches this function was written by
 * `createCompressedObject` (or RDB load through the same path) and
 * validated at install. A header that decodes correctly at install
 * but fails to decode at release time means heap corruption or
 * pointer-aliasing. Silently logging + leaking the buffer would stack
 * dict frame-refs (the dict cannot retire because frame_refs cannot
 * reach 0) until `compression-dict-max-versions` is hit and training
 * freezes — a subtle and delayed operational fault. Panicking here
 * surfaces the memory-safety bug at its actual origin.
 *
 * Note that `compressionHeaderDecode` itself remains an agnostic
 * primitive (returns -1 on bad header) because RDB load needs to
 * reject corrupt files via `rdbReportCorruptRDB` rather than crash
 * the server. The "panic" decision lives at internal-only call sites
 * like this one and `freeCompressedObject`. */
static void releaseCompressedBuffer(void *compressed_buffer) {
    if (compressed_buffer == NULL) return;
    compressedHeader hdr;
    if (compressionHeaderDecode((const unsigned char *)compressed_buffer, &hdr) != 0) {
        serverPanic("Compression: header-decode failure on "
                    "releaseCompressedBuffer indicates heap corruption "
                    "or buffer misuse");
    }
    if (hdr.alg_magic == COMPRESSION_ALG_ZSTD_MAGIC &&
        hdr.alg_meta != COMPRESSION_DICT_ID_NONE) {
        compressionRegistryDecRef(hdr.alg_meta);
    }
    /* Reverse the install-time accounting (R2.10.1; see the
     * createCompressedObject path's
     * compressionAccountInstall(+unc, +comp+HEADER) call in
     * compression_header.c). */
    compressionAccountInstall(-(int64_t)hdr.uncompressed_len,
                              -((int64_t)hdr.compressed_len + COMPRESSION_HEADER_SIZE));
    zfree(compressed_buffer);
}

/* Discard a single transient-view entry: free the temp uncompressed
 * sds, release the saved compressed buffer (decRef dict + reverse
 * accounting via releaseCompressedBuffer — without this the dict
 * frame-ref leaks and `compression_total_*_bytes` go stale because
 * freeStringObject on the dropped robj sees encoding=RAW and skips
 * the decRef path), NULL val_ptr defensively, decRef the pin (which
 * may free the robj if the kvstore reference has gone away), and
 * free the entry struct itself.
 *
 * Used by:
 *   - compressionBeforeSleep on the discard branch (kvstore slot no
 *     longer points at our pinned robj — overwrite/expire/COW).
 *   - compressionShutdown (treats every remaining entry as discard
 *     because the kvstore may already be torn down).
 *   - testOnlyCompressionDrainTransientViewAsDiscard (unit-test
 *     fixture cleanup).
 *
 * The val_ptr=NULL trick prevents freeStringObject (called by
 * decrRefCount when refcount hits 0) from double-freeing the temp sds
 * we just sdsfree'd. freeStringObject for OBJ_ENCODING_RAW is
 * sdsfree(val_ptr); passing NULL is a no-op. */
static inline void discardTransientEntry(compressionTransientEntry *e) {
    sdsfree((sds)e->obj->val_ptr);
    releaseCompressedBuffer(e->compressed_buffer);
    e->obj->val_ptr = NULL;
    decrRefCount(e->obj);
    zfree(e);
}

/* Restore-mode finalize (used by the "slot matches" branch of
 * compressionBeforeSleep when master ∈ {compression, off}). Pointer
 * swap: free the temp sds, put the saved compressed buffer back as
 * val_ptr, flip encoding back to OBJ_ENCODING_COMPRESSED, drop the pin
 * (refcount 2→1 — the kvstore retains its reference). The compressed
 * bytes never went away; we just had val_ptr aliased to the temp sds
 * during the iteration. ZERO recompression cost. */
static inline void restoreTransientEntry(compressionTransientEntry *e) {
    robj *o = e->obj;
    sdsfree((sds)o->val_ptr);
    o->val_ptr = e->compressed_buffer;
    o->encoding = OBJ_ENCODING_COMPRESSED;
    decrRefCount(o);
    zfree(e);
}

/* Permanent-decompress-mode finalize (used by the "slot matches"
 * branch of compressionBeforeSleep when master == decompression).
 * Release the saved compressed buffer (releaseCompressedBuffer
 * handles dict frame-ref decRef and reverse install accounting); the
 * temp sds stays installed as val_ptr; encoding stays
 * OBJ_ENCODING_RAW. The robj is now an ordinary uncompressed string
 * from every observer's perspective — until the master switch flips
 * back to compression and the sweeper or write path picks it up for
 * re-compression. Drop the pin (refcount 2→1; kvstore retains its
 * ref). */
static inline void permanentlyDecompressTransientEntry(compressionTransientEntry *e) {
    releaseCompressedBuffer(e->compressed_buffer);
    decrRefCount(e->obj);
    zfree(e);
}

int transientViewActive(const robj *o) {
    /* Common case: no compressed value has been read in this iteration
     * (or ever). The map is NULL or empty; return 0 immediately without
     * hashing the pointer. The `min_string_size_copy_avoid` threshold
     * means this function is called from the bulk-reply hot path on
     * large strings; the early-out is worth ~10 ns per call. */
    if (transient_view_map == NULL) return 0;
    if (hashtableSize(transient_view_map) == 0) return 0;

    void *found;
    /* hashtableFind takes a non-const key; cast away const because the
     * default hash/compare callbacks treat the key as an opaque pointer
     * value (they don't dereference it). Safe. */
    return hashtableFind(transient_view_map, (void *)o, &found) ? 1 : 0;
}

/* ========================================================================
 * Lifecycle stubs
 * ======================================================================== */

/* Forward declaration: defined later, near the apply hooks block.
 * Called once from compressionInit after boot config is applied. */
static void syncApplyHookCaches(void);

void compressionInit(void) {
    /* Order matters: registry must be ready before workers start, so
     * any in-flight worker that loads the active dict pointer sees a
     * fully-initialized registry. compressionRegistryRelease in
     * compressionShutdown() runs AFTER the workers have been joined
     * (its header comment requires this). */
    compressionRegistryInit();
    if (compressionWorkersStart(server.compression_threads) != 0) {
        serverLog(LL_WARNING,
                  "Compression: worker pool failed to start. The feature "
                  "will be inert until restart or 'CONFIG SET compression-threads' "
                  "succeeds.");
    }
    /* TODO(S1.x): compressionTrainInit(); */
    compressionTrainInit();
    compressionSweepInit();

    /* Boot config has been applied by the time compressionInit runs.
     * Sync the apply-hook static caches with the boot values so any
     * subsequent CONFIG SET sees correct "from" values for transition
     * logging, and emit the non-functional warning if boot landed
     * directly in master=compression + threads=0 (R2.1.6). */
    syncApplyHookCaches();
}

/* Called from finishShutdown in src/server.c. Must run BEFORE
 * compressionRegistryRelease so workers do not race the registry's
 * teardown. */
void compressionShutdown(void) {
    compressionWorkersStop();

    /* Drain the transient-view side-map. In normal operation the
     * map is emptied at every beforeSleep, so it should be empty
     * here. But if shutdown happens mid-iteration (e.g. the server
     * is killed between lookupKey and beforeSleep) we still need to
     * release the pins and free the buffers. We can't safely call
     * the kvstore (it may be torn down by now), so we don't try to
     * restore — every remaining entry is treated as a discard.
     *
     * HASHTABLE_ITER_SAFE pauses incremental rehashing so we can free
     * entry structs inline without confusing the iterator. */
    if (transient_view_map != NULL) {
        hashtableIterator iter;
        hashtableInitIterator(&iter, transient_view_map, HASHTABLE_ITER_SAFE);
        void *raw;
        while (hashtableNext(&iter, &raw)) {
            discardTransientEntry((compressionTransientEntry *)raw);
        }
        hashtableCleanupIterator(&iter);
        hashtableRelease(transient_view_map);
        transient_view_map = NULL;
    }

    compressionSweepShutdown();
    compressionRegistryRelease();
#ifdef USE_ZSTD
    if (server_dctx != NULL) {
        ZSTD_freeDCtx(server_dctx);
        server_dctx = NULL;
    }
#endif
}

void compressionCron(void) {
    /* Training: trigger evaluation, scan advancement, completion polling. */
    compressionTrainCron();
    /* Sweeper: state machine + paced kvstore iteration. (R2.1.2 + R2.1.4) */
    compressionSweepCron();
}

void compressionAfterSleep(void) {
    /* Drain up to 256 results per main-loop iteration. The bound
     * exists so a backlog cannot starve other afterSleep work; it is
     * an internal safety knob, not a tunable config.
     *
     * Why 256: the per-result main-thread cost in the production path
     * (S2.5 onward) is dominated by createCompressedObject + the
     * net-savings guard + the kvstore overwrite — empirically ~5-15
     * µs/result. 256 results × ~10 µs ≈ 2.5 ms, which fits inside the
     * usual afterSleep budget (event loops typically run on the order
     * of 1-10 ms per iteration). 256 is also large enough to absorb a
     * full-pool burst: at 16 workers × ~50 jobs/sec/worker (typical
     * compression rate for 1KB values), one second of queued work
     * fits in two iterations.
     *
     * If we underestimate: the outbox accumulates,
     * compression_outbox_backpressure_total (R2.10.4) climbs, and a
     * worker retries posting rather than dropping completed work —
     * surfacing operationally before correctness is at risk. If we
     * overestimate: a deep outbox can stall the main loop, which
     * shows up in INFO latency. Both ends are observable; 256 is a
     * sound default and tunable later if measurement justifies. */
    compressionWorkersDrainOutbox(256);
}

void compressionBeforeSleep(void) {
    /* Restore transiently-decompressed values per design §2.5.7.
     *
     * Drain mode is gated on the master-switch state (read once here):
     *
     *   - master ∈ {compression, off} → RESTORE mode: pointer-swap
     *     back to compressed. The compressed bytes never went away;
     *     we just had val_ptr aliased to the temp sds during the
     *     iteration. ZERO recompression cost. Cold values stay
     *     compressed across iterations — preserves R2.5.6.
     *
     *   - master == decompression → PERMANENT-DECOMPRESS mode:
     *     release the saved compressed buffer (via
     *     releaseCompressedBuffer — decRef the dict frame-ref, reverse
     *     install accounting, free the buffer); leave the temp sds
     *     installed as val_ptr; encoding stays OBJ_ENCODING_RAW. The
     *     value is now permanently decompressed; the next read pays
     *     no decompression cost. This is the only intentional cross-
     *     mechanism dependency between the master switch and the
     *     transient-view subsystem (R2.5.7).
     *
     *     Permanent-decompress drain matches the operator's
     *     declared intent in master=decompression: a transient view
     *     that restored the compressed form would oscillate every
     *     read-touched key between compressed and uncompressed each
     *     iteration, defeating the drain. With permanent-decompress
     *     drain, a single sweep across the keyspace plus normal
     *     read traffic will fully decompress the dataset.
     *
     * For each entry in the side-map (regardless of mode):
     *   (a) Re-fetch the kvstore slot for the key (via the value
     *       robj's embedded key sds — the pin guarantees the embedded
     *       key is still valid memory).
     *   (b) If the slot points elsewhere (mutation / overwrite /
     *       expire / COW-orphaned): discard via discardTransientEntry.
     *       This is identical for both drain modes — when the kvstore
     *       no longer references our robj, the only sane action is
     *       to free both buffers and drop the pin.
     *   (c) If the slot still points at our pinned robj: do the
     *       mode-specific finalize step (restore vs. permanent-
     *       decompress) and drop the pin (refcount 2→1; kvstore
     *       retains its ref).
     *
     * Iteration uses HASHTABLE_ITER_SAFE so we can free entry structs
     * inline without confusing the iterator (rehashing is paused for
     * the duration). Final hashtableEmpty(map, NULL) drops the bucket
     * pointers (entries already freed). */
    if (transient_view_map == NULL) return;
    if (hashtableSize(transient_view_map) == 0) return;

    int master = server.compression_master_switch;

    hashtableIterator iter;
    hashtableInitIterator(&iter, transient_view_map, HASHTABLE_ITER_SAFE);
    void *raw;
    while (hashtableNext(&iter, &raw)) {
        compressionTransientEntry *e = raw;
        robj *o = e->obj;

        /* Look up the kvstore slot for this key. R2.4.4 + the pin
         * established at materialize time make pointer equality
         * decisive (ABA-safe — see §4.6 concurrency notes). */
        serverAssert(e->dbid >= 0 && e->dbid < server.dbnum);
        serverDb *db = server.db[e->dbid];
        sds key_sds = (sds)objectGetKey(o);
        /* hasembkey is set on every kvstore-stored value by
         * objectSetKeyAndExpire (see dbAddInternal / dbSetValue), so
         * objectGetKey returns non-NULL here. If a future code path
         * stores a value without an embedded key, we'd need a fallback. */
        serverAssert(key_sds != NULL);

        int dict_index = getKVStoreIndexForKey(key_sds);
        void **slot = kvstoreHashtableFindRef(db->keys, dict_index, key_sds);

        if (slot == NULL || *slot != o) {
            /* Stale: kvstore slot no longer points at our pinned robj.
             * discardTransientEntry handles both buffers + pin drop +
             * entry free, regardless of drain mode. */
            discardTransientEntry(e);
            continue;
        }

        if (master == COMPRESSION_MASTER_DECOMPRESSION) {
            /* PERMANENT-DECOMPRESS. The robj is now an ordinary
             * uncompressed string from every observer's perspective
             * — until the master switch flips back to compression
             * and the sweeper or write-path picks it up for
             * re-compression. */
            permanentlyDecompressTransientEntry(e);
        } else {
            /* RESTORE (master ∈ {compression, off}). Pointer-swap
             * back to compressed; cold values stay compressed
             * across iterations (preserves R2.5.6). */
            restoreTransientEntry(e);
        }
    }
    hashtableCleanupIterator(&iter);

    /* Bucket pointers reference now-freed entry structs; clear them
     * with NULL destructor (no per-entry callback — entries already
     * freed inline above). */
    hashtableEmpty(transient_view_map, NULL);

    /* Reset the per-iteration budget. The next iteration starts with
     * the full savings-based cap available. */
    transient_view_uncompressed_bytes = 0;
}

/* ========================================================================
 * Master-switch + active-sweeper apply hooks
 * ========================================================================
 *
 * Implements R2.1.5 (master-switch transition rules) and the warning
 * logic for non-functional combinations (R2.1.6). Both hooks detect
 * transitions by comparing against a function-static prior value;
 * ordering relative to the config layer is fine because the field has
 * already been written by the time the hook runs.
 *
 * The warning logic for the `master=compression + threads=0` non-
 * functional combination is shared between this hook and
 * applyCompressionThreads via maybeWarnNonFunctional().
 */

/* Map COMPRESSION_MASTER_* values to user-visible names for log lines.
 * Stays in sync with the enum block in server.h. */
static const char *masterSwitchName(int v) {
    switch (v) {
    case COMPRESSION_MASTER_OFF: return "off";
    case COMPRESSION_MASTER_COMPRESSION: return "compression";
    case COMPRESSION_MASTER_DECOMPRESSION: return "decompression";
    default: return "?";
    }
}

static const char *automaticSweeperName(int v) {
    return v == COMPRESSION_AUTOMATIC_SWEEPER_ENABLED ? "enabled" : "disabled";
}

/* Warn once on transition INTO the `master=compression + threads=0`
 * state — writes will queue but no work happens, sweeper passes drop
 * candidates (R2.1.6). Called from both apply hooks plus
 * applyCompressionThreads (in config.c) plus once from compressionInit
 * to handle the case where boot config lands directly in this state. */
static int prev_master_for_warning = COMPRESSION_MASTER_OFF;
static int prev_threads_for_warning = 1;

/* Function-static caches for the apply hooks themselves — promoted to
 * file-scope so compressionInit can sync them with the boot config
 * before any apply hook runs. Without this, an operator who boots
 * with `master=compression` and later sets `master=decompression`
 * would see the apply hook log a misleading "off -> decompression"
 * transition (because the static was initialized to the default,
 * not synced with the boot value). */
static int prev_master_for_apply = COMPRESSION_MASTER_OFF;
static int prev_active_sweeper_for_apply = COMPRESSION_AUTOMATIC_SWEEPER_DISABLED;

static void maybeWarnNonFunctional(void) {
    int curr_master = server.compression_master_switch;
    int curr_threads = server.compression_threads;
    int was_nonfunc = (prev_master_for_warning == COMPRESSION_MASTER_COMPRESSION &&
                       prev_threads_for_warning == 0);
    int is_nonfunc = (curr_master == COMPRESSION_MASTER_COMPRESSION &&
                      curr_threads == 0);
    if (is_nonfunc && !was_nonfunc) {
        serverLog(LL_WARNING,
                  "Compression: master-switch=compression but compression-threads=0; "
                  "writes enqueue but no work happens, sweeper passes drop candidates. "
                  "Set compression-threads >= 1 to make this state functional.");
    }
    prev_master_for_warning = curr_master;
    prev_threads_for_warning = curr_threads;
}

/* Called from compressionInit() after the boot config has been parsed
 * and applied. Syncs the apply-hook static caches with the boot
 * values so subsequent CONFIG SET-driven apply hooks see correct
 * "from" values for transition logging. Also fires the non-functional
 * warning if boot landed directly in master=compression + threads=0
 * (apply hooks don't fire during boot-time config load). */
static void syncApplyHookCaches(void) {
    prev_master_for_apply = server.compression_master_switch;
    prev_active_sweeper_for_apply = server.compression_automatic_sweeper;
    /* Force the warning detector to emit if boot landed in the
     * non-functional state. We do this by leaving the warning's prev
     * values at their defaults (OFF, 1) and letting maybeWarnNonFunctional
     * see the transition naturally. */
    maybeWarnNonFunctional();
}

int applyCompressionMasterSwitch(const char **err) {
    /* Field already written by config layer to server.compression_master_switch.
     * The transition is detected against `prev_master_for_apply` which
     * is synced with the boot value via syncApplyHookCaches(). */
    int curr = server.compression_master_switch;
    UNUSED(err);

    if (prev_master_for_apply == curr) return 1;

    /* On any transition INTO `decompression` (from `compression` or
     * from `off`), retire the active dict if there is one. The dict
     * must reach RETIRING state for canFree() to succeed once
     * frame_refs reaches zero (§4.4 QSBR); without this, the dict
     * would stay ACTIVE indefinitely and never be reclaimed even
     * after the sweeper drains all frames. (R2.1.5) */
    if (curr == COMPRESSION_MASTER_DECOMPRESSION) {
        compressionDictPair *active = compressionRegistryActive();
        if (active != NULL) {
            uint32_t id = active->dict_id;
            compressionRegistryRetire(id);
            serverLog(LL_NOTICE,
                      "Compression: master-switch entered decompression; "
                      "retiring active dict %u.",
                      id);
        }
    }

    serverLog(LL_NOTICE,
              "Compression: master-switch %s -> %s.",
              masterSwitchName(prev_master_for_apply), masterSwitchName(curr));

    prev_master_for_apply = curr;
    /* Notify the sweeper of the transition. Necessary because the
     * sweeper polls server.compression_master_switch every cron tick
     * (~100ms); without an explicit edge signal, transitions like
     * compression→off→compression that complete inside one tick window
     * would be invisible to the sweeper (it only ever observes the
     * current value, not the history). */
    compressionSweepNotifyMasterSwitchChanged();
    maybeWarnNonFunctional();
    return 1;
}

int applyCompressionAutomaticSweeper(const char **err) {
    int curr = server.compression_automatic_sweeper;
    UNUSED(err);

    if (prev_active_sweeper_for_apply == curr) return 1;
    serverLog(LL_NOTICE,
              "Compression: automatic-sweeper %s -> %s.",
              automaticSweeperName(prev_active_sweeper_for_apply), automaticSweeperName(curr));
    prev_active_sweeper_for_apply = curr;
    /* Notify the sweeper engine. The hook resets cursor + arms
     * enable_once on disabled→enabled (when master ≠ off), and
     * aborts any in-flight scan on enabled→disabled. */
    compressionSweepNotifyAutomaticSweeperChanged();
    return 1;
}

/* Called by applyCompressionThreads after the worker pool is resized
 * — gives us a chance to update the non-functional-state warning when
 * threads transitions to/from 0. Lives in compression.c so the static
 * prev-state cache is co-located with the master-switch hook. */
void compressionAfterThreadsApplied(void) {
    maybeWarnNonFunctional();
}

/* ========================================================================
 * Hot path
 * ========================================================================
 *
 * objectGetUncompressedView — sync main-thread decoder for compressed
 * values. The encoder (S2.5) produces the buffers this function consumes.
 * compressionIsEligible implements the R2.2 predicate.
 *
 * The decoder is not yet wired into any read path (`getCommand`, the
 * replication feed, etc.) — that's S2.8. This PR delivers the helper
 * and its gtest coverage so S2.8 can plumb it in without touching the
 * decompression logic itself.
 */

robj *objectGetUncompressedView(robj *o, sds *scratch, robj *view_out) {
    serverAssert(scratch != NULL);
    serverAssert(view_out != NULL);

    /* Hot path: uncompressed value. One branch, no allocation, no
     * scratch growth. This is what every read on every NON-compressed
     * key pays — even when the feature is enabled — so it's worth
     * making cheap. */
    if (o->encoding != OBJ_ENCODING_COMPRESSED) return o;

#ifdef USE_ZSTD
    /* Decode the per-value header. The header is the source of truth
     * for the compressed buffer's layout: createCompressedObject
     * validated buffer_len == HEADER + compressed_len at install time
     * (compression_header.h contract), and the encoder writes the
     * header from the actual frame size. We therefore do NOT compute
     * buf_len here.
     *
     * Avoiding a buf_len computation also sidesteps a real bug: the
     * val_ptr for OBJ_ENCODING_COMPRESSED is a raw zmalloc'd buffer
     * (NOT an sds). sdslen() on it would buffer-overflow reading the
     * sds header at a negative offset; AddressSanitizer flags it.
     *
     * The "buffer too short" guard is implicit in compressionHeaderDecode:
     * createCompressedObject's serverAssert ensures buffer_len >=
     * HEADER_SIZE before we ever get here, so the 16-byte header read
     * is in-bounds. */
    const unsigned char *buf = (const unsigned char *)objectGetVal(o);

    compressedHeader hdr;
    if (compressionHeaderDecode(buf, &hdr) != 0) {
        serverLog(LL_WARNING,
                  "Compression: compressed value has unknown algorithm "
                  "magic (corrupt)");
        /* TODO(S4.1): compression_errors_total++ */
        return NULL;
    }

    /* For ZSTD, alg_meta is the dict_id. Look up the DDict in the
     * registry.
     *
     * Lifetime safety: compressionRegistryLookup returns a raw pointer
     * without bumping any refcount. The pair remains valid here because
     * (1) the registry's free path (compressionRegistryTryGc →
     * dictPairFree) is itself main-thread-only, and (2) no code between
     * this lookup and the ZSTD_decompress_usingDDict call below yields
     * to the event loop. So no main-thread code that could free dicts
     * can run in this window. This is the same single-threaded
     * cooperative invariant that protects every lookupKey()-then-use
     * sequence in Valkey.
     *
     * v2 async decompression (Appendix C.2) would need to revisit this
     * — yielding back to the event loop between lookup and decompress
     * requires real reference counting through the registry.
     *
     * NULL return is a legitimate failure case, not a server bug: a
     * frame may reference a dict_id that has since been retired and
     * reclaimed (e.g. an RDB-loaded value whose dict drained to
     * frame_refs == 0 and was GC'd). The caller treats it as a
     * corruption-class read failure (§6.2). */
    compressionDictPair *pair = compressionRegistryLookup(hdr.alg_meta);
    if (pair == NULL || pair->ddict == NULL) {
        serverLog(LL_WARNING,
                  "Compression: dict_id %u not found in registry (frame "
                  "references retired or never-loaded dictionary)",
                  hdr.alg_meta);
        /* TODO(S4.1): compression_errors_total++ */
        return NULL;
    }

    /* Lazy DCtx allocation. */
    ZSTD_DCtx *dctx = compressionGetDCtx();
    if (dctx == NULL) {
        serverLog(LL_WARNING, "Compression: ZSTD_createDCtx() failed (OOM?)");
        /* TODO(S4.1): compression_errors_total++ */
        return NULL;
    }

    /* Grow the scratch sds to fit the decompressed bytes. We trust the
     * header's uncompressed_len because (a) we wrote it ourselves on
     * the encoder side from the actual value length, and (b) any
     * mismatch would already have failed the alg_magic check or the
     * compressed_len check above. ZSTD_decompress_usingDDict will
     * also report an error if the actual decompressed size differs.
     *
     * sdsMakeRoomFor grows; sdsclear resets length to 0 so we can
     * write fresh content. If *scratch is NULL on first call, we
     * allocate a fresh empty sds first. */
    if (*scratch == NULL) {
        *scratch = sdsempty();
    } else {
        sdsclear(*scratch);
    }
    *scratch = sdsMakeRoomFor(*scratch, hdr.uncompressed_len);

    const unsigned char *frame = buf + COMPRESSION_HEADER_SIZE;
    size_t got = ZSTD_decompress_usingDDict(
        dctx,
        *scratch, sdsavail(*scratch) + sdslen(*scratch),
        frame, (size_t)hdr.compressed_len,
        pair->ddict);

    if (ZSTD_isError(got)) {
        serverLog(LL_WARNING,
                  "Compression: ZSTD_decompress_usingDDict failed: %s",
                  ZSTD_getErrorName(got));
        /* TODO(S4.1): compression_errors_total++ */
        return NULL;
    }

    if (got != hdr.uncompressed_len) {
        /* Defensive: header said N bytes but ZSTD produced M.
         * Indicates corruption since we wrote both numbers from the
         * same source value. */
        serverLog(LL_WARNING,
                  "Compression: decompressed size %zu does not match "
                  "header uncompressed_len %u",
                  got, hdr.uncompressed_len);
        /* TODO(S4.1): compression_errors_total++ */
        return NULL;
    }

    sdsIncrLen(*scratch, (ssize_t)got);

    /* Build the view robj. OBJ_STATIC_REFCOUNT marks it as stack-
     * allocated so any accidental decrRefCount is a no-op (would be
     * a use-after-free otherwise — caller's stack frame owns view_out).
     * We use a custom init rather than initStaticStringObject because
     * we want to preserve o->type (forward-compatible with v2 non-
     * STRING types) and clear the LRU/expire fields explicitly. */
    view_out->type = o->type;
    view_out->encoding = OBJ_ENCODING_RAW;
    view_out->hasexpire = 0;
    view_out->hasembkey = 0;
    view_out->hasembval = 0;
    view_out->lru = 0;
    view_out->refcount = OBJ_STATIC_REFCOUNT;
    view_out->val_ptr = *scratch;

    /* TODO(S4.1): compression_decompressions_per_sec++ rate update. */

    return view_out;
#else
    /* USE_ZSTD not compiled in: no path produces OBJ_ENCODING_COMPRESSED
     * robjs, so reaching this branch indicates either memory corruption
     * or a buffer that survived a build-mode change (RDB load with
     * USE_ZSTD off would already have decompressed inline per R2.6.3).
     * Either way, panicking is the right answer — silently returning
     * NULL would mask the inconsistency. */
    UNUSED(scratch);
    UNUSED(view_out);
    serverPanic("OBJ_ENCODING_COMPRESSED encountered with USE_ZSTD disabled");
#endif
}

/* compressionMaterializeTransientView — see compression.h for contract.
 *
 * Decompress, register in side-map, pin, flip encoding. The lookupKey()
 * caller has already established that:
 *   - feature/decoder concerns: master switch is `compression` OR a
 *     compressed value lingers from an earlier productive period, both
 *     of which are fine — the decoder doesn't care about the master
 *     switch.
 *   - encoding == OBJ_ENCODING_COMPRESSED.
 *   - LOOKUP_NO_BYTES was NOT set (caller wants bytes).
 *
 * Failure modes:
 *   - Decoder error (corruption, missing dict): logged + counter; we
 *     leave the robj in its original COMPRESSED state and return -1.
 *     The caller (lookupKey) treats -1 as "key effectively unreadable"
 *     and surfaces it to the client per R6.2.
 *   - OOM: same handling.
 *
 * Side effect on success: o->val_ptr is replaced with a freshly-allocated
 * sds carrying the decompressed bytes; o->encoding becomes
 * OBJ_ENCODING_RAW; o->refcount is bumped (the side-map's pin).
 *
 * Memory ownership is transferred:
 *   - The OLD val_ptr (the compressed buffer, allocated by the encoder
 *     via zmalloc + filled by createCompressedObject) moves into the
 *     side-map entry. compressionBeforeSleep() restores it to val_ptr
 *     (success path) or zfrees it (discard path).
 *   - The NEW val_ptr (the temp sds) is owned by the robj. At restoration
 *     we sdsfree it. */
int compressionMaterializeTransientView(robj *o, int dbid) {
    serverAssert(o != NULL);
    serverAssert(o->type == OBJ_STRING);

    /* Belt-and-suspenders: lookupKey already checked, but if a future
     * caller misuses this, do nothing rather than corrupt state. */
    if (o->encoding != OBJ_ENCODING_COMPRESSED) return 0;

    /* Memory-cap check (R2.5.7 amend / PR #23 review).
     *
     * Peek at the header for the predicted uncompressed_len, then
     * verify the side-map's running total + this value's uncompressed
     * size still fits within the running compression-savings budget.
     * If not, fall back to permanent decompress: that path costs only
     * one buffer (the temp sds replaces val_ptr; the compressed buffer
     * is freed), so it's bounded; transient view costs two simultaneously.
     *
     * The savings-based cap is a derived signal — no operator knob.
     * Invariant: while transient_view_uncompressed_bytes <= savings,
     * peak memory cannot exceed the no-compression baseline.
     *
     * The header-decode here is the same primitive objectGetUncompressedView
     * runs internally; we'd validate again on the success path. To avoid
     * the duplicate decode cost we'd plumb the decoded header through,
     * but the header decode is ~10 ns (a memcmp + a few uint32 reads);
     * not worth the API churn. */
    compressedHeader hdr;
    if (compressionHeaderDecode((const unsigned char *)o->val_ptr, &hdr) != 0) {
        /* Header decode failed — same handling as the decoder error
         * path below. The decoder will log + counter; we just bail. */
        return -1;
    }

    size_t savings = compressionGetSavingsBytes();
    if (transient_view_uncompressed_bytes + hdr.uncompressed_len > savings) {
        compression_transient_view_capped_total++;
        return compressionPermanentlyDecompress(o);
    }

    /* Decompress via the design's single decoder primitive (R2.5.2).
     * The view robj is a stack allocation we use as scratch; we keep
     * only `scratch` (the decompressed sds). */
    sds scratch = NULL;
    robj view;
    robj *u = objectGetUncompressedView(o, &scratch, &view);
    if (u == NULL) {
        /* Decoder logged + incremented counters internally. */
        sdsfree(scratch); /* may have been grown before failure */
        return -1;
    }
    /* `u` aliases `&view`; we don't use either further. The decompressed
     * bytes live in `scratch` regardless. */
    UNUSED(u);

    transientViewMapEnsure();

    /* Register entry first (no point flipping the robj if hashtableAdd
     * could fail). hashtableAdd returns false if the key already exists;
     * for our use that's a programmer bug because the caller checks
     * encoding==COMPRESSED and a registered robj has encoding==RAW —
     * the two states are mutually exclusive. */
    compressionTransientEntry *e = zmalloc(sizeof(*e));
    e->obj = o;
    e->compressed_buffer = o->val_ptr;
    e->dbid = dbid;
    int added = hashtableAdd(transient_view_map, e);
    serverAssert(added);

    /* Pin the robj. R2.4.4 immutable-snapshot invariant: refcount >= 2
     * forces COW on subsequent in-place mutators (dbUnshareStringValue
     * routes through getDecodedObject, which sees refcount != 1 and
     * allocates a fresh robj — leaving our pinned one alive for
     * restoration). The pin is also the ABA-safety mechanism for the
     * pointer-equality stale check at restore time. */
    incrRefCount(o);

    /* Flip encoding + val_ptr. After this point the robj looks like a
     * normal RAW string to every caller of objectGetVal/sdslen. */
    o->val_ptr = scratch;
    o->encoding = OBJ_ENCODING_RAW;

    /* Account the uncompressed bytes against the per-iteration budget. */
    transient_view_uncompressed_bytes += hdr.uncompressed_len;
    return 0;
}

/* compressionPermanentlyDecompress — see compression.h for contract.
 *
 * The write-path counterpart to compressionMaterializeTransientView.
 * Where transient view preserves the compressed form across a read,
 * permanent-decompress drops it: the caller will mutate the value, and
 * we'd just be discarding the side-map entry at beforeSleep anyway.
 * Doing the equivalent work upfront (free compressed buffer, decRef
 * dict, install fresh sds) avoids the side-map registration + COW +
 * kvstore re-fetch overhead.
 *
 * Memory ownership transfer:
 *   - OLD val_ptr (compressed buffer): we read its header for dict_id,
 *     then zfree it and compressionRegistryDecRef the dict.
 *   - NEW val_ptr (the decompressed sds): owned by the robj; will be
 *     freed by sdsfree on the next mutation that grows beyond capacity,
 *     or by freeStringObject when the robj is finally freed.
 *
 * Refcount: unchanged. Unlike materializeTransientView, no pin is
 * needed — there's no side-map entry to track, and no follow-up
 * restoration logic. */
int compressionPermanentlyDecompress(robj *o) {
    serverAssert(o != NULL);
    serverAssert(o->type == OBJ_STRING);

    if (o->encoding != OBJ_ENCODING_COMPRESSED) return 0;

    /* Hold the compressed buffer pointer locally; we'll release it
     * via releaseCompressedBuffer (decode header + decRef dict +
     * reverse accounting + zfree) after we install the decompressed
     * sds. */
    void *compressed_buffer = o->val_ptr;

    /* Decompress via the design's single decoder primitive (R2.5.2). */
    sds scratch = NULL;
    robj view;
    robj *u = objectGetUncompressedView(o, &scratch, &view);
    if (u == NULL) {
        sdsfree(scratch);
        return -1;
    }
    UNUSED(u);

    /* Install the decompressed sds and flip encoding. */
    o->val_ptr = scratch;
    o->encoding = OBJ_ENCODING_RAW;

    /* Release the dict frame-ref + free the old compressed buffer
     * (mirrors freeCompressedObject's logic, but keeps the robj). */
    releaseCompressedBuffer(compressed_buffer);

    return 0;
}

/* compressionEnqueueModified — see compression.h for contract.
 *
 * Hooked into signalModifiedKey() (db.c). Replaces the
 * compressionEnqueueCandidate calls that previously lived in
 * dbAddInternal/dbSetValue:
 *   - signalModifiedKey covers every byte-mutating command path
 *     (SET, APPEND, SETRANGE, INCR, INCRBYFLOAT, BITOP, HSET, XADD,
 *     etc.) — verified by audit.
 *   - The 3 paths that DON'T fire signalModifiedKey are exactly the
 *     ones we DON'T want to enqueue: RDB load (per R2.6.2), worker
 *     drain dbReplaceValue (per R2.9.2), module SETKEY_NO_SIGNAL.
 *
 * dbFind is used (not lookupKey) to bypass spurious LRU touches and
 * keyspace miss notifications; we only want the value robj. */
void compressionEnqueueModified(serverDb *db, robj *key) {
    if (server.compression_master_switch != COMPRESSION_MASTER_COMPRESSION) return;
    robj *val = dbFind(db, objectGetVal(key));
    if (val == NULL) return;
    compressionEnqueueCandidate(key, val, db->id);
}

/* Implements the R2.2 / Q6 eligibility predicate.
 *
 * Returns 1 iff the value is a candidate for background compression.
 * Cheap by construction — every check is a bitfield read, a config
 * comparison, or a `robj->lru` decode; no allocations, no hash lookups.
 * Callable from the write path (dbAdd / dbSetValue / dbOverwrite) and
 * from the sweep cron tick.
 */
int compressionIsEligible(robj *o) {
    /* 1. Master switch. Zero overhead unless master == compression.
     * The decompression and off states block the write-path enqueue
     * since we don't want to install new compressed frames in either
     * (R2.1.5: decompression is the drain mode; off freezes state). */
    if (server.compression_master_switch != COMPRESSION_MASTER_COMPRESSION) return 0;

    /* 2. Type + encoding gate.
     *
     * Only OBJ_STRING values are eligible in v1 (R2.2, Q6c).
     *
     * Of the four string encodings, only OBJ_ENCODING_RAW is a candidate:
     *   - OBJ_ENCODING_INT     — value packed inside robj.val_ptr; already
     *                            memory-optimal.
     *   - OBJ_ENCODING_EMBSTR  — string ≤44 B embedded in the robj
     *                            allocation; per-value compression header
     *                            alone (~16 B) erases any savings, and
     *                            reviewer feedback (Threads #17 and #21)
     *                            specifically excluded EMBSTR.
     *   - OBJ_ENCODING_COMPRESSED — already compressed; double-compressing
     *                               is a defense-in-depth no-op. */
    if (o->type != OBJ_STRING) return 0;
    if (o->encoding != OBJ_ENCODING_RAW) return 0;

    /* 3. Shared RESP constants. They are never installed into a db
     * (lookupKey asserts this), so this is purely defense-in-depth —
     * mirrors the assertion sites in src/db.c. */
    if (o->refcount == OBJ_SHARED_REFCOUNT) return 0;

    /* 4. Size bounds.
     *
     * The lower bound (`compression-min-value-size`, default 256 B) keeps
     * us from spending CPU on values too small to recoup the per-value
     * header (~16 B) plus dict-registry amortized cost.
     *
     * The upper bound (`compression-max-value-size`, default 128 KiB)
     * caps worst-case sync-decompression latency on the main thread
     * (~1 µs/KB at ZSTD level 3 with dictionary). 0 disables the upper
     * bound. */
    size_t len = sdslen((sds)objectGetVal(o));
    if (len < server.compression_min_value_size) return 0;
    if (server.compression_max_value_size > 0 &&
        len > server.compression_max_value_size) return 0;

    /* 5. Hot-key skip — POLICY-AWARE per R2.2.
     *
     * Valkey's 24-bit `robj->lru` field encodes different metrics in
     * different `maxmemory-policy` modes (see src/lrulfu.h):
     *
     *   - LRU and noeviction: seconds-based access time.
     *     `lru_getIdleSecs(o->lru)` returns seconds-since-last-touch
     *     (read OR write — the lru field is touched on every access,
     *     gated only by LOOKUP_NOTOUCH and fork). v1 cannot
     *     distinguish read-recency from write-recency from this
     *     single signal, so a single threshold —
     *     `compression-min-idle-seconds` — gates eligibility on the
     *     "value has been quiet long enough to be worth compressing"
     *     property.
     *
     *   - LFU: 16-bit minutes counter + 8-bit log freq counter. There
     *     is no per-second access timestamp, so the time-based knob
     *     is inactive in this mode. The freq counter IS the
     *     access-recency signal; `compression-lfu-threshold` filters
     *     directly on it.
     *
     * The branch is taken on every eligibility check; the cost is one
     * boolean (`is_using_lfu_policy`) read on the main thread, plus the
     * appropriate decode. */
    if (lrulfu_isUsingLFU()) {
        /* LFU mode. lfu_getFrequency() applies the standard Valkey
         * decay-on-read pattern (matches objectGetLFUFrequency in
         * src/object.c). */
        uint8_t freq;
        o->lru = lfu_getFrequency(o->lru, &freq);
        if (freq >= (uint8_t)server.compression_lfu_threshold) return 0;
    } else {
        /* LRU / noeviction. Read-only — no decay. */
        uint32_t idle_secs = lru_getIdleSecs(o->lru);
        if (idle_secs < (uint32_t)server.compression_min_idle_seconds) return 0;
    }

    /* 6. Post-compression net-savings guard.
     *
     * Per R2.4: when the worker returns a result that fails the
     * net-savings ratio check, the main thread discards the compressed
     * form, leaves the value uncompressed, and increments
     * `compression_skipped_incompressible`. v1 does NOT track per-key
     * rejection state — the rejection rate is part of the drift signal
     * (see S1.4 / R2.3.5 extension). The eligibility predicate
     * therefore has no per-key "don't retry" branch; each sweep tick
     * re-attempts compression of every eligible value.
     *
     * Rationale (see PR #10 design discussion): under a fixed dict,
     * the same value's compression result is deterministic — retrying
     * the same value under the same dict cannot change the outcome.
     * The legitimate trigger for "the rejection might now compress" is
     * **dict change**, which is handled by the drift mechanism: a
     * sustained high rejection rate flags the active dict as a poor
     * fit for the workload and triggers retraining. After promotion,
     * the next sweep tick re-attempts under the new dict naturally. */

    return 1;
}

void compressionEnqueueCandidate(robj *key, robj *value, int dbid) {
    UNUSED(key); /* embedded key in `value` is the authoritative lookup key */

    /* Master switch + eligibility (R2.2). compressionIsEligible
     * already short-circuits unless master == compression, so no
     * separate switch check needed. */
    if (!compressionIsEligible(value)) return;

    /* No active dict yet (R2.1.7 third state). The encoder's worker
     * side would also handle this, but checking here avoids an
     * allocator round-trip and a pin we'd immediately release. */
    if (compressionRegistryActive() == NULL) return;

    /* Pin the value: keeps the sds bytes immutable for the worker
     * (R2.4.4) AND reserves the robj address so the drain handler's
     * pointer-equality stale-check is ABA-safe. */
    incrRefCount(value);

    if (compressionWorkersEnqueue(value, dbid) != 0) {
        /* Pool refused (not started, or future-S2.11 inbox full).
         * Release the pin and drop the candidate; the next sweep
         * (S2.10) will rediscover the value.
         *
         * TODO(S4.1): compression_candidates_dropped_total++ when
         * the bounded inbox lands (S2.11). Pool-not-started today
         * doesn't increment this counter — it's a configuration
         * state, not back-pressure. */
        decrRefCount(value);
    }
}

/* ========================================================================
 * COMPRESSION command surface
 * ======================================================================== */

static const char *kDisabledReply =
    "compression is not enabled in this build (BUILD_ZSTD=no or feature disabled)";

/* Emit the full set of INFO-compression fields as plain "name:value"
 * lines into `out`. Shared between COMPRESSION STATUS and
 * genValkeyInfoString's # Compression section so the two can never
 * diverge (§4.5: "COMPRESSION STATUS returns the INFO compression
 * section as a flat structured reply"). C1 reflects the live master-
 * switch and active-sweeper config values; the other fields stay 0 /
 * "disabled" until later S2 PRs land their counters. */
static sds compressionRenderFields(sds out) {
    return sdscatprintf(out,
                        "compression_master_switch:%s\r\n"
                        "compression_automatic_sweeper:%s\r\n"
                        "compression_automatic_sweeper_interval:%d\r\n"
                        "compression_sweeper_running:%d\r\n"
                        "compression_state:disabled\r\n"
                        "compression_active_dict_id:0\r\n"
                        "compression_known_dicts:0\r\n"
                        "compression_dict_cap_reached:0\r\n"
                        "compression_compressed_objects:0\r\n"
                        "compression_total_uncompressed_bytes:0\r\n"
                        "compression_total_compressed_bytes:0\r\n"
                        "compression_ratio:0\r\n"
                        "compression_live_ratio_10m:0\r\n"
                        "compression_net_saved_bytes:0\r\n"
                        "compression_candidates_pending:0\r\n"
                        "compression_candidates_dropped_total:0\r\n"
                        "compression_sweep_backpressure_total:0\r\n"
                        "compression_sweep_pacing_sleeps_total:0\r\n"
                        "compression_outbox_backpressure_total:0\r\n"
                        "compression_compressions_per_sec:0\r\n"
                        "compression_decompressions_per_sec:0\r\n"
                        "compression_skipped_incompressible:0\r\n"
                        "compression_training_last_duration_ms:0\r\n"
                        "compression_training_last_sample_count:0\r\n"
                        "compression_errors_total:0\r\n",
                        masterSwitchName(server.compression_master_switch),
                        automaticSweeperName(server.compression_automatic_sweeper),
                        server.compression_automatic_sweeper_interval,
                        compressionSweepIsRunning());
}

int compressionStatus(client *c) {
    /* Phase 0: return a static INFO-style bulk string.
     * The field set matches §2.10 R2.10.1 so callers wiring dashboards
     * against Phase 0 servers can do so without waiting for the
     * feature-on observability implementation. */
    sds s = compressionRenderFields(sdsempty());
    addReplyVerbatim(c, s, sdslen(s), "txt");
    sdsfree(s);
    return C_OK;
}

int compressionForceTrain(client *c) {
    addReplyError(c, kDisabledReply);
    return C_ERR;
}

int compressionSweep(client *c, int direction) {
    UNUSED(direction);
    addReplyError(c, kDisabledReply);
    return C_ERR;
}

int compressionDictList(client *c) {
    /* Empty dict list is a legitimate disabled-state reply. */
    addReplyArrayLen(c, 0);
    return C_OK;
}

int compressionDictExport(client *c, uint32_t dict_id) {
    UNUSED(dict_id);
    addReplyError(c, kDisabledReply);
    return C_ERR;
}

int compressionDictImport(client *c, const unsigned char *bytes, size_t len) {
    UNUSED(bytes);
    UNUSED(len);
    addReplyError(c, kDisabledReply);
    return C_ERR;
}

int compressionDictDrop(client *c, uint32_t dict_id) {
    UNUSED(dict_id);
    addReplyError(c, kDisabledReply);
    return C_ERR;
}

/* Dispatch for the top-level COMPRESSION command. Subcommand JSON lives
 * under src/commands/compression-*.json. We handle the common shape
 * (c->argv[1] = subcommand name) here. */
void compressionCommand(client *c) {
    const char *sub = (c->argc >= 2) ? (const char *)objectGetVal(c->argv[1]) : "";

    if (!strcasecmp(sub, "status")) {
        compressionStatus(c);
    } else if (!strcasecmp(sub, "sweep")) {
        /* COMPRESSION SWEEP FORCE — operator-driven one-shot pass.
         * (R2.1.4) Direction is taken implicitly from the current
         * master switch on every cron tick; rejected if master=off. */
        if (c->argc != 3 ||
            strcasecmp((const char *)objectGetVal(c->argv[2]), "FORCE") != 0) {
            addReplyErrorFormat(c,
                                "syntax error: COMPRESSION SWEEP FORCE "
                                "(no other forms accepted)");
            return;
        }
        int rc = compressionSweepForce();
        if (rc == COMPRESSION_SWEEP_FORCE_REJECTED) {
            addReplyError(c,
                          "compression-master-switch is off; cannot sweep "
                          "(set master to compression or decompression first)");
            return;
        }
        addReply(c, shared.ok);
    } else if (!strcasecmp(sub, "help")) {
        const char *help[] = {
            "STATUS",
            "    Return the current compression state (mirrors INFO compression).",
            "SWEEP FORCE",
            "    Trigger a one-shot keyspace pass. Direction follows the",
            "    current compression-master-switch (compression: enqueue",
            "    eligible RAW values; decompression: permanently decompress",
            "    every compressed value). Rejected if master=off. Allowed",
            "    even when compression-automatic-sweeper is disabled.",
            "HELP",
            "    Print this help.",
            "",
            "Note: TRAIN and DICT LIST/DROP/EXPORT/IMPORT land in subsequent",
            "S2 PRs. Operators set master-switch state via 'CONFIG SET",
            "compression-master-switch ...'; legacy ENABLE/DISABLE aliases",
            "are not part of the v1 surface (the 3-state enum doesn't map",
            "cleanly to enable/disable verbs).",
            NULL};
        addReplyHelp(c, help);
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* ========================================================================
 * Test-only entry points
 * ========================================================================
 *
 * These functions are intentionally not declared in compression.h —
 * the production surface stays clean. Tests declare these locally in
 * extern "C" blocks (matches the testOnly* convention used in
 * quicklist.c, intset.c, compression_workers.c).
 */

/* Drain the transient-view side-map by treating every entry as a discard
 * (free temp sds, free saved compressed buffer, decRef pin, free entry).
 * Used by tests that don't have a kvstore set up — they can register
 * entries via materialize and then flush via this helper without going
 * through compressionBeforeSleep's kvstore-aware restore-or-discard
 * branch. The discard path is correctness-preserving: on the orphan-
 * branch in production, this is exactly what beforeSleep does. */
void testOnlyCompressionDrainTransientViewAsDiscard(void) {
    if (transient_view_map == NULL) return;
    if (hashtableSize(transient_view_map) == 0) return;

    hashtableIterator iter;
    hashtableInitIterator(&iter, transient_view_map, HASHTABLE_ITER_SAFE);
    void *raw;
    while (hashtableNext(&iter, &raw)) {
        discardTransientEntry((compressionTransientEntry *)raw);
    }
    hashtableCleanupIterator(&iter);
    hashtableEmpty(transient_view_map, NULL);
}

/* Test-only variant simulating compressionBeforeSleep's "slot matches"
 * branch with master ∈ {compression, off}. Calls the same per-entry
 * RESTORE finalize that production uses, but skips the kvstore lookup
 * (always restores). Lets unit tests verify the restore-mode side
 * effects (encoding flips back to COMPRESSED, val_ptr swaps, refcount
 * drops, side-map empties) without setting up a populated kvstore. */
void testOnlyCompressionDrainTransientViewAsRestore(void) {
    if (transient_view_map == NULL) return;
    if (hashtableSize(transient_view_map) == 0) return;

    hashtableIterator iter;
    hashtableInitIterator(&iter, transient_view_map, HASHTABLE_ITER_SAFE);
    void *raw;
    while (hashtableNext(&iter, &raw)) {
        restoreTransientEntry((compressionTransientEntry *)raw);
    }
    hashtableCleanupIterator(&iter);
    hashtableEmpty(transient_view_map, NULL);
    transient_view_uncompressed_bytes = 0;
}

/* Test-only variant simulating compressionBeforeSleep's "slot matches"
 * branch with master == decompression. Calls the same per-entry
 * PERMANENT-DECOMPRESS finalize that production uses. The compressed
 * buffer is released; the temp sds stays installed; encoding stays
 * RAW. Verifies the drain mode that matches the operator's drain
 * intent under master=decompression. */
void testOnlyCompressionDrainTransientViewAsPermanentDecompress(void) {
    if (transient_view_map == NULL) return;
    if (hashtableSize(transient_view_map) == 0) return;

    hashtableIterator iter;
    hashtableInitIterator(&iter, transient_view_map, HASHTABLE_ITER_SAFE);
    void *raw;
    while (hashtableNext(&iter, &raw)) {
        permanentlyDecompressTransientEntry((compressionTransientEntry *)raw);
    }
    hashtableCleanupIterator(&iter);
    hashtableEmpty(transient_view_map, NULL);
    transient_view_uncompressed_bytes = 0;
}

/* Returns the current number of entries in the transient-view side-map,
 * or 0 if the map has never been allocated. */
size_t testOnlyCompressionTransientViewSize(void) {
    if (transient_view_map == NULL) return 0;
    return hashtableSize(transient_view_map);
}

/* ========================================================================
 * INFO
 * ======================================================================== */

void infoCompression(sds *info) {
    if (!info || !*info) return;
    *info = sdscatprintf(*info, "# Compression\r\n");
    *info = compressionRenderFields(*info);
}
