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
#include "compression_workers.h"
#include "compression_train.h"
#include "lrulfu.h"

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
 * Lifecycle stubs
 * ======================================================================== */

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
}

/* Called from finishShutdown in src/server.c. Must run BEFORE
 * compressionRegistryRelease so workers do not race the registry's
 * teardown. */
void compressionShutdown(void) {
    compressionWorkersStop();
    compressionRegistryRelease();
#ifdef USE_ZSTD
    if (server_dctx != NULL) {
        ZSTD_freeDCtx(server_dctx);
        server_dctx = NULL;
    }
#endif
}

void compressionCron(void) {
    /* Phase 0: no-op. */
    /* TODO(Phase 1): sweep tick + drift-retrain + pacing. */
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

/* ========================================================================
 * Toggle stub
 * ======================================================================== */

int compressionToggle(int enabled, sds *err) {
    UNUSED(enabled);
    /* Phase 0: toggling has no observable effect (feature is hard-off).
     * We accept the toggle silently so the config layer does not error. */
    if (err) *err = NULL;
    return 1;
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
    /* Decode the per-value header. compressionHeaderDecode validates
     * alg_magic; it does not validate that compressed_len matches the
     * buffer length (caller — i.e. us — knows the buffer length from
     * the robj). */
    const unsigned char *buf = (const unsigned char *)objectGetVal(o);
    size_t buf_len = sdslen((sds)objectGetVal(o));

    if (buf_len <= COMPRESSION_HEADER_SIZE) {
        /* Buffer too small to even contain a header — corruption. */
        serverLog(LL_WARNING,
                  "Compression: compressed value too short (%zu bytes; "
                  "minimum %u for header alone)",
                  buf_len, COMPRESSION_HEADER_SIZE);
        /* TODO(S4.1): compression_errors_total++ */
        return NULL;
    }

    compressedHeader hdr;
    if (compressionHeaderDecode(buf, &hdr) != 0) {
        serverLog(LL_WARNING,
                  "Compression: compressed value has unknown algorithm "
                  "magic (corrupt)");
        /* TODO(S4.1): compression_errors_total++ */
        return NULL;
    }

    /* Sanity-check the header's compressed_len against the buffer.
     * Mismatch is corruption. The header carries compressed_len in the
     * frame-only sense (excluding header bytes), so the expected total
     * is HEADER + compressed_len. */
    if ((size_t)hdr.compressed_len + COMPRESSION_HEADER_SIZE != buf_len) {
        serverLog(LL_WARNING,
                  "Compression: header compressed_len %u + %u-byte header "
                  "does not match buffer length %zu",
                  hdr.compressed_len, COMPRESSION_HEADER_SIZE, buf_len);
        /* TODO(S4.1): compression_errors_total++ */
        return NULL;
    }

    /* For ZSTD, alg_meta is the dict_id. Look up the DDict in the
     * registry. The dict may have been retired (refcount > 0 keeps
     * the DDict alive per the QSBR contract; a missing lookup means
     * the registry was force-cleared, which should not happen in a
     * healthy server). */
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

/* Implements the R2.2 / Q6 eligibility predicate.
 *
 * Returns 1 iff the value is a candidate for background compression.
 * Cheap by construction — every check is a bitfield read, a config
 * comparison, or a `robj->lru` decode; no allocations, no hash lookups.
 * Callable from the write path (dbAdd / dbSetValue / dbOverwrite) and
 * from the sweep cron tick.
 */
int compressionIsEligible(robj *o) {
    /* 1. Master switch. Zero overhead when disabled. */
    if (!server.compression_enabled) return 0;

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

void compressionEnqueueCandidate(const sds key, robj *o) {
    UNUSED(key);
    UNUSED(o);
    /* Phase 1: candidate queue is still a no-op sink until S2.4 (worker
     * pool) and S2.5 (encoder path) land. Wiring this up requires the
     * SPMC inbox, which doesn't exist yet. */
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
 * section as a flat structured reply"). Phase 0: every field is 0 /
 * "disabled" because the feature is inert. */
static sds compressionRenderFields(sds out) {
    return sdscatprintf(out,
                        "compression_enabled:0\r\n"
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
                        "compression_errors_total:0\r\n");
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
    } else if (!strcasecmp(sub, "enable") || !strcasecmp(sub, "disable")) {
        /* Phase 0: these are accepted but inert. */
        addReply(c, shared.ok);
    } else if (!strcasecmp(sub, "help")) {
        const char *help[] = {
            "STATUS",
            "    Return the current compression state.",
            "HELP",
            "    Print this help.",
            "",
            "Note: compression is in Phase 0 (skeleton). Additional",
            "subcommands (DICT LIST/DROP/EXPORT/IMPORT, SWEEP, TRAIN,",
            "ENABLE, DISABLE) land in Phase 1.",
            NULL};
        addReplyHelp(c, help);
    } else {
        addReplySubcommandSyntaxError(c);
    }
}

/* ========================================================================
 * INFO
 * ======================================================================== */

void infoCompression(sds *info) {
    if (!info || !*info) return;
    *info = sdscatprintf(*info, "# Compression\r\n");
    *info = compressionRenderFields(*info);
}
