/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#ifndef __COMPRESSION_INCOMPRESSIBLE_H
#define __COMPRESSION_INCOMPRESSIBLE_H

/*
 * compression_incompressible.h — side hashtable of keys whose
 * post-compression net-savings guard rejected them.
 *
 * Implements the dict-ID scoped retry guard from R2.4 / Q6b /
 * PR #1 Thread #20.
 *
 * # Why it exists
 *
 * The compression worker may produce a frame for a value that doesn't
 * actually save space (the value is incompressible — either the bytes
 * are random-looking, or our current dictionary doesn't match this
 * key's content shape). The post-compression guard discards the
 * compressed result when:
 *
 *   compressed_size + header_size >= uncompressed_size *
 *                                    (1 - compression-min-savings-ratio)
 *
 * Without remembering this rejection, the next sweep tick would
 * re-enqueue the same key, the worker would compress it again, and
 * the result would be discarded again. This wastes CPU on values
 * that are systematically incompressible under the current
 * dictionary.
 *
 * # Retry semantics (Q6b / Thread #20)
 *
 * The semantically right retry signal is "the dictionary changed",
 * NOT "enough time has passed". A new active dictionary may now be
 * able to compress what the previous one couldn't (different content
 * patterns, different entropy), so on dict promotion every previously
 * incompressible key becomes retry-eligible.
 *
 * A timestamp fallback (`compression-retry-interval`, default 1 h)
 * handles the edge case where the active dictionary stays stable but
 * the key's content has been overwritten with something more
 * compressible. Without the fallback, an incompressible key under a
 * long-lived dictionary would never be retried even if its content
 * legitimately changed.
 *
 * Concretely:
 *   retry_eligible(key, active_dict_id) ⇔
 *       entry := table[key]
 *       entry is null                                     // never failed
 *    OR entry.failed_dict_id != active_dict_id            // primary: dict changed
 *    OR mstime() - entry.timestamp_ms                     // fallback: stale
 *         >= compression-retry-interval * 1000
 *
 * # Memory
 *
 * The table is a side structure; entries cost
 * `sizeof(incompressibleEntry) + sdslen(key) + sds-overhead` ≈
 * ~48 bytes for typical key lengths. We allocate only after a
 * compression failure, never proactively. When the failed key is
 * subsequently DELed, overwritten with a now-compressible value, or
 * the active dictionary changes (the table is left as-is; entries
 * stay until cleared organically), the entry stays in memory until
 * cleared. v1 does not bound the table; the population is
 * self-limiting because (a) the post-compression guard fires only on
 * eligible keys, (b) the eligibility predicate's other gates cap
 * candidacy, and (c) the table is cleared on any successful
 * compression of the same key.
 *
 * # Threading
 *
 * Every operation runs on the main thread. The worker pool never
 * touches this table — workers post their result to the outbox, and
 * the main thread is the one that runs the post-compression guard
 * and decides whether to record the rejection. No locks needed.
 *
 * # Lifecycle
 *
 * `compressionIncompressibleInit()` is called from `compressionInit()`
 * during server startup. `compressionIncompressibleRelease()` is
 * available for tests; production never calls it (the OS reclaims on
 * exit). All other operations are no-ops if the table is not
 * initialized — so the eligibility predicate can call
 * `Retry Eligible()` safely even before init lands or after release.
 */

#include "fmacros.h"

#include <stddef.h>
#include <stdint.h>

#include "sds.h"

/* Lifecycle. Main thread only. */
void compressionIncompressibleInit(void);
void compressionIncompressibleRelease(void);

/*
 * Record a compression-rejection for `key` under the given
 * `failed_dict_id`. Stamps the current `mstime()` as the
 * retry-fallback timestamp. If the key was already in the table, the
 * entry is overwritten (newer dict_id and timestamp). Caller retains
 * ownership of `key`; this function dups it internally.
 *
 * Main thread only.
 */
void compressionIncompressibleMark(const sds key, uint32_t failed_dict_id);

/*
 * Returns 1 iff the key is retry-eligible per the dict-ID + time
 * fallback rule (see file header). Cheap: one hashtable lookup, plus
 * one mstime() read on the fallback path. Returns 1 unconditionally
 * if the table has not been initialized — Phase 0 / pre-init safe.
 *
 * Main thread only.
 */
int compressionIncompressibleRetryEligible(const sds key, uint32_t active_dict_id);

/*
 * Remove `key` from the table. Called on:
 *   - successful compression (retry succeeded; clear stale rejection)
 *   - DEL of the key (no point retaining a rejection for a key that
 *     no longer exists)
 *
 * No-op if `key` is not in the table. Main thread only.
 */
void compressionIncompressibleClear(const sds key);

/*
 * Test-only / introspection: returns the number of rejection entries
 * currently in the table, or 0 if not initialized. Used by gtests and
 * by future `INFO compression` exposure (`compression_skipped_incompressible`
 * counter is updated independently — see R2.10).
 */
size_t compressionIncompressibleSize(void);

#endif /* __COMPRESSION_INCOMPRESSIBLE_H */
