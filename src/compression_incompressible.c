/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 *
 * compression_incompressible.c — side hashtable of compression-rejected keys.
 * See compression_incompressible.h for the rationale and contract.
 */

#include "server.h"
#include "compression_incompressible.h"
#include "hashtable.h"

/*
 * Entry stored in the side hashtable. Owns its `key` (duped on insert,
 * freed by `incompressibleEntryDestructor`).
 */
typedef struct incompressibleEntry {
    sds key;
    uint32_t failed_dict_id; /* dict_id active at the time of the rejection */
    mstime_t timestamp_ms;   /* `mstime()` snapshot at the time of the rejection */
} incompressibleEntry;

/* ========================================================================
 * Hashtable type callbacks
 * ======================================================================== */

static const void *incompressibleEntryGetKey(const void *entry) {
    const incompressibleEntry *e = entry;
    return e->key;
}

static void incompressibleEntryDestructor(void *entry) {
    incompressibleEntry *e = entry;
    if (e == NULL) return;
    sdsfree(e->key);
    zfree(e);
}

/* sds-keyed hashtable type. The hash function and key comparator are
 * the same ones used by `setHashtableType` in src/server.c — re-using
 * the established sds-key callbacks rather than introducing a new
 * hash. */
static hashtableType incompressibleHashtableType = {
    .entryGetKey = incompressibleEntryGetKey,
    .hashFunction = sdsHashConfigurableSeed,
    .keyCompare = dictSdsKeyCompare,
    .entryDestructor = incompressibleEntryDestructor,
};

/* Module-private state. Main thread only — no atomics, no locks. */
static hashtable *g_incompressible = NULL;

/* ========================================================================
 * Public API
 * ======================================================================== */

void compressionIncompressibleInit(void) {
    serverAssert(g_incompressible == NULL); /* programmer error: double init */
    g_incompressible = hashtableCreate(&incompressibleHashtableType);
}

void compressionIncompressibleRelease(void) {
    if (g_incompressible == NULL) return;
    hashtableRelease(g_incompressible);
    g_incompressible = NULL;
}

void compressionIncompressibleMark(const sds key, uint32_t failed_dict_id) {
    if (g_incompressible == NULL) return; /* feature not initialized; silently drop */
    serverAssert(key != NULL);

    /* Update path: if the key already has an entry, overwrite the dict_id
     * and timestamp instead of allocating a new one. Common when a key
     * fails, the active dict gets promoted (entry is now stale but not yet
     * cleared by a successful compression attempt), and the same key fails
     * again under the new dict. */
    incompressibleEntry *existing;
    if (hashtableFind(g_incompressible, key, (void **)&existing)) {
        existing->failed_dict_id = failed_dict_id;
        existing->timestamp_ms = mstime();
        return;
    }

    incompressibleEntry *e = zmalloc(sizeof(*e));
    e->key = sdsdup(key);
    e->failed_dict_id = failed_dict_id;
    e->timestamp_ms = mstime();
    /* hashtableAdd asserts the key is not already present; we ruled that
     * out via the Find above. */
    int added = hashtableAdd(g_incompressible, e);
    serverAssert(added);
}

int compressionIncompressibleRetryEligible(const sds key, uint32_t active_dict_id) {
    if (g_incompressible == NULL) return 1; /* pre-init: treat as eligible */
    serverAssert(key != NULL);

    incompressibleEntry *e;
    if (!hashtableFind(g_incompressible, key, (void **)&e)) {
        return 1; /* never failed → eligible */
    }

    /* Primary signal: the active dictionary changed since the last
     * rejection. The previous failure was scoped to a specific dict;
     * a different dict may now be able to compress this content. */
    if (e->failed_dict_id != active_dict_id) {
        return 1;
    }

    /* Fallback: catch the case where the dict stayed stable but the
     * key's content has changed and might now be compressible. The
     * fallback uses `compression-retry-interval` seconds (default
     * 1 h). On `compression_retry_interval == 0` every check is
     * eligible — useful for tests; matches §7.1 transparency-mode
     * harness config. */
    if (server.compression_retry_interval == 0) {
        return 1;
    }
    mstime_t age_ms = mstime() - e->timestamp_ms;
    if (age_ms >= (mstime_t)server.compression_retry_interval * 1000) {
        return 1;
    }

    return 0; /* still incompressible under this dict */
}

void compressionIncompressibleClear(const sds key) {
    if (g_incompressible == NULL) return;
    serverAssert(key != NULL);
    /* `hashtableDelete` invokes the destructor on the popped entry, so
     * sds + entry struct are reclaimed automatically. */
    (void)hashtableDelete(g_incompressible, key);
}

size_t compressionIncompressibleSize(void) {
    if (g_incompressible == NULL) return 0;
    return hashtableSize(g_incompressible);
}
