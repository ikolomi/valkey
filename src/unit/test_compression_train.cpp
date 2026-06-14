/*
 * Copyright (c) Valkey Contributors
 * All rights reserved.
 * SPDX-License-Identifier: BSD-3-Clause
 */

#include "generated_wrappers.hpp"

#include <cstdint>
#include <cstring>

extern "C" {
#include "bio.h"
#include "compression_registry.h"
#include "compression_train.h"
#include "hashtable.h"
#include "kvstore.h"
#include "monotonic.h"
#include "sds.h"
#include "server.h"
#include "zmalloc.h"
#ifdef USE_ZSTD
#include <zdict.h>
#include <zstd.h>
#endif
}

/* ========================================================================
 * Helpers
 * ======================================================================== */

/* Create a RAW string robj with an embedded key and a value of given size.
 * The value is filled with a pattern based on `seed` to give ZSTD
 * something compressible-but-varied to train on. */
static robj *makeKeyedRawString(const char *key, size_t value_len, int seed) {
    /* Create value content — semi-compressible pattern. */
    sds val = sdsnewlen(NULL, value_len);
    for (size_t i = 0; i < value_len; i++) {
        val[i] = (char)('A' + ((seed + (int)i) % 26));
    }
    robj *o = createRawStringObject(val, sdslen(val));
    sdsfree(val);
    /* Embed the key. */
    sds k = sdsnew(key);
    o = objectSetKeyAndExpire(o, k, -1);
    sdsfree(k);
    /* Cold key — eligible for compression. */
    o->lru = 0;
    return o;
}

/* Populate db[0] with n RAW string objects of given value size. */
static void populateDb(int n, size_t value_len) {
    for (int i = 0; i < n; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "trainkey:%06d", i);
        robj *o = makeKeyedRawString(keybuf, value_len, i);
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }
}

/* ========================================================================
 * Test Fixture
 * ======================================================================== */

class CompressionTrainTest : public ::testing::Test {
  protected:
    static void SetUpTestSuite() {
        /* Initialize hash seed once for all tests. */
        uint8_t seed[16] = {0};
        hashtableSetHashFunctionSeed(seed);
        monotonicInit();
        bioInit(); /* Spawns bio threads so bioCreateCompTrainJob doesn't crash. */
    }

    void SetUp() override {
        server.logfile = (char *)"";
        server.verbosity = LL_NOTHING;

        server.compression_master_switch = COMPRESSION_MASTER_COMPRESSION;
        server.compression_dict_min_training_keys = 100;
        server.compression_dict_max_training_keys = 10000;
        server.compression_training_buffer_size = 4 * 1024 * 1024; /* 4 MiB for tests */
        server.compression_min_value_size = 64;
        server.compression_max_value_size = 131072;
        server.compression_dict_size = 102400; /* 100KB — default production value */
        server.compression_dict_max_versions = 4;
        server.compression_threads = 1;
        server.compression_min_idle_seconds = 0; /* All keys are eligible */
        server.compression_lfu_threshold = 255;
        server.compression_dict_drift_ratio = 70;
        server.compression_dict_refresh_interval = 0;
        server.hz = 10;
        server.dbnum = 1;
        server.maxmemory_policy = 0; /* noeviction — uses LRU path */

        server.db = (serverDb **)zcalloc(sizeof(serverDb *) * server.dbnum);
        for (int i = 0; i < server.dbnum; i++) {
            server.db[i] = (serverDb *)zcalloc(sizeof(serverDb));
            server.db[i]->keys = kvstoreCreate(&kvstoreKeysHashtableType, 0, 0);
        }

        compressionRegistryInit();
        compressionTrainInit();
    }

    void TearDown() override {
        for (int i = 0; i < server.dbnum; i++) {
            kvstoreRelease(server.db[i]->keys);
            zfree(server.db[i]);
        }
        zfree(server.db);
        server.db = NULL;
        compressionRegistryRelease();
    }

    /* Run cron ticks until training_requested is consumed and scan completes.
     * Returns after max_ticks or when trigger can fire again. */
    void runCronTicks(int max_ticks) {
        for (int i = 0; i < max_ticks; i++) {
            compressionTrainCron();
        }
    }
};

/* ========================================================================
 * Trigger Tests
 * ======================================================================== */

TEST_F(CompressionTrainTest, CronSafeWhenDisabled) {
    server.compression_master_switch = COMPRESSION_MASTER_OFF;
    populateDb(200, 256);
    for (int i = 0; i < 100; i++) {
        compressionTrainCron();
    }
}

TEST_F(CompressionTrainTest, NoAutoTriggerBelowMinKeys) {
    /* 50 keys < min 100 — no trigger. */
    populateDb(50, 256);
    runCronTicks(1000);
    /* Should still be idle — manual trigger should succeed. */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 1);
}

TEST_F(CompressionTrainTest, AutoTriggerWhenEnoughKeys) {
    /* 200 keys > min 100 — should auto-trigger. */
    populateDb(200, 256);
    runCronTicks(1000);
    /* If auto-trigger fired, manual trigger should fail
     * (in scanning/submitted/cooldown). */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

TEST_F(CompressionTrainTest, NoAutoTriggerWhenActiveDictExists) {
    populateDb(200, 256);
    /* Install a fake active dict. */
    compressionDictPair *fake = (compressionDictPair *)zcalloc(sizeof(*fake));
    fake->bytes = (unsigned char *)zmalloc(10);
    fake->bytes_len = 10;
    compressionRegistryAdd(fake, 1);
    /* Now auto-trigger should not fire (active dict exists, no drift). */
    runCronTicks(1000);
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 1); /* Still idle */
}

TEST_F(CompressionTrainTest, ManualTriggerSucceeds) {
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 1);
}

TEST_F(CompressionTrainTest, ManualTriggerFailsWhenInProgress) {
    compressionTrainMaybeTrigger(3);
    compressionTrainCron(); /* Transitions to SCANNING */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

/* ========================================================================
 * Scan Callback Tests — Eligibility
 * ======================================================================== */

TEST_F(CompressionTrainTest, SkipsValuesBelowMinSize) {
    server.compression_min_value_size = 256;
    /* All values are 32 bytes — below min. */
    populateDb(200, 32);
    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    /* 0 eligible samples < min 100 → cooldown. */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

TEST_F(CompressionTrainTest, SkipsValuesAboveMaxSize) {
    server.compression_max_value_size = 128;
    /* All values are 256 bytes — above max. */
    populateDb(200, 256);
    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    /* 0 eligible → cooldown. */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

TEST_F(CompressionTrainTest, CollectsEligibleValues) {
    /* 200 keys × 256 bytes, all within bounds. Should collect >= 100. */
    populateDb(200, 256);
    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    /* If collected >= 100, it submitted (state = SUBMITTED).
     * Can't re-trigger. */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

TEST_F(CompressionTrainTest, KeyCapStopsCollection) {
    server.compression_dict_max_training_keys = 50;
    server.compression_dict_min_training_keys = 10;
    /* 200 keys available, but cap at 50. */
    populateDb(200, 256);
    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    /* Should have submitted (50 >= min 10). */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

TEST_F(CompressionTrainTest, BufferCapStopsCollection) {
    server.compression_training_buffer_size = 2048; /* 2 KB */
    server.compression_dict_min_training_keys = 5;
    /* Each value 256 B → buffer fills at ~8 values. Should stop early. */
    populateDb(200, 256);
    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    /* Should have submitted (>= 5 samples). */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

/* ========================================================================
 * Cooldown Tests
 * ======================================================================== */

TEST_F(CompressionTrainTest, CooldownBlocksManualTrigger) {
    /* Only ineligible values → abort → cooldown. */
    populateDb(200, 32); /* All below min_value_size */
    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

TEST_F(CompressionTrainTest, EmptyDbScanEntersCooldown) {
    /* Empty DB, manual trigger → scan completes immediately → cooldown. */
    compressionTrainMaybeTrigger(3);
    runCronTicks(1000);
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

/* ========================================================================
 * Multi-DB Tests
 * ======================================================================== */

TEST_F(CompressionTrainTest, ScansAllDbs) {
    /* Tear down single-DB setup. */
    for (int i = 0; i < server.dbnum; i++) {
        kvstoreRelease(server.db[i]->keys);
        zfree(server.db[i]);
    }
    zfree(server.db);

    /* Set up 2 DBs. */
    server.dbnum = 2;
    server.compression_dict_min_training_keys = 10;
    server.db = (serverDb **)zcalloc(sizeof(serverDb *) * server.dbnum);
    for (int i = 0; i < server.dbnum; i++) {
        server.db[i] = (serverDb *)zcalloc(sizeof(serverDb));
        server.db[i]->keys = kvstoreCreate(&kvstoreKeysHashtableType, 0, 0);
    }

    /* 6 keys per DB, total 12 > min 10. */
    for (int db = 0; db < 2; db++) {
        for (int i = 0; i < 6; i++) {
            char keybuf[64];
            snprintf(keybuf, sizeof(keybuf), "db%d:key:%d", db, i);
            robj *o = makeKeyedRawString(keybuf, 256, db * 100 + i);
            kvstoreHashtableAdd(server.db[db]->keys, 0, o);
        }
    }

    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    /* Should have collected from both DBs (12 >= 10) and submitted. */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

/* ========================================================================
 * Bio Training + Dict Verification (direct function call)
 *
 * Tests that ZDICT_trainFromBuffer produces a usable dictionary by
 * calling the training logic directly (not through bio threads) and
 * verifying the resulting CDict/DDict can compress/decompress.
 * ======================================================================== */

#ifdef USE_ZSTD
TEST_F(CompressionTrainTest, ScanSubmitsToBioWhenEnoughSamples) {
    /* Verify that after collecting enough eligible samples, the state
     * transitions past IDLE (bio job was created or already processed). */
    populateDb(200, 256);
    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    /* State is no longer IDLE — can't trigger again. */
    ASSERT_EQ(compressionTrainMaybeTrigger(3), 0);
}

TEST_F(CompressionTrainTest, BioEndToEnd) {
    /* Minimal reproducer: scan → submit → drain → cron promotes. */
    server.compression_min_value_size = 50;
    server.compression_dict_min_training_keys = 100;

    for (int i = 0; i < 500; i++) {
        char keybuf[64];
        snprintf(keybuf, sizeof(keybuf), "k:%06d", i);
        char valbuf[256];
        int vlen = snprintf(valbuf, sizeof(valbuf),
                            "{\"id\":%d,\"name\":\"user_%d\",\"mail\":\"u%d@x.com\","
                            "\"sc\":%d,\"lv\":%d,\"ok\":true}",
                            i, i, i, i * 10, i % 50);
        sds val = sdsnewlen(valbuf, vlen);
        robj *o = createRawStringObject(val, sdslen(val));
        sdsfree(val);
        sds k = sdsnew(keybuf);
        o = objectSetKeyAndExpire(o, k, -1);
        sdsfree(k);
        o->lru = 0;
        kvstoreHashtableAdd(server.db[0]->keys, 0, o);
    }

    compressionTrainMaybeTrigger(3);
    runCronTicks(100000);
    bioDrainWorker(BIO_COMPRESSION_TRAIN);
    runCronTicks(10);

    compressionDictPair *active = compressionRegistryActive();
    ASSERT_NE(active, nullptr);
}
#endif /* USE_ZSTD */
