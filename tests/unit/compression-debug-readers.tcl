# Copyright Valkey Contributors.
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
#
# Real-time in-memory value compression — kvstore-direct reader transparency.
#
# Design of record:
#   .agents/planning/realtime-data-compression/design/detailed-design.md
#   §2.5 R2.5.2 (objectGetUncompressedView — the single decoder primitive)
#   §2.5 R2.5.7 (transient-view hook lives in lookupKey*)
#   §2.7 R2.7.1 (OBJECT ENCODING reports "compressed")
#
# What this guards
# ----------------
# A handful of read paths iterate the kvstore DIRECTLY (kvstoreIteratorNext /
# dbFind) and therefore bypass the lookupKey() transient-view decompression
# hook — they see the raw OBJ_ENCODING_COMPRESSED robj. The DEBUG DIGEST
# family is the notable one: it must work on logically-expired keys, so it
# deliberately uses dbFind rather than lookupKey.
#
# Before this guard, DEBUG DIGEST / DIGEST-VALUE fed a compressed value into
# getDecodedObject(), which panics on any encoding other than RAW/EMBSTR/INT
# (serverPanic("Unknown encoding type"), object.c) — crashing the server.
# The fix routes the digest helpers (mixStringObjectDigest /
# xorStringObjectDigest) through objectGetUncompressedView, the same single
# decoder primitive PR #24 used for rdbSaveStringObject / rioWriteBulkObject.
#
# The assertions are stronger than "does not crash": a dataset's digest must
# be IDENTICAL whether its values are stored compressed or uncompressed,
# because the digest is defined over the logical (decompressed) bytes. That
# is the transparency contract — the storage encoding must be invisible to
# DEBUG DIGEST, DEBUG DIGEST-VALUE, and DEBUG RELOAD round-trips.
#
# Skipped under BUILD_ZSTD=no (the gen-zstd-dict helper isn't built and the
# server-side feature returns disabled stubs).
#
# Tagged external:skip — like compression-cow-invariant.tcl, this test churns
# global compression state (master switch, sweeper, an imported dict,
# compressed frames) and cannot restore a pristine registry afterward, so it
# must not run against the shared --external server. Its value is fully
# delivered in normal (dedicated-server) mode, the primary CI mode.

# --- local helpers (file-scoped) ---

# Fails the test if compression_errors_total (R2.10.1) is non-zero.
proc dbg_assert_no_errors {} {
    set status [r compression status]
    if {![regexp {compression_errors_total:(\d+)} $status _ errs]} {
        fail "compression_errors_total field missing from COMPRESSION STATUS"
    }
    if {$errs != 0} {
        fail "compression_errors_total reported $errs > 0"
    }
}

# A compressible, kv-shaped value (the suite dict is kv-trained), padded well
# past compression-min-value-size so it is stored RAW and is eligible. `tag`
# differentiates values across keys.
proc dbg_value {tag} {
    set v "user_id=${tag};status=active;region=us-east;plan=premium;city=seattle;"
    for {set i 0} {$i < 12} {incr i} {
        append v "attr$i=value_${i}_padded_so_the_blob_is_well_over_min_size;"
    }
    return $v
}

# Polls OBJECT ENCODING for `key` until it equals `expected`.
proc dbg_wait_encoding {key expected {maxtries 200} {delay 50}} {
    wait_for_condition $maxtries $delay {
        [r object encoding $key] eq $expected
    } else {
        fail "key '$key' did not reach encoding '$expected' (last: '[r object encoding $key]')"
    }
}

# Force every key in `keys` into the compressed state (deterministic trigger).
# Imports a fresh active dict first: a prior test may have left master in
# decompression, which auto-retires the active dict (R2.1.5), leaving no dict
# to compress with (R2.1.7 third state). Re-importing makes each test
# independent of the order/state left by the previous one.
proc dbg_compress_all {keys} {
    import_dict [gen_kv_samples 200 42]
    r config set compression-master-switch compression
    r compression sweep force
    foreach k $keys { dbg_wait_encoding $k compressed }
}

# Drain every key in `keys` back to RAW via the operator decompress path
# (master=decompression + SWEEP FORCE decompresses on the main thread).
proc dbg_decompress_all {keys} {
    r config set compression-master-switch decompression
    r compression sweep force
    foreach k $keys { dbg_wait_encoding $k raw }
}

start_server {tags {"compression" "external:skip"}} {
    if {![file exists "tests/helpers/gen-zstd-dict"]} {
        test {compression DEBUG-reader tests skipped under BUILD_ZSTD=no} {
            skip "BUILD_ZSTD=no — gen-zstd-dict helper not built"
        }
    } else {
        r flushall
        r config set compression-dict-max-versions 16
        set dict_id [import_dict [gen_kv_samples 200 42]]
        if {$dict_id <= 0} {
            fail "import_dict returned non-positive id ($dict_id)"
        }

        # Aggressive eligibility so every kv value compresses promptly and
        # nothing is skipped for being "hot" or "too small".
        proc dbg_configure {} {
            r config set compression-automatic-sweeper enabled
            r config set compression-sweep-max-cpu-pct 100
            r config set compression-threads 1
            r config set compression-min-value-size 32
            r config set compression-max-value-size 0
            r config set compression-min-idle-seconds 0
            r config set compression-lfu-threshold 255
            r config set compression-min-savings-ratio 0
        }
        dbg_configure

        test {DEBUG DIGEST does not crash on compressed values and is encoding-transparent} {
            r flushall
            set keys {}
            for {set i 0} {$i < 20} {incr i} {
                set k "dk:$i"
                r set $k [dbg_value $i]
                lappend keys $k
            }

            dbg_compress_all $keys
            # DEBUG DIGEST iterates the kvstore directly (not via lookupKey),
            # so it observes the compressed robjs. Pre-fix this panicked.
            set digest_compressed [r debug digest]
            assert_equal "PONG" [r ping]
            dbg_assert_no_errors

            dbg_decompress_all $keys
            set digest_raw [r debug digest]

            # Transparency: identical digest regardless of storage encoding.
            assert_equal $digest_compressed $digest_raw
            dbg_assert_no_errors
        }

        test {DEBUG DIGEST-VALUE does not crash on a compressed key and is encoding-transparent} {
            r flushall
            r set single [dbg_value single]
            dbg_compress_all {single}

            set dv_compressed [r debug digest-value single]
            assert_equal "PONG" [r ping]

            dbg_decompress_all {single}
            set dv_raw [r debug digest-value single]

            assert_equal $dv_compressed $dv_raw
            dbg_assert_no_errors
        }

        test {DEBUG RELOAD round-trips compressed values (digest + values preserved)} {
            r flushall
            set keys {}
            array set expect {}
            for {set i 0} {$i < 20} {incr i} {
                set k "rk:$i"
                set v [dbg_value "reload$i"]
                r set $k $v
                set expect($k) $v
                lappend keys $k
            }
            dbg_compress_all $keys
            set digest_before [r debug digest]

            # RDB save decompresses on the fly (PR #24); reload restores RAW,
            # and the digest — over logical bytes — must be unchanged.
            r debug reload

            assert_equal $digest_before [r debug digest]
            foreach k $keys { assert_equal $expect($k) [r get $k] }
            assert_equal "PONG" [r ping]
            dbg_assert_no_errors
        }
    }
}
