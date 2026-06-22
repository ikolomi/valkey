# Copyright (c) Valkey Contributors
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
#
# Real-time in-memory value compression — COW-invariant merge-blocker test.
#
# Design of record:
#   .agents/planning/realtime-data-compression/design/detailed-design.md
#   §2.4 R2.4.4 (immutable-snapshot invariant)
#   §2.4 R2.4.5 (per-site COW audit checklist — this test is its runtime form)
#   §2.5 R2.5.7 (transient-view / permanent-decompress read+write hooks)
#   §7.2 (compression-cow-invariant.tcl listed as a merge blocker)
#
# What this guards
# ----------------
# The feature relies on two invariants for correctness when a value is, or
# is being, compressed:
#
#   (1) Decompress-before-mutate. lookupKey(...,LOOKUP_WRITE) permanently
#       decompresses any OBJ_ENCODING_COMPRESSED value to RAW before the
#       command handler runs (compressionPermanentlyDecompress, db.c). So no
#       in-place byte-mutating command (APPEND, SETRANGE, SETBIT, BITFIELD
#       SET/INCRBY, ...) ever operates on a compressed frame. If this
#       invariant ever broke, the handler would either mutate frame bytes as
#       if they were the logical string (silent corruption) or hit
#       getDecodedObject()'s serverPanic("Unknown encoding type") — COMPRESSED
#       is neither sdsEncodedObject nor INT.
#
#   (2) Worker-snapshot immutability (R2.4.4). While a background worker reads
#       a value's sds bytes (job->src), the value is pinned (incrRefCount →
#       refcount>=2); a concurrent in-place mutation must therefore COW via
#       dbUnshareStringValue, leaving the worker's original bytes untouched.
#
# Both invariants are code-discipline, not type-enforced. This test exercises
# them at runtime: it mutates values that ARE compressed and values that are
# being churned by the live worker pool, asserting the result always matches
# the value semantics computed independently in Tcl, and that no
# decode/worker error is ever recorded. A regression in either invariant
# surfaces as a wrong value, a crash, or compression_errors_total > 0.
#
# Skipped under BUILD_ZSTD=no (the gen-zstd-dict helper isn't built and the
# server-side feature returns disabled stubs).
#
# Tagged external:skip — this test deliberately churns global compression
# state (master switch, sweeper, an imported dictionary, compressed frames)
# and cannot restore a pristine registry afterward (a dict that ever held
# frames is not reliably reclaimable today). Against a shared --external
# server it would pollute state for sibling compression tests (e.g.
# unit/type/compression's documented-defaults assertions) and leave
# compressed values that crash kvstore-direct readers such as DEBUG DIGEST.
# Its COW-correctness value is fully delivered in normal (dedicated-server)
# mode, which is the primary CI mode.

# --- local helpers (file-scoped; the integration suite keeps its own copies
#     so the two files stay independently runnable) ---

# Fails the test if compression_errors_total (R2.10.1) is non-zero.
proc cow_assert_no_errors {} {
    set status [r compression status]
    if {![regexp {compression_errors_total:(\d+)} $status _ errs]} {
        fail "compression_errors_total field missing from COMPRESSION STATUS"
    }
    if {$errs != 0} {
        fail "compression_errors_total reported $errs > 0 (COW invariant likely violated)"
    }
}

# Polls OBJECT ENCODING until it matches, fails on timeout.
proc cow_wait_encoding {key expected {maxtries 200} {delay 50}} {
    wait_for_condition $maxtries $delay {
        [r object encoding $key] eq $expected
    } else {
        fail "key '$key' did not reach encoding '$expected' (last: '[r object encoding $key]')"
    }
}

# A compressible value: kv-shaped text (the suite dict is kv-trained) with a
# caller-controlled leading byte so bit/byte-addressed mutations have a known
# expected result. Padded well past compression-min-value-size so the value
# is stored RAW (not EMBSTR) and is compression-eligible.
proc cow_value {{lead_byte 0}} {
    set v [binary format c $lead_byte]
    append v "user_id=alice;status=active;region=us-east;plan=premium;city=seattle;"
    for {set i 0} {$i < 12} {incr i} {
        append v "attr$i=value_${i}_padded_so_the_blob_is_well_over_min_size;"
    }
    return $v
}

# Put `key` into the compressed state from a clean RAW write of `val`.
# Uses COMPRESSION SWEEP FORCE (the deterministic, operator-facing
# trigger) rather than relying on the one-shot automatic sweeper pass
# or write-path enqueue timing — matches the integration suite's
# pattern and is robust across the many keys this file churns.
proc cow_set_compressed {key val} {
    r del $key
    r set $key $val
    r compression sweep force
    cow_wait_encoding $key compressed
}

# Deterministically re-compress a (post-mutation) value and verify it
# still round-trips to `expected`. Uses COMPRESSION SWEEP FORCE so the
# re-compression doesn't depend on async signalModifiedKey-enqueue
# timing — this file tests mutation CORRECTNESS, not the compression
# trigger mechanism (the latter is covered by the integration suite's
# write-path test).
proc cow_recompress_and_verify {key expected} {
    r compression sweep force
    cow_wait_encoding $key compressed
    assert_equal $expected [r get $key]
    cow_assert_no_errors
}

start_server {tags {"compression" "external:skip"}} {
    if {![file exists "tests/helpers/gen-zstd-dict"]} {
        test {compression COW-invariant tests skipped under BUILD_ZSTD=no} {
            skip "BUILD_ZSTD=no — gen-zstd-dict helper not built"
        }
    } else {
        # In --external shared-server mode this file runs against the same
        # server as the integration suite, so the registry may already hold
        # dicts (and the frame-ref leak tracked for the registry owner means
        # frame-holding dicts don't retire). Clear keys and guarantee
        # headroom before importing, so the import is never refused by the
        # compression-dict-max-versions cap.
        r flushall
        r config set compression-dict-max-versions 16
        # Install a kv-shaped dict once for the whole file.
        set dict_id [import_dict [gen_kv_samples 200 42]]
        if {$dict_id <= 0} {
            fail "import_dict returned non-positive id ($dict_id)"
        }

        # Aggressive config so every eligible value compresses promptly and
        # nothing is skipped for being "hot". Each test re-asserts the full
        # set it depends on (no reliance on inherited state — matters in
        # --external shared-server mode).
        proc cow_configure {} {
            r config set compression-master-switch compression
            r config set compression-automatic-sweeper enabled
            r config set compression-sweep-max-cpu-pct 100
            r config set compression-threads 1
            r config set compression-min-value-size 32
            r config set compression-max-value-size 0
            r config set compression-min-idle-seconds 0
            r config set compression-lfu-threshold 255
        }

        test {APPEND on a compressed value decompresses then mutates correctly} {
            cow_configure
            set base [cow_value]
            cow_set_compressed k $base
            r append k "_APPENDED_SUFFIX"
            assert_equal "${base}_APPENDED_SUFFIX" [r get k]
            cow_assert_no_errors
            # Post-mutation value is itself compressible: it must round-trip
            # after re-compression too.
            cow_recompress_and_verify k "${base}_APPENDED_SUFFIX"
        }

        test {SETRANGE on a compressed value decompresses then mutates correctly} {
            cow_configure
            set base [cow_value]
            cow_set_compressed k $base
            r setrange k 5 "XYZ"
            assert_equal [string replace $base 5 7 "XYZ"] [r get k]
            cow_assert_no_errors
            cow_recompress_and_verify k [string replace $base 5 7 "XYZ"]
        }

        test {SETBIT on a compressed value decompresses then mutates correctly} {
            cow_configure
            # Leading byte 0x00 → setting bit 7 (LSB of byte 0) yields 0x01,
            # leaving the rest of the value byte-identical. Deterministic
            # whole-value expectation.
            set base [cow_value 0]
            cow_set_compressed k $base
            assert_equal 0 [r getbit k 7]
            r setbit k 7 1
            assert_equal 1 [r getbit k 7]
            set expected [cow_value 1]
            assert_equal $expected [r get k]
            assert_equal [string length $base] [r strlen k]
            cow_assert_no_errors
            cow_recompress_and_verify k $expected
        }

        test {BITFIELD write on a compressed value decompresses then mutates correctly} {
            cow_configure
            set base [cow_value 0]
            cow_set_compressed k $base
            # SET u8 at bit offset 0 returns the previous byte-0 value (0).
            assert_equal {0} [r bitfield k set u8 0 200]
            assert_equal {200} [r bitfield k get u8 0]
            set expected [cow_value 200]
            assert_equal $expected [r get k]
            cow_assert_no_errors
            cow_recompress_and_verify k $expected
            assert_equal {200} [r bitfield k get u8 0]
        }

        test {GETSET on a compressed value returns old bytes intact, installs new} {
            cow_configure
            set base [cow_value]
            cow_set_compressed k $base
            set newval [cow_value 7]
            assert_equal $base [r getset k $newval]
            assert_equal $newval [r get k]
            cow_assert_no_errors
            cow_recompress_and_verify k $newval
        }

        test {GETDEL on a compressed value returns bytes intact, then removes} {
            cow_configure
            set base [cow_value]
            cow_set_compressed k $base
            assert_equal $base [r getdel k]
            assert_equal 0 [r exists k]
            cow_assert_no_errors
        }

        test {SET overwrite of a compressed value installs fresh value} {
            cow_configure
            set base [cow_value]
            cow_set_compressed k $base
            set newval [cow_value 9]
            r set k $newval
            assert_equal $newval [r get k]
            cow_assert_no_errors
            cow_recompress_and_verify k $newval
        }

        # R2.4.4 worker-snapshot race: repeatedly mutate a key while the live
        # worker pool + sweeper churn it. Each iteration the value is (or is
        # racing toward) compressed; the mutation must COW off any pinned
        # worker snapshot. We verify the value semantics hold every iteration
        # and no decode/worker error is ever recorded. The companion ASan run
        # (see the PR's verification) covers the memory-safety dimension of
        # the same race.
        test {mutation storm against live worker pool preserves correctness} {
            cow_configure
            set expected [cow_value]
            r del k
            r set k $expected
            for {set i 0} {$i < 200} {incr i} {
                # Alternate the producer: SET-overwrite (replace value) and
                # APPEND (in-place mutate → dbUnshareStringValue COW). Both
                # enqueue a fresh compression job via signalModifiedKey.
                if {$i % 2 == 0} {
                    set expected [cow_value [expr {$i % 256}]]
                    r set k $expected
                } else {
                    set suffix "#$i"
                    r append k $suffix
                    append expected $suffix
                }
                # Read back immediately — exercises the read path (transient
                # view) racing the just-enqueued compression job.
                assert_equal $expected [r get k]
            }
            cow_assert_no_errors
            # Let it settle compressed, final round-trip.
            cow_recompress_and_verify k $expected
        }

        # Defense-in-depth: a value that compressed while cold, then is read
        # (transient view materialized) and mutated in the SAME logical
        # sequence, must still mutate correctly. Catches a regression where
        # the write-path permanent-decompress and the read-path transient
        # view interfere on the same key.
        test {read (transient view) followed by in-place mutation is correct} {
            cow_configure
            set base [cow_value]
            cow_set_compressed k $base
            assert_equal $base [r get k]      ;# materialize transient view
            r append k "_TAIL"                ;# write path on same key
            assert_equal "${base}_TAIL" [r get k]
            cow_assert_no_errors
            cow_recompress_and_verify k "${base}_TAIL"
        }
    }
}
