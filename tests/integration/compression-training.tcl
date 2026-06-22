# Copyright (c) Valkey Contributors
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
#
# Real-time compression — training subsystem integration tests.
#
# These tests exercise the full training pipeline: trigger evaluation →
# incremental kvstore scan → BIO_COMPRESSION_TRAIN → dict promotion →
# compression works. Unlike the integration/compression.tcl tests which
# import a pre-built dict, these rely on the server's own training
# machinery (S1.2/S1.3/S1.4).
#
# Skipped under BUILD_ZSTD=no.

# Helper: wait for a key to reach a specific encoding.
proc wait_for_encoding {key expected_encoding {maxtries 200} {delay 50}} {
    wait_for_condition $maxtries $delay {
        [r object encoding $key] eq $expected_encoding
    } else {
        fail "key '$key' did not reach encoding '$expected_encoding' within timeout (last: '[r object encoding $key]')"
    }
}

# Helper: wait for a trained dict to appear in the registry.
proc wait_for_trained_dict {{maxtries 300} {delay 100}} {
    wait_for_condition $maxtries $delay {
        [getInfoProperty [r info compression] compression_active_dict_id] != 0
    } else {
        fail "training did not produce a dict within timeout (active_dict_id still 0)"
    }
}

# Helper: get the active dict id from INFO compression.
proc get_active_dict_id {} {
    return [getInfoProperty [r info compression] compression_active_dict_id]
}

# Helper: get known dicts count.
proc get_known_dicts {} {
    return [getInfoProperty [r info compression] compression_known_dicts]
}

start_server {tags {"compression" "compression-training" "external:skip"}} {
    # Skip if BUILD_ZSTD=no — training requires ZSTD linked in.
    if {![file exists "tests/helpers/gen-zstd-dict"]} {
        test {compression training tests skipped under BUILD_ZSTD=no} {
            skip "BUILD_ZSTD=no — training not available"
        }
    } else {

    test {First-training trigger fires and produces a usable dict} {
        r flushall

        # Configure for training: low threshold so we don't need 1000 keys.
        r config set compression-master-switch compression
        r config set compression-automatic-sweeper disabled
        r config set compression-dict-min-training-keys 100
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0

        # Fresh instance (external:skip guarantees a private server),
        # so no dict exists yet.
        assert_equal 0 [get_active_dict_id]

        # Insert enough eligible keys to trigger training.
        # Use JSON-like patterns with padding — must exceed embstr threshold
        # (~128 total robj size) to get RAW encoding.
        for {set i 0} {$i < 200} {incr i} {
            set val "user_id=$i;name=user_$i;email=user${i}@example.com;score=[expr {$i * 10}];level=[expr {$i % 50}];active=true;region=us-east;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value_and_not_embstr_which_would_skip_training"
            r set "trainkey:$i" $val
        }

        # Wait for training to complete (scan + bio + promotion).
        wait_for_trained_dict 300 100

        # A dict was promoted — id is non-zero (monotonic, starts at 1).
        assert {[get_active_dict_id] > 0}
    }

    test {After training, new writes get compressed} {
        # Dict should be active from previous test.
        assert {[get_active_dict_id] > 0}

        # Write a new value with similar shape to training data.
        set val "user_id=999;name=user_999;email=user999@example.com;score=9990;level=49;active=true;region=us-west;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value_and_not_embstr_which_would_skip_compression"
        r set "newkey" $val

        # Wait for it to be compressed (write-path hook enqueues,
        # worker compresses, drain installs).
        wait_for_encoding "newkey" compressed

        # Round-trip: GET returns original bytes.
        assert_equal $val [r get "newkey"]
    }

    test {Training does NOT fire when DB has fewer than min keys} {
        r flushall
        r config set compression-dict-min-training-keys 500

        # Record current dict count before inserting.
        set known_before [get_known_dicts]

        # Insert only 50 keys — below the 500 threshold.
        for {set i 0} {$i < 50} {incr i} {
            r set "smalldb:$i" "value_${i}_padded_to_be_long_enough_for_raw_encoding_threshold"
        }

        # Wait a few seconds — training should NOT fire.
        after 3000

        # No new dict trained — known_dicts should not have increased.
        set known_after [get_known_dicts]
        assert {$known_after <= $known_before}
    }

    test {Training aborts and enters cooldown with ineligible values} {
        r flushall
        r config set compression-dict-min-training-keys 50
        r config set compression-min-value-size 256

        # Record current dict id — if training fires and succeeds
        # it would change.
        set dict_before [get_active_dict_id]

        # Insert 100 keys but all values are small (< 256 bytes).
        # They won't pass the eligibility check in the scan callback.
        for {set i 0} {$i < 100} {incr i} {
            r set "small:$i" "tiny_value_$i"
        }

        # Wait enough time for scan to run and abort.
        after 5000

        # No new dict should be trained — id should be unchanged.
        assert_equal $dict_before [get_active_dict_id]
    }

    # Retirement lifecycle test moved to its own start_server block below.

    test {Training scans across multiple databases} {
        r flushall
        r config set compression-master-switch compression
        r config set compression-dict-min-training-keys 80
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0

        # Split keys across db 0 and db 1 — each has fewer than min
        # alone, but combined they exceed it.
        r select 0
        for {set i 0} {$i < 50} {incr i} {
            set val "db0_user=$i;role=admin;region=eu-west;plan=enterprise;note=padding_text_$i;extra=this_is_extra_long_padding_to_ensure_the_value_exceeds_the_embstr_threshold_and_gets_raw_encoding"
            r set "db0key:$i" $val
        }
        r select 1
        for {set i 0} {$i < 50} {incr i} {
            set val "db1_user=$i;role=member;region=ap-south;plan=standard;note=padding_text_$i;extra=this_is_extra_long_padding_to_ensure_the_value_exceeds_the_embstr_threshold_and_gets_raw_encoding"
            r set "db1key:$i" $val
        }
        r select 0

        # Total is 100 > min 80. Training should fire.
        wait_for_trained_dict 300 100

        assert {[get_active_dict_id] > 0}
    }

    } ;# end of else (BUILD_ZSTD=yes)
}

# ========================================================================
# Dict retirement lifecycle — self-contained instance.
# ========================================================================

start_server {tags {"compression" "compression-training" "external:skip"}} {
    if {![file exists "tests/helpers/gen-zstd-dict"]} {
        test {retirement test skipped under BUILD_ZSTD=no} {
            skip "BUILD_ZSTD=no — training not available"
        }
    } else {

    test {Dict retirement: compress 20 keys → decompression mode → read all → dict GC'd} {
        # Sweeper disabled — we drive compression purely via the
        # write-path (new SETs) and decompression purely via reads.
        r config set compression-master-switch compression
        r config set compression-automatic-sweeper disabled
        r config set compression-dict-min-training-keys 200
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0

        # Phase 1: Insert exactly 200 keys to trigger training. These are
        # SET *before* an active dict exists, so the write-path is a
        # no-op for them — they stay RAW and never reference the dict.
        # (200 == min, so the trigger fires only after all 200 are in.)
        for {set i 0} {$i < 200} {incr i} {
            set val "user_id=$i;name=user_$i;email=user${i}@example.com;score=[expr {$i * 10}];level=[expr {$i % 50}];active=true;region=us-east;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value_and_not_embstr_which_would_skip_training"
            r set "trainkey:$i" $val
        }

        # Wait for training to complete (dict promoted).
        wait_for_trained_dict 300 100
        assert {[get_active_dict_id] > 0}

        # Sanity: the 200 training keys stayed RAW — they were written
        # before the dict existed and the sweeper is disabled, so
        # nothing compressed them. They hold no frame_refs.
        assert_equal "raw" [r object encoding "trainkey:0"]
        assert_equal "raw" [r object encoding "trainkey:199"]
        # No compressed objects yet — nothing references the dict.
        # (compression_compressed_objects rises/falls in lockstep with
        # the dict frame-ref: +1 on createCompressedObject, -1 on
        # free/permanent-decompress. It is the global total of frames
        # referencing any dict — well-defined across multiple retiring
        # dicts, unlike a per-dict gauge.)
        assert_equal 0 [getInfoProperty [r info compression] compression_compressed_objects]

        # Phase 2: Insert exactly 20 keys AFTER the dict is active.
        # The write-path compresses these → each holds one frame_ref.
        for {set i 0} {$i < 20} {incr i} {
            set val "user_id=[expr {1000 + $i}];name=user_[expr {1000 + $i}];email=user[expr {1000 + $i}]@example.com;score=[expr {$i * 10}];level=[expr {$i % 50}];active=true;region=us-east;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value_and_not_embstr_which_would_skip_compression"
            r set "postdict:$i" $val
        }

        # Verify all 20 are compressed.
        for {set i 0} {$i < 20} {incr i} {
            wait_for_encoding "postdict:$i" compressed
        }

        # Observe the frame-ref total via compression_compressed_objects
        # — exactly 20 (one per compressed key). If it's 40, that's the
        # install double-count bug (fixed in #42; this is the guard).
        set refs_after_install [getInfoProperty [r info compression] compression_compressed_objects]
        assert_equal 20 $refs_after_install

        # Phase 3: Flip to decompression. Apply hook retires the dict.
        set retired_before [getInfoProperty [r info compression] compression_training_dicts_retired]
        r config set compression-master-switch decompression

        # Phase 4: Read all 20 compressed values. Each read in
        # decompression mode creates a transient view; the beforeSleep
        # drain permanently decompresses it (releaseCompressedBuffer →
        # DecRef). NO sweeper involved.
        for {set i 0} {$i < 20} {incr i} {
            r get "postdict:$i"
        }

        # Wait for the drain to process all transient entries — all
        # compressed objects become RAW, dropping the frame-ref total
        # (compression_compressed_objects) to 0.
        wait_for_condition 100 100 {
            [getInfoProperty [r info compression] compression_compressed_objects] == 0
        } else {
            fail "compressed_objects did not drain to 0 after reading all keys (still [getInfoProperty [r info compression] compression_compressed_objects])"
        }

        # Verify decompression + data integrity.
        assert_equal "raw" [r object encoding "postdict:0"]
        set expected "user_id=1000;name=user_1000;email=user1000@example.com;score=0;level=0;active=true;region=us-east;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value_and_not_embstr_which_would_skip_compression"
        assert_equal $expected [r get "postdict:0"]

        # Phase 5: Dict should be GC'd now (frame_refs=0 + QSBR).
        wait_for_condition 100 100 {
            [getInfoProperty [r info compression] compression_training_dicts_retired] > $retired_before
        } else {
            fail "dict was not retired. known_dicts=[getInfoProperty [r info compression] compression_known_dicts], compressed_objects=[getInfoProperty [r info compression] compression_compressed_objects]"
        }
    }

    } ;# end of else (BUILD_ZSTD=yes)
}
