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

    test {Below min keys: training does NOT trigger} {
        r flushall
        r config set compression-master-switch compression
        r config set compression-automatic-sweeper disabled
        r config set compression-dict-min-training-keys 500
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0

        # Fresh instance (external:skip): no dict, no scans yet.
        assert_equal 0 [get_active_dict_id]
        assert_equal 0 [getInfoProperty [r info compression] compression_training_scans_started]

        # Insert 50 eligible (RAW, padded) keys — below the 500 trigger.
        for {set i 0} {$i < 50} {incr i} {
            set val "user_id=$i;name=user_$i;email=user${i}@example.com;score=[expr {$i * 10}];level=[expr {$i % 50}];active=true;region=us-east;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value_and_not_embstr"
            r set "lowkey:$i" $val
        }

        # Give cron ~10 ticks (hz=10). The trigger is a synchronous
        # check; if it were going to fire it would on the first tick
        # after the keys land. scans_started staying 0 proves the
        # min-keys gate held.
        after 1000
        assert_equal 0 [getInfoProperty [r info compression] compression_training_scans_started]
        assert_equal 0 [get_active_dict_id]
    }

    test {Enough keys but none eligible: scan aborts, no dict} {
        r flushall
        # min-value-size 256 makes the small values below ineligible.
        r config set compression-dict-min-training-keys 50
        r config set compression-min-value-size 256

        # Insert 100 keys, all values well under 256 bytes (also EMBSTR).
        # Total keys (100) >= min (50) so the TRIGGER fires (no active
        # dict yet), but the scan collects 0 eligible samples → abort.
        for {set i 0} {$i < 100} {incr i} {
            r set "tiny:$i" "tiny_value_$i"
        }

        # Deterministic via metrics: a scan started (trigger fired) and
        # it failed (insufficient eligible samples). No dict promoted.
        wait_for_condition 200 50 {
            [getInfoProperty [r info compression] compression_training_scans_started] == 1 &&
            [getInfoProperty [r info compression] compression_training_failures] == 1
        } else {
            fail "expected one started+failed scan; got started=[getInfoProperty [r info compression] compression_training_scans_started] failures=[getInfoProperty [r info compression] compression_training_failures]"
        }
        assert_equal 0 [get_active_dict_id]
        # The abort armed a 30s cooldown (hardcoded). The next test
        # waits it out before first-training can fire.
    }

    test {First-training fires across multiple DBs and produces a dict} {
        r flushall
        r config set compression-dict-min-training-keys 100
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0

        # Split eligible keys across db0 and db1 — each below min alone,
        # together (120) over the 100 trigger. Exercises both the
        # first-training trigger AND multi-DB scan iteration.
        r select 0
        for {set i 0} {$i < 60} {incr i} {
            set val "db0_user=$i;name=user_$i;email=user${i}@example.com;score=[expr {$i * 10}];level=[expr {$i % 50}];active=true;region=eu-west;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value"
            r set "db0key:$i" $val
        }
        r select 1
        for {set i 0} {$i < 60} {incr i} {
            set val "db1_user=$i;name=user_$i;email=user${i}@example.com;score=[expr {$i * 10}];level=[expr {$i % 50}];active=true;region=ap-south;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value"
            r set "db1key:$i" $val
        }
        r select 0

        # Generous timeout (60s): must absorb the remainder of the 30s
        # cooldown armed by the previous test, then scan + bio + promote.
        wait_for_trained_dict 600 100
        assert {[get_active_dict_id] > 0}
        assert_equal 1 [getInfoProperty [r info compression] compression_training_successes]
    }

    test {After training, new writes get compressed} {
        # Dict active from the previous test.
        assert {[get_active_dict_id] > 0}

        set val "user_id=999;name=user_999;email=user999@example.com;score=9990;level=49;active=true;region=us-west;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value_and_not_embstr"
        r set "newkey" $val

        # Write-path hook enqueues → worker compresses → drain installs.
        wait_for_encoding "newkey" compressed
        assert_equal $val [r get "newkey"]
    }

    test {Active dict blocks a new first-training (no retrain while active)} {
        # With an active dict present, first-training must NOT fire
        # again regardless of key count — the trigger is gated on
        # (!active). Drift / refresh triggers are not yet wired.
        assert {[get_active_dict_id] > 0}
        set dict_before [get_active_dict_id]
        set scans_before [getInfoProperty [r info compression] compression_training_scans_started]

        r flushall
        r config set compression-dict-min-training-keys 50
        r config set compression-min-value-size 32

        # Insert plenty of eligible keys (>= min) — would trigger
        # first-training if no dict were active.
        for {set i 0} {$i < 200} {incr i} {
            set val "user_id=$i;name=user_$i;email=user${i}@example.com;score=[expr {$i * 10}];level=[expr {$i % 50}];active=true;region=us-east;padding=this_is_extra_padding_to_ensure_raw_encoding_is_used_for_this_value_and_not_embstr"
            r set "again:$i" $val
        }

        # Let cron run several ticks.
        after 2000

        # No new scan started, dict unchanged — active dict suppressed it.
        assert_equal $scans_before [getInfoProperty [r info compression] compression_training_scans_started]
        assert_equal $dict_before [get_active_dict_id]
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
