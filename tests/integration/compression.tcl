# Copyright (c) Valkey Contributors
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
#
# Real-time in-memory value compression — integration stress tests.
#
# These tests exercise the merged S2.x stack end-to-end against a real
# workload: write-path enqueue → worker pool encode → outbox drain →
# read-path transient view + restore → sweeper engine → registry. Each
# test imports a runtime-trained dict via tests/support/compression-
# helpers.tcl (PR-A's infrastructure), avoiding any static fixture.
#
# Skipped under BUILD_ZSTD=no (the gen-zstd-dict helper isn't built and
# the server-side feature returns disabled stubs).
#
# Design of record:
#   .agents/planning/realtime-data-compression/design/detailed-design.md
#   §7.6 (test infrastructure rationale)
#   §2.1 (master switch + sweeper mechanics)
#   §2.4–§2.5 (write/read path)

# Asserts that `compression_errors_total` (R2.10.1) is 0 — i.e. no
# decode/decompress/worker errors occurred during the test. Real gate
# now that S4.1 wired the counter to live state (the renderer reads
# compressionGetErrorsTotal()).
proc assert_no_compression_errors {} {
    set status [r compression status]
    if {![regexp {compression_errors_total:(\d+)} $status _ errs]} {
        fail "compression_errors_total field missing from INFO"
    }
    if {$errs != 0} {
        fail "compression_errors_total reported $errs > 0"
    }
}

# Polls OBJECT ENCODING on `key` until it matches `expected_encoding`,
# fails the test on timeout. The polling cadence (50ms × maxtries)
# gives the server's event loop time to drain the worker outbox and
# run afterSleep / cron between observations.
#
# Per-key truth via OBJECT ENCODING (rather than the population-level
# `compression_compressed_objects` counter) — for these tests we want
# to assert "this specific key reached state X", not "≥N keys are
# compressed". The counter does exist in INFO post-S4.1 and could
# back a population-level wait helper if a future test needs it.
proc wait_for_encoding {key expected_encoding {maxtries 200} {delay 50}} {
    wait_for_condition $maxtries $delay {
        [r object encoding $key] eq $expected_encoding
    } else {
        fail "key '$key' did not reach encoding '$expected_encoding' within timeout (last: '[r object encoding $key]')"
    }
}

# Counts how many of the listed keys currently report OBJECT ENCODING
# == $encoding. Useful when the test wants to assert that "most" of a
# population was compressed by the sweeper, without enumerating
# every key individually.
proc count_keys_with_encoding {keys encoding} {
    set n 0
    foreach k $keys {
        if {[r object encoding $k] eq $encoding} {
            incr n
        }
    }
    return $n
}

# Polls until at least `target` of the listed keys are at `encoding`.
proc wait_for_at_least_n_keys_with_encoding {keys target encoding {maxtries 200} {delay 50}} {
    wait_for_condition $maxtries $delay {
        [count_keys_with_encoding $keys $encoding] >= $target
    } else {
        set actual [count_keys_with_encoding $keys $encoding]
        fail "only $actual of [llength $keys] keys reached encoding '$encoding' (target $target)"
    }
}

# Mirror, for "decompression drain" tests — at most `target` keys
# remain at the given encoding.
proc wait_for_at_most_n_keys_with_encoding {keys target encoding {maxtries 200} {delay 50}} {
    wait_for_condition $maxtries $delay {
        [count_keys_with_encoding $keys $encoding] <= $target
    } else {
        set actual [count_keys_with_encoding $keys $encoding]
        fail "$actual of [llength $keys] keys still at encoding '$encoding' (max-target $target)"
    }
}

# Parse a single numeric INFO-compression field. Fails if absent.
proc compression_info_int {field} {
    set status [r compression status]
    if {![regexp [format {%s:(\-?\d+)} $field] $status _ v]} {
        fail "$field missing from COMPRESSION STATUS"
    }
    return $v
}

# Parse a single string INFO-compression field. Fails if absent.
proc compression_info_str {field} {
    set status [r compression status]
    if {![regexp [format {%s:([^\r\n]+)} $field] $status _ v]} {
        fail "$field missing from COMPRESSION STATUS"
    }
    return $v
}

# Reliably reach "no active dict" with room in the registry for a new
# one. This is the precondition for the first-training auto-trigger and
# for any test that wants to install a fresh dict from a known state.
#
# Mechanic:
#   1. flushall — drops all keys, so every compressed frame is gone and
#      the frame counters (compressed_objects, total_*_bytes) read 0.
#   2. compression-dict-max-versions = 16 — guarantees headroom so a
#      subsequent train/import is never refused by the registry cap,
#      regardless of how many dicts prior tests left behind.
#   3. master=decompression — auto-retires the active dict (R2.1.5), so
#      compression_active_dict_id converges to 0. (Retiring dicts that
#      are still frame-referenced, or awaiting QSBR reclamation, may
#      linger in known_dicts; we deliberately do NOT depend on the
#      registry draining all the way to empty — only on the active
#      pointer clearing, which is reliable.)
#
# Reliable in --external shared-server mode: it depends only on the
# active-pointer-clears-on-decompression invariant, not on prior state.
proc compression_clear_active_dict {} {
    r flushall
    r config set compression-dict-max-versions 16
    r config set compression-master-switch decompression
    r config set compression-automatic-sweeper enabled
    r config set compression-sweep-max-cpu-pct 100
    wait_for_condition 200 50 {
        [compression_info_int compression_active_dict_id] == 0
    } else {
        fail "active dict did not clear after master=decompression (active=[compression_info_int compression_active_dict_id])"
    }
}

start_server {tags {"compression"}} {
    if {![file exists "tests/helpers/gen-zstd-dict"]} {
        # Whole file unusable without the helper. Use a single skip
        # stub so runtest reports the situation clearly under
        # BUILD_ZSTD=no rather than failing each individual test.
        test {compression integration tests skipped under BUILD_ZSTD=no} {
            skip "BUILD_ZSTD=no — gen-zstd-dict helper not built"
        }
    } else {    # Install a kv-shaped dict at suite startup. The same dict is used
    # by every test below; tests don't need different data shapes for
    # this PR (drift / retraining tests will land alongside S1.x).
    set startup_samples [gen_kv_samples 200 42]
    set dict_id [import_dict $startup_samples]
    if {$dict_id <= 0} {
        fail "import_dict returned non-positive id ($dict_id)"
    }

    test {Write-path round-trip: SET → worker drain → OBJECT ENCODING reports compressed} {
        # Isolate the write path: master=compression but sweeper
        # disabled, so the only enqueue source is signalModifiedKey
        # (R2.4.1's write-path producer hook).
        r config set compression-master-switch compression
        r config set compression-automatic-sweeper disabled
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0

        # Compressible content: dictionary was trained on kv-shaped
        # samples, so kv-shaped values get good ratios.
        set value "user_id=alice;status=active;region=us-east;plan=premium;city=seattle"
        # Pad to make sure we exceed compression-min-value-size and
        # produce meaningful compression.
        for {set i 0} {$i < 10} {incr i} {
            append value ";note$i=text${i}_padded_so_compression_actually_runs"
        }

        r del k1
        r set k1 $value
        wait_for_encoding k1 compressed

        # Round-trip: GET decompresses transparently via the read-path
        # transient view; bytes must match.
        assert_equal $value [r get k1]
        assert_no_compression_errors
    }

    test {Sweeper compresses pre-existing keys when flipped to compression mode} {
        r flushall
        r config set compression-master-switch off
        r config set compression-automatic-sweeper disabled

        # Populate uncompressed: master=off → write path is a no-op.
        # Each value padded to comfortably exceed 128 bytes so it's
        # RAW (embstr threshold per shouldEmbedStringObject).
        set N 100
        set base "session=anonymous;ip=127.0.0.1;cookie=blank;ua=tcl-test;trace=integration-stress;path=/index/page/section/checkpoint;"
        # Track expected values so we can verify EVERY key round-trips
        # at the end (not just spot-check a handful).
        set expected_vals [dict create]
        for {set i 0} {$i < $N} {incr i} {
            set v "$base;index=$i;hash=[string repeat x [expr {64 + $i % 16}]]"
            r set "k$i" $v
            dict set expected_vals $i $v
            lappend keys "k$i"
        }
        assert_equal $N [r dbsize]
        # Verify ALL keys are RAW before we engage compression — proves
        # the population is uniformly uncompressed at the starting line.
        for {set i 0} {$i < $N} {incr i} {
            assert_equal "raw" [r object encoding "k$i"]
        }

        # Flip to compression + enable sweeper. Speed up the sweep so
        # the test doesn't take forever — 100% pacing means the cron
        # runs the full per-tick budget without sleeping.
        r config set compression-master-switch compression
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0
        r config set compression-sweep-max-cpu-pct 100
        r config set compression-automatic-sweeper enabled

        # The sweeper iterates the whole keyspace in passes spaced by
        # compression-automatic-sweeper-interval (default 0 = single
        # pass on direction change). On direction-change the apply
        # hook resets the cursor, so this single pass should compress
        # the full population.
        wait_for_at_least_n_keys_with_encoding $keys $N compressed

        # Verify EVERY key round-trips cleanly.
        for {set i 0} {$i < $N} {incr i} {
            assert_equal [dict get $expected_vals $i] [r get "k$i"]
        }
        assert_no_compression_errors
    }

    test {master=decompression with sweeper drains compressed keys back to RAW} {
        # Continue from the compressed state of the previous test.
        # Flip to decompression — master-switch apply hook auto-retires
        # the active dict (R2.1.5) and the sweeper now per-key calls
        # compressionPermanentlyDecompress on every compressed value.
        set N 100
        # Reconstruct expected values (test 2's values; same formula).
        set base "session=anonymous;ip=127.0.0.1;cookie=blank;ua=tcl-test;trace=integration-stress;path=/index/page/section/checkpoint;"
        set expected_vals [dict create]
        set keys {}
        for {set i 0} {$i < $N} {incr i} {
            lappend keys "k$i"
            dict set expected_vals $i \
                "$base;index=$i;hash=[string repeat x [expr {64 + $i % 16}]]"
        }

        r config set compression-master-switch decompression
        r config set compression-automatic-sweeper enabled
        r config set compression-sweep-max-cpu-pct 100

        wait_for_at_most_n_keys_with_encoding $keys 0 compressed

        # EVERY key must be RAW now and must round-trip.
        for {set i 0} {$i < $N} {incr i} {
            assert_equal "raw" [r object encoding "k$i"]
            assert_equal [dict get $expected_vals $i] [r get "k$i"]
        }
        assert_no_compression_errors
    }

    test {COMPRESSION SWEEP FORCE compresses keys when automatic sweeper is disabled} {
        r flushall
        # Need a fresh active dict — the previous test retired ours
        # by flipping to decompression mode.
        set startup_samples [gen_kv_samples 200 42]
        set new_dict_id [import_dict $startup_samples]
        if {$new_dict_id <= 0} {
            fail "import_dict returned non-positive id"
        }

        r config set compression-master-switch off
        r config set compression-automatic-sweeper disabled

        # Populate uncompressed. Pad each value to >128 bytes so it's
        # RAW-encoded (tryObjectEncoding embeds anything <=128 bytes
        # total including robj+sds overhead — see shouldEmbedStringObject).
        set N 50
        set base "force-sweep-test;trace=force;cookie=session;path=/index/page/section;client_id=tcl-integration-stress-suite;"
        set expected_vals [dict create]
        set keys {}
        for {set i 0} {$i < $N} {incr i} {
            set v "$base;n=$i;pad=[string repeat z [expr {120 + $i % 8}]]"
            r set "fs$i" $v
            dict set expected_vals $i $v
            lappend keys "fs$i"
        }
        # Verify ALL keys are RAW before we engage the force sweep —
        # proves the population is uniformly uncompressed.
        for {set i 0} {$i < $N} {incr i} {
            assert_equal "raw" [r object encoding "fs$i"]
        }

        # Flip to compression with sweeper STILL disabled.
        # Then issue an explicit FORCE — should compress the keyspace
        # in a single pass.
        r config set compression-master-switch compression
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0
        r config set compression-sweep-max-cpu-pct 100
        r compression sweep force

        wait_for_at_least_n_keys_with_encoding $keys $N compressed

        # EVERY key round-trips.
        for {set i 0} {$i < $N} {incr i} {
            assert_equal [dict get $expected_vals $i] [r get "fs$i"]
        }
        assert_no_compression_errors
    }

    test {Mixed workload preserves data integrity with live sweeper} {
        r flushall
        # Fresh dict.
        set new_dict_id [import_dict [gen_kv_samples 200 42]]
        if {$new_dict_id <= 0} {
            fail "import_dict returned non-positive id"
        }

        r config set compression-master-switch compression
        r config set compression-min-value-size 32
        r config set compression-min-idle-seconds 0
        r config set compression-sweep-max-cpu-pct 50
        r config set compression-automatic-sweeper enabled

        # Populate base set. Values are large enough (>= 200 chars
        # before any extension) to stay RAW under tryObjectEncoding's
        # 128-byte embstr threshold even after a few APPEND operations.
        set N 200
        set ops 500
        set pad_len 200
        # Track expected value per key as a Tcl dict so we can verify
        # round-trip. Using `dict` instead of `array set` avoids a
        # name-shadowing pitfall with the test framework's locals.
        set expected_vals [dict create]
        for {set i 0} {$i < $N} {incr i} {
            set v "kv;k=$i;data=[string repeat a $pad_len]"
            dict set expected_vals $i $v
            r set "mw$i" $v
        }

        # Mixed ops with a deterministic LCG (compression_test::rng_*).
        # Each branch exercises a distinct read- or write-path code
        # path that the compression feature must handle correctly:
        #
        #   GET       → read-path transient view materialize + restore
        #   SET       → write-path enqueue + outbox install (overwrite)
        #   APPEND    → mutating write that triggers permanent decompress
        #               on a compressed value (R2.7.6 path via
        #               dbUnshareStringValue)
        #   SETRANGE  → another mutating write, distinct code path from
        #               APPEND in t_string.c (in-place mutation seam)
        #
        # DEL is intentionally NOT in the mix here: it doesn't read or
        # write value bytes (only the kvstore unlink), so it isn't
        # exercising any compression-specific code path; including it
        # would also reduce the population of keys we can validate at
        # the end. The "delete a compressed key works" case lives in
        # its own focused test below.
        ::compression_test::rng_seed "mw" 12345
        for {set step 0} {$step < $ops} {incr step} {
            set i [::compression_test::rng_int "mw" 0 [expr {$N - 1}]]
            set k "mw$i"
            switch [::compression_test::rng_int "mw" 0 3] {
                0 {
                    # GET — round-trip check. Every key in the population
                    # is always present (no DEL in the mix), so we can
                    # assert unconditionally.
                    assert_equal [dict get $expected_vals $i] [r get $k]
                }
                1 {
                    # Overwrite with a different compressible value.
                    set v "kv;k=$i;rewrite=$step;data=[string repeat b $pad_len]"
                    r set $k $v
                    dict set expected_vals $i $v
                }
                2 {
                    # APPEND — value extends; round-trip must still hold.
                    set suffix ";+$step"
                    r append $k $suffix
                    dict set expected_vals $i \
                        "[dict get $expected_vals $i]$suffix"
                }
                3 {
                    # SETRANGE — overwrite a fixed-position run with
                    # known bytes. Targets bytes that exist in every
                    # populated value so we can predict the result
                    # exactly.
                    set patch "ZZZZZZZZ"
                    set offset 6 ;# overwrites "k=NN;da" portion
                    r setrange $k $offset $patch
                    set v [dict get $expected_vals $i]
                    set v [string replace $v $offset \
                               [expr {$offset + [string length $patch] - 1}] \
                               $patch]
                    dict set expected_vals $i $v
                }
            }
        }

        # Final sweep through all $N keys (no deletions in the workload):
        # every one must round-trip exactly.
        for {set i 0} {$i < $N} {incr i} {
            set v [dict get $expected_vals $i]
            set actual [r get "mw$i"]
            if {$actual ne $v} {
                fail "key mw$i mismatch: expected '[string range $v 0 80]...' got '[string range $actual 0 80]...'"
            }
        }
        assert_no_compression_errors
    }

    test {Ineligibility: values outside the size envelope and hot keys are NOT compressed} {
        # Verify the eligibility predicate (R2.2) gates correctly:
        #   - Below compression-min-value-size  → ineligible (too small).
        #   - Above compression-max-value-size  → ineligible (too large).
        #   - lru_idle_secs(obj) < compression-min-idle-seconds → ineligible (hot).
        # Ensure NONE of these get compressed even with the sweeper
        # working at maximum cadence.
        r flushall
        set new_dict_id [import_dict [gen_kv_samples 200 42]]
        if {$new_dict_id <= 0} {
            fail "import_dict returned non-positive id"
        }

        # Tight size bounds so the test can exercise both ends with
        # short, predictable inputs. min=64 / max=512.
        r config set compression-master-switch compression
        r config set compression-min-value-size 64
        r config set compression-max-value-size 512
        # Set a strict idle threshold so a freshly-written key is
        # explicitly "hot" by the predicate (idle_secs < 5).
        r config set compression-min-idle-seconds 5
        r config set compression-sweep-max-cpu-pct 100
        r config set compression-automatic-sweeper enabled

        # Below-min: 50 bytes — under 64-byte floor.
        # NOTE: also under embstr threshold (128 bytes total robj+sds),
        # so OBJECT ENCODING reports "embstr" rather than "raw". The
        # ineligibility check is "did NOT become compressed", regardless
        # of which non-compressed encoding it ended up at.
        set tiny [string repeat "a" 50]
        r set "ineligible:tiny" $tiny

        # Above-max: 600 bytes — over 512-byte ceiling.
        set huge [string repeat "x" 600]
        r set "ineligible:huge" $huge

        # Hot key: just-written 200-byte value (in-bounds for size)
        # but freshly-touched, so idle_secs=0 < 5. Sweeper must skip.
        set hot_value "kv;k=hot;data=[string repeat h 180]"
        r set "ineligible:hot" $hot_value

        # Eligible control: 200 bytes, in-bounds. To make it eligible
        # under min-idle-seconds=5 we need to age it; CONFIG-set the
        # threshold back to 0 just for this control key after the
        # sweeper has had time to act. Actually simpler: just verify
        # the control gets compressed once we lower min-idle-seconds.
        set eligible_value "kv;k=eligible;data=[string repeat e 180]"
        r set "ineligible:control" $eligible_value

        # Trigger a force pass so we don't depend on timer cadence.
        # (master is `compression`; FORCE is allowed.)
        r compression sweep force

        # Wait long enough for the sweep + worker to complete, then
        # assert: the three ineligible keys MUST NOT be compressed.
        # We can't easily prove "the sweep finished" from outside;
        # use a fixed delay (200ms = 2 sweep cycles at default hz=10).
        after 200

        # Tiny: should be embstr or raw — never compressed.
        # (assert_match uses glob, not regex; check both options
        # with an explicit OR.)
        set enc [r object encoding "ineligible:tiny"]
        if {$enc ne "embstr" && $enc ne "raw"} {
            fail "tiny key got encoded as '$enc' (expected embstr or raw)"
        }
        assert_equal $tiny [r get "ineligible:tiny"]

        # Huge: should be raw (over embstr threshold), never compressed.
        assert_equal "raw" [r object encoding "ineligible:huge"]
        assert_equal $huge [r get "ineligible:huge"]

        # Hot key: should still be raw — sweeper skipped it because
        # idle_secs < min-idle-seconds.
        assert_equal "raw" [r object encoding "ineligible:hot"]
        assert_equal $hot_value [r get "ineligible:hot"]

        # Control: lower the idle threshold to 0 and FORCE another
        # pass. This must compress the control key, proving the
        # mechanism is working — without this proof we can't be sure
        # the sweeper isn't broken in a way that just-happens to skip
        # everything.
        r config set compression-min-idle-seconds 0
        r compression sweep force
        wait_for_encoding "ineligible:control" compressed
        assert_equal $eligible_value [r get "ineligible:control"]
        assert_no_compression_errors
    }

    test {INFO compression fields reflect live state through compress / decompress lifecycle} {
        # Deterministic clean baseline: empty registry, no active dict.
        # (Reliable in --external shared-server mode too — see
        # compression_clear_active_dict.) Then install exactly one dict so
        # the productive-state assertions below have something to
        # compress against.
        compression_clear_active_dict
        set did [import_dict [gen_kv_samples 200 42]]
        if {$did <= 0} { fail "import_dict returned non-positive id ($did)" }

        # ---- Baseline: master=off → state==disabled, no frames yet ----
        r flushall
        r config set compression-master-switch off
        r config set compression-automatic-sweeper disabled
        assert_equal "disabled" [compression_info_str compression_state]
        assert_equal 0 [compression_info_int compression_compressed_objects]
        assert_equal 0 [compression_info_int compression_total_uncompressed_bytes]
        assert_equal 0 [compression_info_int compression_total_compressed_bytes]
        assert_equal 0 [compression_info_int compression_net_saved_bytes]

        # ---- Populate compressible content + flip to compression ----
        set N 50
        set base "session=anonymous;ip=127.0.0.1;cookie=blank;ua=tcl-test;trace=info-fields;path=/index/page/section;"
        for {set i 0} {$i < $N} {incr i} {
            r set "ifk$i" "$base;index=$i;hash=[string repeat x [expr {64 + $i % 16}]]"
        }

        r config set compression-master-switch compression
        r config set compression-min-value-size 32
        r config set compression-max-value-size 0
        r config set compression-min-idle-seconds 0
        r config set compression-sweep-max-cpu-pct 100
        r config set compression-automatic-sweeper enabled

        # Wait for population to compress, then verify the fields.
        set keys {}
        for {set i 0} {$i < $N} {incr i} { lappend keys "ifk$i" }
        wait_for_at_least_n_keys_with_encoding $keys $N compressed

        # State == "active" (master=compression + active dict).
        assert_equal "active" [compression_info_str compression_state]

        # Every one of the N keys is compressed → counter == N exactly.
        assert_equal $N [compression_info_int compression_compressed_objects]
        set unc [compression_info_int compression_total_uncompressed_bytes]
        set comp [compression_info_int compression_total_compressed_bytes]
        if {$unc <= 0} { fail "compression_total_uncompressed_bytes=$unc not > 0" }
        if {$comp <= 0} { fail "compression_total_compressed_bytes=$comp not > 0" }
        if {$comp >= $unc} { fail "compressed ($comp) should be < uncompressed ($unc)" }

        # net_saved = unc - comp, exact relation.
        set net_saved [compression_info_int compression_net_saved_bytes]
        assert_equal [expr {$unc - $comp}] $net_saved

        # compression_ratio = compressed/uncompressed, rendered as a
        # 4-decimal float. For our compressible kv-shaped content it is
        # strictly in (0, 1): it can only be 0 when there are no
        # compressed frames (total_uncompressed == 0, the divide-by-zero
        # guard in the renderer), which is not the case here since we
        # just confirmed N frames exist; and it is < 1 because the
        # net-savings guard rejects anything that doesn't actually save.
        set status [r compression status]
        if {![regexp {compression_ratio:([0-9.]+)} $status _ ratio_str]} {
            fail "compression_ratio missing from COMPRESSION STATUS"
        }
        if {$ratio_str <= 0.0 || $ratio_str >= 1.0} {
            fail "compression_ratio=$ratio_str outside (0, 1) for compressible content"
        }

        # candidates_pending is a sampled gauge whose exact value races
        # with the worker drain — there is no reliable way to pin it to
        # a specific number from the client without synchronization the
        # server doesn't expose. We assert only that the field is
        # present and non-negative (its lifecycle is covered by the
        # back-pressure gtests in src/unit/).
        if {![regexp {compression_candidates_pending:([0-9]+)} $status _ pending]} {
            fail "compression_candidates_pending missing from COMPRESSION STATUS"
        }

        assert_equal 0 [compression_info_int compression_errors_total]

        # ---- Flip to decompression: state → idle; sweep drains ----
        # State transitions to idle (active dict was retired per R2.1.5).
        r config set compression-master-switch decompression
        wait_for_condition 50 50 {
            [compression_info_str compression_state] eq "idle"
        } else {
            fail "state did not transition to idle after master=decompression"
        }

        # Wait for drain — every key back to RAW.
        wait_for_at_most_n_keys_with_encoding $keys 0 compressed

        # All compressed buffers released → counters back to zero.
        wait_for_condition 50 50 {
            [compression_info_int compression_compressed_objects] == 0
        } else {
            fail "compression_compressed_objects did not return to 0 after drain"
        }
        assert_equal 0 [compression_info_int compression_total_uncompressed_bytes]
        assert_equal 0 [compression_info_int compression_total_compressed_bytes]
        assert_equal 0 [compression_info_int compression_net_saved_bytes]
        assert_equal 0 [compression_info_int compression_errors_total]
    }

    test {compression_skipped_incompressible counts net-savings-guard rejections} {
        # Deterministic precondition: clean registry, then install
        # exactly one dict so the worker has something to compress
        # against (the net-savings guard only runs after a compression
        # attempt, which requires an active dict).
        compression_clear_active_dict
        set did [import_dict [gen_kv_samples 200 42]]
        if {$did <= 0} { fail "import_dict returned non-positive id ($did)" }

        r flushall
        set before [compression_info_int compression_skipped_incompressible]

        r config set compression-master-switch compression
        r config set compression-min-value-size 256
        r config set compression-max-value-size 0
        r config set compression-min-idle-seconds 0
        r config set compression-sweep-max-cpu-pct 100
        r config set compression-automatic-sweeper enabled

        # Generate high-entropy bytes deterministically from
        # /dev/urandom (available on every CI host we target).
        # 30 keys × 1 KB each; each blob has zero patterns so ZSTD
        # with a dict can't beat the default 10% net-savings ratio →
        # every one is rejected by the post-compression guard.
        # NB: max-value-size is set to 0 (unbounded) above because a
        # prior test in this shared server may have lowered it below
        # 1 KB, which would make these keys ineligible (too large).
        set rng [open "/dev/urandom" rb]
        set N 30
        for {set i 0} {$i < $N} {incr i} {
            r set "rng:$i" [read $rng 1024]
        }
        close $rng

        # The sweeper attempts all N keys; each rejection increments
        # the counter. Wait until it has risen by at least N (every
        # key rejected). Using ">= before+N" rather than just
        # "> before" makes the assertion exact about the expected
        # number of rejections, not merely "something was rejected".
        wait_for_condition 200 50 {
            [compression_info_int compression_skipped_incompressible] >= ($before + $N)
        } else {
            set now [compression_info_int compression_skipped_incompressible]
            fail "compression_skipped_incompressible=$now, expected >= [expr {$before + $N}] ($N rejections on top of baseline $before)"
        }

        # Every rng:* key must remain RAW (rejection leaves the value
        # uncompressed).
        for {set i 0} {$i < $N} {incr i} {
            assert_equal "raw" [r object encoding "rng:$i"]
        }

        assert_no_compression_errors
    }
    } ;# end if helper exists
}

# ============================================================================
# Tests for fields that need controlled registry state
# ----------------------------------------------------------------------------
# These two fields need a deterministic registry baseline:
#   - compression_training_last_duration_ms / _sample_count → need NO active
#     dict so the first-training auto-trigger can fire.
#   - compression_dict_cap_reached → needs a known, stable dict count.
#
# Both reach their preconditions via compression_clear_active_dict (drain to
# empty) rather than depending on a fresh process — so they're deterministic
# in --external shared-server mode too.
# ============================================================================

start_server {tags {"compression"}} {
    if {![file exists "tests/helpers/gen-zstd-dict"]} {
        test {compression INFO controlled-state tests skipped under BUILD_ZSTD=no} {
            skip "BUILD_ZSTD=no — gen-zstd-dict helper not built"
        }
    } else {
    test {compression_training_last_*: populated after an auto-training run} {
        # The first-training auto-trigger (evaluateTriggers in
        # compression_train.c) fires when ALL hold:
        #   1. master=compression
        #   2. no active dict (compressionRegistryActive() == NULL)
        #   3. totalDbKeys() >= compression-dict-min-training-keys
        #   4. registry has room (count < compression-dict-max-versions)
        #
        # compression_clear_active_dict guarantees (2) and (4) by draining
        # the registry to empty — reliable regardless of prior tests
        # (including --external shared-server mode). We then satisfy
        # (1) and (3) and wait for the promotion.
        compression_clear_active_dict

        # Lower the training-keys threshold so we needn't populate
        # 1000 keys; lower min-value-size so all our values qualify.
        r config set compression-dict-min-training-keys 50
        r config set compression-dict-max-training-keys 200
        r config set compression-min-value-size 32
        r config set compression-max-value-size 0
        r config set compression-min-idle-seconds 0

        # Exactly 100 compressible kv-style values, each ≥256 bytes →
        # guaranteed RAW (not EMBSTR; shouldEmbedStringObject in
        # object.c embeds when robj+key+sds total ≤128 bytes). flushall
        # already ran inside compression_clear_active_dict, so these are
        # the only keys → the training scan collects exactly 100.
        set N 100
        for {set i 0} {$i < $N} {incr i} {
            set v "session=anonymous;ip=127.0.0.1;cookie=blank;trace=train-test;index=$i;hash=[string repeat x 200]"
            r set "tk:$i" $v
        }

        r config set compression-master-switch compression
        r config set compression-automatic-sweeper enabled
        r config set compression-sweep-max-cpu-pct 100

        # Wait for training to complete. The completion path registers
        # a fresh dict via compressionRegistryAdd → active pointer
        # swap, then snapshots last_duration_ms / last_sample_count,
        # all in one main-thread cron tick.
        wait_for_condition 200 100 {
            [compression_info_int compression_active_dict_id] > 0
        } else {
            # Surface registry state so a regression is self-diagnosing.
            set m [compression_info_str compression_master_switch]
            set st [compression_info_str compression_state]
            set kd [compression_info_int compression_known_dicts]
            fail "training did not produce an active dict within timeout (master=$m state=$st known=$kd dbsize=[r dbsize])"
        }

        # sample_count is exact: we populated exactly 100 eligible keys
        # and the per-scan cap (200) is well above that, so the scan
        # collects all 100.
        assert_equal 100 [compression_info_int compression_training_last_sample_count]

        # duration spans scan-start → bio-completion (a bio round trip
        # across at least one cron cycle), so it is strictly positive.
        set dur [compression_info_int compression_training_last_duration_ms]
        if {$dur <= 0} {
            fail "compression_training_last_duration_ms=$dur not > 0 after training"
        }

        assert_no_compression_errors
    }

    test {compression_dict_cap_reached: reflects known_dicts vs cap, both directions} {
        # compression_dict_cap_reached is a pure derived boolean:
        # (known_dicts >= compression-dict-max-versions). We verify the
        # relationship in both directions by reading the live
        # known_dicts and moving the cap around it — rather than
        # assuming a specific count, which isn't controllable in
        # --external shared-server mode where prior tests leave dicts
        # behind.
        #
        # We first guarantee at least 2 stable, non-reclaimable dicts so
        # the count can't drain out from under the assertions:
        #   - dict A made active, then pinned by compressing frames that
        #     reference it (frame_refs > 0 → never GC'd while the frames
        #     live);
        #   - dict B imported on top, so A retires but stays pinned and
        #     B is the (never-GC'd) active dict.
        compression_clear_active_dict

        r config set compression-min-value-size 32
        r config set compression-max-value-size 0
        r config set compression-min-idle-seconds 0
        r config set compression-sweep-max-cpu-pct 100
        r config set compression-dict-max-versions 16

        # dict A, then pin it with compressed frames.
        set a [import_dict [gen_kv_samples 200 11]]
        if {$a <= 0} { fail "import dict A returned non-positive id ($a)" }
        r config set compression-master-switch compression
        r config set compression-automatic-sweeper enabled
        set keys {}
        for {set i 0} {$i < 20} {incr i} {
            r set "capk$i" "kv;k=$i;data=[string repeat a 200]"
            lappend keys "capk$i"
        }
        wait_for_at_least_n_keys_with_encoding $keys 20 compressed

        # dict B — A retires (pinned, won't drain), B active.
        set b [import_dict [gen_kv_samples 200 22]]
        if {$b <= 0} { fail "import dict B returned non-positive id ($b)" }

        # Let known_dicts settle: any reclaimable leftover dicts drain
        # via the cron GC; the pinned A + active B (and any genuinely
        # stuck residue) remain. "Settled" == 3 consecutive equal reads.
        set prev -1
        set stable 0
        for {set t 0} {$t < 100 && $stable < 3} {incr t} {
            after 50
            set k [compression_info_int compression_known_dicts]
            if {$k == $prev} { incr stable } else { set stable 0; set prev $k }
        }
        set K [compression_info_int compression_known_dicts]
        if {$K < 2} {
            fail "expected known_dicts >= 2 after pinning A + active B, got $K"
        }
        # The cap config maxes out at 16; to exercise the "below cap"
        # direction we need headroom above K. K > 15 would mean the
        # registry is nearly full of un-reclaimable dicts — a real
        # problem worth surfacing, not silently skipping.
        if {$K > 15} {
            fail "known_dicts=$K leaves no headroom below the max cap (16); registry is not reclaiming dicts"
        }

        # Cap above the live count → flag 0.
        r config set compression-dict-max-versions 16
        assert_equal 0 [compression_info_int compression_dict_cap_reached]

        # Cap == the live count → flag 1 (known_dicts >= cap).
        r config set compression-dict-max-versions $K
        assert_equal 1 [compression_info_int compression_dict_cap_reached]

        # Cap back above the count → flag clears (proves it is derived
        # live each render, not latched once set).
        r config set compression-dict-max-versions 16
        assert_equal 0 [compression_info_int compression_dict_cap_reached]

        assert_no_compression_errors
    }
    } ;# end if helper exists
}
