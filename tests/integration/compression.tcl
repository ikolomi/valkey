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

# TODO(S4.x): assert_no_compression_errors is currently vacuous —
# `compression_errors_total` is hardcoded to 0 in compressionRenderFields
# (the comment there says "stays 0 until later S2 PRs land their
# counters"). Until that wiring lands, this helper only catches the
# pathological case where the field disappears entirely from INFO.
# The intent is preserved so each test calls it as a final gate;
# when the counter is wired, this becomes a real correctness assertion
# without test code changes.
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
# TODO(S4.x): used in lieu of a per-server compressed-object counter
# (`compression_compressed_objects` in INFO is hardcoded to 0 until
# that S4.x ticket wires it). When the counter goes live we can keep
# this helper for per-key truth or migrate to a counter-based wait
# for population-level assertions.
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
    } ;# end if helper exists
}
