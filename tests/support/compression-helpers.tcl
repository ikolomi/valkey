# Copyright (c) Valkey Contributors
# All rights reserved.
# SPDX-License-Identifier: BSD-3-Clause
#
# Compression test helpers — sample generators and dict trainers.
#
# These helpers let integration tests exercise the compression hot path
# without baking static dict fixtures into the repo. See design §7.6
# (Runtime dict-generation test infrastructure) for the rationale.
#
# Naming convention:
#   gen_<shape>_samples count seed [extra args]
#       Generates a list of N sample byte strings drawn from a shape-
#       specific distribution. Reproducible: same (count, seed) →
#       same output. Returns a Tcl list of binary strings.
#   gen_drifted_samples count seed shape_a shape_b drift
#       Mixes two shapes: `drift` ∈ [0,1] is the fraction of samples
#       drawn from shape_b instead of shape_a.
#   train_dict_from_samples samples
#       Pipes samples to tests/helpers/gen-zstd-dict, returns base64-
#       encoded dictionary bytes.
#   import_dict samples
#       Convenience: train_dict_from_samples | r compression dict-import.
#       Returns the assigned dict_id.

# ---------------------------------------------------------------
# Reproducible PRNG — Tcl's `expr {rand()}` is process-global; keep
# our own state so tests don't drift when run in different orders.
# ---------------------------------------------------------------

# Linear congruential generator. State is a single integer 0..2^31-1.
# Keyed by `name` so multiple parallel streams don't collide.
namespace eval ::compression_test {
    variable rng
    array set rng {}
}

proc ::compression_test::rng_seed {name seed} {
    variable rng
    set rng($name) [expr {$seed & 0x7fffffff}]
}

proc ::compression_test::rng_next {name} {
    variable rng
    if {![info exists rng($name)]} {
        error "unknown rng stream '$name' — call rng_seed first"
    }
    # Numerical Recipes LCG. 32-bit modulus; keep the result positive.
    set rng($name) [expr {($rng($name) * 1103515245 + 12345) & 0x7fffffff}]
    return $rng($name)
}

proc ::compression_test::rng_int {name lo hi} {
    set r [rng_next $name]
    return [expr {$lo + ($r % ($hi - $lo + 1))}]
}

proc ::compression_test::rng_pick {name list} {
    set i [rng_int $name 0 [expr {[llength $list] - 1}]]
    return [lindex $list $i]
}

proc ::compression_test::rng_random_word {name minlen maxlen} {
    set len [rng_int $name $minlen $maxlen]
    set chars "abcdefghijklmnopqrstuvwxyz0123456789"
    set out ""
    for {set i 0} {$i < $len} {incr i} {
        set k [rng_int $name 0 [expr {[string length $chars] - 1}]]
        append out [string index $chars $k]
    }
    return $out
}

# ---------------------------------------------------------------
# Sample shapes
# ---------------------------------------------------------------
#
# Each generator returns a list of N samples. The shapes are chosen to
# represent the most common compression-interesting workloads on
# valkey: structured key/value records, JSON-like documents, and
# log lines.

# `kv` shape — flat semicolon-delimited key=value pairs. Models small
# session/user records and similar.
proc gen_kv_samples {count seed} {
    set name "kv-$seed"
    ::compression_test::rng_seed $name $seed
    set keys {user_id session role region status created_at last_seen plan tier}
    set values {active inactive pending verified disabled trial premium standard locked}
    set samples [list]
    for {set i 0} {$i < $count} {incr i} {
        set n_pairs [::compression_test::rng_int $name 3 6]
        set parts [list]
        for {set j 0} {$j < $n_pairs} {incr j} {
            set k [::compression_test::rng_pick $name $keys]
            if {$k eq "status"} {
                set v [::compression_test::rng_pick $name $values]
            } else {
                set v [::compression_test::rng_random_word $name 4 12]
            }
            lappend parts "${k}=${v}"
        }
        lappend samples [join $parts ";"]
    }
    return $samples
}

# `json` shape — small JSON-shaped objects with mixed string/numeric
# values. Models DTO-style cached responses.
proc gen_json_samples {count seed} {
    set name "json-$seed"
    ::compression_test::rng_seed $name $seed
    set keys {id name email phone country city zip status role count price total}
    set samples [list]
    for {set i 0} {$i < $count} {incr i} {
        set n_fields [::compression_test::rng_int $name 3 7]
        set obj "{"
        for {set j 0} {$j < $n_fields} {incr j} {
            set k [::compression_test::rng_pick $name $keys]
            # ~70% string values, ~30% numeric.
            if {[::compression_test::rng_int $name 0 9] < 7} {
                set v "\"[::compression_test::rng_random_word $name 6 14]\""
            } else {
                set v [::compression_test::rng_int $name 1 99999]
            }
            append obj "\"$k\":$v"
            if {$j < [expr {$n_fields - 1}]} {append obj ","}
        }
        append obj "}"
        lappend samples $obj
    }
    return $samples
}

# `log` shape — timestamped log-line-style strings with a level token,
# a request ID, and a free-form message.
proc gen_log_samples {count seed} {
    set name "log-$seed"
    ::compression_test::rng_seed $name $seed
    set levels {INFO WARN ERROR DEBUG}
    set actions {request response retry timeout connect disconnect commit rollback}
    set samples [list]
    for {set i 0} {$i < $count} {incr i} {
        set ts_h [::compression_test::rng_int $name 0 23]
        set ts_m [::compression_test::rng_int $name 0 59]
        set ts_s [::compression_test::rng_int $name 0 59]
        set ts [format "%02d:%02d:%02d" $ts_h $ts_m $ts_s]
        set level [::compression_test::rng_pick $name $levels]
        set req_id [::compression_test::rng_random_word $name 8 16]
        set action [::compression_test::rng_pick $name $actions]
        set msg [::compression_test::rng_random_word $name 10 30]
        lappend samples "$ts \[$level\] req=$req_id action=$action msg=$msg"
    }
    return $samples
}

# Drift mixer — return `count` samples drawn from shape A with
# `drift` fraction replaced by samples from shape B. A/B are shape
# names ("kv" / "json" / "log"). drift ∈ [0,1].
proc gen_drifted_samples {count seed shape_a shape_b drift} {
    if {$drift < 0 || $drift > 1} {
        error "drift must be in \[0,1\], got $drift"
    }
    set n_b [expr {int($count * $drift)}]
    set n_a [expr {$count - $n_b}]
    set samples [concat \
        [gen_${shape_a}_samples $n_a $seed] \
        [gen_${shape_b}_samples $n_b [expr {$seed + 1}]]]
    return $samples
}

# ---------------------------------------------------------------
# Dict training via the external helper binary
# ---------------------------------------------------------------

# Call the gen-zstd-dict helper, returning the trained dict's raw bytes.
# Caller is responsible for base64-encoding (see train_dict_from_samples).
#
# Tests run with cwd == repo root (per runtest convention; see other
# uses of tests/helpers/*.tcl in tests/support/util.tcl), so the
# helper binary lives at a fixed relative path.
proc ::compression_test::helper_path {} {
    set helper "tests/helpers/gen-zstd-dict"
    if {![file exists $helper]} {
        error "gen-zstd-dict helper not found at $helper — did the build complete? (BUILD_ZSTD=yes)"
    }
    return $helper
}

# Send samples to the helper via stdin, read trained dict from a temp
# file, return the bytes.
proc train_dict_from_samples {samples} {
    if {[llength $samples] == 0} {
        error "no samples provided"
    }
    set helper [::compression_test::helper_path]
    set tmp_dict [tmpfile "compression-dict.bin"]

    # Open a pipe to the helper, write samples in the binary protocol.
    set pipe [open "|$helper $tmp_dict" "wb"]
    fconfigure $pipe -translation binary
    foreach s $samples {
        # Force binary representation of $s. Tcl strings can be either
        # text or binary; binary scan/format keep us in byte mode.
        set bytes [encoding convertto utf-8 $s]
        set len [string length $bytes]
        puts -nonewline $pipe [binary format "I" $len]
        puts -nonewline $pipe $bytes
    }
    if {[catch {close $pipe} err]} {
        if {[file exists $tmp_dict]} {file delete $tmp_dict}
        error "gen-zstd-dict failed: $err"
    }

    set f [open $tmp_dict "rb"]
    fconfigure $f -translation binary
    set dict_bytes [read $f]
    close $f
    file delete $tmp_dict

    if {[string length $dict_bytes] < 4} {
        error "trained dict is too small ([string length $dict_bytes] bytes) — corrupt?"
    }
    return $dict_bytes
}

# Convenience: train + base64-encode + COMPRESSION DICT-IMPORT.
# Returns the dict_id assigned by the registry.
proc import_dict {samples} {
    set raw [train_dict_from_samples $samples]
    set b64 [binary encode base64 -maxlen 0 $raw]
    return [r compression dict-import $b64]
}
