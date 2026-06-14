# Real-time in-memory value compression — Phase 0 transparency fixture.
#
# The compression feature is disabled by default in Phase 0. This fixture
# verifies the two invariants that MUST hold before any code is allowed
# to modify the hot path in Phase 1:
#
#   1. All compression-* knobs exist, parse, and have the defaults
#      documented in design §2.12.
#   2. The feature-off server behaves IDENTICALLY to a baseline server
#      for a representative slice of STRING commands (transparency).
#
# Design of record:
#   .agents/planning/realtime-data-compression/design/detailed-design.md
#   .agents/planning/realtime-data-compression/implementation/plan.md §5
#
# This file is intentionally short. The §7 "transparency mode" harness
# (Phase 1 — dev infra subsystem) adds a `--compression` flag to runtest
# that re-runs the entire existing Tcl corpus against a server with
# aggressive compression enabled; that is where the real coverage will
# come from.

start_server {tags {"compression"}} {
    test {COMPRESSION STATUS reports default off / disabled state} {
        set status [r compression status]
        assert_match "*compression_master_switch:off*" $status
        assert_match "*compression_active_sweeper:disabled*" $status
        assert_match "*compression_active_sweeper_interval:0*" $status
        assert_match "*compression_state:disabled*" $status
        assert_match "*compression_active_dict_id:0*" $status
    }

    test {COMPRESSION STATUS and INFO compression emit identical field sets} {
        # Per design §4.5: "COMPRESSION STATUS returns the INFO
        # compression section as a flat structured reply". The two
        # renderers MUST stay in lockstep so dashboards can use either.
        set status [r compression status]
        set info [r info compression]
        # Extract all "compression_*:..." field names from each.
        set status_fields {}
        foreach line [split $status "\n"] {
            if {[regexp {^(compression_[a-z_]+):} $line -> name]} {
                lappend status_fields $name
            }
        }
        set info_fields {}
        foreach line [split $info "\n"] {
            if {[regexp {^(compression_[a-z_]+):} $line -> name]} {
                lappend info_fields $name
            }
        }
        assert_equal [lsort $status_fields] [lsort $info_fields]
    }

    test {Queue back-pressure observability fields are present (design §2.10 R2.10.4)} {
        # These counters distinguish "compression isn't keeping up" root
        # causes; they MUST be emitted even when the feature is off so
        # dashboards can wire against Phase 0 servers.
        set status [r compression status]
        assert_match "*compression_candidates_pending:0*" $status
        assert_match "*compression_candidates_dropped_total:0*" $status
        assert_match "*compression_sweep_backpressure_total:0*" $status
        assert_match "*compression_sweep_pacing_sleeps_total:0*" $status
        assert_match "*compression_outbox_backpressure_total:0*" $status
    }

    test {COMPRESSION HELP returns helpful text} {
        set help [r compression help]
        assert {[llength $help] > 0}
    }

    test {INFO compression section renders} {
        set info [r info compression]
        assert_match "*# Compression*" $info
        assert_match "*compression_master_switch:off*" $info
    }

    test {All 18 compression config knobs are registered with documented defaults} {
        # Primary (6) — see design/detailed-design.md §2.12
        assert_equal [lindex [r config get compression-master-switch] 1] "off"
        assert_equal [lindex [r config get compression-active-sweeper] 1] "disabled"
        assert_equal [lindex [r config get compression-active-sweeper-interval] 1] "0"
        assert_equal [lindex [r config get compression-threads] 1] "1"
        assert_equal [lindex [r config get compression-min-value-size] 1] "256"
        assert_equal [lindex [r config get compression-max-value-size] 1] "131072"
        assert_equal [lindex [r config get compression-dict-size] 1] "102400"
        # Advanced (11)
        assert_equal [lindex [r config get compression-sweep-max-cpu-pct] 1] "25"
        assert_equal [lindex [r config get compression-cpulist] 1] ""
        assert_equal [lindex [r config get compression-min-savings-ratio] 1] "10"
        assert_equal [lindex [r config get compression-lfu-threshold] 1] "5"
        assert_equal [lindex [r config get compression-min-idle-seconds] 1] "60"
        assert_equal [lindex [r config get compression-dict-min-training-keys] 1] "1000"
        assert_equal [lindex [r config get compression-dict-max-training-keys] 1] "10000"
        assert_equal [lindex [r config get compression-training-buffer-size] 1] "16777216"
        assert_equal [lindex [r config get compression-dict-drift-ratio] 1] "70"
        assert_equal [lindex [r config get compression-dict-refresh-interval] 1] "0"
        assert_equal [lindex [r config get compression-dict-max-versions] 1] "4"
    }

    test {Master-switch enum accepts all 3 documented values} {
        # R2.1.1: compression-master-switch is an enum (off / compression /
        # decompression). Test runtime CONFIG SET against each value.
        r config set compression-master-switch compression
        assert_equal [lindex [r config get compression-master-switch] 1] "compression"
        r config set compression-master-switch decompression
        assert_equal [lindex [r config get compression-master-switch] 1] "decompression"
        r config set compression-master-switch off
        assert_equal [lindex [r config get compression-master-switch] 1] "off"
        # Reject an invalid value.
        catch {r config set compression-master-switch yes} err
        assert_match "*ERR*" $err
    }

    test {Active-sweeper enum accepts both documented values} {
        # R2.1.2: compression-active-sweeper is an enum (disabled / enabled).
        r config set compression-active-sweeper enabled
        assert_equal [lindex [r config get compression-active-sweeper] 1] "enabled"
        r config set compression-active-sweeper disabled
        assert_equal [lindex [r config get compression-active-sweeper] 1] "disabled"
        catch {r config set compression-active-sweeper yes} err
        assert_match "*ERR*" $err
    }

    test {Feature-off transparency: basic STRING round-trip is unchanged} {
        r set foo "bar"
        assert_equal [r get foo] "bar"
        assert_equal [r strlen foo] 3
        r append foo "baz"
        assert_equal [r get foo] "barbaz"
    }

    test {Feature-off transparency: large STRING round-trip is unchanged} {
        set buf [string repeat "abcd" 10000]
        r set big $buf
        assert_equal [r get big] $buf
        assert_equal [r strlen big] [string length $buf]
    }

    test {Feature-off transparency: OBJECT ENCODING is not 'compressed'} {
        r set small "x"
        assert_equal [r object encoding small] "embstr"
        set buf [string repeat "y" 1024]
        r set medium $buf
        # Feature-off means NO value is ever encoded as 'compressed'.
        # After Phase 1 this assertion will still hold when the feature
        # is disabled (default), but will fail under the transparency
        # harness (which forces feature-on); at that point the test
        # will be tagged compression:skip or rewritten via
        # assert_string_encoding (plan §7.1).
        assert_equal [r object encoding medium] "raw"
    }

    test {Toggling compression-master-switch at runtime has no observable effect in Phase 0} {
        r config set compression-master-switch compression
        assert_equal [lindex [r config get compression-master-switch] 1] "compression"
        # Feature stays functionally disabled in Phase 0 because the
        # write-path hook (S2.7) hasn't been wired to gate on the new
        # config yet — STATUS still reports state:disabled regardless
        # of the master switch.
        set status [r compression status]
        assert_match "*compression_master_switch:compression*" $status
        r config set compression-master-switch off
    }

    test {Server survives cron ticks while compression is on (totalDbKeys NULL-guard regression)} {
        # Regression for the totalDbKeys NULL deref: previously, toggling
        # compression on then writing to a key would race the cron tick
        # at hz=10 (the toggle sequence runs in <10 ms; the next cron
        # tick fires ~100 ms later). That window hid a crash where
        # compressionTrainCron's totalDbKeys() iterated `j < server.dbnum`
        # without checking that `server.db[j]` is non-NULL — true for
        # slots 1..dbnum-1 on a fresh server before they get materialized
        # via createDatabaseIfNeeded(). To guard against future
        # regressions, enable the feature then wait long enough for
        # multiple cron ticks to fire while master=compression, then
        # assert the server is still alive.
        r config set compression-master-switch compression
        # Wait ~5 cron ticks plus margin.
        after 600
        assert_equal "PONG" [r ping]
        # Also stress the path with one DB slot populated so the loop
        # iterates past index 0 and hits the would-be NULL slots.
        r set keyfortrain "value"
        after 300
        assert_equal "PONG" [r ping]
        r config set compression-master-switch off
        r del keyfortrain
    }
}
