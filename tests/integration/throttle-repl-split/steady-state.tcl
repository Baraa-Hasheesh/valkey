# Integration tests for replication throttle (throttle_repl.c).
#
# We drive the throttling by SIGSTOP-ing the replica,
# so its output buffer on the primary grows and never drains.

proc throttle_rate {r} {
    getInfoProperty [{*}$r info throttling] repl_throttle_rate
}

# Check whether the given client is currently throttled.
proc client_throttled {r wid} {
    set flags ""
    regexp {flags=(\S+)} [{*}$r CLIENT LIST ID $wid] -> flags
    string match {*h*} $flags
}

# Keep issuing writes until the writer client is observed being throttled.
proc wait_throttled_client {r writer wid} {
    for {set k 0} {$k < 1000} {incr k} {
        for {set j 0} {$j < 500} {incr j} {
            $writer set nudge v
        }
        if {[client_throttled $r $wid] &&
            [getInfoProperty [{*}$r info debug] repl_throttle_current_clients] > 0} {
            return 1
        }
    }
    return 0
}

# Set up primary/replica replication with throttling enabled and a COB limit configured.
proc setup_throttle_replication {primary replica primary_host primary_port} {
    $primary replicaof no one
    $primary flushall
    $primary config set repl-throttling-enabled yes
    $primary config set repl-backlog-size 1mb
    $primary config set client-output-buffer-limit "replica 1024mb 1mb 3600"
    $primary config set repl-timeout 1800
    $replica replicaof no one
    $replica flushall
    $replica replicaof $primary_host $primary_port
    wait_for_sync $replica
    wait_replica_online $primary
    wait_for_condition 50 100 {
        [throttle_rate $primary] == -1
    } else {
        fail "repl throttler doesn't setup correctly"
    }
}

# Tear down after a test so the next test starts from a clean state.
proc teardown_throttle_replication {primary replica} {

    if {[catch {$primary ping} err]} {
        fail "primary stopped responding during teardown: $err"
    }

    catch {$primary config set repl-throttling-enabled no}
    wait_for_condition 100 100 {
        [throttle_rate $primary] == -1 &&
        [getInfoProperty [$primary info debug] repl_throttle_current_clients] == 0
    } else {
        fail "repl throttler didn't tear down after the test"
    }

    # The replica must be fully synced and hold the same dataset.
    wait_for_sync $replica
    wait_replica_online $primary
    wait_for_ofs_sync $primary $replica
    assert_equal [$primary dbsize] [$replica dbsize]

    # Detach replication
    catch {$replica replicaof no one}
}

start_server {tags {"throttle repl external:skip valgrind:skip"}} {
    set replica [srv 0 client]
    set replica_host [srv 0 host]
    set replica_port [srv 0 port]
    set replica_pid [srv 0 pid]
    start_server {} {
        set primary [srv 0 client]
        set primary_host [srv 0 host]
        set primary_port [srv 0 port]

        for {set iter 0} {$iter < 100} {incr iter} {
        test {Steady-state throttle happy case} {
            setup_throttle_replication $primary $replica $primary_host $primary_port

            # Freeze the replica so its output buffer on the primary grows monotonically.
            pause_process $replica_pid

            set writer [valkey_deferring_client]
            $writer CLIENT ID
            set wid [$writer read]

            # Flood writes to grow the replica's COB and activate the throttler.
            for {set i 0} {$i < 5000} {incr i} {
                $writer set key:$i [string repeat x 1000]
            }
            wait_for_condition 50 100 {
                [throttle_rate $primary] >= 0
            } else {
                resume_process $replica_pid
                fail "throttle did not activate while the replica's COB was growing"
            }
            # Keep writing until the client is actually throttled.
            if {![wait_throttled_client $primary $writer $wid]} {
                resume_process $replica_pid
                fail "client was not throttled while the replica's COB was growing"
            }

            set ti [$primary info throttling]
            set td [$primary info debug]
            # Throttling section
            assert {[getInfoProperty $ti repl_throttle_rate] >= 0}
            assert {[getInfoProperty $ti repl_throttle_activation_events] >= 1}
            assert {[getInfoProperty $ti repl_throttle_below_guardrail_secs] >= 0}
            assert {[getInfoProperty $ti repl_throttle_total_commands] > 0}
            assert {[getInfoProperty $ti total_throttled_commands] > 0}

            # Debug section
            assert {[getInfoProperty $td repl_throttle_more_events] >= 1}
            assert {[getInfoProperty $td repl_throttle_less_events] >= 0}
            $writer close
            resume_process $replica_pid

            teardown_throttle_replication $primary $replica
        }
        }
    }
}
