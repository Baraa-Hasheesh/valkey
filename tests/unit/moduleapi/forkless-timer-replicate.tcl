set testmodule [file normalize tests/modules/forklesstimer.so]

# Module writes that run outside call() (timers, post-notification jobs) while a
# forkless full sync is running. tests/modules/forklesstimer.c replicates them
# with ValkeyModule_Replicate. The replica loads the same module so it can
# execute the replicated commands. Each case gets its own pair of servers,
# because a replica that already synced would not start a new full sync.

set forkless_overrides {save {} forkless-infrastructure-enabled yes forkless-policy prefer-forkless repl-diskless-sync yes repl-diskless-sync-delay 0 repl-timeout 60}

# Values of key:0 .. key:999 in DB 0 that differ between primary and replica.
proc forkless_timer_diverged {primary replica} {
    set keys {}
    for {set i 0} {$i < 1000} {incr i} { lappend keys key:$i }
    set pv [$primary mget {*}$keys]
    $replica select 0
    set rv [$replica mget {*}$keys]
    set diverged {}
    for {set i 0} {$i < 1000} {incr i} {
        if {[lindex $pv $i] ne [lindex $rv $i]} { lappend diverged [lindex $keys $i] }
    }
    return $diverged
}

# Start the full sync and wait until the scan has passed some of the 1000 keys
# of DB 0, so that the sync is in progress when the module writes.
proc forkless_timer_start_sync {primary primary_host primary_port replica} {
    test_config_set $primary rdb-key-save-delay 5000
    $replica replicaof $primary_host $primary_port
    wait_for_condition 500 10 {
        [s -1 rdb_bgsave_in_progress] == 1
    } else {
        fail "full sync did not start"
    }
    assert_equal {forkless} [s -1 rdb_current_bgsave_type]
    wait_for_condition 500 10 {
        [s -1 current_save_keys_processed] >= 100
    } else {
        fail "the sync did not progress"
    }
}

# Wait until the module timer started by SETALL or MULTISETALL is done, and
# return its stats: {done set skipped}.
proc forkless_timer_wait_done {primary} {
    wait_for_condition 500 10 {
        [lindex [$primary forklesstimer.stats] 0] == 1
    } else {
        fail "the timer did not run"
    }
    return [$primary forklesstimer.stats]
}

# Let the sync finish, then check that neither side crashed: no duplicate-key
# panic on the replica, no assertion on the primary.
proc forkless_timer_finish_sync {primary replica} {
    $primary config set rdb-key-save-delay 0
    wait_for_sync $replica
    wait_for_ofs_sync $primary $replica
    assert_equal 0 [count_log_message 0 "Duplicated key found in RDB file"]
    assert_equal 0 [count_log_message -1 "ASSERTION FAILED"]
    assert_equal {PONG} [$primary ping]
    assert_equal {PONG} [$replica ping]
}

proc forkless_timer_assert_in_sync {primary replica {detail ""}} {
    set diverged [forkless_timer_diverged $primary $replica]
    assert_equal 0 [llength $diverged] \
        "$detail replica differs on [llength $diverged] keys, first: [lrange $diverged 0 4]"
}

# The last client write before the timer sets the cached replication decision
# that the server would use for a command outside call().
foreach case {unreached-key new-key} {
start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    # DB 0 is scanned first. DB 9 is not reached until DB 0 is done.
    $primary select 0
    $primary debug populate 1000 key 1000
    $primary select 9
    $primary set other old
    $primary select 0

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test "Module timer Replicate during a forkless full sync keeps the replica in sync (last client write: $case)" {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            if {$case eq "unreached-key"} {
                $primary select 9
                $primary set other new
                $primary select 0
            } else {
                $primary set brandnew x
            }

            assert_equal {OK} [$primary forklesstimer.setall key: 1000 new]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            # OpenKey(WRITE) refuses only keys the sync is serializing at that moment.
            assert_lessthan $nskipped 50 "timer set=$nset skipped=$nskipped"
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            forkless_timer_assert_in_sync $primary $replica "timer set=$nset skipped=$nskipped;"
        }
    }
}
}

# Keys the timer creates land in buckets the scan has already passed or not yet
# reached. The ones behind the scan reach the replica only by replication.
start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 1000

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module timer that creates new keys during a forkless full sync replicates them} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            assert_equal {OK} [$primary forklesstimer.setall newkey: 500 new]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            assert_equal 500 $nset "timer set=$nset skipped=$nskipped"
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            $replica select 0
            set missing {}
            for {set i 0} {$i < 500} {incr i} {
                if {[$replica get newkey:$i] ne {new}} { lappend missing newkey:$i }
            }
            assert_equal 0 [llength $missing] \
                "replica lacks [llength $missing] of 500 new keys, first: [lrange $missing 0 4]"
        }
    }
}

start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 1000

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module keyless write from a timer during a forkless full sync reaches the replica} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            # Keyless: no key is opened, so the write is always replicated.
            assert_equal {OK} [$primary forklesstimer.bumpall 200]
            wait_for_condition 500 10 {
                [$primary forklesstimer.bumpstats] == 200
            } else {
                fail "the bump timer did not finish"
            }
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            assert_equal 200 [$replica forklesstimer.bumpstats]
        }
    }
}

start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 1000

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module MSET of two keys, both opened for WRITE outside call(), during a forkless full sync aborts the sync on an unsafe span without duplicating keys} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            # Each Replicate covers a pair of keys. A pair with one key passed and
            # one not yet reached is unsafe: the primary terminates that replica's
            # forkless sync, and the replica syncs again.
            assert_equal {OK} [$primary forklesstimer.multisetall key: 1000 new]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            # Guard against a pass with every pair skipped.
            assert_morethan $nset 0 "timer set=$nset skipped=$nskipped; no pair was written"

            forkless_timer_finish_sync $primary $replica
            assert_morethan [count_log_message -1 "module propagated a keyed command"] 0
            forkless_timer_assert_in_sync $primary $replica
        }
    }
}

start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 1000

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Post-notification job that replicates on active expiry during a forkless full sync reaches the replica} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            # Keys expire by active expiry while the sync runs. For each expired
            # expire:* key, the module's keyspace callback adds a post-notification
            # job, which counts the key and replicates a keyless bump outside call().
            for {set i 0} {$i < 50} {incr i} {
                $primary set expire:$i v px 100
            }
            wait_for_condition 500 10 {
                [$primary forklesstimer.bumpstats] >= 50
            } else {
                fail "the expire callback did not run"
            }
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            assert_equal [$primary forklesstimer.bumpstats] [$replica forklesstimer.bumpstats]
        }
    }
}

# Hashes a client creates during the sync are sent early. The module timer then
# deletes their only field, which removes each key, and replicates an HDEL: the
# replica must drop the keys too, not keep the early copy.
start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 1000

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module timer HDEL of the last field of a hash sent early during a forkless full sync reaches the replica} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            for {set i 0} {$i < 500} {incr i} {
                $primary hset hash:$i f v
            }
            assert_equal {OK} [$primary forklesstimer.hdelall hash: 500 f]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            assert_morethan $nset 0 "timer set=$nset skipped=$nskipped; no field was deleted"
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            $replica select 0
            set stale {}
            for {set i 0} {$i < 500} {incr i} {
                if {[$primary exists hash:$i] != [$replica exists hash:$i]} { lappend stale hash:$i }
            }
            assert_equal 0 [llength $stale] \
                "timer set=$nset skipped=$nskipped; replica differs on [llength $stale] hashes, first: [lrange $stale 0 4]"
        }
    }
}

# A client multi-key write during the sync sends small keys the scan has not
# reached early. The module timer then rewrites them, and ValkeyModule_StringSet deletes
# and re-creates each key: the new entry must stay marked as sent early, or the
# scan sends the key a second time and the replica panics on a duplicate key.
start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 100

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module timer rewrite of keys a client write sent early during a forkless full sync does not duplicate them} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            # A client MSET whose keys the scan reached at different times sends
            # the unreached one early (client path). Pair keys from both ends.
            for {set i 0} {$i < 500} {incr i} {
                $primary mset key:$i client key:[expr {999 - $i}] client
            }
            assert_equal {OK} [$primary forklesstimer.setall key: 1000 new]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            assert_morethan $nset 0 "timer set=$nset skipped=$nskipped; no key was written"
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            forkless_timer_assert_in_sync $primary $replica "timer set=$nset skipped=$nskipped;"
        }
    }
}

# A client RPUSH during the sync creates lists the scan has not reached, and
# sends them early. The module timer then pops the only element of each list,
# which deletes the key, and replicates LPOP. The replica already holds the
# early copy, so it must get the LPOP, or it keeps a list the primary deleted.
start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 1000

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module timer pop of the last element of a list a client created during a forkless full sync reaches the replica} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            for {set i 0} {$i < 500} {incr i} {
                $primary rpush lst:$i x
            }
            assert_equal {OK} [$primary forklesstimer.popall lst: 500]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            assert_equal 500 $nset "timer set=$nset skipped=$nskipped"
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            $replica select 0
            set stale {}
            for {set i 0} {$i < 500} {incr i} {
                if {[$replica exists lst:$i]} { lappend stale lst:$i }
            }
            assert_equal 0 [llength $stale] \
                "replica keeps [llength $stale] lists the primary deleted, first: [lrange $stale 0 4]"
        }
    }
}

# The same write as the timer case, from a module thread that locks the GIL with
# a thread-safe context. It runs outside call() too, so it must be decided by
# scan position: keys the sync already sent must be replicated.
start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 1000

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module thread Replicate during a forkless full sync keeps the replica in sync} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            assert_equal {OK} [$primary forklesstimer.threadsetall key: 1000 new]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            assert_lessthan $nskipped 50 "thread set=$nset skipped=$nskipped"
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            forkless_timer_assert_in_sync $primary $replica "thread set=$nset skipped=$nskipped;"
        }
    }
}

# True once the replica (srv 0) is up or has logged a duplicate-key panic. A
# replica that dies while answering INFO must not abort the test client.
proc forkless_timer_replica_loaded_or_panicked {} {
    if {[count_log_message 0 "Duplicated key found in RDB file"] > 0} { return 1 }
    if {[catch {s 0 master_link_status} status]} { return 0 }
    return [expr {$status eq {up}}]
}

# As above, but the timer runs a ValkeyModule_Call write between each StringSet
# and its Replicate. The re-created key must still be marked as sent early when
# the module's SET is propagated.
start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 100

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module timer rewrite with a Call before Replicate of keys a client write sent early during a forkless full sync does not duplicate them} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            for {set i 0} {$i < 500} {incr i} {
                $primary mset key:$i client key:[expr {999 - $i}] client
            }
            assert_equal {OK} [$primary forklesstimer.setcallall key: 1000 new]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            assert_morethan $nset 0 "timer set=$nset skipped=$nskipped; no key was written"
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            # Stop waiting as soon as the replica panics on the load.
            $primary config set rdb-key-save-delay 0
            wait_for_condition 500 20 {
                [forkless_timer_replica_loaded_or_panicked]
            } else {
                fail "replica did not finish the sync"
            }
            assert_equal 0 [count_log_message 0 "Duplicated key found in RDB file"] "replica panicked on a duplicate key"
            forkless_timer_finish_sync $primary $replica
            forkless_timer_assert_in_sync $primary $replica "timer set=$nset skipped=$nskipped;"
            assert_equal [$primary get counter] [$replica get counter]
        }
    }
}

# Keys a client creates during the sync are sent early. Active expiry then
# deletes them outside call(), and its own DEL replicates the deletion. A later
# module timer re-creates the same names: the replica must end with the new
# values, with no duplicate key, so the expiry's records must not mark the
# re-created keys as sent early.
start_server [list tags {"modules repl external:skip"} overrides $forkless_overrides] {
    r module load $testmodule
    set primary [srv 0 client]
    set primary_host [srv 0 host]
    set primary_port [srv 0 port]
    $primary select 0
    $primary debug populate 1000 key 1000

    start_server {overrides {save {}}} {
        set replica [srv 0 client]
        $replica module load $testmodule

        test {Module timer re-creating keys that active expiry deleted after they were sent early during a forkless full sync keeps the replica in sync} {
            forkless_timer_start_sync $primary $primary_host $primary_port $replica

            set expkeys {}
            for {set i 0} {$i < 300} {incr i} {
                $primary set exp:$i v px 100
                lappend expkeys exp:$i
            }
            # Wait for active expiry, without touching the keys (a lookup would
            # expire them lazily, inside call()).
            wait_for_condition 500 10 {
                [s -1 expired_keys] >= 300
            } else {
                fail "active expiry did not delete the keys"
            }
            assert_equal 0 [$primary exists {*}$expkeys]
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            assert_equal {OK} [$primary forklesstimer.setall exp: 300 new]
            lassign [forkless_timer_wait_done $primary] done nset nskipped
            assert_equal 300 $nset "timer set=$nset skipped=$nskipped"
            assert_equal 1 [s -1 rdb_bgsave_in_progress]

            forkless_timer_finish_sync $primary $replica
            $replica select 0
            set diverged {}
            for {set i 0} {$i < 300} {incr i} {
                if {[$replica get exp:$i] ne {new}} { lappend diverged exp:$i }
            }
            assert_equal 0 [llength $diverged] \
                "replica differs on [llength $diverged] of 300 keys, first: [lrange $diverged 0 4]"
        }
    }
}
