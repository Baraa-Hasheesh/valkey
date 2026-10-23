# Regression test for the crash-time glibc arena deadlock.
# Linux-glibc-only: the arenaloc module relies on glibc malloc internals.

set testmodule [file normalize tests/modules/arenalock.so]

#if 0 {

# Only meaningful on Linux + libc (glibc). Anywhere else, do nothing.
if {$::tcl_platform(os) eq "Linux"} {
    start_server {tags {"modules"}} {
        set allocator [s mem_allocator]
        if {[string match {*libc*} $allocator]} {
            r module load $testmodule

            test {arenalock.crash: crash handler completes without deadlocking} {
                set pid [s process_id]
                set logfile [srv 0 stdout]

                # Fire the crash. The server aborts inside realloc() while holding the arena lock
                catch {r arenalock.crash} e

                # On an async-signal-safe handler the report finishes quickly.
                # On today's buggy handler this condition never becomes true
                # (deadlock).
                wait_for_condition 500 100 {
                    [string match "*BUG REPORT END*" [exec tail -n 200 $logfile]]
                } else {
                    fail "crash handler did not finish the report (deadlock?) -- see Report.md"
                }

                # The process must actually be gone, not wedged on the lock.
                wait_for_condition 50 100 {
                    ![process_is_alive $pid]
                } else {
                    fail "server process still alive after crash report -- handler deadlocked"
                }
            }
        } else {
            # jemalloc / tcmalloc: the glibc arena-lock reproduction does not
            # apply, so there is nothing to assert here.
        }
    }
}

#} ;# if 0 -- enable by changing to `if 1` once the deadlocks are fixed
