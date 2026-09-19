//! **A write that returned is still there after the process is killed.**
//!
//! Every other recovery test in this suite closes the database cleanly and reopens it,
//! which exercises the reopen and not the crash. This kills a writer mid-flight with
//! `SIGKILL` — no unwinding, no destructors, no flush — and asks what survived.
//!
//! # The invariant, and why it is a prefix
//!
//! Each writer commits its own keys **in order**, one commit at a time, so a commit has
//! returned before the next begins. Recovery replays the journal, which is written in
//! commit order, so the keys a writer left behind must be a *prefix* of the ones it was
//! going to write: `0, 1, 2 …` up to wherever it got. A **hole** — key 7 present with key
//! 5 missing — is a lost or reordered journal record, and is the fault this exists to
//! catch.
//!
//! It is asserted per writer rather than globally because writers interleave: whose
//! commit reached the journal first is a scheduling question, and only the order *within*
//! one writer is promised.
//!
//! # What it does and does not prove about releasing the journal writer early
//!
//! `WriteBatch::commit` applies to the memtables after it releases the journal writer, so
//! there is a window in which a record is in the log and not in memory. Recovery reads the
//! log, so that window is invisible to it — and this test passes on either arrangement.
//! It is not a regression test for that change; it is the crash coverage the change made
//! its absence obvious.

use fjall::{Database, KeyspaceCreateOptions, PersistMode};
use std::time::Duration;

const CHILD: &str = "crashing_writer_child_process";
const ROOT_VAR: &str = "FJALL_CRASH_ROOT";
const WRITERS_VAR: &str = "FJALL_CRASH_WRITERS";

/// Per writer, per commit. Small enough that a kill lands mid-run rather than after it.
const PER_WRITER: u128 = 2_000;

fn key(writer: u128, n: u128) -> Vec<u8> {
    let mut k = writer.to_be_bytes().to_vec();
    k.extend_from_slice(&n.to_be_bytes());
    k
}

/// **Killed while writing, at several points, and the prefix holds wherever it lands.**
#[test]
fn a_killed_writer_leaves_a_prefix_of_what_it_wrote() {
    // One writer and several, because the second is the arrangement where commits from
    // different threads interleave through one journal and apply to memtables
    // concurrently — the case with more ways to go wrong.
    for writers in [1_u128, 4] {
        // Several delays: one would only ever cut in one place. The floor is set by how
        // long the child needs to exist at all — a new process, a database open, a
        // keyspace — and the ceiling by not letting it finish; an `fsync` per commit
        // makes the run long enough that both are comfortable.
        for delay_ms in [600_u64, 1_200, 2_500] {
            let dir = tempfile::tempdir().expect("a scratch directory");
            let root = dir.path().to_string_lossy().to_string();

            let mut child = std::process::Command::new(
                std::env::current_exe().expect("this test binary"),
            )
            .args([CHILD, "--exact", "--ignored", "--nocapture"])
            .env(ROOT_VAR, &root)
            .env(WRITERS_VAR, writers.to_string())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("the child starts");

            std::thread::sleep(Duration::from_millis(delay_ms));

            // SIGKILL: nothing runs on the way out, which is the point.
            child.kill().expect("the child is killed");
            child.wait().expect("the child is reaped");

            let db = Database::builder(&root).open().expect("it recovers");
            let ks = db
                .keyspace("data", KeyspaceCreateOptions::default)
                .expect("the keyspace survives");

            let mut total = 0_u128;

            for writer in 0..writers {
                // How far this writer got: the first key it is missing.
                let mut reached = 0_u128;
                while reached < PER_WRITER
                    && ks
                        .contains_key(key(writer, reached))
                        .expect("a read")
                {
                    reached += 1;
                }
                total += reached;

                // **No holes above it.** A key present past the first gap means a record
                // was lost or replayed out of order, which is the fault being hunted.
                for n in reached..PER_WRITER {
                    assert!(
                        !ks.contains_key(key(writer, n)).expect("a read"),
                        "writers={writers} delay={delay_ms}ms: writer {writer} is missing \
                         key {reached} but holds key {n} — the journal lost or reordered a \
                         record"
                    );
                }
            }

            // Non-vacuity: a kill that landed before any write teaches nothing, and a run
            // that finished teaches nothing about crashing.
            assert!(
                total > 0,
                "writers={writers} delay={delay_ms}ms: nothing was written before the \
                 kill, so the crash case is vacuous"
            );
        }
    }
}

/// Not a guard: the crashing half of the test above, run as a child process.
///
/// Writes its keys in order and then waits to be killed. It never returns — the parent's
/// `SIGKILL` is the only exit — so a run that completes is one the parent cut too late.
#[test]
#[ignore = "not a guard: child process of a_killed_writer_leaves_a_prefix_of_what_it_wrote"]
fn crashing_writer_child_process() {
    let root = std::env::var(ROOT_VAR).expect("the parent sets the root");
    let writers: u128 = std::env::var(WRITERS_VAR)
        .expect("the parent sets the writer count")
        .parse()
        .expect("a number");

    let db = Database::builder(&root).open().expect("a database");
    let ks = db
        .keyspace("data", KeyspaceCreateOptions::default)
        .expect("a keyspace");

    std::thread::scope(|scope| {
        for writer in 0..writers {
            let (db, ks) = (&db, &ks);
            scope.spawn(move || {
                for n in 0..PER_WRITER {
                    let mut batch = db.batch();
                    batch.insert(ks, key(writer, n), n.to_be_bytes());

                    // **What `SIGKILL` does and does not test.** Killing a process does
                    // not lose what it already handed the OS, so buffered writes survive
                    // this and the prefix would hold without any `fsync` at all — this
                    // tests the application's atomicity, not the storage's durability.
                    // Syncing anyway makes the surviving prefix a claim about bytes on
                    // disk rather than about page cache, which is the stronger statement
                    // and the one worth asserting; losing power is a harness this cannot
                    // be.
                    batch
                        .durability(Some(PersistMode::SyncAll))
                        .commit()
                        .expect("a commit");
                }
            });
        }
    });

    // Finished without being killed: sleep so the parent's kill still lands, rather than
    // exiting cleanly and turning the case vacuous.
    std::thread::sleep(Duration::from_secs(60));
}
