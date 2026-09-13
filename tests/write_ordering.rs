//! **A write that has not landed is never visible.**
//!
//! `WriteBatch::commit` releases the journal writer before it applies its rows, so
//! between those two points a sequence number is taken and its rows are nowhere. Making
//! that window invisible is the whole job of the in-flight register in
//! `snapshot_tracker`, and it is the part of the change that can fail silently: a
//! watermark that runs ahead of an unapplied write does not panic, it hands a reader a
//! consistent-looking snapshot with rows missing from it.
//!
//! Visibility is one watermark, so the rule is a single sentence: **the watermark may
//! never reach a sequence number that is still being applied.** Every test here holds
//! one batch inside that window, runs another writer to completion, and asserts it.
//!
//! There are five callers that take a sequence number — `WriteBatch::commit`,
//! `Keyspace::insert`, `remove`, `remove_weak` and `clear` — and each gets a case,
//! because the register only works if all of them are in it. The four keyspace paths
//! hold the journal writer across their whole operation, so they cannot themselves be
//! caught mid-apply; what they can do is *overtake* a batch that is, which is what these
//! check.

use fjall::{Database, KeyspaceCreateOptions, PersistMode, Readable};
use std::sync::atomic::{AtomicU64, Ordering::SeqCst};
use std::sync::{Arc, Barrier, Mutex, MutexGuard, OnceLock};
use std::thread::ThreadId;

/// **These tests cannot run beside one another.** The hook is one slot in the process,
/// so a second test arming it while the first is parked would leave the first waiting on
/// a barrier nobody reaches. Poisoning is recovered rather than propagated: a panic in
/// one case should fail that case, not turn every later one into a second failure.
fn one_at_a_time() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// A batch held open inside the window, and the sequence number it is carrying.
struct Held {
    /// Released to let the parked batch finish.
    go: Arc<Barrier>,
    /// Reached once the batch is inside the window and has parked.
    parked: Arc<Barrier>,
    seqno: Arc<AtomicU64>,
}

impl Held {
    fn new() -> Self {
        Self {
            go: Arc::new(Barrier::new(2)),
            parked: Arc::new(Barrier::new(2)),
            seqno: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }

    /// Park **the calling thread's** next commit inside the window.
    ///
    /// Keyed to a thread rather than to "the next commit anywhere": flush and journal
    /// maintenance commit on their own schedule, and one of those arriving first would
    /// consume the pause and leave the test asserting at a moment of its choosing.
    fn arm_for_this_thread(&self) {
        let (go, parked, seqno) = (self.go.clone(), self.parked.clone(), self.seqno.clone());
        let target: ThreadId = std::thread::current().id();

        fjall::write_hook::after_journal_released(Arc::new(move |at| {
            if std::thread::current().id() != target {
                return;
            }
            // Once only — the parked thread commits nothing else, but a retry inside
            // fjall would otherwise park against a barrier already spent.
            if seqno.swap(at, SeqCst) != u64::MAX {
                return;
            }
            parked.wait();
            go.wait();
        }));
    }

    fn seqno(&self) -> u64 {
        self.seqno.load(SeqCst)
    }
}

impl Drop for Held {
    fn drop(&mut self) {
        fjall::write_hook::clear();
    }
}

fn db() -> (tempfile::TempDir, Database) {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let db = Database::builder(dir.path()).open().expect("a database");
    (dir, db)
}

/// Run `race` while one batch is parked mid-apply, then let the batch finish.
///
/// `prepare` runs first, with nothing in flight — a racing operation needs its subject
/// to exist already, and creating a keyspace inside the window would be testing keyspace
/// creation rather than the ordering.
fn while_a_batch_is_mid_apply(
    what: &str,
    prepare: impl FnOnce(&Database),
    race: impl FnOnce(&Database) + Send,
) {
    let _serial = one_at_a_time();
    let (_dir, db) = db();
    let ks = db
        .keyspace("data", KeyspaceCreateOptions::default)
        .expect("a keyspace");

    // `seed` is the untouched control: no racer writes it, so the epilogue can tell a
    // lost row from a deliberately removed one. `victim` is what a racing remove takes.
    ks.insert("seed", "seed").expect("seeded");
    ks.insert("victim", "victim").expect("seeded");
    prepare(&db);

    let held = Held::new();

    // **The check is taken inside the window and asserted outside it.** A failing
    // assertion inside the scope would unwind with the batch still parked on a barrier
    // nobody will reach, and `thread::scope` would then wait for it forever — a test
    // that hangs instead of failing is worse than no test.
    let (visible, holding) = std::thread::scope(|scope| {
        let slow = scope.spawn(|| {
            held.arm_for_this_thread();
            let mut batch = db.batch();
            batch.insert(&ks, "held", "held");
            batch.commit().expect("the held batch commits");
        });

        // Wait until it is genuinely inside the window rather than hoping.
        held.parked.wait();

        race(&db);
        let seen = (db.visible_seqno(), held.seqno());

        held.go.wait();
        slow.join().expect("the held batch finished");
        seen
    });

    assert!(
        visible <= holding,
        "{what}: the watermark reached {visible} while sequence number {holding} is \
         still being applied — a reader at {visible} is promised rows that are not in \
         a memtable yet"
    );

    // And once it lands, everything is visible and nothing was lost.
    assert_eq!(
        Some("held".as_bytes().into()),
        ks.get("held").expect("read"),
        "{what}: the held batch's row never arrived"
    );
    assert_eq!(
        Some("seed".as_bytes().into()),
        ks.get("seed").expect("read"),
        "{what}: the seed row was lost"
    );
}

/// The keyspace a test races against, opened without creating anything new.
fn data(db: &Database) -> fjall::Keyspace {
    db.keyspace("data", KeyspaceCreateOptions::default)
        .expect("the keyspace")
}

#[test]
fn a_batch_does_not_publish_past_a_batch_that_is_still_applying() {
    while_a_batch_is_mid_apply(
        "a second batch",
        |_| {},
        |db| {
            let ks = data(db);
            let mut batch = db.batch();
            batch.insert(&ks, "racer", "racer");
            batch.commit().expect("the racing batch commits");
        },
    );
}

#[test]
fn an_insert_does_not_publish_past_a_batch_that_is_still_applying() {
    while_a_batch_is_mid_apply(
        "Keyspace::insert",
        |_| {},
        |db| {
            data(db).insert("racer", "racer").expect("the racing insert");
        },
    );
}

#[test]
fn a_remove_does_not_publish_past_a_batch_that_is_still_applying() {
    while_a_batch_is_mid_apply(
        "Keyspace::remove",
        |_| {},
        |db| {
            data(db).remove("victim").expect("the racing remove");
        },
    );
}

#[test]
fn a_weak_remove_does_not_publish_past_a_batch_that_is_still_applying() {
    while_a_batch_is_mid_apply(
        "Keyspace::remove_weak",
        |_| {},
        |db| {
            data(db)
                .remove_weak("victim")
                .expect("the racing weak remove");
        },
    );
}

#[test]
fn a_clear_does_not_publish_past_a_batch_that_is_still_applying() {
    while_a_batch_is_mid_apply(
        "Keyspace::clear",
        // Created and filled before anything is in flight: `clear` is what this races,
        // not the making of a keyspace.
        |db| {
            let other = db
                .keyspace("other", KeyspaceCreateOptions::default)
                .expect("a second keyspace");
            other.insert("x", "x").expect("something to clear");
        },
        |db| {
            let other = db
                .keyspace("other", KeyspaceCreateOptions::default)
                .expect("the second keyspace");
            other.clear().expect("the racing clear");
        },
    );
}

/// **A snapshot does not change under the reader.**
///
/// The watermark assertions above catch the fault at its source. This catches the
/// symptom a user would actually report, and needs no knowledge of sequence numbers: a
/// snapshot is a fixed instant, so a key that is absent at one may never appear at it
/// later. A watermark that ran ahead of an unapplied write produces exactly that — the
/// snapshot is opened over rows that are still on their way.
#[test]
fn a_snapshot_never_gains_a_row_it_did_not_have() {
    let _serial = one_at_a_time();
    let (_dir, db) = db();
    let ks = db
        .keyspace("data", KeyspaceCreateOptions::default)
        .expect("a keyspace");
    ks.insert("seed", "seed").expect("seeded");

    let held = Held::new();

    let (before, after) = std::thread::scope(|scope| {
        let slow = scope.spawn(|| {
            held.arm_for_this_thread();
            let mut batch = db.batch();
            batch.insert(&ks, "held", "held");
            batch.commit().expect("the held batch commits");
        });

        held.parked.wait();

        // Another writer finishes and publishes while the batch above is mid-apply.
        ks.insert("racer", "racer").expect("the racing insert");

        // The instant is fixed here, with the held row demonstrably not in a memtable.
        let snapshot = db.snapshot();
        let before = snapshot.get(&ks, "held").expect("read");

        held.go.wait();
        slow.join().expect("the held batch finished");

        (before, snapshot.get(&ks, "held").expect("read"))
    });

    assert_eq!(
        before, after,
        "the snapshot gained a row while the reader held it: absent, then present, at \
         one instant"
    );

    db.persist(PersistMode::SyncAll).expect("durable");
}
