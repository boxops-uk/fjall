//! **A database that has finished writing can drop its write-ahead log.**
//!
//! Once every memtable is flushed the journal is pure duplication: the data is in the
//! tables, and replaying it at every open costs the open time and makes
//! `approximate_len` report double. Flushing alone does not reclaim it — journal
//! maintenance only considers *sealed* journals, and the active one is rotated only when
//! its write position passes the flush worker's threshold, so a database whose whole
//! history fits under that threshold keeps its journal forever however often it flushes.
//!
//! [`Database::checkpoint`] is that rotation on demand. These are the two halves of
//! believing it: that it reclaims, and that it reclaims nothing it should not.

use fjall::{Database, KeyspaceCreateOptions, PersistMode};

const ENTRIES: u32 = 200_000;

fn write(db: &Database, ks: &fjall::Keyspace, from: u32, to: u32) {
    for i in from..to {
        ks.insert(i.to_be_bytes(), b"a reasonably sized value, as facts go")
            .expect("an insert");
    }
}

/// **Flushing is not enough, and a checkpoint is.**
///
/// Asserted on what a reader sees rather than on bytes: `journal_disk_space` reports the
/// *preallocated* file, so a rotation swaps one 64 MiB file for another and the number
/// does not move. What moves is the reopen — a journal the tables already hold is
/// replayed into a memtable that duplicates them, so `approximate_len` comes back double
/// and the open pays for it.
///
/// The control is the same database without the checkpoint. It is here so a `checkpoint`
/// that did nothing at all would fail this rather than pass it.
#[test]
fn a_sealed_database_reclaims_a_journal_its_tables_already_hold() {
    fn seal(checkpointed: bool) -> usize {
        let dir = tempfile::tempdir().expect("a scratch directory");
        let path = dir.path().to_path_buf();

        {
            let db = Database::builder(&path).open().expect("a database");
            let ks = db
                .keyspace("data", KeyspaceCreateOptions::default)
                .expect("a keyspace");

            write(&db, &ks, 0, ENTRIES);

            ks.rotate_memtable_and_wait().expect("everything to tables");
            db.persist(PersistMode::SyncAll).expect("durable");

            if checkpointed {
                db.checkpoint().expect("a checkpoint");
            }
        }

        let db = Database::builder(&path).open().expect("it recovers");
        let ks = db
            .keyspace("data", KeyspaceCreateOptions::default)
            .expect("the keyspace");

        // Still all there, whichever path got here.
        assert_eq!(
            Some(b"a reasonably sized value, as facts go".as_slice().into()),
            ks.get((ENTRIES - 1).to_be_bytes()).expect("a read"),
        );

        ks.approximate_len()
    }

    let exact = ENTRIES as usize;

    // The control: without a checkpoint the journal is replayed over the tables.
    let unchecked = seal(false);
    assert!(
        unchecked > exact,
        "the journal was already reclaimed without a checkpoint, so this proves nothing: \
         {unchecked}"
    );

    let checkpointed = seal(true);
    assert_eq!(
        exact, checkpointed,
        "after a checkpoint the reopen still replayed a journal the tables hold: \
         {checkpointed} against {exact}, with {unchecked} for the database that did not \
         checkpoint"
    );
}

/// **It reclaims nothing the tables do not hold.**
///
/// A checkpoint with rows still only in a memtable must leave their journal alone — the
/// eviction rule is that every keyspace's *tables* have persisted past the journal's last
/// sequence number, and a caller who forgets to flush must not lose the difference.
#[test]
fn a_checkpoint_keeps_what_only_the_memtable_holds() {
    let dir = tempfile::tempdir().expect("a scratch directory");
    let path = dir.path().to_path_buf();

    {
        let db = Database::builder(&path).open().expect("a database");
        let ks = db
            .keyspace("data", KeyspaceCreateOptions::default)
            .expect("a keyspace");

        write(&db, &ks, 0, 1_000);
        ks.rotate_memtable_and_wait().expect("the first lot lands");

        // Deliberately unflushed, and deliberately checkpointed anyway.
        write(&db, &ks, 1_000, 2_000);
        db.persist(PersistMode::SyncAll).expect("durable");
        db.checkpoint().expect("a checkpoint");
    }

    let db = Database::builder(&path).open().expect("it recovers");
    let ks = db
        .keyspace("data", KeyspaceCreateOptions::default)
        .expect("the keyspace");

    for i in 0..2_000_u32 {
        assert!(
            ks.contains_key(i.to_be_bytes()).expect("a read"),
            "key {i} was lost: a checkpoint reclaimed a journal the tables did not hold"
        );
    }
}
