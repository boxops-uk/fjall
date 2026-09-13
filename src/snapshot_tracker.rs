// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, SeqNo};
use dashmap::DashMap;
use lsm_tree::SequenceNumberCounter;
use std::collections::BTreeSet;
use std::sync::{atomic::AtomicU64, Arc, Mutex, RwLock};

/// Writes that have taken a sequence number but have not finished applying.
///
/// **Why this exists.** Visibility is one watermark — [`SnapshotTracker::publish`] is
/// `fetch_max(seqno + 1)` — so a write may only advance it once every *lower* write has
/// landed in its memtable. Today that holds for free, because every writer applies its
/// batch while still holding the journal writer, and the journal writer is one mutex:
/// completions cannot overtake one another because they are not concurrent.
///
/// Moving the memtable apply out of that mutex is the whole point of this change, and
/// it takes the ordering with it. Without something in its place, a batch at seqno 6
/// could publish while the batch at seqno 5 is still inserting, and a reader snapshotted
/// at 7 would miss rows that a lower sequence number promises it can see.
///
/// So the ordering is stated explicitly rather than inherited from a lock: a writer
/// registers its sequence number before it can be observed missing, and on finishing
/// the watermark advances only as far as the lowest one still in flight allows.
#[derive(Default)]
struct InFlight {
    /// Sequence numbers taken and not yet applied.
    taken: BTreeSet<SeqNo>,
    /// The highest that has finished, whether or not it is publishable yet.
    done: SeqNo,
}

/// Keeps track of open snapshots
pub struct SnapshotTrackerInner {
    seqno: SequenceNumberCounter,

    gc_lock: RwLock<()>,

    // TODO: maybe use rustc_hash or ahash
    data: DashMap<SeqNo, usize, xxhash_rust::xxh3::Xxh3Builder>,

    freed_count: AtomicU64,

    /// See [`InFlight`]. Held for a set insert or removal and nothing else — never
    /// across the journal, the memtable, or any I/O.
    in_flight: Mutex<InFlight>,

    pub(crate) lowest_freed_instant: AtomicU64,
}

#[derive(Clone)]
pub struct SnapshotTracker(Arc<SnapshotTrackerInner>);

impl std::ops::Deref for SnapshotTracker {
    type Target = SnapshotTrackerInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl SnapshotTracker {
    pub fn new(seqno: SequenceNumberCounter) -> Self {
        Self(Arc::new(SnapshotTrackerInner {
            data: DashMap::default(),
            freed_count: AtomicU64::default(),
            in_flight: Mutex::default(),
            lowest_freed_instant: AtomicU64::default(),
            seqno,
            gc_lock: RwLock::default(),
        }))
    }

    pub fn get_ref(&self) -> SequenceNumberCounter {
        self.seqno.clone()
    }

    /// Used in database recovery.
    ///
    /// # Caution
    ///
    /// Don't use this to get a snapshot read.
    pub fn get(&self) -> SeqNo {
        self.seqno.get()
    }

    /// Used in database recovery.
    pub fn set(&self, value: SeqNo) {
        self.seqno.fetch_max(value);
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn open_snapshots(&self) -> usize {
        self.data.iter().map(|r| *r.value()).sum()
    }

    pub fn open(&self) -> SnapshotNonce {
        #[expect(clippy::expect_used)]
        let _lock = self.gc_lock.read().expect("lock is poisoned");

        let seqno = self.seqno.get();

        self.data
            .entry(seqno)
            .and_modify(|x| {
                *x += 1;
            })
            .or_insert(1);

        SnapshotNonce::new(seqno, self.clone())
    }

    pub fn clone_snapshot(&self, nonce: &SnapshotNonce) -> SnapshotNonce {
        #[expect(clippy::expect_used)]
        let _lock = self.gc_lock.read().expect("lock is poisoned");

        self.data
            .entry(nonce.instant)
            .and_modify(|x| {
                *x += 1;
            })
            .or_insert(1);

        SnapshotNonce::new(nonce.instant, self.clone())
    }

    pub fn close(&self, nonce: &SnapshotNonce) {
        self.close_raw(nonce.instant);
    }

    pub(crate) fn close_raw(&self, instant: SeqNo) {
        #[expect(clippy::expect_used)]
        let lock = self.gc_lock.read().expect("lock is poisoned");

        self.data.alter(&instant, |_, v| v.saturating_sub(1));

        let freed = self
            .freed_count
            .fetch_add(1, std::sync::atomic::Ordering::AcqRel)
            + 1;

        drop(lock);

        if freed.is_multiple_of(10_000) {
            self.gc();
        }
    }

    /// Register a sequence number as taken, before anything can observe it missing.
    ///
    /// Must be paired with [`SnapshotTracker::publish`], on the error paths too: a
    /// sequence number that is taken and never finished holds the watermark still.
    /// Finishing one that wrote nothing is harmless, since nothing exists at it.
    pub fn begin(&self, seqno: SeqNo) {
        #[expect(clippy::expect_used)]
        let mut in_flight = self.in_flight.lock().expect("lock is poisoned");
        in_flight.taken.insert(seqno);
    }

    /// Publish write completion, **no further than the lowest write still in flight**.
    pub fn publish(&self, batch_seqno: SeqNo) {
        #[expect(clippy::expect_used)]
        let mut in_flight = self.in_flight.lock().expect("lock is poisoned");

        in_flight.taken.remove(&batch_seqno);
        in_flight.done = in_flight.done.max(batch_seqno);

        // Everything below the lowest sequence number still being applied has landed,
        // so that is how far a reader may see. With nothing in flight, everything has.
        let visible = in_flight
            .taken
            .first()
            .map_or(in_flight.done, |lowest| lowest.saturating_sub(1));

        // `fetch_max` under the same lock that decides the bound, so two writers
        // finishing at once cannot interleave into a watermark neither computed.
        self.seqno.fetch_max(visible + 1);
    }

    // TODO: after recovery, we may need to set the GC watermark once to current_seqno - 1
    // so there cannot be compactions scheduled immediately with gc_watermark=0
    pub fn get_seqno_safe_to_gc(&self) -> SeqNo {
        self.lowest_freed_instant
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub(crate) fn pullup(&self) {
        #[expect(clippy::expect_used)]
        let _lock = self.gc_lock.write().expect("lock is poisoned");

        if self.data.is_empty() {
            self.lowest_freed_instant.store(
                self.seqno.get().saturating_sub(1),
                std::sync::atomic::Ordering::Release,
            );
        }
    }

    pub(crate) fn gc(&self) {
        #[expect(clippy::expect_used)]
        let _lock = self.gc_lock.write().expect("lock is poisoned");

        let seqno_threshold = self.seqno.get();

        let mut lowest_retained = 0;
        let mut none_retained = true;

        self.data.retain(|&k, v| {
            let should_be_retained = *v > 0 || k >= seqno_threshold;

            if should_be_retained {
                lowest_retained = match lowest_retained {
                    0 => k,
                    lo => lo.min(k),
                };
                none_retained = false;
            }

            should_be_retained
        });

        if none_retained {
            lowest_retained = seqno_threshold;
        }

        self.lowest_freed_instant.fetch_max(
            lowest_retained.saturating_sub(1),
            std::sync::atomic::Ordering::AcqRel,
        );
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used)]
mod tests {
    use super::*;
    use test_log::test;

    #[test]
    fn snapshot_tracker_mvcc_watermark_1() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let db = crate::Database::builder(&dir).open()?;
        let tree = db.keyspace("default", Default::default)?;

        tree.insert("a", "a")?;
        tree.insert("a", "b")?;
        assert_eq!(b"b", &*tree.get("a")?.unwrap());

        tree.rotate_memtable_and_wait()?;
        assert_eq!(b"b", &*tree.get("a")?.unwrap());

        db.supervisor.snapshot_tracker.gc();
        assert_eq!(b"b", &*tree.get("a")?.unwrap());

        Ok(())
    }

    #[test]
    fn snapshot_tracker_mvcc_watermark_2() -> crate::Result<()> {
        let dir = tempfile::tempdir()?;

        let db = crate::Database::builder(&dir).open()?;
        let tree = db.keyspace("default", Default::default)?;

        tree.insert("a", "a")?;
        tree.insert("a", "b")?;
        assert_eq!(b"b", &*tree.get("a")?.unwrap());

        db.supervisor.snapshot_tracker.gc();
        assert_eq!(b"b", &*tree.get("a")?.unwrap());

        tree.rotate_memtable_and_wait()?;
        assert_eq!(b"b", &*tree.get("a")?.unwrap());

        Ok(())
    }

    #[test]
    fn snapshot_tracker_mvcc_watermark_3() -> crate::Result<()> {
        use crate::Readable;

        let dir = tempfile::tempdir()?;

        let db = crate::Database::builder(&dir).open()?;
        let tree = db.keyspace("default", Default::default)?;

        tree.insert("a", "a")?;

        let snapshot = db.snapshot();

        tree.insert("a", "b")?;
        assert_eq!(b"b", &*tree.get("a")?.unwrap());
        assert_eq!(b"a", &*snapshot.get(&tree, "a")?.unwrap());

        db.supervisor.snapshot_tracker.gc();
        assert_eq!(b"b", &*tree.get("a")?.unwrap());
        assert_eq!(b"a", &*snapshot.get(&tree, "a")?.unwrap());

        tree.rotate_memtable_and_wait()?;
        assert_eq!(b"b", &*tree.get("a")?.unwrap());
        assert_eq!(b"a", &*snapshot.get(&tree, "a")?.unwrap());

        Ok(())
    }

    #[test]
    fn snapshot_tracker_mvcc_watermark_4() -> crate::Result<()> {
        use crate::Readable;

        let dir = tempfile::tempdir()?;

        let db = crate::Database::builder(&dir).open()?;
        let tree = db.keyspace("default", Default::default)?;

        tree.insert("a", "a")?;

        let snapshot = db.snapshot();

        tree.insert("a", "b")?;
        assert_eq!(b"b", &*tree.get("a")?.unwrap());
        assert_eq!(b"a", &*snapshot.get(&tree, "a")?.unwrap());

        tree.rotate_memtable_and_wait()?;
        assert_eq!(b"b", &*tree.get("a")?.unwrap());
        assert_eq!(b"a", &*snapshot.get(&tree, "a")?.unwrap());

        db.supervisor.snapshot_tracker.gc();
        assert_eq!(b"b", &*tree.get("a")?.unwrap());
        assert_eq!(b"a", &*snapshot.get(&tree, "a")?.unwrap());

        Ok(())
    }

    #[test]
    fn snapshot_tracker_basic() {
        let global_seqno = SequenceNumberCounter::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        let nonce = map.open();
        assert_eq!(0, nonce.instant);
        drop(nonce);

        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        let _ = global_seqno.next();

        let nonce = map.open();
        assert_eq!(1, nonce.instant);
        drop(nonce);
    }

    #[test]
    fn snapshot_tracker_increase_watermark() {
        let global_seqno = SequenceNumberCounter::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        // Simulates some tx committing
        for _ in 0..100_000 {
            let _ = global_seqno.next();
            let nonce = map.open();
            drop(nonce);
        }

        assert!(map.get_seqno_safe_to_gc() > 0);
    }

    #[test]
    fn snapshot_tracker_prevent_watermark() {
        let global_seqno = SequenceNumberCounter::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        // This nonce prevents the watermark from increasing
        let _old_nonce = map.open();

        // Simlates some inserts happening
        for _ in 0..1_000 {
            let _ = global_seqno.next();
        }

        // Simulates more read tx opening and closing
        for _ in 0..10_000 {
            let nonce = map.open();
            drop(nonce);
        }

        assert_eq!(map.get_seqno_safe_to_gc(), 0);
    }

    #[test]
    fn snapshot_tracker_close_never_opened_does_not_underflow_or_panic() {
        let global_seqno = SequenceNumberCounter::default();
        let map = SnapshotTracker::new(global_seqno);

        assert_eq!(map.len(), 0);
        map.close_raw(42);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);
    }

    #[test]
    fn snapshot_tracker_concurrent_open_same_seqno_counts_correctly() {
        let global_seqno = SequenceNumberCounter::default();
        let map = SnapshotTracker::new(global_seqno);

        // make sure seqno doesn't change between two opens
        let n1 = map.open();
        let n2 = map.open();
        assert_eq!(n1.instant, n2.instant);
        assert_eq!(map.open_snapshots(), 2);

        // closing one decreases count
        map.close(&n1);
        assert_eq!(map.open_snapshots(), 1);

        // closing the other removes the last
        map.close(&n2);
        assert_eq!(map.open_snapshots(), 0);
    }

    #[test]
    fn snapshot_tracker_publish_moves_seqno_forward_and_ignores_older() {
        let global_seqno = SequenceNumberCounter::default();
        let map = SnapshotTracker::new(global_seqno);

        let big = 100u64;
        map.publish(big);
        assert!(map.get() == (big + 1));

        let before = map.get();
        map.publish(1);
        assert_eq!(map.get(), before);
    }

    #[test]
    fn snapshot_tracker_clone_snapshot_behaves_like_second_open() {
        let global_seqno = SequenceNumberCounter::default();
        let map = SnapshotTracker::new(global_seqno);

        let orig = map.open();
        let clone = map.clone_snapshot(&orig);

        assert_eq!(orig.instant, clone.instant);
        assert_eq!(map.open_snapshots(), 2);

        // closing one leaves one
        map.close(&orig);
        assert_eq!(map.open_snapshots(), 1);

        // closing the clone removes all
        map.close(&clone);
        assert_eq!(map.open_snapshots(), 0);
    }
}
