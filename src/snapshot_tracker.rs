// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{snapshot_nonce::SnapshotNonce, SeqNo};
use dashmap::DashMap;
use lsm_tree::{Pending, VisibleSeqno};
use std::sync::{atomic::AtomicU64, Arc, RwLock};

/// Keeps track of open snapshots
pub struct SnapshotTrackerInner {
    seqno: VisibleSeqno,

    gc_lock: RwLock<()>,

    // TODO: maybe use rustc_hash or ahash
    data: DashMap<SeqNo, usize, xxhash_rust::xxh3::Xxh3Builder>,

    freed_count: AtomicU64,


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
    pub fn new(seqno: VisibleSeqno) -> Self {
        Self(Arc::new(SnapshotTrackerInner {
            data: DashMap::default(),
            freed_count: AtomicU64::default(),
            lowest_freed_instant: AtomicU64::default(),
            seqno,
            gc_lock: RwLock::default(),
        }))
    }

    pub fn get_ref(&self) -> VisibleSeqno {
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
        self.seqno.restore(value);
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

    /// Take `seqno` out of circulation until the returned guard is finished.
    ///
    /// **The rule lives on the counter, in `lsm_tree`, and not here.** It has to: a
    /// version upgrade takes a sequence number too — memtable rotation, ingest,
    /// compaction on its own thread — and a register this crate kept to itself would be
    /// one those could walk past. This is the same gate they go through.
    #[must_use = "the watermark cannot pass this sequence number until it is finished"]
    pub fn begin(&self, seqno: SeqNo) -> Pending {
        self.seqno.begin(seqno)
    }

    /// Take a whole contiguous run out of circulation — see
    /// [`VisibleSeqno::begin_range`]. One entry for a group commit, rather than one per
    /// write in it.
    #[must_use = "the watermark cannot pass this run until it is finished"]
    pub fn begin_range(&self, base: SeqNo, count: u64) -> Pending {
        self.seqno.begin_range(base, count)
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
        let global_seqno = VisibleSeqno::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        let nonce = map.open();
        assert_eq!(0, nonce.instant);
        drop(nonce);

        assert_eq!(map.get_seqno_safe_to_gc(), 0);

        global_seqno.restore(global_seqno.get() + 1);

        let nonce = map.open();
        assert_eq!(1, nonce.instant);
        drop(nonce);
    }

    #[test]
    fn snapshot_tracker_increase_watermark() {
        let global_seqno = VisibleSeqno::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        // Simulates some tx committing
        for _ in 0..100_000 {
            global_seqno.restore(global_seqno.get() + 1);
            let nonce = map.open();
            drop(nonce);
        }

        assert!(map.get_seqno_safe_to_gc() > 0);
    }

    #[test]
    fn snapshot_tracker_prevent_watermark() {
        let global_seqno = VisibleSeqno::default();

        let map = SnapshotTracker::new(global_seqno.clone());

        // This nonce prevents the watermark from increasing
        let _old_nonce = map.open();

        // Simlates some inserts happening
        for _ in 0..1_000 {
            global_seqno.restore(global_seqno.get() + 1);
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
        let global_seqno = VisibleSeqno::default();
        let map = SnapshotTracker::new(global_seqno);

        assert_eq!(map.len(), 0);
        map.close_raw(42);
        assert_eq!(map.get_seqno_safe_to_gc(), 0);
    }

    #[test]
    fn snapshot_tracker_concurrent_open_same_seqno_counts_correctly() {
        let global_seqno = VisibleSeqno::default();
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
        let global_seqno = VisibleSeqno::default();
        let map = SnapshotTracker::new(global_seqno);

        let big = 100u64;
        map.begin(big).publish();
        assert!(map.get() == (big + 1));

        let before = map.get();
        map.begin(1).publish();
        assert_eq!(map.get(), before);
    }

    #[test]
    fn snapshot_tracker_clone_snapshot_behaves_like_second_open() {
        let global_seqno = VisibleSeqno::default();
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
