// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! **Group commit that keeps the memtable fill parallel.**
//!
//! The queue and the leader-lease shape are from marvin-j97's `write-pipeline-mpsc`
//! draft, which follows [flat combining] and fjall issue #96. What differs is where the
//! leader stops.
//!
//! # Why a leader at all
//!
//! A commit of `r` rows holds the journal writer for `F + r·(e + m)`: a fixed cost `F`
//! per commit — handoff, sequence number, journal header, publish, `fsync` where
//! durability is on — plus per-row costs for the journal (`e`) and the memtable (`m`).
//! All of it is serialised across the database.
//!
//! A leader that batches `k` commits pays `F` once for the group, so the section becomes
//! `F/k + r(e + m)`. That is an enormous win when commits are small, because a one-row
//! commit is almost entirely `F` — measured on stock fjall, eight writers doing
//! single-row writes achieve **40% of what one writer achieves**, which is the queueing
//! this exists to remove.
//!
//! # Why the leader stops after the journal
//!
//! Amortising `F` does nothing for `r·(e + m)`, which is the same total work whichever
//! thread performs it. So a leader that also fills the memtables caps the whole database
//! at one thread's throughput, and above roughly `r = F/m` rows per commit that is worse
//! than the contended path it replaced.
//!
//! The journal genuinely must be serial — it is a log, and its order is the recovery
//! order. Memtables need not be: each keyspace's memtable takes its own lock, and the
//! sequence number ordering these rows against every other writer's is assigned before
//! anyone applies anything. So the leader writes the log for the whole group and then
//! **releases its members to fill their own memtables in parallel**, leaving `F/k + r·e`
//! — smaller than either arrangement alone, at every commit size.
//!
//! # What that costs, and how it is paid
//!
//! Members then finish out of order, so the group cannot publish until the last of them
//! has landed. That is [`Group`]: a countdown, and one
//! [`Pending`](lsm_tree::Pending) held over the group's whole contiguous run of
//! sequence numbers. Because the leader hands those numbers out in queue order, the run
//! is contiguous and the bookkeeping is a single entry per *group* rather than one per
//! write — which is the cheapest form the ordering rule takes anywhere.
//!
//! [flat combining]: https://people.csail.mit.edu/shanir/publications/Flat%20Combining%20SPAA%2010.pdf

// The lockless ring buffer is the one place in this crate that needs raw memory
// handling: a slot is written before its publication flag is set, which no safe
// abstraction expresses without a lock in the middle of the write path.
#[allow(unsafe_code)]
mod fifo;

use crate::batch::item::Item as BatchItem;
use fifo::Queue;
use lsm_tree::{AbstractTree, Pending, SeqNo, ValueType};
use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering::AcqRel, Ordering::Acquire, Ordering::Relaxed, Ordering::Release};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

/// Queued, and the leader has not reached it.
const WAITING: u8 = 0;
/// Journalled and numbered: go and fill your own memtables.
const JOURNALED: u8 = 1;
/// The queue is not empty and the lease is yours.
const TAKE_LEASE: u8 = 2;
/// The group's journal write failed; the database is poisoned.
const FAILED: u8 = 3;

/// The most commits one leader will take on before handing the lease back.
///
/// Bounded so a writer's latency does not scale with how busy the database is, and so
/// the leader's own rows do not wait behind an unbounded amount of other people's
/// journalling.
const MAX_GROUP: usize = 64;

/// The most bytes one leader will journal in a group, for the same reason.
const MAX_GROUP_BYTES: usize = 1_024 * 1_024;

/// How long a member spins for its leader before yielding its core.
const SPINS_BEFORE_YIELD: u32 = 256;

/// What a writer is asking the pipeline to do.
pub(crate) enum Work {
    /// A `WriteBatch`, which may span keyspaces.
    Batch(Vec<BatchItem>),
    /// A single `insert`, `remove` or `remove_weak`.
    Single(BatchItem),
}

impl Work {
    fn items(&self) -> &[BatchItem] {
        match self {
            Self::Batch(items) => items,
            Self::Single(item) => std::slice::from_ref(item),
        }
    }

    fn bytes(&self) -> usize {
        self.items()
            .iter()
            .map(|item| item.key.len() + item.value.len())
            .sum()
    }
}

/// A run of sequence numbers handed out together, and the count still to land.
///
/// The `Pending` covers the whole run and is dropped by whoever decrements the last
/// member — so the watermark reaches the group only once every member's rows are
/// readable, whatever order they got there in.
struct Group {
    outstanding: AtomicUsize,
    pending: Mutex<Option<Pending>>,
}

impl Group {
    fn landed(&self) {
        if self.outstanding.fetch_sub(1, AcqRel) == 1 {
            #[expect(clippy::expect_used)]
            let mut pending = self.pending.lock().expect("lock is poisoned");
            // Dropping it publishes, bounded by any earlier group still outstanding.
            pending.take();
        }
    }
}

/// What the leader hands back to a member: its number, and the group to report to.
struct Ticket {
    seqno: SeqNo,
    group: Arc<Group>,
}

/// One writer's place in the queue.
pub(crate) struct Record {
    work: Work,
    durability: Option<crate::PersistMode>,
    state: AtomicU8,
    ticket: OnceLock<Ticket>,
}

impl Record {
    pub(crate) fn new(work: Work, durability: Option<crate::PersistMode>) -> Self {
        Self {
            work,
            durability,
            state: AtomicU8::new(WAITING),
            ticket: OnceLock::new(),
        }
    }
}

pub(crate) struct Pipeline {
    queue: Queue<Arc<Record>>,
    /// Whether the lease is free. Taken with a compare-exchange, so exactly one writer
    /// becomes the leader.
    lease: std::sync::atomic::AtomicBool,
    /// Held for reading while pushing, for writing while the lease changes hands, so a
    /// writer cannot enqueue into a group that has just been closed and then wait for a
    /// leader that will never come.
    handover: RwLock<()>,
}

impl Pipeline {
    pub(crate) fn new() -> Self {
        Self {
            queue: Queue::with_capacity(1_024),
            lease: std::sync::atomic::AtomicBool::new(true),
            handover: RwLock::default(),
        }
    }
}

impl Pipeline {
    /// Put `record` through the pipeline and return once its rows are in their
    /// memtables.
    ///
    /// The caller either becomes the leader — journalling for everyone queued — or waits
    /// for one, and in both cases fills its **own** memtables afterwards.
    pub(crate) fn submit(
        &self,
        supervisor: &crate::supervisor::Supervisor,
        poisoned: &crate::poison::PoisonSignal,
        record: &Arc<Record>,
    ) -> crate::Result<()> {
        let mut leading = {
            #[expect(clippy::expect_used)]
            let _queueing = self.handover.read().expect("lock is poisoned");

            while self.queue.try_push(record.clone()).is_none() {
                std::hint::spin_loop();
            }

            // Exactly one writer wins this, and it owes the queue a group.
            self.lease
                .compare_exchange(true, false, AcqRel, Relaxed)
                .is_ok()
        };

        let mut waited = 0u32;

        loop {
            if leading {
                let led = self.lead(supervisor, poisoned);
                self.hand_over();
                leading = false;
                waited = 0;
                led?;
            }

            match record.state.load(Acquire) {
                JOURNALED => break,
                TAKE_LEASE => leading = true,
                FAILED => return Err(crate::Error::Poisoned),
                _ => {
                    // **Spin briefly, then get out of the way.** The wait is usually a
                    // few microseconds, so parking outright would cost more than it
                    // saves — but a member that spins hot is a core the leader and the
                    // other members are not using to journal and fill memtables, and
                    // there are as many spinners as writers. Measured: unbounded
                    // spinning cost 24% at a hundred rows a commit, where the group is
                    // small enough that the wait is long relative to the work.
                    if waited < SPINS_BEFORE_YIELD {
                        waited += 1;
                        std::hint::spin_loop();
                    } else {
                        std::thread::yield_now();
                    }
                }
            }
        }

        // **The part the leader deliberately did not do.** Every member fills its own
        // memtables, concurrently with every other member and with the next group's
        // leader, ordered against them by the sequence number already assigned.
        #[expect(clippy::expect_used)]
        let ticket = record.ticket.get().expect("set before the state was published");

        // The window every ordering rule here exists to cover: this write's sequence
        // number is assigned and its journal record is durable, and not one of its rows
        // is in a memtable yet.
        #[cfg(feature = "__internal_whitebox")]
        crate::write_hook::journal_released(ticket.seqno);

        let (bytes, touched) = apply(&record.work, ticket.seqno);
        ticket.group.landed();

        supervisor.write_buffer_size.allocate(bytes);
        for keyspace in touched {
            let size = keyspace.tree.active_memtable().size();
            keyspace.check_memtable_rotate(size);
            keyspace.local_backpressure();
        }

        Ok(())
    }

    /// Drain what is queued, journal all of it under one acquisition, and release the
    /// members to fill their own memtables.
    fn lead(
        &self,
        supervisor: &crate::supervisor::Supervisor,
        poisoned: &crate::poison::PoisonSignal,
    ) -> crate::Result<()> {
        let mut members: Vec<Arc<Record>> = Vec::new();
        let mut bytes = 0;

        while members.len() < MAX_GROUP && bytes < MAX_GROUP_BYTES {
            let Some(record) = self.queue.try_pop() else {
                break;
            };
            bytes += record.work.bytes();
            members.push(record);
        }

        if members.is_empty() {
            return Ok(());
        }

        let count = members.len() as u64;

        #[expect(clippy::expect_used)]
        let mut writer = match supervisor.journal.get_writer() {
            Ok(writer) => writer,
            Err(e) => {
                fail(&members);
                return Err(e);
            }
        };

        // IMPORTANT: after the journal mutex, or the check races the poisoning.
        if poisoned.is_poisoned() {
            fail(&members);
            return Err(crate::Error::Poisoned);
        }

        // **One contiguous run for the group**, handed out in queue order, so the
        // ordering rule needs a single entry rather than one per member.
        let base = supervisor.seqno.next_n(count);
        let group = Arc::new(Group {
            outstanding: AtomicUsize::new(members.len()),
            pending: Mutex::new(Some(
                supervisor.snapshot_tracker.begin_range(base, count),
            )),
        });

        let mut durability: Option<crate::PersistMode> = None;

        for (at, member) in members.iter().enumerate() {
            let seqno = base + at as u64;

            let wrote = match &member.work {
                Work::Batch(items) => writer.write_batch(items.iter(), items.len(), seqno),
                Work::Single(item) => writer.write_raw(
                    item.keyspace.id,
                    &item.key,
                    &item.value,
                    item.value_type,
                    seqno,
                ),
            };

            if let Err(e) = wrote {
                log::error!("journal write failed, which is FATAL: {e:?}");
                poisoned.poison();
                // The group publishes nothing and holds nothing: dropping the guard
                // hands the run back so the watermark is not frozen behind a write that
                // never happened.
                group.pending.lock().ok().and_then(|mut p| p.take());
                fail(&members);
                return Err(crate::Error::Poisoned);
            }

            durability = strongest(durability, member.durability);
        }

        if let Some(mode) = durability {
            if let Err(e) = writer.persist(mode) {
                log::error!("persist failed, which is FATAL: {e:?}");
                poisoned.poison();
                group.pending.lock().ok().and_then(|mut p| p.take());
                fail(&members);
                return Err(crate::Error::Poisoned);
            }
        }

        // **The log is written; the memtables are nobody's business but their owners'.**
        drop(writer);

        for (at, member) in members.iter().enumerate() {
            let _ = member.ticket.set(Ticket {
                seqno: base + at as u64,
                group: group.clone(),
            });
            member.state.store(JOURNALED, Release);
        }

        Ok(())
    }

    /// Give the lease to the next writer in line, or put it back.
    ///
    /// Under the write half of `handover` so a writer cannot enqueue between the peek
    /// and the release and then wait for a leader nobody will become.
    fn hand_over(&self) {
        #[expect(clippy::expect_used)]
        let _closing = self.handover.write().expect("lock is poisoned");

        if let Some(next) = self.queue.peek() {
            next.state.store(TAKE_LEASE, Release);
        } else {
            self.lease.store(true, Release);
        }
    }
}

/// **The group persists as hard as its most demanding member asked for.**
///
/// A group mixes writers, and one of them may have asked for `fsync` while the rest did
/// not. Persisting once at the strongest level is what makes the group safe to merge:
/// every member gets at least the durability it requested, and the members that asked
/// for less get more, which is never wrong.
fn strongest(
    a: Option<crate::PersistMode>,
    b: Option<crate::PersistMode>,
) -> Option<crate::PersistMode> {
    fn rank(mode: crate::PersistMode) -> u8 {
        match mode {
            crate::PersistMode::Buffer => 1,
            crate::PersistMode::SyncData => 2,
            crate::PersistMode::SyncAll => 3,
        }
    }

    match (a, b) {
        (Some(a), Some(b)) => Some(if rank(a) >= rank(b) { a } else { b }),
        (only, None) | (None, only) => only,
    }
}

/// Tell every member the group is not happening.
fn fail(members: &[Arc<Record>]) {
    for member in members {
        member.state.store(FAILED, Release);
    }
}

/// Put one record's rows into their memtables, returning the bytes added and the
/// keyspaces that need a stall check.
fn apply(work: &Work, seqno: SeqNo) -> (u64, Vec<crate::Keyspace>) {
    let mut bytes = 0u64;
    let mut touched: Vec<crate::Keyspace> = Vec::new();

    for item in work.items() {
        let (added, _) = match item.value_type {
            ValueType::Value => {
                item.keyspace
                    .tree
                    .insert(item.key.clone(), item.value.clone(), seqno)
            }
            ValueType::Tombstone => item.keyspace.tree.remove(item.key.clone(), seqno),
            ValueType::WeakTombstone => item.keyspace.tree.remove_weak(item.key.clone(), seqno),
            ValueType::Indirection => unreachable!("not a user-visible write"),
        };

        bytes += added;

        if !touched.iter().any(|k| k.id == item.keyspace.id) {
            touched.push(item.keyspace.clone());
        }
    }

    (bytes, touched)
}
