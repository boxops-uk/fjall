// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

pub mod item;

use crate::{Database, Keyspace, PersistMode};
use item::Item;
use lsm_tree::{AbstractTree, UserKey, UserValue, ValueType};
use std::collections::HashSet;

/// An atomic write batch
///
/// Allows atomically writing across keyspaces inside the [`Database`].
pub struct WriteBatch {
    pub(crate) data: Vec<Item>,
    db: Database,
    durability: Option<PersistMode>,
}

impl WriteBatch {
    /// Initializes a new write batch.
    ///
    /// This function is called by [`Database::batch`].
    pub(crate) fn new(db: Database) -> Self {
        Self {
            data: Vec::new(),
            db,
            durability: None,
        }
    }

    /// Initializes a new write batch with preallocated capacity.
    ///
    /// ### Note
    ///
    /// "Capacity" refers to the number of batch item slots, not their size in memory.
    #[must_use]
    pub fn with_capacity(db: Database, capacity: usize) -> Self {
        Self {
            data: Vec::with_capacity(capacity),
            db,
            durability: None,
        }
    }

    /// Gets the number of batched items.
    #[must_use]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if there are no batches items (yet).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Sets the durability level.
    #[must_use]
    pub fn durability(mut self, mode: Option<PersistMode>) -> Self {
        self.durability = mode;
        self
    }

    /// Inserts a key-value pair into the batch.
    pub fn insert<K: Into<UserKey>, V: Into<UserValue>>(&mut self, p: &Keyspace, key: K, value: V) {
        self.data
            .push(Item::new(p.clone(), key, value, ValueType::Value));
    }

    /// Removes a key-value pair.
    pub fn remove<K: Into<UserKey>>(&mut self, p: &Keyspace, key: K) {
        self.data
            .push(Item::new(p.clone(), key, vec![], ValueType::Tombstone));
    }

    /// Adds a weak tombstone marker for a key.
    ///
    /// The tombstone marker of this delete operation will vanish when it
    /// collides with its corresponding insertion.
    /// This may cause older versions of the value to be resurrected, so it should
    /// only be used and preferred in scenarios where a key is only ever written once.
    ///
    /// # Experimental
    ///
    /// This function is currently experimental.
    #[doc(hidden)]
    pub fn remove_weak<K: Into<UserKey>>(&mut self, p: &Keyspace, key: K) {
        self.data
            .push(Item::new(p.clone(), key, vec![], ValueType::WeakTombstone));
    }

    /// Commits the batch to the [`Database`] atomically.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    #[allow(clippy::missing_panics_doc)]
    pub fn commit(mut self) -> crate::Result<()> {
        if self.is_empty() {
            return Ok(());
        }

        // **The journal is written by whichever writer is leading, not necessarily this
        // one.** Batches from several threads are logged under one acquisition of the
        // journal writer and one `persist`, which is what makes a small commit cheap;
        // this thread then fills its own memtables, concurrently with the rest of the
        // group. See [`crate::write_pipeline`] for why the leader stops where it does.
        let record = std::sync::Arc::new(crate::write_pipeline::Record::new(
            crate::write_pipeline::Work::Batch(std::mem::take(&mut self.data)),
            self.durability,
        ));

        self.db
            .supervisor
            .pipeline
            .submit(&self.db.supervisor, &self.db.is_poisoned, &record)
    }
}
