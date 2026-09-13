// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

//! **A seam for testing write ordering**, behind `__internal_whitebox`.
//!
//! A batch releases the journal writer and *then* applies its rows to the memtables, so
//! there is a window in which its sequence number is taken but its rows are not yet
//! there. Every ordering rule in [`crate::snapshot_tracker`] exists to make that window
//! invisible to a reader.
//!
//! The window is real but short, so a test that waits for it to happen by luck is one
//! that passes for the wrong reason on a slow machine. This lets a batch be held open
//! inside it, deterministically, while another writer runs to completion.
//!
//! Compiled out entirely without the feature: the call site is a `#[cfg]`, not a branch.

use std::sync::{Arc, Mutex, OnceLock};

/// Called in the committing thread, with the sequence number that commit is carrying.
pub type Hook = Arc<dyn Fn(crate::SeqNo) + Send + Sync>;

fn slot() -> &'static Mutex<Option<Hook>> {
    static SLOT: OnceLock<Mutex<Option<Hook>>> = OnceLock::new();
    SLOT.get_or_init(|| Mutex::new(None))
}

fn set(hook: Option<Hook>) {
    #[expect(clippy::expect_used)]
    let mut slot = slot().lock().expect("lock is poisoned");
    *slot = hook;
}

/// Run `hook` in every committing thread once it has released the journal writer and
/// before it has applied a single row.
pub fn after_journal_released(hook: Hook) {
    set(Some(hook));
}

/// Remove the hook. Tests share a process, so one left armed reaches into the next.
pub fn clear() {
    set(None);
}

pub(crate) fn journal_released(seqno: crate::SeqNo) {
    // Cloned out from under the lock rather than called with it held: a hook that blocks
    // is the entire point, and blocking with this held would serialise the writers this
    // exists to overlap.
    let hook = {
        #[expect(clippy::expect_used)]
        let slot = slot().lock().expect("lock is poisoned");
        slot.clone()
    };

    if let Some(hook) = hook {
        hook(seqno);
    }
}
