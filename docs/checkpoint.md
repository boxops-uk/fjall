# Reclaiming a write-ahead journal the tables already hold

**Status: implemented on this fork as `Database::checkpoint`** (`src/db.rs`), with tests in
`tests/checkpoint.rs`. This is the write-up it came from, kept because it is the reasoning
and the reproduction — and because it is the description this becomes if it is ever offered
upstream, where the machinery is still `pub(crate)`.

One thing the original did not know, found when wiring it into a real sealing path: **one
keyspace short of flushed is the same as none.** A journal is evicted only once *every*
watermarked keyspace has persisted past it, and a keyspace with a resident memtable and no
tables has persisted past nothing — so a single unflushed keyspace stops the whole reclaim
silently. The rotation still happens; the eviction does not, and you are left holding both
journals. Flush everything, including whatever metadata keyspace the application keeps, then
checkpoint.

---

# Feature request: a way to say "I am finished writing" and drop the redundant WAL

**Version:** 3.1.10 (also checked 3.1.8; identical in the relevant code)

## What we're doing

We build write-once databases. An index is written, then sealed and never written to
again — after sealing we flush every memtable to tables (`Keyspace::rotate_memtable_and_wait`
on every keyspace), compact, and fsync. From that point the artifact is immutable and is
opened read-only, often by short-lived processes.

## What happens

After sealing, the write-ahead journal still holds every entry that was ever written, and
it is replayed into a memtable at **every open**. The data is already in the tables, so the
replay is pure duplication: it costs the open time, and it makes `approximate_len` report
double (tables + memtable).

Measured on one of our databases — 550,000 entries, same database in two states:

| | open | `approximate_len` | journal on disk |
|---|---|---|---|
| journal still present | **3,105 ms** | 1,100,000 (double) | 87 MB |
| journal gone | **1.7 ms** | 550,000 (exact) | 0 |

A smaller one, 200,000 entries, keeps ~31 MB of journal indefinitely, through any number
of flushes.

## Why it happens (as far as we can tell)

1. The active journal is only rotated when its write position passes a hardcoded
   threshold — `worker_pool.rs:194`, `if journal_writer.pos()? > 64_000_000`, and only
   inside the `WorkerMessage::Flush` handler.
2. `JournalManager::maintenance()` (`journal/manager.rs:114`) only considers `self.items`,
   which are the **sealed** journals. The active journal is never a candidate for eviction.

So a database whose entire write history fits under ~64 MB of journal never rotates, and
therefore never evicts — no matter how many times we flush every memtable to a table. Above
that threshold it rotates during ingest and the residue is whatever the active journal
holds at the end, which is why our 550,000-entry case sometimes ends up clean and our
200,000-entry case never does.

## What we're asking for

A public method on `Database` meaning **"roll the active journal now, then run journal
maintenance"** — so an application that knows it has stopped writing can drop a WAL that is
entirely redundant with the tables. Something like:

```rust
/// Roll the active journal and reclaim any journal fully covered by tables.
///
/// For an application that has finished writing: after flushing memtables to tables,
/// this drops the write-ahead log those tables already contain.
pub fn checkpoint(&self) -> crate::Result<()>
```

The machinery already exists and is exactly what the 64 MB path calls —
`JournalManager::rotate_journal` followed by `JournalManager::maintenance` — both currently
`pub(crate)`. We would be happy with any name and any shape that has that effect.

**A smaller alternative**, if a new method is unwelcome: make the `64_000_000` rotation
trigger configurable, the way `max_journaling_size` already is. We could then set it low
before a final flush. A method is nicer for us because we only want this at seal time and
not during normal writing, but either would solve it.

## What we tried that does not work

- **Flushing every memtable** (`rotate_memtable_and_wait` on every keyspace, including the
  one holding our own metadata), then fsyncing. The flush lands, but nothing rotates the
  active journal, so maintenance has no sealed journal to collect.
- **A second flush pass afterwards**, in case maintenance was running before the first
  flush had landed. No effect; and on an already-reopened database it makes things worse,
  because the replayed memtable gets flushed into *new* tables and `approximate_len` then
  reports triple.
- **Waiting.** Sleeping with the database open does not trigger it — maintenance runs from
  the flush worker, and with nothing to flush there is nothing to run it.
- **Lowering `max_journaling_size`.** That value is only consulted *after* the hardcoded
  rotation has happened, to ask straggler keyspaces to flush; it does not cause rotation.

## Reproduction

Standalone, `fjall = "3.1.10"`, nothing else:

```rust
use fjall::{Database, KeyspaceCreateOptions, PersistMode};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let dir = std::path::Path::new("/tmp/fjcheck/data");
    let _ = std::fs::remove_dir_all(dir);

    {
        let db = Database::builder(dir).open()?;
        let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;
        for i in 0..200_000u32 {
            ks.insert(i.to_be_bytes(), b"a reasonably sized value, as facts go")?;
        }

        ks.rotate_memtable_and_wait()?;   // everything is in tables now
        db.persist(PersistMode::SyncAll)?;

        println!(
            "after flushing everything to tables: journal {} bytes in {} file(s)",
            db.journal_disk_space()?,
            db.journal_count()
        );
    }

    let t = std::time::Instant::now();
    let db = Database::builder(dir).open()?;
    let ks = db.keyspace("data", KeyspaceCreateOptions::default)?;
    println!(
        "reopened in {:?}: approximate_len {} (should be 200000), journal {} bytes",
        t.elapsed(),
        ks.approximate_len(),
        db.journal_disk_space()?
    );
    Ok(())
}
```

Prints:

```
after flushing everything to tables: journal 67108864 bytes in 1 file(s)
reopened in 620.792304ms: approximate_len 400000 (should be 200000), journal 17600000 bytes
```

Every one of the 200,000 entries is in the tables *and* still in the journal, and the
reopen spends 620 ms replaying them into a memtable that duplicates the tables exactly.
(The 67,108,864 before closing is the preallocated journal file; 17,600,000 after reopening
is what it actually holds.)

There is nothing we can call between the flush and the close that would reclaim it.
