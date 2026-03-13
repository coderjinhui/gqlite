//! Transaction management with SWMR (Single Writer Multiple Reader) concurrency.
//!
//! - Multiple concurrent read-only transactions are allowed.
//! - At most one read-write transaction can be active at a time.
//! - Write exclusion is enforced via a `Mutex`.

pub mod wal;

use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::{Mutex, RwLock};

use crate::error::GqliteError;

// ── Types ────────────────────────────────────────────────────────

/// Monotonically increasing transaction identifier.
pub type TxnId = u64;

/// Transaction state machine: Active → Committed | RolledBack.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

/// Whether this transaction may mutate the database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionType {
    ReadOnly,
    ReadWrite,
}

// ── Transaction ──────────────────────────────────────────────────

/// A single transaction handle.
#[derive(Debug)]
pub struct Transaction {
    pub id: TxnId,
    pub tx_type: TransactionType,
    pub state: TransactionState,
    /// Snapshot timestamp — the `last_committed_id` at the time this txn began.
    pub start_ts: TxnId,
}

impl Transaction {
    /// Whether this transaction is still active.
    pub fn is_active(&self) -> bool {
        self.state == TransactionState::Active
    }

    /// Whether this transaction is read-write.
    pub fn is_read_write(&self) -> bool {
        self.tx_type == TransactionType::ReadWrite
    }
}

// ── WriteGuard ───────────────────────────────────────────────────

/// RAII guard that holds the write lock for the duration of a write transaction.
/// When this guard is dropped the write lock is released, allowing another
/// write transaction to begin.
pub struct WriteGuard<'a> {
    _guard: parking_lot::MutexGuard<'a, ()>,
}

// ── TransactionManager ──────────────────────────────────────────

/// Manages transaction lifecycle with SWMR semantics.
pub struct TransactionManager {
    /// Next transaction id to assign.
    next_txn_id: AtomicU64,
    /// Mutex that enforces at most one active write transaction.
    write_lock: Mutex<()>,
    /// Set of currently active read-only transaction ids.
    active_read_txns: RwLock<Vec<TxnId>>,
    /// The id of the most recently committed transaction.
    last_committed_id: AtomicU64,
}

impl TransactionManager {
    /// Create a new TransactionManager.
    pub fn new() -> Self {
        Self {
            next_txn_id: AtomicU64::new(1),
            write_lock: Mutex::new(()),
            active_read_txns: RwLock::new(Vec::new()),
            last_committed_id: AtomicU64::new(0),
        }
    }

    /// Begin a read-only transaction. Never blocks.
    pub fn begin_read_only(&self) -> Transaction {
        let id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let snapshot = self.last_committed_id.load(Ordering::SeqCst);
        self.active_read_txns.write().push(id);
        Transaction {
            id,
            tx_type: TransactionType::ReadOnly,
            state: TransactionState::Active,
            start_ts: snapshot,
        }
    }

    /// Begin a read-write transaction.
    ///
    /// Returns `Err` if another write transaction is already active.
    /// The returned [`WriteGuard`] must be kept alive for the duration of the
    /// transaction — dropping it releases the write lock.
    pub fn begin_read_write(&self) -> Result<(Transaction, WriteGuard<'_>), GqliteError> {
        let guard = self
            .write_lock
            .try_lock()
            .ok_or_else(|| GqliteError::Transaction("another write transaction is active".into()))?;
        let id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let snapshot = self.last_committed_id.load(Ordering::SeqCst);
        Ok((
            Transaction {
                id,
                tx_type: TransactionType::ReadWrite,
                state: TransactionState::Active,
                start_ts: snapshot,
            },
            WriteGuard { _guard: guard },
        ))
    }

    /// Commit a transaction. For read-write transactions, this advances the
    /// `last_committed_id`.
    pub fn commit(&self, txn: &mut Transaction) {
        assert!(txn.is_active(), "cannot commit an inactive transaction");
        txn.state = TransactionState::Committed;
        if txn.is_read_write() {
            self.last_committed_id.store(txn.id, Ordering::SeqCst);
        } else {
            self.remove_active_read(txn.id);
        }
    }

    /// Roll back a transaction, discarding any changes.
    pub fn rollback(&self, txn: &mut Transaction) {
        assert!(txn.is_active(), "cannot rollback an inactive transaction");
        txn.state = TransactionState::RolledBack;
        if !txn.is_read_write() {
            self.remove_active_read(txn.id);
        }
    }

    /// The id of the most recently committed transaction.
    pub fn last_committed_id(&self) -> TxnId {
        self.last_committed_id.load(Ordering::SeqCst)
    }

    /// Return the set of active read transaction ids (for debugging / testing).
    pub fn active_read_count(&self) -> usize {
        self.active_read_txns.read().len()
    }

    // -- internal --

    fn remove_active_read(&self, id: TxnId) {
        let mut reads = self.active_read_txns.write();
        if let Some(pos) = reads.iter().position(|&x| x == id) {
            reads.swap_remove(pos);
        }
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

// ── Tests ────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_only_transactions_dont_block() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin_read_only();
        let t2 = mgr.begin_read_only();
        let t3 = mgr.begin_read_only();
        assert!(t1.is_active());
        assert!(t2.is_active());
        assert!(t3.is_active());
        assert_eq!(mgr.active_read_count(), 3);
    }

    #[test]
    fn write_transaction_excludes_second_writer() {
        let mgr = TransactionManager::new();
        let (_txn1, _guard1) = mgr.begin_read_write().unwrap();
        // Second write should fail
        let result = mgr.begin_read_write();
        assert!(result.is_err());
    }

    #[test]
    fn commit_releases_write_lock() {
        let mgr = TransactionManager::new();
        {
            let (mut txn, _guard) = mgr.begin_read_write().unwrap();
            mgr.commit(&mut txn);
            // _guard drops here, releasing the write lock
        }
        // Now a new write transaction should succeed
        let (txn2, _guard2) = mgr.begin_read_write().unwrap();
        assert!(txn2.is_active());
    }

    #[test]
    fn rollback_releases_write_lock() {
        let mgr = TransactionManager::new();
        {
            let (mut txn, _guard) = mgr.begin_read_write().unwrap();
            mgr.rollback(&mut txn);
        }
        let (txn2, _guard2) = mgr.begin_read_write().unwrap();
        assert!(txn2.is_active());
    }

    #[test]
    fn commit_advances_last_committed_id() {
        let mgr = TransactionManager::new();
        assert_eq!(mgr.last_committed_id(), 0);

        let (mut txn, _guard) = mgr.begin_read_write().unwrap();
        let txn_id = txn.id;
        mgr.commit(&mut txn);
        assert_eq!(mgr.last_committed_id(), txn_id);
    }

    #[test]
    fn read_txn_snapshot_captures_committed_state() {
        let mgr = TransactionManager::new();

        // Start a read before any commit
        let r1 = mgr.begin_read_only();
        assert_eq!(r1.start_ts, 0);

        // Commit a write
        {
            let (mut w, _g) = mgr.begin_read_write().unwrap();
            mgr.commit(&mut w);
        }

        // Start a read after commit — snapshot should reflect committed txn
        let r2 = mgr.begin_read_only();
        assert!(r2.start_ts > 0);
    }

    #[test]
    fn concurrent_read_and_write() {
        let mgr = TransactionManager::new();
        // Read transaction active
        let _r = mgr.begin_read_only();
        // Write should still succeed (reads don't block writes)
        let result = mgr.begin_read_write();
        assert!(result.is_ok());
    }

    #[test]
    fn commit_read_removes_from_active() {
        let mgr = TransactionManager::new();
        let mut r = mgr.begin_read_only();
        assert_eq!(mgr.active_read_count(), 1);
        mgr.commit(&mut r);
        assert_eq!(mgr.active_read_count(), 0);
    }

    #[test]
    fn rollback_read_removes_from_active() {
        let mgr = TransactionManager::new();
        let mut r = mgr.begin_read_only();
        assert_eq!(mgr.active_read_count(), 1);
        mgr.rollback(&mut r);
        assert_eq!(mgr.active_read_count(), 0);
    }

    #[test]
    fn txn_ids_are_monotonic() {
        let mgr = TransactionManager::new();
        let t1 = mgr.begin_read_only();
        let t2 = mgr.begin_read_only();
        let (t3, _g) = mgr.begin_read_write().unwrap();
        assert!(t1.id < t2.id);
        assert!(t2.id < t3.id);
    }
}
