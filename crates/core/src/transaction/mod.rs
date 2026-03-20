//! Transaction management with SWMR (Single Writer Multiple Reader) concurrency.
//!
//! - Multiple concurrent read-only transactions are allowed.
//! - At most one read-write transaction can be active at a time.
//! - Write exclusion is enforced via a `Mutex`.

pub mod wal;
pub mod write_set;

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

    /// Create a TransactionManager with state recovered from WAL replay.
    ///
    /// `last_committed` is the highest committed transaction ID found during
    /// WAL recovery. The next transaction ID will start after it.
    pub fn with_recovered_state(last_committed: u64) -> Self {
        Self {
            next_txn_id: AtomicU64::new(last_committed + 1),
            write_lock: Mutex::new(()),
            active_read_txns: RwLock::new(Vec::new()),
            last_committed_id: AtomicU64::new(last_committed),
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
        let guard = self.write_lock.try_lock().ok_or_else(|| {
            GqliteError::Transaction("another write transaction is active".into())
        })?;
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

    /// Return the minimum `start_ts` among all active read transactions.
    /// Returns `None` if no read transactions are active (meaning all versions
    /// older than `last_committed_id` are safe to garbage-collect).
    pub fn min_active_read_ts(&self) -> Option<TxnId> {
        let reads = self.active_read_txns.read();
        // Active read txns store their txn id, but their snapshot is `start_ts`
        // which was captured at begin time.  Since txn ids are monotonic and
        // `start_ts` is always <= txn.id, the minimum txn id in the active set
        // gives us a conservative lower bound.
        reads.iter().copied().min()
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
