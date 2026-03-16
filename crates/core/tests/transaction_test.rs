use gqlite_core::transaction::TransactionManager;

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
