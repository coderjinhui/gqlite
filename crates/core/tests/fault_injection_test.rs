use gqlite_core::testing::fault_injection::{
    check_fault, FaultInjector, CHECKPOINT_FAULT, STORAGE_WRITE_FAULT, WAL_WRITE_FAULT,
};

#[test]
fn injector_disabled_by_default() {
    let inj = FaultInjector::new();
    assert!(!inj.check());
    assert!(!inj.check());
    assert!(!inj.was_triggered());
}

#[test]
fn injector_triggers_after_n_hits() {
    let inj = FaultInjector::new();
    inj.arm(3);

    assert!(!inj.check()); // hit 1
    assert!(!inj.check()); // hit 2
    assert!(inj.check()); // hit 3 — triggers!
    assert!(inj.was_triggered());

    // After trigger, subsequent checks don't re-trigger
    assert!(!inj.check());

    inj.disarm();
    assert!(!inj.was_triggered());
}

#[test]
fn check_fault_returns_error_on_trigger() {
    let inj = FaultInjector::new();
    inj.arm(1);

    let result = check_fault(&inj, "test fault");
    assert!(result.is_err());
    let err = format!("{}", result.err().unwrap());
    assert!(err.contains("fault injection: test fault"));

    inj.disarm();
}

#[test]
fn global_injectors_are_independent() {
    WAL_WRITE_FAULT.disarm();
    CHECKPOINT_FAULT.disarm();
    STORAGE_WRITE_FAULT.disarm();

    WAL_WRITE_FAULT.arm(1);
    assert!(WAL_WRITE_FAULT.check());
    assert!(!CHECKPOINT_FAULT.check());
    assert!(!STORAGE_WRITE_FAULT.check());

    WAL_WRITE_FAULT.disarm();
}
