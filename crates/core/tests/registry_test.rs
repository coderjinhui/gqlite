use gqlite_core::functions::registry::FunctionRegistry;

#[test]
fn registry_case_insensitive() {
    let reg = FunctionRegistry::new();
    assert!(reg.get_scalar("lower").is_some());
    assert!(reg.get_scalar("LOWER").is_some());
    assert!(reg.get_scalar("Lower").is_some());
}

#[test]
fn unknown_function() {
    let reg = FunctionRegistry::new();
    assert!(reg.get_scalar("nonexistent").is_none());
}
