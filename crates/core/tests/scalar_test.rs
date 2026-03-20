use gqlite_core::functions::scalar::*;
use gqlite_core::types::value::Value;

#[test]
fn test_lower() {
    assert_eq!(fn_lower(&[Value::String("HELLO".into())]).unwrap(), Value::String("hello".into()));
}

#[test]
fn test_upper() {
    assert_eq!(fn_upper(&[Value::String("hello".into())]).unwrap(), Value::String("HELLO".into()));
}

#[test]
fn test_trim() {
    assert_eq!(fn_trim(&[Value::String("  hi  ".into())]).unwrap(), Value::String("hi".into()));
}

#[test]
fn test_length() {
    assert_eq!(fn_length(&[Value::String("hello".into())]).unwrap(), Value::Int(5));
}

#[test]
fn test_concat() {
    assert_eq!(
        fn_concat(&[Value::String("a".into()), Value::String("b".into())]).unwrap(),
        Value::String("ab".into())
    );
}

#[test]
fn test_coalesce() {
    assert_eq!(fn_coalesce(&[Value::Null, Value::Int(42)]).unwrap(), Value::Int(42));
    assert_eq!(fn_coalesce(&[Value::Null]).unwrap(), Value::Null);
}

#[test]
fn test_null_propagation() {
    assert_eq!(fn_lower(&[Value::Null]).unwrap(), Value::Null);
}

#[test]
fn test_ltrim() {
    assert_eq!(fn_ltrim(&[Value::String("  hi  ".into())]).unwrap(), Value::String("hi  ".into()));
}

#[test]
fn test_rtrim() {
    assert_eq!(fn_rtrim(&[Value::String("  hi  ".into())]).unwrap(), Value::String("  hi".into()));
}

#[test]
fn test_substring() {
    assert_eq!(
        fn_substring(&[Value::String("hello".into()), Value::Int(1)]).unwrap(),
        Value::String("ello".into())
    );
    assert_eq!(
        fn_substring(&[Value::String("hello".into()), Value::Int(1), Value::Int(3)]).unwrap(),
        Value::String("ell".into())
    );
    assert_eq!(
        fn_substring(&[Value::String("hello".into()), Value::Int(10)]).unwrap(),
        Value::String(String::new())
    );
}

#[test]
fn test_replace() {
    assert_eq!(
        fn_replace(&[
            Value::String("hello world".into()),
            Value::String("world".into()),
            Value::String("rust".into())
        ])
        .unwrap(),
        Value::String("hello rust".into())
    );
}

#[test]
fn test_reverse() {
    assert_eq!(fn_reverse(&[Value::String("abc".into())]).unwrap(), Value::String("cba".into()));
}

#[test]
fn test_left() {
    assert_eq!(
        fn_left(&[Value::String("hello".into()), Value::Int(3)]).unwrap(),
        Value::String("hel".into())
    );
}

#[test]
fn test_right() {
    assert_eq!(
        fn_right(&[Value::String("hello".into()), Value::Int(3)]).unwrap(),
        Value::String("llo".into())
    );
}

#[test]
fn test_lpad() {
    assert_eq!(
        fn_lpad(&[Value::String("hi".into()), Value::Int(5)]).unwrap(),
        Value::String("   hi".into())
    );
    assert_eq!(
        fn_lpad(&[Value::String("hi".into()), Value::Int(5), Value::String("0".into())]).unwrap(),
        Value::String("000hi".into())
    );
}

#[test]
fn test_rpad() {
    assert_eq!(
        fn_rpad(&[Value::String("hi".into()), Value::Int(5)]).unwrap(),
        Value::String("hi   ".into())
    );
    assert_eq!(
        fn_rpad(&[Value::String("hi".into()), Value::Int(5), Value::String("0".into())]).unwrap(),
        Value::String("hi000".into())
    );
}

#[test]
fn test_repeat() {
    assert_eq!(
        fn_repeat(&[Value::String("ab".into()), Value::Int(3)]).unwrap(),
        Value::String("ababab".into())
    );
}

#[test]
fn test_contains() {
    assert_eq!(
        fn_contains(&[Value::String("hello".into()), Value::String("ell".into())]).unwrap(),
        Value::Bool(true)
    );
    assert_eq!(
        fn_contains(&[Value::String("hello".into()), Value::String("xyz".into())]).unwrap(),
        Value::Bool(false)
    );
}

#[test]
fn test_starts_with() {
    assert_eq!(
        fn_starts_with(&[Value::String("hello".into()), Value::String("hel".into())]).unwrap(),
        Value::Bool(true)
    );
}

#[test]
fn test_ends_with() {
    assert_eq!(
        fn_ends_with(&[Value::String("hello".into()), Value::String("llo".into())]).unwrap(),
        Value::Bool(true)
    );
}

#[test]
#[allow(clippy::approx_constant)]
fn test_abs() {
    assert_eq!(fn_abs(&[Value::Int(-5)]).unwrap(), Value::Int(5));
    assert_eq!(fn_abs(&[Value::Float(-3.14)]).unwrap(), Value::Float(3.14));
}

#[test]
fn test_to_string() {
    assert_eq!(fn_to_string(&[Value::Int(42)]).unwrap(), Value::String("42".into()));
}

// --- List function tests ---

fn sample_list() -> Value {
    Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
}

#[test]
fn test_list_len() {
    assert_eq!(fn_list_len(&[sample_list()]).unwrap(), Value::Int(3));
    assert_eq!(fn_list_len(&[Value::List(vec![])]).unwrap(), Value::Int(0));
}

#[test]
fn test_list_extract() {
    // 1-based indexing
    assert_eq!(fn_list_extract(&[sample_list(), Value::Int(1)]).unwrap(), Value::Int(1));
    assert_eq!(fn_list_extract(&[sample_list(), Value::Int(3)]).unwrap(), Value::Int(3));
    // negative index
    assert_eq!(fn_list_extract(&[sample_list(), Value::Int(-1)]).unwrap(), Value::Int(3));
    // out of bounds
    assert_eq!(fn_list_extract(&[sample_list(), Value::Int(10)]).unwrap(), Value::Null);
}

#[test]
fn test_list_append() {
    assert_eq!(
        fn_list_append(&[sample_list(), Value::Int(4)]).unwrap(),
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)])
    );
}

#[test]
fn test_list_prepend() {
    assert_eq!(
        fn_list_prepend(&[Value::Int(0), sample_list()]).unwrap(),
        Value::List(vec![Value::Int(0), Value::Int(1), Value::Int(2), Value::Int(3)])
    );
}

#[test]
fn test_list_concat() {
    let a = Value::List(vec![Value::Int(1), Value::Int(2)]);
    let b = Value::List(vec![Value::Int(3), Value::Int(4)]);
    assert_eq!(
        fn_list_concat(&[a, b]).unwrap(),
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4)])
    );
}

#[test]
fn test_list_contains() {
    assert_eq!(fn_list_contains(&[sample_list(), Value::Int(2)]).unwrap(), Value::Bool(true));
    assert_eq!(fn_list_contains(&[sample_list(), Value::Int(9)]).unwrap(), Value::Bool(false));
}

#[test]
fn test_list_reverse() {
    assert_eq!(
        fn_list_reverse(&[sample_list()]).unwrap(),
        Value::List(vec![Value::Int(3), Value::Int(2), Value::Int(1)])
    );
}

#[test]
fn test_list_sort() {
    let unsorted = Value::List(vec![Value::Int(3), Value::Int(1), Value::Int(2)]);
    assert_eq!(
        fn_list_sort(&[unsorted]).unwrap(),
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
    );
}

#[test]
fn test_list_distinct() {
    let dupes = Value::List(vec![
        Value::Int(1),
        Value::Int(2),
        Value::Int(1),
        Value::Int(3),
        Value::Int(2),
    ]);
    assert_eq!(
        fn_list_distinct(&[dupes]).unwrap(),
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
    );
}

#[test]
fn test_range() {
    assert_eq!(
        fn_range(&[Value::Int(1), Value::Int(5)]).unwrap(),
        Value::List(vec![
            Value::Int(1),
            Value::Int(2),
            Value::Int(3),
            Value::Int(4),
            Value::Int(5)
        ])
    );
    // with step
    assert_eq!(
        fn_range(&[Value::Int(0), Value::Int(10), Value::Int(3)]).unwrap(),
        Value::List(vec![Value::Int(0), Value::Int(3), Value::Int(6), Value::Int(9)])
    );
    // descending
    assert_eq!(
        fn_range(&[Value::Int(5), Value::Int(1)]).unwrap(),
        Value::List(vec![
            Value::Int(5),
            Value::Int(4),
            Value::Int(3),
            Value::Int(2),
            Value::Int(1)
        ])
    );
}
