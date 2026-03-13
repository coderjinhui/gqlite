//! Built-in scalar functions.

use crate::error::GqliteError;
use crate::types::value::Value;

pub fn fn_lower(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.to_lowercase())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("lower() expects a string".into())),
    }
}

pub fn fn_upper(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.to_uppercase())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("upper() expects a string".into())),
    }
}

pub fn fn_trim(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.trim().to_string())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("trim() expects a string".into())),
    }
}

pub fn fn_length(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::Int(s.len() as i64)),
        Some(Value::List(l)) => Ok(Value::Int(l.len() as i64)),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("length() expects a string or list".into())),
    }
}

pub fn fn_concat(args: &[Value]) -> Result<Value, GqliteError> {
    let mut result = String::new();
    for arg in args {
        match arg {
            Value::String(s) => result.push_str(s),
            Value::Null => {}
            other => result.push_str(&other.to_string()),
        }
    }
    Ok(Value::String(result))
}

pub fn fn_contains(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("contains() expects 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(haystack), Value::String(needle)) => {
            Ok(Value::Bool(haystack.contains(needle.as_str())))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("contains() expects strings".into())),
    }
}

pub fn fn_starts_with(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("starts_with() expects 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::String(prefix)) => {
            Ok(Value::Bool(s.starts_with(prefix.as_str())))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("starts_with() expects strings".into())),
    }
}

pub fn fn_ends_with(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("ends_with() expects 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::String(suffix)) => {
            Ok(Value::Bool(s.ends_with(suffix.as_str())))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("ends_with() expects strings".into())),
    }
}

pub fn fn_to_string(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(v) => Ok(Value::String(v.to_string())),
        None => Err(GqliteError::Execution("to_string() expects 1 argument".into())),
    }
}

pub fn fn_coalesce(args: &[Value]) -> Result<Value, GqliteError> {
    for arg in args {
        if !arg.is_null() {
            return Ok(arg.clone());
        }
    }
    Ok(Value::Null)
}

pub fn fn_abs(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Int(i.abs())),
        Some(Value::Float(f)) => Ok(Value::Float(f.abs())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("abs() expects a number".into())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lower() {
        assert_eq!(
            fn_lower(&[Value::String("HELLO".into())]).unwrap(),
            Value::String("hello".into())
        );
    }

    #[test]
    fn test_upper() {
        assert_eq!(
            fn_upper(&[Value::String("hello".into())]).unwrap(),
            Value::String("HELLO".into())
        );
    }

    #[test]
    fn test_trim() {
        assert_eq!(
            fn_trim(&[Value::String("  hi  ".into())]).unwrap(),
            Value::String("hi".into())
        );
    }

    #[test]
    fn test_length() {
        assert_eq!(
            fn_length(&[Value::String("hello".into())]).unwrap(),
            Value::Int(5)
        );
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
        assert_eq!(
            fn_coalesce(&[Value::Null, Value::Int(42)]).unwrap(),
            Value::Int(42)
        );
        assert_eq!(fn_coalesce(&[Value::Null]).unwrap(), Value::Null);
    }

    #[test]
    fn test_null_propagation() {
        assert_eq!(fn_lower(&[Value::Null]).unwrap(), Value::Null);
    }
}
