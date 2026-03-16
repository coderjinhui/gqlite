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

pub fn fn_ltrim(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.trim_start().to_string())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("ltrim() expects a string".into())),
    }
}

pub fn fn_rtrim(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.trim_end().to_string())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("rtrim() expects a string".into())),
    }
}

pub fn fn_substring(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() < 2 {
        return Err(GqliteError::Execution("substring() expects 2-3 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Int(start)) => {
            let start = (*start).max(0) as usize;
            if start >= s.len() {
                return Ok(Value::String(String::new()));
            }
            if args.len() >= 3 {
                if let Value::Int(len) = &args[2] {
                    let len = (*len).max(0) as usize;
                    Ok(Value::String(s.chars().skip(start).take(len).collect()))
                } else {
                    Ok(Value::String(s[start..].to_string()))
                }
            } else {
                Ok(Value::String(s[start..].to_string()))
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("substring() expects (string, int[, int])".into())),
    }
}

pub fn fn_replace(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 3 {
        return Err(GqliteError::Execution("replace() expects 3 arguments".into()));
    }
    match (&args[0], &args[1], &args[2]) {
        (Value::String(s), Value::String(from), Value::String(to)) => {
            Ok(Value::String(s.replace(from.as_str(), to.as_str())))
        }
        (Value::Null, _, _) | (_, Value::Null, _) | (_, _, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("replace() expects strings".into())),
    }
}

pub fn fn_reverse(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => Ok(Value::String(s.chars().rev().collect())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("reverse() expects a string".into())),
    }
}

pub fn fn_left(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("left() expects 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Int(n)) => {
            let n = (*n).max(0) as usize;
            Ok(Value::String(s.chars().take(n).collect()))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("left() expects (string, int)".into())),
    }
}

pub fn fn_right(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("right() expects 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Int(n)) => {
            let n = (*n).max(0) as usize;
            let len = s.chars().count();
            let skip = if n >= len { 0 } else { len - n };
            Ok(Value::String(s.chars().skip(skip).collect()))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("right() expects (string, int)".into())),
    }
}

pub fn fn_lpad(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() < 2 {
        return Err(GqliteError::Execution("lpad() expects 2-3 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Int(width)) => {
            let width = (*width).max(0) as usize;
            let pad_char = if args.len() >= 3 {
                if let Value::String(p) = &args[2] {
                    p.chars().next().unwrap_or(' ')
                } else {
                    ' '
                }
            } else {
                ' '
            };
            let len = s.chars().count();
            if len >= width {
                Ok(Value::String(s.chars().take(width).collect()))
            } else {
                let padding: String = std::iter::repeat(pad_char).take(width - len).collect();
                Ok(Value::String(format!("{}{}", padding, s)))
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("lpad() expects (string, int[, string])".into())),
    }
}

pub fn fn_rpad(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() < 2 {
        return Err(GqliteError::Execution("rpad() expects 2-3 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Int(width)) => {
            let width = (*width).max(0) as usize;
            let pad_char = if args.len() >= 3 {
                if let Value::String(p) = &args[2] {
                    p.chars().next().unwrap_or(' ')
                } else {
                    ' '
                }
            } else {
                ' '
            };
            let len = s.chars().count();
            if len >= width {
                Ok(Value::String(s.chars().take(width).collect()))
            } else {
                let padding: String = std::iter::repeat(pad_char).take(width - len).collect();
                Ok(Value::String(format!("{}{}", s, padding)))
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("rpad() expects (string, int[, string])".into())),
    }
}

pub fn fn_repeat(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("repeat() expects 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::String(s), Value::Int(n)) => {
            let n = (*n).max(0) as usize;
            Ok(Value::String(s.repeat(n)))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("repeat() expects (string, int)".into())),
    }
}

// ---------------------------------------------------------------------------
// List functions
// ---------------------------------------------------------------------------

pub fn fn_list_len(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::List(l)) => Ok(Value::Int(l.len() as i64)),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_len() expects a list".into())),
    }
}

pub fn fn_list_extract(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("list_extract() expects 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::List(l), Value::Int(idx)) => {
            // 1-based indexing; negative counts from end
            let len = l.len() as i64;
            let i = if *idx > 0 {
                *idx - 1
            } else if *idx < 0 {
                len + *idx
            } else {
                return Ok(Value::Null);
            };
            if i < 0 || i >= len {
                Ok(Value::Null)
            } else {
                Ok(l[i as usize].clone())
            }
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_extract() expects (list, int)".into())),
    }
}

pub fn fn_list_append(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("list_append() expects 2 arguments".into()));
    }
    match &args[0] {
        Value::List(l) => {
            let mut new_list = l.clone();
            new_list.push(args[1].clone());
            Ok(Value::List(new_list))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_append() expects (list, value)".into())),
    }
}

pub fn fn_list_prepend(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("list_prepend() expects 2 arguments".into()));
    }
    match &args[1] {
        Value::List(l) => {
            let mut new_list = vec![args[0].clone()];
            new_list.extend_from_slice(l);
            Ok(Value::List(new_list))
        }
        Value::Null => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_prepend() expects (value, list)".into())),
    }
}

pub fn fn_list_concat(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("list_concat() expects 2 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::List(a), Value::List(b)) => {
            let mut new_list = a.clone();
            new_list.extend_from_slice(b);
            Ok(Value::List(new_list))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_concat() expects (list, list)".into())),
    }
}

pub fn fn_list_contains(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() != 2 {
        return Err(GqliteError::Execution("list_contains() expects 2 arguments".into()));
    }
    match &args[0] {
        Value::List(l) => Ok(Value::Bool(l.contains(&args[1]))),
        Value::Null => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_contains() expects (list, value)".into())),
    }
}

pub fn fn_list_reverse(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::List(l)) => {
            let mut new_list = l.clone();
            new_list.reverse();
            Ok(Value::List(new_list))
        }
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_reverse() expects a list".into())),
    }
}

pub fn fn_list_sort(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::List(l)) => {
            let mut new_list = l.clone();
            new_list.sort_by(|a, b| value_cmp(a, b));
            Ok(Value::List(new_list))
        }
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_sort() expects a list".into())),
    }
}

pub fn fn_list_distinct(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::List(l)) => {
            let mut seen = Vec::new();
            for v in l {
                if !seen.contains(v) {
                    seen.push(v.clone());
                }
            }
            Ok(Value::List(seen))
        }
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("list_distinct() expects a list".into())),
    }
}

pub fn fn_range(args: &[Value]) -> Result<Value, GqliteError> {
    if args.len() < 2 || args.len() > 3 {
        return Err(GqliteError::Execution("range() expects 2-3 arguments".into()));
    }
    match (&args[0], &args[1]) {
        (Value::Int(start), Value::Int(end)) => {
            let step = if args.len() == 3 {
                match &args[2] {
                    Value::Int(s) => *s,
                    _ => return Err(GqliteError::Execution("range() step must be int".into())),
                }
            } else if end >= start {
                1
            } else {
                -1
            };
            if step == 0 {
                return Err(GqliteError::Execution("range() step cannot be 0".into()));
            }
            let mut result = Vec::new();
            let mut cur = *start;
            if step > 0 {
                while cur <= *end {
                    result.push(Value::Int(cur));
                    cur += step;
                }
            } else {
                while cur >= *end {
                    result.push(Value::Int(cur));
                    cur += step;
                }
            }
            Ok(Value::List(result))
        }
        (Value::Null, _) | (_, Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("range() expects (int, int[, int])".into())),
    }
}

/// Simple comparison for sorting values.
fn value_cmp(a: &Value, b: &Value) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => Ordering::Less,
        (_, Value::Null) => Ordering::Greater,
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        (Value::Int(x), Value::Int(y)) => x.cmp(y),
        (Value::Int(x), Value::Float(y)) => (*x as f64).partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::Float(x), Value::Int(y)) => x.partial_cmp(&(*y as f64)).unwrap_or(Ordering::Equal),
        (Value::Float(x), Value::Float(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        _ => Ordering::Equal,
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

    #[test]
    fn test_ltrim() {
        assert_eq!(
            fn_ltrim(&[Value::String("  hi  ".into())]).unwrap(),
            Value::String("hi  ".into())
        );
    }

    #[test]
    fn test_rtrim() {
        assert_eq!(
            fn_rtrim(&[Value::String("  hi  ".into())]).unwrap(),
            Value::String("  hi".into())
        );
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
        assert_eq!(
            fn_reverse(&[Value::String("abc".into())]).unwrap(),
            Value::String("cba".into())
        );
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
            fn_lpad(&[Value::String("hi".into()), Value::Int(5), Value::String("0".into())])
                .unwrap(),
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
            fn_rpad(&[Value::String("hi".into()), Value::Int(5), Value::String("0".into())])
                .unwrap(),
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
    fn test_abs() {
        assert_eq!(fn_abs(&[Value::Int(-5)]).unwrap(), Value::Int(5));
        assert_eq!(fn_abs(&[Value::Float(-3.14)]).unwrap(), Value::Float(3.14));
    }

    #[test]
    fn test_to_string() {
        assert_eq!(
            fn_to_string(&[Value::Int(42)]).unwrap(),
            Value::String("42".into())
        );
    }

    // --- List function tests ---

    fn sample_list() -> Value {
        Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
    }

    #[test]
    fn test_list_len() {
        assert_eq!(fn_list_len(&[sample_list()]).unwrap(), Value::Int(3));
        assert_eq!(
            fn_list_len(&[Value::List(vec![])]).unwrap(),
            Value::Int(0)
        );
    }

    #[test]
    fn test_list_extract() {
        // 1-based indexing
        assert_eq!(
            fn_list_extract(&[sample_list(), Value::Int(1)]).unwrap(),
            Value::Int(1)
        );
        assert_eq!(
            fn_list_extract(&[sample_list(), Value::Int(3)]).unwrap(),
            Value::Int(3)
        );
        // negative index
        assert_eq!(
            fn_list_extract(&[sample_list(), Value::Int(-1)]).unwrap(),
            Value::Int(3)
        );
        // out of bounds
        assert_eq!(
            fn_list_extract(&[sample_list(), Value::Int(10)]).unwrap(),
            Value::Null
        );
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
        assert_eq!(
            fn_list_contains(&[sample_list(), Value::Int(2)]).unwrap(),
            Value::Bool(true)
        );
        assert_eq!(
            fn_list_contains(&[sample_list(), Value::Int(9)]).unwrap(),
            Value::Bool(false)
        );
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
        let dupes = Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(1), Value::Int(3), Value::Int(2)]);
        assert_eq!(
            fn_list_distinct(&[dupes]).unwrap(),
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3)])
        );
    }

    #[test]
    fn test_range() {
        assert_eq!(
            fn_range(&[Value::Int(1), Value::Int(5)]).unwrap(),
            Value::List(vec![Value::Int(1), Value::Int(2), Value::Int(3), Value::Int(4), Value::Int(5)])
        );
        // with step
        assert_eq!(
            fn_range(&[Value::Int(0), Value::Int(10), Value::Int(3)]).unwrap(),
            Value::List(vec![Value::Int(0), Value::Int(3), Value::Int(6), Value::Int(9)])
        );
        // descending
        assert_eq!(
            fn_range(&[Value::Int(5), Value::Int(1)]).unwrap(),
            Value::List(vec![Value::Int(5), Value::Int(4), Value::Int(3), Value::Int(2), Value::Int(1)])
        );
    }
}
