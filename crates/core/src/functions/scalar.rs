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
        Some(Value::List(l)) => {
            // If the list looks like a path (all InternalId), return number of
            // edges (len - 1).  Otherwise return list length.
            let is_path = !l.is_empty()
                && l.iter()
                    .all(|v| matches!(v, Value::InternalId(_)));
            if is_path {
                Ok(Value::Int((l.len() as i64) - 1))
            } else {
                Ok(Value::Int(l.len() as i64))
            }
        }
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
// Path functions
// ---------------------------------------------------------------------------

/// `nodes(path)` — returns the list of node IDs in a path.
pub fn fn_nodes(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::List(l)) => Ok(Value::List(l.clone())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("nodes() expects a path/list".into())),
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

// ---------------------------------------------------------------------------
// Math functions
// ---------------------------------------------------------------------------

pub fn fn_ceil(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Float((*i as f64).ceil())),
        Some(Value::Float(f)) => Ok(Value::Float(f.ceil())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("ceil() expects a number".into())),
    }
}

pub fn fn_floor(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Float((*i as f64).floor())),
        Some(Value::Float(f)) => Ok(Value::Float(f.floor())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("floor() expects a number".into())),
    }
}

pub fn fn_round(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Float((*i as f64).round())),
        Some(Value::Float(f)) => Ok(Value::Float(f.round())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("round() expects a number".into())),
    }
}

pub fn fn_sqrt(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Float((*i as f64).sqrt())),
        Some(Value::Float(f)) => Ok(Value::Float(f.sqrt())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("sqrt() expects a number".into())),
    }
}

pub fn fn_log(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Float((*i as f64).ln())),
        Some(Value::Float(f)) => Ok(Value::Float(f.ln())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("log() expects a number".into())),
    }
}

pub fn fn_log10(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Float((*i as f64).log10())),
        Some(Value::Float(f)) => Ok(Value::Float(f.log10())),
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("log10() expects a number".into())),
    }
}

pub fn fn_sign(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Int(i.signum())),
        Some(Value::Float(f)) => {
            if f.is_nan() {
                Ok(Value::Int(0))
            } else if *f > 0.0 {
                Ok(Value::Int(1))
            } else if *f < 0.0 {
                Ok(Value::Int(-1))
            } else {
                Ok(Value::Int(0))
            }
        }
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution("sign() expects a number".into())),
    }
}

pub fn fn_rand(args: &[Value]) -> Result<Value, GqliteError> {
    if !args.is_empty() {
        return Err(GqliteError::Execution("rand() expects no arguments".into()));
    }
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .subsec_nanos();
    Ok(Value::Float(nanos as f64 / 1_000_000_000.0))
}

pub fn fn_pi(args: &[Value]) -> Result<Value, GqliteError> {
    if !args.is_empty() {
        return Err(GqliteError::Execution("pi() expects no arguments".into()));
    }
    Ok(Value::Float(std::f64::consts::PI))
}

pub fn fn_e(args: &[Value]) -> Result<Value, GqliteError> {
    if !args.is_empty() {
        return Err(GqliteError::Execution("e() expects no arguments".into()));
    }
    Ok(Value::Float(std::f64::consts::E))
}

pub fn fn_to_integer(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Int(*i)),
        Some(Value::Float(f)) => Ok(Value::Int(*f as i64)),
        Some(Value::String(s)) => match s.parse::<i64>() {
            Ok(i) => Ok(Value::Int(i)),
            Err(_) => Err(GqliteError::Execution(
                format!("toInteger() cannot parse '{}' as integer", s),
            )),
        },
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution(
            "toInteger() expects a number or string".into(),
        )),
    }
}

pub fn fn_to_float(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Int(i)) => Ok(Value::Float(*i as f64)),
        Some(Value::Float(f)) => Ok(Value::Float(*f)),
        Some(Value::String(s)) => match s.parse::<f64>() {
            Ok(f) => Ok(Value::Float(f)),
            Err(_) => Err(GqliteError::Execution(
                format!("toFloat() cannot parse '{}' as float", s),
            )),
        },
        Some(Value::Null) => Ok(Value::Null),
        _ => Err(GqliteError::Execution(
            "toFloat() expects a number or string".into(),
        )),
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

