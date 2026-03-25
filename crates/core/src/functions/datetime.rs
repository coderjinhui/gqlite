//! Date/time constructor functions.

use crate::error::GqliteError;
use crate::types::value::Value;
use chrono::NaiveDate;

/// date('2024-01-15') -> Value::Date
/// date() -> today's date
pub fn fn_date(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => {
            let d = NaiveDate::parse_from_str(s, "%Y-%m-%d")
                .map_err(|e| GqliteError::Execution(format!("invalid date format: {}", e)))?;
            Ok(Value::Date(d))
        }
        Some(Value::Null) => Ok(Value::Null),
        None => {
            // No args: return current date
            let today = chrono::Local::now().date_naive();
            Ok(Value::Date(today))
        }
        _ => Err(GqliteError::Execution("date() expects a string argument".into())),
    }
}

/// datetime('2024-01-15T10:30:00') -> Value::DateTime
/// datetime() -> current datetime
pub fn fn_datetime(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::String(s)) => {
            let dt = chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S")
                .map_err(|e| GqliteError::Execution(format!("invalid datetime format: {}", e)))?;
            Ok(Value::DateTime(dt))
        }
        Some(Value::Null) => Ok(Value::Null),
        None => {
            let now = chrono::Local::now().naive_local();
            Ok(Value::DateTime(now))
        }
        _ => Err(GqliteError::Execution("datetime() expects a string argument".into())),
    }
}

/// timestamp() -> current Unix timestamp in seconds as Int
pub fn fn_timestamp(args: &[Value]) -> Result<Value, GqliteError> {
    match args.first() {
        Some(Value::Null) => Ok(Value::Null),
        None => {
            let ts = chrono::Utc::now().timestamp();
            Ok(Value::Int(ts))
        }
        Some(Value::DateTime(dt)) => Ok(Value::Int(dt.and_utc().timestamp())),
        _ => Err(GqliteError::Execution("timestamp() expects no arguments or a datetime".into())),
    }
}
