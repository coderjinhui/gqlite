use std::time::Instant;

use serde::Serialize;
use tauri::State;

use crate::state::AppState;
use gqlite_core::types::value::Value;

#[derive(Serialize)]
pub struct ColumnDesc {
    pub name: String,
    pub data_type: String,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub columns: Vec<ColumnDesc>,
    pub rows: Vec<Vec<serde_json::Value>>,
    pub row_count: usize,
    pub elapsed_ms: f64,
}

pub fn value_to_json(v: &Value) -> serde_json::Value {
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(b) => serde_json::json!(*b),
        Value::Int(i) => serde_json::json!(*i),
        Value::Float(f) => serde_json::json!(*f),
        Value::String(s) => serde_json::json!(s),
        Value::Bytes(b) => serde_json::json!(format!("<bytes:{}>", b.len())),
        Value::InternalId(id) => serde_json::json!(format!("{}:{}", id.table_id, id.offset)),
        Value::List(items) => serde_json::Value::Array(items.iter().map(value_to_json).collect()),
        Value::Date(d) => serde_json::json!(d.to_string()),
        Value::DateTime(dt) => serde_json::json!(dt.to_string()),
        Value::Duration(ms) => serde_json::json!(format!("{}ms", ms)),
    }
}

#[tauri::command]
pub fn execute_query(query: String, state: State<AppState>) -> Result<QueryResponse, String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;

    let start = Instant::now();
    let result = db.execute(&query).map_err(|e| e.to_string())?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let columns: Vec<ColumnDesc> = result
        .columns
        .iter()
        .map(|c| ColumnDesc { name: c.name.clone(), data_type: format!("{:?}", c.data_type) })
        .collect();

    let rows: Vec<Vec<serde_json::Value>> =
        result.rows().iter().map(|row| row.values.iter().map(value_to_json).collect()).collect();

    let row_count = rows.len();

    Ok(QueryResponse { columns, rows, row_count, elapsed_ms })
}
