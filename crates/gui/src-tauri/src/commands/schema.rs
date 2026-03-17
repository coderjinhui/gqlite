use serde::Serialize;
use tauri::State;

use crate::state::AppState;
use crate::commands::query::{ColumnDesc, QueryResponse, value_to_json};

#[derive(Serialize)]
pub struct TableInfo {
    pub name: String,
    pub row_count: usize,
    pub column_count: usize,
}

#[derive(Serialize)]
pub struct TablesResponse {
    pub node_tables: Vec<TableInfo>,
    pub rel_tables: Vec<TableInfo>,
}

#[tauri::command]
pub fn get_tables(state: State<AppState>) -> Result<TablesResponse, String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;

    let node_tables = db.node_table_names().into_iter().map(|name| {
        let schema = db.table_schema(&name).unwrap_or_default();
        let column_count = schema.len();
        let row_count = db
            .execute(&format!("MATCH (n:{}) RETURN count(n)", name))
            .ok()
            .and_then(|r| r.rows().first().and_then(|row| row.get_int(0)).map(|v| v as usize))
            .unwrap_or(0);
        TableInfo { name, row_count, column_count }
    }).collect();

    let rel_tables = db.rel_table_names().into_iter().map(|name| {
        let schema = db.table_schema(&name).unwrap_or_default();
        let column_count = schema.len();
        let row_count = db
            .execute(&format!("MATCH ()-[r:{}]->() RETURN count(r)", name))
            .ok()
            .and_then(|r| r.rows().first().and_then(|row| row.get_int(0)).map(|v| v as usize))
            .unwrap_or(0);
        TableInfo { name, row_count, column_count }
    }).collect();

    Ok(TablesResponse { node_tables, rel_tables })
}

#[tauri::command]
pub fn get_table_schema(
    table_name: String,
    state: State<AppState>,
) -> Result<Vec<ColumnDesc>, String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;

    let schema = db
        .table_schema(&table_name)
        .ok_or_else(|| format!("Table '{}' not found", table_name))?;

    Ok(schema
        .into_iter()
        .map(|(name, dt)| ColumnDesc {
            name,
            data_type: format!("{:?}", dt),
        })
        .collect())
}

#[tauri::command]
pub fn get_table_data(
    table_name: String,
    limit: usize,
    offset: usize,
    state: State<AppState>,
) -> Result<QueryResponse, String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;

    // Build RETURN clause with all columns expanded (e.g. n.name, n.age)
    // so we get actual property values instead of just the InternalId.
    let schema = db.table_schema(&table_name).unwrap_or_default();
    let return_clause = if schema.is_empty() {
        "n".to_string()
    } else {
        schema
            .iter()
            .map(|(col_name, _)| format!("n.{}", col_name))
            .collect::<Vec<_>>()
            .join(", ")
    };

    let gql = format!(
        "MATCH (n:{}) RETURN {} SKIP {} LIMIT {}",
        table_name, return_clause, offset, limit
    );

    let start = std::time::Instant::now();
    let result = db.execute(&gql).map_err(|e| e.to_string())?;
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    let columns: Vec<ColumnDesc> = result
        .columns
        .iter()
        .map(|c| ColumnDesc {
            name: c.name.clone(),
            data_type: format!("{:?}", c.data_type),
        })
        .collect();

    let rows: Vec<Vec<serde_json::Value>> = result
        .rows()
        .iter()
        .map(|row| row.values.iter().map(value_to_json).collect())
        .collect();

    let row_count = rows.len();

    Ok(QueryResponse {
        columns,
        rows,
        row_count,
        elapsed_ms,
    })
}
