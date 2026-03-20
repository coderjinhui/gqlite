use serde::Serialize;
use tauri::State;

use crate::commands::query::{value_to_json, ColumnDesc, QueryResponse};
use crate::state::AppState;

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

/// Look up src/dst table names for a relationship table from the catalog.
fn rel_endpoint_names(db: &gqlite_core::Database, rel_name: &str) -> Option<(String, String)> {
    let catalog = db.inner.catalog.read().unwrap();
    let rel_entry = catalog.get_rel_table(rel_name)?;
    let src = catalog.get_node_table_by_id(rel_entry.src_table_id)?;
    let dst = catalog.get_node_table_by_id(rel_entry.dst_table_id)?;
    Some((src.name.clone(), dst.name.clone()))
}

#[tauri::command]
pub fn get_tables(state: State<AppState>) -> Result<TablesResponse, String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;

    let node_tables = db
        .node_table_names()
        .into_iter()
        .map(|name| {
            let schema = db.table_schema(&name).unwrap_or_default();
            let column_count = schema.len();
            let row_count = db
                .execute(&format!("MATCH (n:{}) RETURN count(n)", name))
                .ok()
                .and_then(|r| r.rows().first().and_then(|row| row.get_int(0)).map(|v| v as usize))
                .unwrap_or(0);
            TableInfo { name, row_count, column_count }
        })
        .collect();

    let rel_tables = db
        .rel_table_names()
        .into_iter()
        .map(|name| {
            let schema = db.table_schema(&name).unwrap_or_default();
            let column_count = schema.len();
            // Must use labeled endpoints; anonymous ()-[r:X]->() is not supported
            let row_count = rel_endpoint_names(db, &name)
                .and_then(|(src, dst)| {
                    db.execute(&format!(
                        "MATCH (a:{})-[:{}]->(b:{}) RETURN count(a)",
                        src, name, dst
                    ))
                    .ok()
                    .and_then(|r| {
                        r.rows().first().and_then(|row| row.get_int(0)).map(|v| v as usize)
                    })
                })
                .unwrap_or(0);
            TableInfo { name, row_count, column_count }
        })
        .collect();

    Ok(TablesResponse { node_tables, rel_tables })
}

#[tauri::command]
pub fn get_table_schema(
    table_name: String,
    state: State<AppState>,
) -> Result<Vec<ColumnDesc>, String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;

    let schema =
        db.table_schema(&table_name).ok_or_else(|| format!("Table '{}' not found", table_name))?;

    Ok(schema
        .into_iter()
        .map(|(name, dt)| ColumnDesc { name, data_type: format!("{:?}", dt) })
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

    // Check if this is a node table or relationship table
    let is_node_table = db.node_table_names().contains(&table_name);

    let gql = if is_node_table {
        // Node table: MATCH (n:T) RETURN n.col1, n.col2, ...
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
        format!("MATCH (n:{}) RETURN {} SKIP {} LIMIT {}", table_name, return_clause, offset, limit)
    } else {
        // Relationship table: MATCH (a:Src)-[:Rel]->(b:Dst) RETURN a.pk, b.pk, r_props...
        let (src_name, dst_name) = rel_endpoint_names(db, &table_name)
            .ok_or_else(|| format!("Relationship table '{}' not found", table_name))?;

        // Get src/dst PK names for display
        let src_pk = {
            let catalog = db.inner.catalog.read().unwrap();
            catalog
                .get_node_table(&src_name)
                .map(|e| e.columns[e.primary_key_idx].name.clone())
                .unwrap_or_else(|| "id".to_string())
        };
        let dst_pk = {
            let catalog = db.inner.catalog.read().unwrap();
            catalog
                .get_node_table(&dst_name)
                .map(|e| e.columns[e.primary_key_idx].name.clone())
                .unwrap_or_else(|| "id".to_string())
        };

        // Build columns: src.pk, dst.pk (rel properties can't be accessed via GQL)
        let mut return_parts = vec![format!("a.{}", src_pk), format!("b.{}", dst_pk)];

        // Also include other src/dst columns for context
        let src_schema = db.table_schema(&src_name).unwrap_or_default();
        for (col, _) in &src_schema {
            if col != &src_pk {
                return_parts.push(format!("a.{}", col));
            }
        }
        let dst_schema = db.table_schema(&dst_name).unwrap_or_default();
        for (col, _) in &dst_schema {
            if col != &dst_pk {
                return_parts.push(format!("b.{}", col));
            }
        }

        format!(
            "MATCH (a:{})-[:{}]->(b:{}) RETURN {} SKIP {} LIMIT {}",
            src_name,
            table_name,
            dst_name,
            return_parts.join(", "),
            offset,
            limit
        )
    };

    let start = std::time::Instant::now();
    let result = db.execute(&gql).map_err(|e| e.to_string())?;
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
