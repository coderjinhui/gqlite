use serde::Serialize;
use tauri::State;

use crate::state::AppState;
use crate::commands::query::value_to_json;
use gqlite_core::types::value::Value;

#[derive(Serialize)]
pub struct GraphNode {
    pub id: String,
    pub label: String,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Serialize)]
pub struct GraphEdge {
    pub source: String,
    pub target: String,
    pub label: String,
    pub properties: serde_json::Map<String, serde_json::Value>,
}

#[derive(Serialize)]
pub struct GraphData {
    pub nodes: Vec<GraphNode>,
    pub edges: Vec<GraphEdge>,
    pub primary_key: Option<String>,
}

#[tauri::command]
pub fn get_graph_data(
    node_table: String,
    rel_table: Option<String>,
    limit: usize,
    state: State<AppState>,
) -> Result<GraphData, String> {
    let db_guard = state.db.lock().unwrap();
    let db = db_guard.as_ref().ok_or("No database is open")?;

    // Get primary key column name from catalog
    let primary_key = {
        let catalog = db.inner.catalog.read().unwrap();
        catalog.get_node_table(&node_table).map(|entry| {
            entry.columns[entry.primary_key_idx].name.clone()
        })
    };

    let mut nodes = Vec::new();
    let mut node_ids = std::collections::HashSet::new();

    // Build RETURN clause: "n, n.col1, n.col2, ..." so we get both InternalId and properties
    let schema = db.table_schema(&node_table).unwrap_or_default();
    let prop_cols: Vec<String> = schema.iter().map(|(name, _)| format!("n.{}", name)).collect();
    let return_clause = if prop_cols.is_empty() {
        "n".to_string()
    } else {
        format!("n, {}", prop_cols.join(", "))
    };

    let node_gql = format!(
        "MATCH (n:{}) RETURN {} LIMIT {}",
        node_table, return_clause, limit
    );
    let result = db.execute(&node_gql).map_err(|e| e.to_string())?;

    for row in result.rows() {
        let mut props = serde_json::Map::new();
        let mut id_str = String::new();

        for (i, val) in row.values.iter().enumerate() {
            let col_name = if i < result.columns.len() {
                result.columns[i].name.clone()
            } else {
                format!("col_{}", i)
            };

            // The first column "n" is the InternalId — use it as node ID
            if let Value::InternalId(id) = val {
                id_str = format!("{}:{}", id.table_id, id.offset);
            }

            // Skip the raw InternalId column "n" from display properties
            if i == 0 && matches!(val, Value::InternalId(_)) {
                continue;
            }

            props.insert(col_name, value_to_json(val));
        }

        if id_str.is_empty() {
            id_str = format!("node_{}", nodes.len());
        }

        if node_ids.insert(id_str.clone()) {
            nodes.push(GraphNode {
                id: id_str,
                label: node_table.clone(),
                properties: props,
            });
        }
    }

    // Fetch edges if rel_table is specified
    let mut edges = Vec::new();
    if let Some(ref rt) = rel_table {
        // Note: gqlite does not support RETURN on relationship variables (r),
        // so we only return the source and target node InternalIds.
        let edge_gql = format!(
            "MATCH (a:{})-[:{}]->(b) RETURN a, b LIMIT {}",
            node_table, rt, limit
        );
        let edge_result = db.execute(&edge_gql).map_err(|e| e.to_string())?;

        for row in edge_result.rows() {
            let mut source_id = String::new();
            let mut target_id = String::new();

            if row.values.len() >= 2 {
                // Column 0: a (source InternalId)
                if let Value::InternalId(id) = &row.values[0] {
                    source_id = format!("{}:{}", id.table_id, id.offset);
                }
                // Column 1: b (target InternalId)
                if let Value::InternalId(id) = &row.values[1] {
                    target_id = format!("{}:{}", id.table_id, id.offset);
                }
            }

            if !source_id.is_empty() && !target_id.is_empty() {
                // Ensure both endpoints exist as nodes
                // (target might be in a different table — add it if missing)
                if !node_ids.contains(&target_id) {
                    node_ids.insert(target_id.clone());
                    let mut tp = serde_json::Map::new();
                    tp.insert("_id".to_string(), value_to_json(&row.values[1]));
                    nodes.push(GraphNode {
                        id: target_id.clone(),
                        label: "?".to_string(),
                        properties: tp,
                    });
                }

                edges.push(GraphEdge {
                    source: source_id,
                    target: target_id,
                    label: rt.clone(),
                    properties: serde_json::Map::new(),
                });
            }
        }
    }

    Ok(GraphData { nodes, edges, primary_key })
}
