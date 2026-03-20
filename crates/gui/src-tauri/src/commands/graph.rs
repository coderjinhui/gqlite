use serde::Serialize;
use tauri::State;

use crate::commands::query::value_to_json;
use crate::state::AppState;
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
    pub dst_primary_key: Option<String>,
}

/// Helper: query nodes from a table and collect them into (nodes, node_ids).
/// Returns the nodes with InternalId-based IDs and all properties.
fn fetch_nodes(
    db: &gqlite_core::Database,
    table_name: &str,
    limit: usize,
    alias: &str,
) -> Result<Vec<GraphNode>, String> {
    let schema = db.table_schema(table_name).unwrap_or_default();
    let prop_cols: Vec<String> =
        schema.iter().map(|(name, _)| format!("{}.{}", alias, name)).collect();
    let return_clause = if prop_cols.is_empty() {
        alias.to_string()
    } else {
        format!("{}, {}", alias, prop_cols.join(", "))
    };

    let gql = format!("MATCH ({}:{}) RETURN {} LIMIT {}", alias, table_name, return_clause, limit);
    let result = db.execute(&gql).map_err(|e| e.to_string())?;

    let mut nodes = Vec::new();
    for row in result.rows() {
        let mut props = serde_json::Map::new();
        let mut id_str = String::new();

        for (i, val) in row.values.iter().enumerate() {
            let col_name = if i < result.columns.len() {
                result.columns[i].name.clone()
            } else {
                format!("col_{}", i)
            };

            if let Value::InternalId(id) = val {
                id_str = format!("{}:{}", id.table_id, id.offset);
            }

            // Skip the raw InternalId column from display properties
            if i == 0 && matches!(val, Value::InternalId(_)) {
                continue;
            }

            props.insert(col_name, value_to_json(val));
        }

        if id_str.is_empty() {
            id_str = format!("node_{}", nodes.len());
        }

        nodes.push(GraphNode { id: id_str, label: table_name.to_string(), properties: props });
    }

    Ok(nodes)
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

    // Get primary key column name and destination table info from catalog
    let (primary_key, dst_table_name, dst_primary_key) = {
        let catalog = db.inner.catalog.read().unwrap();
        let pk = catalog
            .get_node_table(&node_table)
            .map(|entry| entry.columns[entry.primary_key_idx].name.clone());

        let (dst_name, dst_pk) = rel_table
            .as_ref()
            .and_then(|rt| {
                catalog.get_rel_table(rt).and_then(|rel_entry| {
                    catalog.get_node_table_by_id(rel_entry.dst_table_id).map(|dst_entry| {
                        let name = dst_entry.name.clone();
                        let pk = dst_entry.columns[dst_entry.primary_key_idx].name.clone();
                        (name, pk)
                    })
                })
            })
            .map(|(n, p)| (Some(n), Some(p)))
            .unwrap_or((None, None));

        (pk, dst_name, dst_pk)
    };

    let mut nodes = Vec::new();
    let mut node_ids = std::collections::HashSet::new();

    // 1. Fetch source nodes (e.g. Person)
    let src_nodes = fetch_nodes(db, &node_table, limit, "n")?;
    for node in src_nodes {
        if node_ids.insert(node.id.clone()) {
            nodes.push(node);
        }
    }

    // 2. If relationship is selected, also fetch destination table nodes
    //    and build edges
    let mut edges = Vec::new();
    if let Some(ref rt) = rel_table {
        // Fetch destination table nodes if it's a different table
        if let Some(ref dst_name) = dst_table_name {
            if dst_name != &node_table {
                let dst_nodes = fetch_nodes(db, dst_name, limit, "n")?;
                for node in dst_nodes {
                    if node_ids.insert(node.id.clone()) {
                        nodes.push(node);
                    }
                }
            }
        }

        // Fetch edges: MATCH (a:Person)-[:LIVES_IN]->(b) RETURN a, b
        let edge_gql =
            format!("MATCH (a:{})-[:{}]->(b) RETURN a, b LIMIT {}", node_table, rt, limit);
        let edge_result = db.execute(&edge_gql).map_err(|e| e.to_string())?;

        for row in edge_result.rows() {
            let mut source_id = String::new();
            let mut target_id = String::new();

            if row.values.len() >= 2 {
                if let Value::InternalId(id) = &row.values[0] {
                    source_id = format!("{}:{}", id.table_id, id.offset);
                }
                if let Value::InternalId(id) = &row.values[1] {
                    target_id = format!("{}:{}", id.table_id, id.offset);
                }
            }

            if !source_id.is_empty() && !target_id.is_empty() {
                // If target node wasn't fetched (e.g. exceeded limit), add a stub
                if !node_ids.contains(&target_id) {
                    node_ids.insert(target_id.clone());
                    let label = dst_table_name.as_deref().unwrap_or("?");
                    let mut tp = serde_json::Map::new();
                    tp.insert("_id".to_string(), value_to_json(&row.values[1]));
                    nodes.push(GraphNode {
                        id: target_id.clone(),
                        label: label.to_string(),
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

    Ok(GraphData { nodes, edges, primary_key, dst_primary_key })
}
