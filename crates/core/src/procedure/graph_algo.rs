//! Graph algorithm procedures (degree centrality, etc.).

use std::collections::HashMap;

use super::{Procedure, ProcedureRow};
use crate::error::GqliteError;
use crate::types::value::Value;

/// Computes degree centrality for all nodes connected by a given relationship table.
///
/// Usage: `CALL degree_centrality('REL_NAME') YIELD node_id, out_degree, in_degree`
///
/// Returns one row per node that participates as source or destination in the
/// specified relationship table, with its out-degree and in-degree counts.
pub struct DegreeCentrality;

impl Procedure for DegreeCentrality {
    fn name(&self) -> &str {
        "degree_centrality"
    }

    fn output_columns(&self) -> Vec<String> {
        vec![
            "node_id".to_string(),
            "out_degree".to_string(),
            "in_degree".to_string(),
        ]
    }

    fn execute(
        &self,
        args: &[Value],
        db: &crate::DatabaseInner,
    ) -> Result<Vec<ProcedureRow>, GqliteError> {
        // Extract relationship table name from arguments
        let rel_name = match args.first() {
            Some(Value::String(s)) => s.clone(),
            _ => {
                return Err(GqliteError::Execution(
                    "degree_centrality requires a string argument (rel table name)".into(),
                ))
            }
        };

        let catalog = db.catalog.read().unwrap();
        let storage = db.storage.read().unwrap();

        // Find the relationship table entry in the catalog
        let rel_entry = catalog.get_rel_table(&rel_name).ok_or_else(|| {
            GqliteError::Execution(format!("relation table '{}' not found", rel_name))
        })?;

        let rel_table_id = rel_entry.table_id;
        let src_table_id = rel_entry.src_table_id;
        let dst_table_id = rel_entry.dst_table_id;

        // Get the RelTable from storage
        let rel_table = storage.rel_tables.get(&rel_table_id).ok_or_else(|| {
            GqliteError::Execution(format!(
                "relation table '{}' not found in storage",
                rel_name
            ))
        })?;

        // Collect all node offsets from both source and destination node tables.
        // Use a HashMap to accumulate (out_degree, in_degree) per node offset.
        // Key: (table_id, offset) to distinguish nodes from different tables.
        let mut degree_map: HashMap<(u32, u64), (i64, i64)> = HashMap::new();

        // Scan source node table
        if let Some(src_node_table) = storage.node_tables.get(&src_table_id) {
            for (offset, _row) in src_node_table.scan() {
                let out_count = rel_table.get_rels_from(offset).len() as i64;
                let entry = degree_map.entry((src_table_id, offset)).or_insert((0, 0));
                entry.0 = out_count;
            }
        }

        // Scan destination node table
        if src_table_id == dst_table_id {
            // Self-referencing rel table (e.g., FROM N TO N)
            // Nodes already in the map from source scan; just add in-degree
            if let Some(dst_node_table) = storage.node_tables.get(&dst_table_id) {
                for (offset, _row) in dst_node_table.scan() {
                    let in_count = rel_table.get_rels_to(offset).len() as i64;
                    let entry = degree_map.entry((dst_table_id, offset)).or_insert((0, 0));
                    entry.1 = in_count;
                }
            }
        } else {
            // Different source and destination tables
            if let Some(dst_node_table) = storage.node_tables.get(&dst_table_id) {
                for (offset, _row) in dst_node_table.scan() {
                    let in_count = rel_table.get_rels_to(offset).len() as i64;
                    let entry = degree_map.entry((dst_table_id, offset)).or_insert((0, 0));
                    entry.1 = in_count;
                }
            }
        }

        // Build result rows sorted by (table_id, offset) for deterministic output
        let mut entries: Vec<_> = degree_map.into_iter().collect();
        entries.sort_by_key(|&((tid, off), _)| (tid, off));

        let rows: Vec<ProcedureRow> = entries
            .into_iter()
            .map(|((_tid, offset), (out_deg, in_deg))| {
                vec![
                    Value::Int(offset as i64),
                    Value::Int(out_deg),
                    Value::Int(in_deg),
                ]
            })
            .collect();

        Ok(rows)
    }
}
