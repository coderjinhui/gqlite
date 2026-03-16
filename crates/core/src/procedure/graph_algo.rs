//! Graph algorithm procedures (degree centrality, WCC, etc.).

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

// ── Union-Find (Disjoint Set Union) ────────────────────────────

/// Union-Find data structure with path compression and union by rank.
struct UnionFind {
    parent: Vec<usize>,
    rank: Vec<usize>,
}

impl UnionFind {
    fn new(n: usize) -> Self {
        UnionFind {
            parent: (0..n).collect(),
            rank: vec![0; n],
        }
    }

    fn find(&mut self, mut x: usize) -> usize {
        while self.parent[x] != x {
            self.parent[x] = self.parent[self.parent[x]]; // path compression
            x = self.parent[x];
        }
        x
    }

    fn union(&mut self, x: usize, y: usize) {
        let rx = self.find(x);
        let ry = self.find(y);
        if rx == ry {
            return;
        }
        if self.rank[rx] < self.rank[ry] {
            self.parent[rx] = ry;
        } else if self.rank[rx] > self.rank[ry] {
            self.parent[ry] = rx;
        } else {
            self.parent[ry] = rx;
            self.rank[rx] += 1;
        }
    }
}

// ── WCC (Weakly Connected Components) ──────────────────────────

/// Computes weakly connected components for all nodes connected by a given
/// relationship table using Union-Find.
///
/// Usage: `CALL wcc('REL_NAME') YIELD node_id, component_id`
///
/// Returns one row per node that participates as source or destination in the
/// specified relationship table. Each node gets a `component_id` — nodes in
/// the same connected component share the same `component_id`.
/// Edges are treated as undirected.
pub struct Wcc;

impl Procedure for Wcc {
    fn name(&self) -> &str {
        "wcc"
    }

    fn output_columns(&self) -> Vec<String> {
        vec!["node_id".to_string(), "component_id".to_string()]
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
                    "wcc requires a string argument (rel table name)".into(),
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

        // Collect all node offsets. Key: (table_id, offset), mapped to a
        // contiguous index for Union-Find.
        let mut node_list: Vec<(u32, u64)> = Vec::new();
        let mut node_index: HashMap<(u32, u64), usize> = HashMap::new();

        // Helper closure to add nodes from a node table
        let mut add_nodes = |table_id: u32, offsets: Vec<u64>| {
            for offset in offsets {
                let key = (table_id, offset);
                if !node_index.contains_key(&key) {
                    let idx = node_list.len();
                    node_list.push(key);
                    node_index.insert(key, idx);
                }
            }
        };

        // Scan source node table
        if let Some(src_node_table) = storage.node_tables.get(&src_table_id) {
            let offsets: Vec<u64> = src_node_table.scan().map(|(off, _)| off).collect();
            add_nodes(src_table_id, offsets);
        }

        // Scan destination node table (if different from source)
        if src_table_id != dst_table_id {
            if let Some(dst_node_table) = storage.node_tables.get(&dst_table_id) {
                let offsets: Vec<u64> = dst_node_table.scan().map(|(off, _)| off).collect();
                add_nodes(dst_table_id, offsets);
            }
        }

        // Initialize Union-Find
        let n = node_list.len();
        let mut uf = UnionFind::new(n);

        // For each source node, iterate its outgoing edges and union src with dst.
        // Since we treat edges as undirected, scanning forward edges is sufficient.
        if let Some(src_node_table) = storage.node_tables.get(&src_table_id) {
            for (src_offset, _) in src_node_table.scan() {
                let rels = rel_table.get_rels_from(src_offset);
                for (dst_offset, _rel_id) in rels {
                    let src_key = (src_table_id, src_offset);
                    let dst_key = (dst_table_id, dst_offset);
                    if let (Some(&src_idx), Some(&dst_idx)) =
                        (node_index.get(&src_key), node_index.get(&dst_key))
                    {
                        uf.union(src_idx, dst_idx);
                    }
                }
            }
        }

        // Build result rows sorted by (table_id, offset) for deterministic output.
        // component_id = the representative's offset (the root's node_id).
        let mut rows: Vec<(u32, u64, i64)> = Vec::with_capacity(n);
        for &(tid, offset) in &node_list {
            let idx = node_index[&(tid, offset)];
            let root = uf.find(idx);
            let (_root_tid, root_offset) = node_list[root];
            rows.push((tid, offset, root_offset as i64));
        }
        rows.sort_by_key(|&(tid, off, _)| (tid, off));

        let result: Vec<ProcedureRow> = rows
            .into_iter()
            .map(|(_tid, offset, comp_id)| {
                vec![Value::Int(offset as i64), Value::Int(comp_id)]
            })
            .collect();

        Ok(result)
    }
}
