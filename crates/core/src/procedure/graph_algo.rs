//! Graph algorithm procedures (degree centrality, WCC, Dijkstra, LPA, etc.).

use std::cmp::Reverse;
use std::collections::{BinaryHeap, HashMap, HashSet, VecDeque};

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
        vec!["node_id".to_string(), "out_degree".to_string(), "in_degree".to_string()]
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
            GqliteError::Execution(format!("relation table '{}' not found in storage", rel_name))
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
                vec![Value::Int(offset as i64), Value::Int(out_deg), Value::Int(in_deg)]
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
        UnionFind { parent: (0..n).collect(), rank: vec![0; n] }
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
            GqliteError::Execution(format!("relation table '{}' not found in storage", rel_name))
        })?;

        // Collect all node offsets. Key: (table_id, offset), mapped to a
        // contiguous index for Union-Find.
        let mut node_list: Vec<(u32, u64)> = Vec::new();
        let mut node_index: HashMap<(u32, u64), usize> = HashMap::new();

        // Helper closure to add nodes from a node table
        let mut add_nodes = |table_id: u32, offsets: Vec<u64>| {
            for offset in offsets {
                let key = (table_id, offset);
                node_index.entry(key).or_insert_with(|| {
                    let idx = node_list.len();
                    node_list.push(key);
                    idx
                });
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
            .map(|(_tid, offset, comp_id)| vec![Value::Int(offset as i64), Value::Int(comp_id)])
            .collect();

        Ok(result)
    }
}

// ── Dijkstra (Weighted Shortest Path) ──────────────────────────

/// Computes the weighted shortest path between two nodes using Dijkstra's algorithm.
///
/// Usage: `CALL dijkstra(source_id, target_id, 'REL_TYPE', 'weight_prop') YIELD path, cost`
///
/// Arguments:
/// - source_id (Int): primary key of the source node
/// - target_id (Int): primary key of the target node
/// - rel_type (String): name of the relationship table
/// - weight_prop (String): name of the property column storing edge weights
///
/// Returns one row with `path` (List of node IDs) and `cost` (Float),
/// or zero rows if no path exists.
pub struct Dijkstra;

impl Procedure for Dijkstra {
    fn name(&self) -> &str {
        "dijkstra"
    }

    fn output_columns(&self) -> Vec<String> {
        vec!["path".to_string(), "cost".to_string()]
    }

    fn execute(
        &self,
        args: &[Value],
        db: &crate::DatabaseInner,
    ) -> Result<Vec<ProcedureRow>, GqliteError> {
        // Parse arguments: source_id, target_id, rel_type, weight_property
        if args.len() < 4 {
            return Err(GqliteError::Execution(
                "dijkstra requires 4 arguments: source_id, target_id, rel_type, weight_property"
                    .into(),
            ));
        }

        let source_pk = &args[0];
        let target_pk = &args[1];
        let rel_name = match &args[2] {
            Value::String(s) => s.clone(),
            _ => {
                return Err(GqliteError::Execution(
                    "dijkstra: 3rd argument (rel_type) must be a string".into(),
                ))
            }
        };
        let weight_prop = match &args[3] {
            Value::String(s) => s.clone(),
            _ => {
                return Err(GqliteError::Execution(
                    "dijkstra: 4th argument (weight_property) must be a string".into(),
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

        // Get the RelTable from storage
        let rel_table = storage.rel_tables.get(&rel_table_id).ok_or_else(|| {
            GqliteError::Execution(format!("relation table '{}' not found in storage", rel_name))
        })?;

        // Get source node table for PK lookup
        let src_node_table = storage.node_tables.get(&src_table_id).ok_or_else(|| {
            GqliteError::Execution("source node table not found in storage".into())
        })?;

        // Resolve source and target node offsets from primary key values
        let source_offset = src_node_table.lookup_pk(source_pk).ok_or_else(|| {
            GqliteError::Execution(format!("dijkstra: source node with id {} not found", source_pk))
        })?;

        let target_offset = src_node_table.lookup_pk(target_pk).ok_or_else(|| {
            GqliteError::Execution(format!("dijkstra: target node with id {} not found", target_pk))
        })?;

        // Dijkstra's algorithm using a min-heap
        // Wrapper to make f64 orderable for BinaryHeap
        #[derive(PartialEq)]
        struct OrdF64(f64);
        impl Eq for OrdF64 {}
        impl PartialOrd for OrdF64 {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for OrdF64 {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                self.0.partial_cmp(&other.0).unwrap_or(std::cmp::Ordering::Equal)
            }
        }

        let mut heap: BinaryHeap<Reverse<(OrdF64, u64)>> = BinaryHeap::new();
        let mut dist: HashMap<u64, f64> = HashMap::new();
        let mut parent: HashMap<u64, u64> = HashMap::new();

        heap.push(Reverse((OrdF64(0.0), source_offset)));
        dist.insert(source_offset, 0.0);

        let mut found = false;

        while let Some(Reverse((OrdF64(cost), node))) = heap.pop() {
            if node == target_offset {
                found = true;
                break;
            }

            if cost > *dist.get(&node).unwrap_or(&f64::INFINITY) {
                continue; // stale entry
            }

            // Get neighbors via forward CSR
            let neighbors = rel_table.get_rels_from(node);
            for (neighbor_offset, rel_id) in neighbors {
                // Read edge weight from rel properties
                let edge_weight = match rel_table.get_rel_property(rel_id, &weight_prop) {
                    Some(Value::Float(w)) => w,
                    Some(Value::Int(w)) => w as f64,
                    _ => 1.0, // fallback to 1.0 if weight not found
                };

                let new_cost = cost + edge_weight;
                if new_cost < *dist.get(&neighbor_offset).unwrap_or(&f64::INFINITY) {
                    dist.insert(neighbor_offset, new_cost);
                    parent.insert(neighbor_offset, node);
                    heap.push(Reverse((OrdF64(new_cost), neighbor_offset)));
                }
            }
        }

        if !found {
            return Ok(vec![]); // No path found
        }

        // Reconstruct path from parent map
        let total_cost = *dist.get(&target_offset).unwrap_or(&0.0);
        let mut path_offsets = vec![target_offset];
        let mut current = target_offset;
        while let Some(&prev) = parent.get(&current) {
            path_offsets.push(prev);
            current = prev;
        }
        path_offsets.reverse();

        // Convert offsets back to PK values for user-friendly output
        let pk_idx =
            catalog.get_node_table_by_id(src_table_id).map(|e| e.primary_key_idx).unwrap_or(0);

        let path_values: Vec<Value> = path_offsets
            .iter()
            .map(|&offset| {
                src_node_table
                    .read(offset)
                    .ok()
                    .and_then(|row| row.get(pk_idx).cloned())
                    .unwrap_or(Value::Int(offset as i64))
            })
            .collect();

        Ok(vec![vec![Value::List(path_values), Value::Float(total_cost)]])
    }
}

// ── PageRank ────────────────────────────────────────────────────

/// Computes PageRank scores for all nodes connected by a given relationship table
/// using power iteration.
///
/// Usage: `CALL pagerank('REL_NAME') YIELD node_id, score`
///
/// Returns one row per node with its PageRank score. Scores sum to approximately 1.0.
/// Uses damping factor d=0.85, max 20 iterations, convergence tolerance 1e-6.
pub struct PageRank;

impl Procedure for PageRank {
    fn name(&self) -> &str {
        "pagerank"
    }

    fn output_columns(&self) -> Vec<String> {
        vec!["node_id".to_string(), "score".to_string()]
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
                    "pagerank requires a string argument (rel table name)".into(),
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
            GqliteError::Execution(format!("relation table '{}' not found in storage", rel_name))
        })?;

        // Collect all node offsets into a contiguous index.
        // Key: (table_id, offset) -> contiguous index
        let mut node_list: Vec<(u32, u64)> = Vec::new();
        let mut node_index: HashMap<(u32, u64), usize> = HashMap::new();

        let mut add_nodes = |table_id: u32, offsets: Vec<u64>| {
            for offset in offsets {
                let key = (table_id, offset);
                node_index.entry(key).or_insert_with(|| {
                    let idx = node_list.len();
                    node_list.push(key);
                    idx
                });
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

        let n = node_list.len();
        if n == 0 {
            return Ok(vec![]);
        }

        let d = 0.85_f64;
        let max_iter = 20;
        let tolerance = 1e-6;

        // Precompute out-degree and outgoing neighbor indices for each node
        let mut out_neighbors: Vec<Vec<usize>> = vec![Vec::new(); n];
        for (idx, &(tid, offset)) in node_list.iter().enumerate() {
            // Only source-side nodes have outgoing edges
            if tid == src_table_id {
                let rels = rel_table.get_rels_from(offset);
                for (dst_offset, _rel_id) in rels {
                    let dst_key = (dst_table_id, dst_offset);
                    if let Some(&dst_idx) = node_index.get(&dst_key) {
                        out_neighbors[idx].push(dst_idx);
                    }
                }
            }
        }

        // Power iteration
        let mut scores: Vec<f64> = vec![1.0 / n as f64; n];

        for _ in 0..max_iter {
            // Sum up scores of dangling nodes (nodes with no outgoing edges).
            // Their rank is redistributed uniformly to all nodes.
            let dangling_sum: f64 =
                (0..n).filter(|&idx| out_neighbors[idx].is_empty()).map(|idx| scores[idx]).sum();

            let mut new_scores = vec![((1.0 - d) + d * dangling_sum) / n as f64; n];

            for idx in 0..n {
                let out_degree = out_neighbors[idx].len();
                if out_degree > 0 {
                    let contribution = scores[idx] / out_degree as f64;
                    for &neighbor_idx in &out_neighbors[idx] {
                        new_scores[neighbor_idx] += d * contribution;
                    }
                }
            }

            // Check convergence: max absolute difference
            let max_diff = scores
                .iter()
                .zip(new_scores.iter())
                .map(|(a, b)| (a - b).abs())
                .fold(0.0_f64, f64::max);

            scores = new_scores;
            if max_diff < tolerance {
                break;
            }
        }

        // Build result rows: convert offset to PK value, sorted by (table_id, offset)
        let mut entries: Vec<(u32, u64, f64)> = node_list
            .iter()
            .enumerate()
            .map(|(idx, &(tid, offset))| (tid, offset, scores[idx]))
            .collect();
        entries.sort_by_key(|&(tid, off, _)| (tid, off));

        let rows: Vec<ProcedureRow> = entries
            .into_iter()
            .map(|(tid, offset, score)| {
                // Try to get the PK value for user-friendly output
                let pk_val = catalog
                    .get_node_table_by_id(tid)
                    .and_then(|entry| {
                        let pk_idx = entry.primary_key_idx;
                        storage
                            .node_tables
                            .get(&tid)
                            .and_then(|nt| nt.read(offset).ok())
                            .and_then(|row| row.get(pk_idx).cloned())
                    })
                    .unwrap_or(Value::Int(offset as i64));
                vec![pk_val, Value::Float(score)]
            })
            .collect();

        Ok(rows)
    }
}

// ── Label Propagation (LPA) ─────────────────────────────────────

/// Detects communities using the Label Propagation Algorithm (LPA).
///
/// Usage: `CALL label_propagation('REL_NAME') YIELD node_id, community`
///
/// Algorithm:
/// 1. Initialize each node's label to its own contiguous index.
/// 2. Iterate: set each node's label to the most frequent neighbor label
///    (break ties by smallest label value).
/// 3. Stop when no labels change or after 100 iterations.
/// 4. Edges are treated as undirected (both forward and backward CSR).
///
/// Returns one row per node with its community label (the node offset of the
/// representative node).
pub struct LabelPropagation;

impl Procedure for LabelPropagation {
    fn name(&self) -> &str {
        "label_propagation"
    }

    fn output_columns(&self) -> Vec<String> {
        vec!["node_id".to_string(), "community".to_string()]
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
                    "label_propagation requires a string argument (rel table name)".into(),
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
            GqliteError::Execution(format!("relation table '{}' not found in storage", rel_name))
        })?;

        // Collect all node offsets into a contiguous index.
        // Key: (table_id, offset) -> contiguous index
        let mut node_list: Vec<(u32, u64)> = Vec::new();
        let mut node_index: HashMap<(u32, u64), usize> = HashMap::new();

        let mut add_nodes = |table_id: u32, offsets: Vec<u64>| {
            for offset in offsets {
                let key = (table_id, offset);
                node_index.entry(key).or_insert_with(|| {
                    let idx = node_list.len();
                    node_list.push(key);
                    idx
                });
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

        let n = node_list.len();
        if n == 0 {
            return Ok(vec![]);
        }

        // Precompute undirected neighbor lists.
        // For undirected behavior, combine both get_rels_from (forward CSR)
        // and get_rels_to (backward CSR).
        let mut neighbors: Vec<Vec<usize>> = vec![Vec::new(); n];

        for (idx, &(tid, offset)) in node_list.iter().enumerate() {
            // Forward edges: this node as source
            if tid == src_table_id {
                for (dst_offset, _rel_id) in rel_table.get_rels_from(offset) {
                    let dst_key = (dst_table_id, dst_offset);
                    if let Some(&dst_idx) = node_index.get(&dst_key) {
                        neighbors[idx].push(dst_idx);
                    }
                }
            }
            // Backward edges: this node as destination
            if tid == dst_table_id {
                for (src_offset, _rel_id) in rel_table.get_rels_to(offset) {
                    let src_key = (src_table_id, src_offset);
                    if let Some(&src_idx) = node_index.get(&src_key) {
                        neighbors[idx].push(src_idx);
                    }
                }
            }
        }

        // Deduplicate neighbor lists (an edge A->B appears as both forward
        // from A and backward to B, but we only want each neighbor once).
        for nbrs in &mut neighbors {
            nbrs.sort_unstable();
            nbrs.dedup();
        }

        // Initialize labels: each node's label = its contiguous index
        let mut labels: Vec<usize> = (0..n).collect();

        let max_iter = 100;

        for _ in 0..max_iter {
            let mut changed = false;

            for idx in 0..n {
                if neighbors[idx].is_empty() {
                    continue; // isolated node keeps its own label
                }

                // Count frequency of each neighbor label
                let mut freq: HashMap<usize, usize> = HashMap::new();
                for &nbr_idx in &neighbors[idx] {
                    *freq.entry(labels[nbr_idx]).or_insert(0) += 1;
                }

                // Find the most frequent label; break ties by smallest label
                let mut best_label = labels[idx];
                let mut best_count = 0;
                for (&label, &count) in &freq {
                    if count > best_count || (count == best_count && label < best_label) {
                        best_label = label;
                        best_count = count;
                    }
                }

                if best_label != labels[idx] {
                    labels[idx] = best_label;
                    changed = true;
                }
            }

            if !changed {
                break;
            }
        }

        // Build result rows sorted by (table_id, offset) for deterministic output.
        // Community ID = the offset of the representative node (the node whose
        // contiguous index equals the label).
        let mut entries: Vec<(u32, u64, i64)> = node_list
            .iter()
            .enumerate()
            .map(|(idx, &(tid, offset))| {
                let label_idx = labels[idx];
                let (_label_tid, label_offset) = node_list[label_idx];
                (tid, offset, label_offset as i64)
            })
            .collect();
        entries.sort_by_key(|&(tid, off, _)| (tid, off));

        let rows: Vec<ProcedureRow> = entries
            .into_iter()
            .map(|(_tid, offset, community)| vec![Value::Int(offset as i64), Value::Int(community)])
            .collect();

        Ok(rows)
    }
}

// ── Triangle Count ──────────────────────────────────────────────

/// Counts the number of triangles each node participates in.
///
/// Usage: `CALL triangle_count('REL_NAME') YIELD node_id, triangles`
///
/// Algorithm (neighbor-intersection, edges treated as undirected):
/// 1. Precompute sorted undirected neighbor sets for every node.
/// 2. For each edge (u, v) where u_idx < v_idx, intersect N(u) and N(v).
///    For each common neighbor w with w_idx > v_idx, increment counts for u, v, w.
/// 3. Return per-node triangle counts. Nodes with 0 triangles are included.
pub struct TriangleCount;

impl Procedure for TriangleCount {
    fn name(&self) -> &str {
        "triangle_count"
    }

    fn output_columns(&self) -> Vec<String> {
        vec!["node_id".to_string(), "triangles".to_string()]
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
                    "triangle_count requires a string argument (rel table name)".into(),
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
            GqliteError::Execution(format!("relation table '{}' not found in storage", rel_name))
        })?;

        // Collect all node offsets into a contiguous index.
        // Key: (table_id, offset) -> contiguous index
        let mut node_list: Vec<(u32, u64)> = Vec::new();
        let mut node_index: HashMap<(u32, u64), usize> = HashMap::new();

        let mut add_nodes = |table_id: u32, offsets: Vec<u64>| {
            for offset in offsets {
                let key = (table_id, offset);
                node_index.entry(key).or_insert_with(|| {
                    let idx = node_list.len();
                    node_list.push(key);
                    idx
                });
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

        let n = node_list.len();
        if n == 0 {
            return Ok(vec![]);
        }

        // Precompute undirected neighbor sets (as sorted Vecs for fast intersection).
        // Combine forward (get_rels_from) and backward (get_rels_to) edges, dedup via HashSet.
        let mut neighbors: Vec<Vec<usize>> = vec![Vec::new(); n];

        for (idx, &(tid, offset)) in node_list.iter().enumerate() {
            let mut nbr_set: HashSet<usize> = HashSet::new();

            // Forward edges: this node as source
            if tid == src_table_id {
                for (dst_offset, _rel_id) in rel_table.get_rels_from(offset) {
                    let dst_key = (dst_table_id, dst_offset);
                    if let Some(&dst_idx) = node_index.get(&dst_key) {
                        if dst_idx != idx {
                            nbr_set.insert(dst_idx);
                        }
                    }
                }
            }
            // Backward edges: this node as destination
            if tid == dst_table_id {
                for (src_offset, _rel_id) in rel_table.get_rels_to(offset) {
                    let src_key = (src_table_id, src_offset);
                    if let Some(&src_idx) = node_index.get(&src_key) {
                        if src_idx != idx {
                            nbr_set.insert(src_idx);
                        }
                    }
                }
            }

            let mut sorted_nbrs: Vec<usize> = nbr_set.into_iter().collect();
            sorted_nbrs.sort_unstable();
            neighbors[idx] = sorted_nbrs;
        }

        // Count triangles using sorted-intersection approach:
        // For each edge (u, v) where u < v, intersect N(u) and N(v),
        // and for each w in the intersection where w > v, increment all three counts.
        let mut triangles: Vec<i64> = vec![0; n];

        for u in 0..n {
            for &v in &neighbors[u] {
                if v <= u {
                    continue; // only process edges where u < v
                }
                // Sorted intersection of neighbors[u] and neighbors[v],
                // only counting w > v.
                let nu = &neighbors[u];
                let nv = &neighbors[v];
                let mut i = 0;
                let mut j = 0;
                while i < nu.len() && j < nv.len() {
                    if nu[i] < nv[j] {
                        i += 1;
                    } else if nu[i] > nv[j] {
                        j += 1;
                    } else {
                        // nu[i] == nv[j] — common neighbor
                        let w = nu[i];
                        if w > v {
                            triangles[u] += 1;
                            triangles[v] += 1;
                            triangles[w] += 1;
                        }
                        i += 1;
                        j += 1;
                    }
                }
            }
        }

        // Build result rows sorted by (table_id, offset) for deterministic output.
        let mut entries: Vec<(u32, u64, i64)> = node_list
            .iter()
            .enumerate()
            .map(|(idx, &(tid, offset))| (tid, offset, triangles[idx]))
            .collect();
        entries.sort_by_key(|&(tid, off, _)| (tid, off));

        let rows: Vec<ProcedureRow> = entries
            .into_iter()
            .map(|(_tid, offset, tri_count)| vec![Value::Int(offset as i64), Value::Int(tri_count)])
            .collect();

        Ok(rows)
    }
}

// ── Betweenness Centrality (Brandes) ────────────────────────────

/// Computes betweenness centrality for all nodes connected by a given
/// relationship table using the Brandes algorithm (O(VE) for unweighted).
///
/// Usage: `CALL betweenness('REL_NAME') YIELD node_id, score`
///
/// Edges are treated as undirected. The final scores are divided by 2
/// to account for the undirected double-counting inherent in Brandes.
pub struct Betweenness;

impl Procedure for Betweenness {
    fn name(&self) -> &str {
        "betweenness"
    }

    fn output_columns(&self) -> Vec<String> {
        vec!["node_id".to_string(), "score".to_string()]
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
                    "betweenness requires a string argument (rel table name)".into(),
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
            GqliteError::Execution(format!("relation table '{}' not found in storage", rel_name))
        })?;

        // Collect all node offsets into a contiguous index.
        let mut node_list: Vec<(u32, u64)> = Vec::new();
        let mut node_index: HashMap<(u32, u64), usize> = HashMap::new();

        let mut add_nodes = |table_id: u32, offsets: Vec<u64>| {
            for offset in offsets {
                let key = (table_id, offset);
                node_index.entry(key).or_insert_with(|| {
                    let idx = node_list.len();
                    node_list.push(key);
                    idx
                });
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

        let n = node_list.len();
        if n == 0 {
            return Ok(vec![]);
        }

        // Precompute undirected neighbor lists (forward + backward CSR, dedup).
        let mut neighbors: Vec<Vec<usize>> = vec![Vec::new(); n];

        for (idx, &(tid, offset)) in node_list.iter().enumerate() {
            let mut nbr_set: HashSet<usize> = HashSet::new();

            // Forward edges: this node as source
            if tid == src_table_id {
                for (dst_offset, _rel_id) in rel_table.get_rels_from(offset) {
                    let dst_key = (dst_table_id, dst_offset);
                    if let Some(&dst_idx) = node_index.get(&dst_key) {
                        if dst_idx != idx {
                            nbr_set.insert(dst_idx);
                        }
                    }
                }
            }
            // Backward edges: this node as destination
            if tid == dst_table_id {
                for (src_offset, _rel_id) in rel_table.get_rels_to(offset) {
                    let src_key = (src_table_id, src_offset);
                    if let Some(&src_idx) = node_index.get(&src_key) {
                        if src_idx != idx {
                            nbr_set.insert(src_idx);
                        }
                    }
                }
            }

            let mut sorted_nbrs: Vec<usize> = nbr_set.into_iter().collect();
            sorted_nbrs.sort_unstable();
            neighbors[idx] = sorted_nbrs;
        }

        // Brandes algorithm for betweenness centrality
        let mut cb: Vec<f64> = vec![0.0; n];

        for s in 0..n {
            // BFS from source s
            let mut stack: Vec<usize> = Vec::new();
            let mut predecessors: Vec<Vec<usize>> = vec![Vec::new(); n];
            let mut sigma: Vec<f64> = vec![0.0; n];
            sigma[s] = 1.0;
            let mut dist: Vec<i64> = vec![-1; n];
            dist[s] = 0;
            let mut queue: VecDeque<usize> = VecDeque::new();
            queue.push_back(s);

            while let Some(v) = queue.pop_front() {
                stack.push(v);
                for &w in &neighbors[v] {
                    // First visit to w
                    if dist[w] < 0 {
                        dist[w] = dist[v] + 1;
                        queue.push_back(w);
                    }
                    // Shortest path via v
                    if dist[w] == dist[v] + 1 {
                        sigma[w] += sigma[v];
                        predecessors[w].push(v);
                    }
                }
            }

            // Back-propagation of dependencies
            let mut delta: Vec<f64> = vec![0.0; n];
            while let Some(w) = stack.pop() {
                for &v in &predecessors[w] {
                    delta[v] += (sigma[v] / sigma[w]) * (1.0 + delta[w]);
                }
                if w != s {
                    cb[w] += delta[w];
                }
            }
        }

        // For undirected graphs, divide all scores by 2
        for score in &mut cb {
            *score /= 2.0;
        }

        // Build result rows sorted by (table_id, offset) for deterministic output.
        let mut entries: Vec<(u32, u64, f64)> = node_list
            .iter()
            .enumerate()
            .map(|(idx, &(tid, offset))| (tid, offset, cb[idx]))
            .collect();
        entries.sort_by_key(|&(tid, off, _)| (tid, off));

        let rows: Vec<ProcedureRow> = entries
            .into_iter()
            .map(|(_tid, offset, score)| vec![Value::Int(offset as i64), Value::Float(score)])
            .collect();

        Ok(rows)
    }
}
