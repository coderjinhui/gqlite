use serde::{Deserialize, Serialize};

use crate::types::value::Value;

/// CSR header: offsets and lengths per source node.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CSRHeader {
    /// offsets[i] = start index in neighbor_ids for source node i.
    pub offsets: Vec<u64>,
    /// lengths[i] = number of neighbors for source node i.
    pub lengths: Vec<u64>,
}

/// A pending edge insert that hasn't been compacted into the main CSR structure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PendingEdge {
    pub src_offset: u64,
    pub dst_offset: u64,
    pub rel_id: u64,
    pub properties: Vec<Value>,
}

/// CSR-based adjacency storage for one direction (FWD or BWD) of a NodeGroup.
///
/// Stores the adjacency list in compressed form: for each source node, its
/// neighbors are in `neighbor_ids[offsets[src]..offsets[src]+lengths[src]]`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CSRNodeGroup {
    pub group_idx: u32,
    pub header: CSRHeader,
    /// Target node offsets, packed.
    pub neighbor_ids: Vec<u64>,
    /// Relationship IDs, parallel to neighbor_ids.
    pub rel_ids: Vec<u64>,
    /// Edges inserted after the CSR was built, not yet merged.
    pub pending_inserts: Vec<PendingEdge>,
    /// Number of source nodes this CSR covers.
    node_count: usize,
}

impl CSRNodeGroup {
    /// Create an empty CSR for `node_count` source nodes.
    pub fn new(group_idx: u32, node_count: usize) -> Self {
        Self {
            group_idx,
            header: CSRHeader {
                offsets: vec![0; node_count],
                lengths: vec![0; node_count],
            },
            neighbor_ids: Vec::new(),
            rel_ids: Vec::new(),
            pending_inserts: Vec::new(),
            node_count,
        }
    }

    /// Build a CSR from a list of (src_offset, dst_offset, rel_id) tuples.
    /// All src_offset values must be < node_count.
    pub fn build_from_edges(
        group_idx: u32,
        edges: &[(u64, u64, u64)],
        node_count: usize,
    ) -> Self {
        // Count edges per source node
        let mut counts = vec![0u64; node_count];
        for &(src, _, _) in edges {
            if (src as usize) < node_count {
                counts[src as usize] += 1;
            }
        }

        // Compute offsets (prefix sum)
        let mut offsets = vec![0u64; node_count];
        let mut total = 0u64;
        for i in 0..node_count {
            offsets[i] = total;
            total += counts[i];
        }

        // Fill neighbor and rel arrays
        let mut neighbor_ids = vec![0u64; total as usize];
        let mut rel_ids = vec![0u64; total as usize];
        let mut pos = offsets.clone();

        for &(src, dst, rel_id) in edges {
            let s = src as usize;
            if s < node_count {
                let idx = pos[s] as usize;
                neighbor_ids[idx] = dst;
                rel_ids[idx] = rel_id;
                pos[s] += 1;
            }
        }

        Self {
            group_idx,
            header: CSRHeader {
                offsets,
                lengths: counts,
            },
            neighbor_ids,
            rel_ids,
            pending_inserts: Vec::new(),
            node_count,
        }
    }

    /// Get the neighbor offsets for a source node.
    pub fn get_neighbors(&self, src_offset: u64) -> &[u64] {
        let s = src_offset as usize;
        if s >= self.node_count {
            return &[];
        }
        let start = self.header.offsets[s] as usize;
        let len = self.header.lengths[s] as usize;
        &self.neighbor_ids[start..start + len]
    }

    /// Get the relationship IDs for a source node.
    pub fn get_rel_ids(&self, src_offset: u64) -> &[u64] {
        let s = src_offset as usize;
        if s >= self.node_count {
            return &[];
        }
        let start = self.header.offsets[s] as usize;
        let len = self.header.lengths[s] as usize;
        &self.rel_ids[start..start + len]
    }

    /// Get the number of neighbors for a source node.
    pub fn get_neighbor_count(&self, src_offset: u64) -> u64 {
        let s = src_offset as usize;
        if s >= self.node_count {
            return 0;
        }
        self.header.lengths[s]
    }

    /// Insert a pending edge (will be merged on compact).
    pub fn insert_edge(&mut self, edge: PendingEdge) {
        self.pending_inserts.push(edge);
    }

    /// Merge all pending inserts into the main CSR structure.
    pub fn compact(&mut self) {
        if self.pending_inserts.is_empty() {
            return;
        }

        // Collect all existing edges + pending into a flat list
        let mut all_edges: Vec<(u64, u64, u64)> = Vec::new();

        // Existing edges
        for src in 0..self.node_count {
            let start = self.header.offsets[src] as usize;
            let len = self.header.lengths[src] as usize;
            for i in start..start + len {
                all_edges.push((src as u64, self.neighbor_ids[i], self.rel_ids[i]));
            }
        }

        // Pending edges
        for pe in &self.pending_inserts {
            all_edges.push((pe.src_offset, pe.dst_offset, pe.rel_id));
        }

        // Ensure we cover any new nodes from pending
        let max_src = all_edges
            .iter()
            .map(|(s, _, _)| *s as usize + 1)
            .max()
            .unwrap_or(self.node_count);
        let new_node_count = std::cmp::max(self.node_count, max_src);

        // Rebuild
        let rebuilt =
            Self::build_from_edges(self.group_idx, &all_edges, new_node_count);
        self.header = rebuilt.header;
        self.neighbor_ids = rebuilt.neighbor_ids;
        self.rel_ids = rebuilt.rel_ids;
        self.node_count = new_node_count;
        self.pending_inserts.clear();
    }

    /// Return all edges as (global_src_offset, global_dst_offset, rel_id).
    /// Includes both compacted and pending edges.
    pub fn all_edges(&self) -> Vec<(u64, u64, u64)> {
        let base = self.group_idx as u64 * crate::storage::format::NODE_GROUP_SIZE as u64;
        let mut edges = Vec::new();

        // Compacted edges
        for src in 0..self.node_count {
            let start = self.header.offsets[src] as usize;
            let len = self.header.lengths[src] as usize;
            for i in start..start + len {
                edges.push((base + src as u64, self.neighbor_ids[i], self.rel_ids[i]));
            }
        }

        // Pending edges
        for pe in &self.pending_inserts {
            edges.push((base + pe.src_offset, pe.dst_offset, pe.rel_id));
        }

        edges
    }

    pub fn node_count(&self) -> usize {
        self.node_count
    }

    /// Total number of edges (excluding pending).
    pub fn edge_count(&self) -> usize {
        self.neighbor_ids.len()
    }

    /// Total edges including pending.
    pub fn total_edge_count(&self) -> usize {
        self.neighbor_ids.len() + self.pending_inserts.len()
    }
}
