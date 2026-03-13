use std::collections::HashMap;
use std::fmt;

use serde::{Deserialize, Serialize};

use super::value::Value;

/// Number of rows per NodeGroup (2^17 = 131072).
pub const NODE_GROUP_SIZE: u64 = 1 << 17;

/// Universal internal identifier for nodes and edges.
///
/// Combines a table ID with a row offset within that table's storage.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq, Serialize, Deserialize)]
pub struct InternalId {
    pub table_id: u32,
    pub offset: u64,
}

impl InternalId {
    pub fn new(table_id: u32, offset: u64) -> Self {
        Self { table_id, offset }
    }

    /// Returns the NodeGroup index this row belongs to.
    pub fn node_group_idx(&self) -> u32 {
        (self.offset >> 17) as u32
    }

    /// Returns the offset within the NodeGroup.
    pub fn offset_in_group(&self) -> u64 {
        self.offset & 0x1FFFF
    }
}

impl fmt::Display for InternalId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.table_id, self.offset)
    }
}

/// A label attached to a node or edge (e.g. `:Person`, `:KNOWS`).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Label(pub String);

/// A property graph node.
#[derive(Debug, Clone)]
pub struct Node {
    pub id: InternalId,
    pub labels: Vec<Label>,
    pub properties: HashMap<String, Value>,
}

/// A property graph edge (directed).
#[derive(Debug, Clone)]
pub struct Edge {
    pub id: InternalId,
    pub label: Label,
    pub source: InternalId,
    pub target: InternalId,
    pub properties: HashMap<String, Value>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn internal_id_group_calculation() {
        // offset 0 → group 0, offset_in_group 0
        let id = InternalId::new(1, 0);
        assert_eq!(id.node_group_idx(), 0);
        assert_eq!(id.offset_in_group(), 0);

        // offset 131071 (NODE_GROUP_SIZE - 1) → group 0
        let id = InternalId::new(1, NODE_GROUP_SIZE - 1);
        assert_eq!(id.node_group_idx(), 0);
        assert_eq!(id.offset_in_group(), NODE_GROUP_SIZE - 1);

        // offset 131072 → group 1, offset_in_group 0
        let id = InternalId::new(1, NODE_GROUP_SIZE);
        assert_eq!(id.node_group_idx(), 1);
        assert_eq!(id.offset_in_group(), 0);

        // offset 131073 → group 1, offset_in_group 1
        let id = InternalId::new(2, NODE_GROUP_SIZE + 1);
        assert_eq!(id.node_group_idx(), 1);
        assert_eq!(id.offset_in_group(), 1);
    }

    #[test]
    fn internal_id_display() {
        let id = InternalId::new(3, 42);
        assert_eq!(format!("{id}"), "3:42");
    }

    #[test]
    fn internal_id_serde() {
        let id = InternalId::new(5, 999);
        let encoded = bincode::serialize(&id).unwrap();
        let decoded: InternalId = bincode::deserialize(&encoded).unwrap();
        assert_eq!(id, decoded);
    }
}
