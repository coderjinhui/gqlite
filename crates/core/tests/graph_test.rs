use gqlite_core::types::graph::{InternalId, NODE_GROUP_SIZE};

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
