use gqlite_core::storage::csr::{CSRNodeGroup, PendingEdge};

#[test]
fn build_from_edges_and_query() {
    // 4 nodes: 0,1,2,3
    // edges: 0→1, 0→2, 1→2, 2→3
    let edges = vec![(0, 1, 100), (0, 2, 101), (1, 2, 102), (2, 3, 103)];
    let csr = CSRNodeGroup::build_from_edges(0, &edges, 4);

    assert_eq!(csr.get_neighbor_count(0), 2);
    let n0 = csr.get_neighbors(0);
    assert!(n0.contains(&1));
    assert!(n0.contains(&2));

    assert_eq!(csr.get_neighbor_count(1), 1);
    assert_eq!(csr.get_neighbors(1), &[2]);

    assert_eq!(csr.get_neighbor_count(2), 1);
    assert_eq!(csr.get_neighbors(2), &[3]);

    assert_eq!(csr.get_neighbor_count(3), 0);
    assert!(csr.get_neighbors(3).is_empty());
}

#[test]
fn empty_node_returns_empty() {
    let csr = CSRNodeGroup::new(0, 5);
    assert!(csr.get_neighbors(0).is_empty());
    assert_eq!(csr.get_neighbor_count(0), 0);
    assert!(csr.get_neighbors(99).is_empty()); // out of range
}

#[test]
fn insert_and_compact() {
    let edges = vec![(0, 1, 100)];
    let mut csr = CSRNodeGroup::build_from_edges(0, &edges, 3);

    assert_eq!(csr.get_neighbor_count(0), 1);

    // Insert pending edge
    csr.insert_edge(PendingEdge {
        src_offset: 0,
        dst_offset: 2,
        rel_id: 200,
        properties: vec![],
    });
    csr.insert_edge(PendingEdge {
        src_offset: 1,
        dst_offset: 2,
        rel_id: 201,
        properties: vec![],
    });

    // Before compact, main CSR unchanged
    assert_eq!(csr.get_neighbor_count(0), 1);
    assert_eq!(csr.total_edge_count(), 3);

    // After compact
    csr.compact();
    assert_eq!(csr.get_neighbor_count(0), 2);
    let n0 = csr.get_neighbors(0);
    assert!(n0.contains(&1));
    assert!(n0.contains(&2));
    assert_eq!(csr.get_neighbor_count(1), 1);
    assert_eq!(csr.edge_count(), 3);
    assert!(csr.pending_inserts.is_empty());
}

#[test]
fn rel_ids_parallel() {
    let edges = vec![(0, 1, 100), (0, 2, 101)];
    let csr = CSRNodeGroup::build_from_edges(0, &edges, 3);

    let neighbors = csr.get_neighbors(0);
    let rel_ids = csr.get_rel_ids(0);
    assert_eq!(neighbors.len(), rel_ids.len());
    // They should be in the same order
    for (i, &n) in neighbors.iter().enumerate() {
        if n == 1 {
            assert_eq!(rel_ids[i], 100);
        } else if n == 2 {
            assert_eq!(rel_ids[i], 101);
        }
    }
}
