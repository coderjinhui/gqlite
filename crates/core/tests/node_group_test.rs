use gqlite_core::storage::format::NODE_GROUP_SIZE;
use gqlite_core::storage::node_group::NodeGroup;
use gqlite_core::types::data_type::DataType;
use gqlite_core::types::value::Value;

fn test_data_types() -> Vec<DataType> {
    vec![DataType::Int64, DataType::String]
}

#[test]
fn append_and_read() {
    let mut ng = NodeGroup::new(0, test_data_types());
    for i in 0..10 {
        let row = vec![
            Value::Int(i),
            Value::String(format!("name_{}", i)),
        ];
        let off = ng.append_row(&row).unwrap();
        assert_eq!(off, i as u64);
    }
    assert_eq!(ng.num_rows(), 10);

    for i in 0..10 {
        let row = ng.read_row(i as u64).unwrap();
        assert_eq!(row[0], Value::Int(i));
        assert_eq!(row[1], Value::String(format!("name_{}", i)));
    }
}

#[test]
fn cross_chunk_boundary() {
    let mut ng = NodeGroup::new(0, vec![DataType::Int64]);
    // Write 4096 rows — crosses 2 chunks of 2048 each
    for i in 0..4096u64 {
        ng.append_row(&[Value::Int(i as i64)]).unwrap();
    }
    assert_eq!(ng.num_rows(), 4096);
    assert_eq!(ng.chunks().len(), 2);

    // Check boundary values
    assert_eq!(ng.read_row(0).unwrap()[0], Value::Int(0));
    assert_eq!(ng.read_row(2047).unwrap()[0], Value::Int(2047));
    assert_eq!(ng.read_row(2048).unwrap()[0], Value::Int(2048));
    assert_eq!(ng.read_row(4095).unwrap()[0], Value::Int(4095));
}

#[test]
fn locate() {
    assert_eq!(NodeGroup::locate(0), (0, 0));
    assert_eq!(
        NodeGroup::locate(NODE_GROUP_SIZE as u64 - 1),
        (0, NODE_GROUP_SIZE as u64 - 1)
    );
    assert_eq!(NodeGroup::locate(NODE_GROUP_SIZE as u64), (1, 0));
    assert_eq!(NodeGroup::locate(NODE_GROUP_SIZE as u64 + 5), (1, 5));
}

#[test]
fn update_cell() {
    let mut ng = NodeGroup::new(0, vec![DataType::Int64, DataType::String]);
    ng.append_row(&[Value::Int(1), Value::String("a".into())])
        .unwrap();
    ng.append_row(&[Value::Int(2), Value::String("b".into())])
        .unwrap();

    ng.set_value(0, 1, &Value::String("updated".into()))
        .unwrap();
    let row = ng.read_row(0).unwrap();
    assert_eq!(row[1], Value::String("updated".into()));
}

#[test]
fn is_full_check() {
    // Use a small NodeGroup size concept — we can't fill 131072 in a test easily,
    // but we can verify the check
    let ng = NodeGroup::new(0, vec![DataType::Int64]);
    assert!(!ng.is_full());
}
