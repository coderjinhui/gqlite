use gqlite_core::catalog::{Catalog, ColumnDef, TableRef};
use gqlite_core::types::data_type::DataType;

fn make_columns() -> Vec<ColumnDef> {
    vec![
        ColumnDef {
            column_id: 0,
            name: "id".to_string(),
            data_type: DataType::Int64,
            nullable: false,
        },
        ColumnDef {
            column_id: 1,
            name: "name".to_string(),
            data_type: DataType::String,
            nullable: true,
        },
    ]
}

#[test]
fn create_node_table() {
    let mut catalog = Catalog::new();
    let tid = catalog.create_node_table("Person", make_columns(), "id").unwrap();
    assert_eq!(tid, 0);

    let entry = catalog.get_node_table("Person").unwrap();
    assert_eq!(entry.table_id, 0);
    assert_eq!(entry.name, "Person");
    assert_eq!(entry.primary_key_idx, 0);
    assert_eq!(entry.columns.len(), 2);
}

#[test]
fn create_rel_table() {
    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();
    catalog.create_node_table("Movie", make_columns(), "id").unwrap();

    let tid = catalog.create_rel_table("ACTED_IN", "Person", "Movie", vec![]).unwrap();
    assert_eq!(tid, 2);

    let entry = catalog.get_rel_table("ACTED_IN").unwrap();
    assert_eq!(entry.src_table_id, 0);
    assert_eq!(entry.dst_table_id, 1);
}

#[test]
fn duplicate_table_name() {
    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();
    let result = catalog.create_node_table("Person", make_columns(), "id");
    assert!(result.is_err());
}

#[test]
fn create_rel_table_missing_src() {
    let mut catalog = Catalog::new();
    let result = catalog.create_rel_table("KNOWS", "Person", "Person", vec![]);
    assert!(result.is_err());
}

#[test]
fn drop_rel_table() {
    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();
    catalog.create_rel_table("KNOWS", "Person", "Person", vec![]).unwrap();

    catalog.drop_table("KNOWS").unwrap();
    assert!(catalog.get_rel_table("KNOWS").is_none());
}

#[test]
fn drop_node_table_with_refs() {
    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();
    catalog.create_rel_table("KNOWS", "Person", "Person", vec![]).unwrap();

    let result = catalog.drop_table("Person");
    assert!(result.is_err()); // can't drop, referenced by KNOWS
}

#[test]
fn drop_node_table_after_rel_removed() {
    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();
    catalog.create_rel_table("KNOWS", "Person", "Person", vec![]).unwrap();

    catalog.drop_table("KNOWS").unwrap();
    catalog.drop_table("Person").unwrap();
    assert!(catalog.get_node_table("Person").is_none());
}

#[test]
fn table_id_auto_increment() {
    let mut catalog = Catalog::new();
    let t0 = catalog.create_node_table("A", make_columns(), "id").unwrap();
    let t1 = catalog.create_node_table("B", make_columns(), "id").unwrap();
    let t2 = catalog.create_rel_table("R", "A", "B", vec![]).unwrap();
    assert_eq!(t0, 0);
    assert_eq!(t1, 1);
    assert_eq!(t2, 2);
}

#[test]
fn get_table_by_id() {
    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();

    assert!(matches!(catalog.get_table_by_id(0), Some(TableRef::Node(_))));
    assert!(catalog.get_table_by_id(99).is_none());
}

#[test]
fn pk_column_not_found() {
    let mut catalog = Catalog::new();
    let result = catalog.create_node_table("Person", make_columns(), "nonexistent");
    assert!(result.is_err());
}

// ── Plan 007 tests: bincode serialization ──

#[test]
fn bincode_roundtrip() {
    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();
    catalog.create_node_table("Movie", make_columns(), "id").unwrap();
    catalog.create_rel_table("ACTED_IN", "Person", "Movie", vec![]).unwrap();

    let bytes = catalog.to_bytes().unwrap();
    let restored = Catalog::from_bytes(&bytes).unwrap();

    assert_eq!(restored.node_tables().len(), 2);
    assert_eq!(restored.rel_tables().len(), 1);
    assert_eq!(restored.get_node_table("Person").unwrap().table_id, 0);
    assert_eq!(restored.get_rel_table("ACTED_IN").unwrap().src_table_id, 0);
}

#[test]
fn bincode_empty_catalog() {
    let catalog = Catalog::new();
    let bytes = catalog.to_bytes().unwrap();
    let restored = Catalog::from_bytes(&bytes).unwrap();
    assert!(restored.node_tables().is_empty());
    assert!(restored.rel_tables().is_empty());
}

#[test]
fn pager_persistence_roundtrip() {
    use gqlite_core::storage::pager::Pager;
    use std::fs;

    let path = std::env::temp_dir().join("gqlite_test").join("test_catalog_persist.graph");
    fs::create_dir_all(path.parent().unwrap()).ok();
    fs::remove_file(&path).ok();

    let mut pager = Pager::create(&path).unwrap();

    // Build a catalog with some data
    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();
    catalog.create_node_table("Movie", make_columns(), "id").unwrap();
    catalog.create_rel_table("ACTED_IN", "Person", "Movie", vec![]).unwrap();

    // Save to page 1
    let catalog_page = pager.allocate_page().unwrap();
    catalog.save_to(&mut pager, catalog_page).unwrap();
    pager.sync().unwrap();

    // Load back
    let restored = Catalog::load_from(&pager, catalog_page).unwrap();
    assert_eq!(restored.node_tables().len(), 2);
    assert_eq!(restored.rel_tables().len(), 1);
    assert_eq!(restored.get_node_table("Person").unwrap().columns.len(), 2);

    drop(pager);
    fs::remove_file(&path).ok();
}

#[test]
fn catalog_v2_persistence_with_checksum() {
    use gqlite_core::storage::format::{verify_page_header, PageType, PAGE_HEADER_SIZE};
    use std::fs;

    let path = std::env::temp_dir().join(format!("gqlite_cat_v2_{}.graph", std::process::id()));
    fs::remove_file(&path).ok();

    let mut catalog = Catalog::new();
    catalog.create_node_table("Person", make_columns(), "id").unwrap();
    catalog.create_rel_table("KNOWS", "Person", "Person", vec![]).unwrap();

    // Save with v2 format (version=2 by default since FORMAT_VERSION=2)
    {
        let mut pager = gqlite_core::storage::pager::Pager::create(&path).unwrap();
        let start = 1u64;
        catalog.save_to(&mut pager, start).unwrap();
        pager.sync().unwrap();
    }

    // Verify pages have correct checksums
    {
        let pager = gqlite_core::storage::pager::Pager::open(&path).unwrap();
        assert!(pager.header().version >= 2);

        let mut page = vec![0u8; pager.page_size() as usize];
        pager.read_page(1, &mut page).unwrap();

        // Page 1 should be CatalogRoot with valid checksum
        let pt = verify_page_header(&page, 1).unwrap();
        assert_eq!(pt, PageType::CatalogRoot);

        // Payload starts after 8-byte header
        let total_len =
            u64::from_le_bytes(page[PAGE_HEADER_SIZE..PAGE_HEADER_SIZE + 8].try_into().unwrap());
        assert!(total_len > 0, "catalog payload should be non-empty");
    }

    // Load back and verify
    {
        let pager = gqlite_core::storage::pager::Pager::open(&path).unwrap();
        let restored = Catalog::load_from(&pager, 1).unwrap();
        assert_eq!(restored.node_tables().len(), 1);
        assert_eq!(restored.rel_tables().len(), 1);
        assert_eq!(restored.get_node_table("Person").unwrap().columns.len(), 2);
    }

    fs::remove_file(&path).ok();
}

#[test]
fn catalog_v2_detects_corruption() {
    use std::fs;
    let path =
        std::env::temp_dir().join(format!("gqlite_cat_v2_corrupt_{}.graph", std::process::id()));
    fs::remove_file(&path).ok();

    let mut catalog = Catalog::new();
    catalog.create_node_table("A", make_columns(), "id").unwrap();

    {
        let mut pager = gqlite_core::storage::pager::Pager::create(&path).unwrap();
        catalog.save_to(&mut pager, 1).unwrap();
        pager.sync().unwrap();
    }

    // Corrupt a byte in page 1's payload
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut f = fs::OpenOptions::new().read(true).write(true).open(&path).unwrap();
        // Corrupt byte at offset 4096 + 20 (inside page 1, in the payload area)
        f.seek(SeekFrom::Start(4096 + 20)).unwrap();
        f.write_all(&[0xFF]).unwrap();
        f.sync_all().unwrap();
    }

    // Loading should fail with checksum error
    {
        let pager = gqlite_core::storage::pager::Pager::open(&path).unwrap();
        let result = Catalog::load_from(&pager, 1);
        assert!(result.is_err(), "corrupted page should be detected");
        let err = format!("{}", result.err().unwrap());
        assert!(err.contains("checksum"), "error should mention checksum: {}", err);
    }

    fs::remove_file(&path).ok();
}
