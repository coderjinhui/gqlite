use serde::{Deserialize, Serialize};

use crate::error::GqliteError;
use crate::types::data_type::DataType;

/// Definition of a single column in a table.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    pub column_id: u32,
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
}

/// Metadata for a node table (e.g. `Person`, `Movie`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeTableEntry {
    pub table_id: u32,
    pub name: String,
    pub columns: Vec<ColumnDef>,
    /// Index into `columns` for the primary key column.
    pub primary_key_idx: usize,
    /// Total number of rows in this table.
    pub row_count: u64,
    /// Next value for SERIAL columns (auto-increment counter).
    #[serde(default)]
    pub next_serial: u64,
}

/// Metadata for a relationship table (e.g. `KNOWS`, `ACTED_IN`).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RelTableEntry {
    pub table_id: u32,
    pub name: String,
    pub src_table_id: u32,
    pub dst_table_id: u32,
    pub columns: Vec<ColumnDef>,
    pub row_count: u64,
}

/// A reference to either a node or relationship table entry.
pub enum TableRef<'a> {
    Node(&'a NodeTableEntry),
    Rel(&'a RelTableEntry),
}

/// The catalog manages schema information for all tables in the database.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Catalog {
    node_tables: Vec<NodeTableEntry>,
    rel_tables: Vec<RelTableEntry>,
    next_table_id: u32,
}

impl Catalog {
    pub fn new() -> Self {
        Self {
            node_tables: Vec::new(),
            rel_tables: Vec::new(),
            next_table_id: 0,
        }
    }

    /// Create a new node table. `pk` is the name of the primary key column (must exist in `columns`).
    /// Returns the assigned table ID.
    pub fn create_node_table(
        &mut self,
        name: &str,
        columns: Vec<ColumnDef>,
        pk: &str,
    ) -> Result<u32, GqliteError> {
        // Check for duplicate name
        if self.node_tables.iter().any(|t| t.name == name)
            || self.rel_tables.iter().any(|t| t.name == name)
        {
            return Err(GqliteError::Other(format!(
                "table '{}' already exists",
                name
            )));
        }

        // Find primary key column index
        let pk_idx = columns
            .iter()
            .position(|c| c.name == pk)
            .ok_or_else(|| {
                GqliteError::Other(format!("primary key column '{}' not found", pk))
            })?;

        let table_id = self.next_table_id;
        self.next_table_id += 1;

        self.node_tables.push(NodeTableEntry {
            table_id,
            name: name.to_string(),
            columns,
            primary_key_idx: pk_idx,
            row_count: 0,
            next_serial: 0,
        });

        Ok(table_id)
    }

    /// Create a new relationship table between `src` and `dst` node tables.
    /// Returns the assigned table ID.
    pub fn create_rel_table(
        &mut self,
        name: &str,
        src: &str,
        dst: &str,
        columns: Vec<ColumnDef>,
    ) -> Result<u32, GqliteError> {
        // Check for duplicate name
        if self.node_tables.iter().any(|t| t.name == name)
            || self.rel_tables.iter().any(|t| t.name == name)
        {
            return Err(GqliteError::Other(format!(
                "table '{}' already exists",
                name
            )));
        }

        // Find src and dst node tables
        let src_id = self
            .node_tables
            .iter()
            .find(|t| t.name == src)
            .map(|t| t.table_id)
            .ok_or_else(|| {
                GqliteError::Other(format!("source node table '{}' not found", src))
            })?;
        let dst_id = self
            .node_tables
            .iter()
            .find(|t| t.name == dst)
            .map(|t| t.table_id)
            .ok_or_else(|| {
                GqliteError::Other(format!("destination node table '{}' not found", dst))
            })?;

        let table_id = self.next_table_id;
        self.next_table_id += 1;

        self.rel_tables.push(RelTableEntry {
            table_id,
            name: name.to_string(),
            src_table_id: src_id,
            dst_table_id: dst_id,
            columns,
            row_count: 0,
        });

        Ok(table_id)
    }

    /// Drop a table by name. Fails if a node table still has relationship tables referencing it.
    pub fn drop_table(&mut self, name: &str) -> Result<(), GqliteError> {
        // Check if it's a node table
        if let Some(pos) = self.node_tables.iter().position(|t| t.name == name) {
            let table_id = self.node_tables[pos].table_id;
            // Ensure no rel tables reference this node table
            let has_refs = self
                .rel_tables
                .iter()
                .any(|r| r.src_table_id == table_id || r.dst_table_id == table_id);
            if has_refs {
                return Err(GqliteError::Other(format!(
                    "cannot drop node table '{}': referenced by relationship table(s)",
                    name
                )));
            }
            self.node_tables.remove(pos);
            return Ok(());
        }

        // Check if it's a rel table
        if let Some(pos) = self.rel_tables.iter().position(|t| t.name == name) {
            self.rel_tables.remove(pos);
            return Ok(());
        }

        Err(GqliteError::Other(format!(
            "table '{}' not found",
            name
        )))
    }

    pub fn get_node_table(&self, name: &str) -> Option<&NodeTableEntry> {
        self.node_tables.iter().find(|t| t.name == name)
    }

    pub fn get_rel_table(&self, name: &str) -> Option<&RelTableEntry> {
        self.rel_tables.iter().find(|t| t.name == name)
    }

    pub fn get_node_table_by_id(&self, id: u32) -> Option<&NodeTableEntry> {
        self.node_tables.iter().find(|t| t.table_id == id)
    }

    pub fn get_node_table_mut_by_id(&mut self, id: u32) -> Option<&mut NodeTableEntry> {
        self.node_tables.iter_mut().find(|t| t.table_id == id)
    }

    pub fn get_rel_table_by_id(&self, id: u32) -> Option<&RelTableEntry> {
        self.rel_tables.iter().find(|t| t.table_id == id)
    }

    pub fn get_table_by_id(&self, id: u32) -> Option<TableRef<'_>> {
        if let Some(n) = self.get_node_table_by_id(id) {
            return Some(TableRef::Node(n));
        }
        if let Some(r) = self.get_rel_table_by_id(id) {
            return Some(TableRef::Rel(r));
        }
        None
    }

    pub fn node_tables(&self) -> &[NodeTableEntry] {
        &self.node_tables
    }

    pub fn rel_tables(&self) -> &[RelTableEntry] {
        &self.rel_tables
    }

    // ── Plan 048: ALTER TABLE ────────────────────────────────────

    /// Add a column to a node table.
    pub fn add_column_to_node_table(
        &mut self,
        table_name: &str,
        col: ColumnDef,
    ) -> Result<(), GqliteError> {
        let entry = self
            .node_tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or_else(|| GqliteError::Other(format!("node table '{}' not found", table_name)))?;
        if entry.columns.iter().any(|c| c.name == col.name) {
            return Err(GqliteError::Other(format!(
                "column '{}' already exists in table '{}'",
                col.name, table_name
            )));
        }
        entry.columns.push(col);
        Ok(())
    }

    /// Add a column to a relationship table.
    pub fn add_column_to_rel_table(
        &mut self,
        table_name: &str,
        col: ColumnDef,
    ) -> Result<(), GqliteError> {
        let entry = self
            .rel_tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or_else(|| GqliteError::Other(format!("rel table '{}' not found", table_name)))?;
        if entry.columns.iter().any(|c| c.name == col.name) {
            return Err(GqliteError::Other(format!(
                "column '{}' already exists in table '{}'",
                col.name, table_name
            )));
        }
        entry.columns.push(col);
        Ok(())
    }

    /// Drop a column from a node table.
    pub fn drop_column_from_node_table(
        &mut self,
        table_name: &str,
        col_name: &str,
    ) -> Result<(), GqliteError> {
        let entry = self
            .node_tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or_else(|| GqliteError::Other(format!("node table '{}' not found", table_name)))?;
        // Can't drop the primary key column
        if entry.columns[entry.primary_key_idx].name == col_name {
            return Err(GqliteError::Other(format!(
                "cannot drop primary key column '{}'",
                col_name
            )));
        }
        let pos = entry
            .columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or_else(|| {
                GqliteError::Other(format!(
                    "column '{}' not found in table '{}'",
                    col_name, table_name
                ))
            })?;
        entry.columns.remove(pos);
        // Adjust primary_key_idx if needed
        if pos < entry.primary_key_idx {
            entry.primary_key_idx -= 1;
        }
        Ok(())
    }

    /// Drop a column from a relationship table.
    pub fn drop_column_from_rel_table(
        &mut self,
        table_name: &str,
        col_name: &str,
    ) -> Result<(), GqliteError> {
        let entry = self
            .rel_tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or_else(|| GqliteError::Other(format!("rel table '{}' not found", table_name)))?;
        let pos = entry
            .columns
            .iter()
            .position(|c| c.name == col_name)
            .ok_or_else(|| {
                GqliteError::Other(format!(
                    "column '{}' not found in table '{}'",
                    col_name, table_name
                ))
            })?;
        entry.columns.remove(pos);
        Ok(())
    }

    /// Rename a table (node or rel).
    pub fn rename_table(
        &mut self,
        old_name: &str,
        new_name: &str,
    ) -> Result<(), GqliteError> {
        // Check that new name doesn't conflict
        if self.node_tables.iter().any(|t| t.name == new_name)
            || self.rel_tables.iter().any(|t| t.name == new_name)
        {
            return Err(GqliteError::Other(format!(
                "table '{}' already exists",
                new_name
            )));
        }
        if let Some(entry) = self.node_tables.iter_mut().find(|t| t.name == old_name) {
            entry.name = new_name.to_string();
            return Ok(());
        }
        if let Some(entry) = self.rel_tables.iter_mut().find(|t| t.name == old_name) {
            entry.name = new_name.to_string();
            return Ok(());
        }
        Err(GqliteError::Other(format!(
            "table '{}' not found",
            old_name
        )))
    }

    /// Rename a column in a node table.
    pub fn rename_column_in_node_table(
        &mut self,
        table_name: &str,
        old_col: &str,
        new_col: &str,
    ) -> Result<(), GqliteError> {
        let entry = self
            .node_tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or_else(|| GqliteError::Other(format!("node table '{}' not found", table_name)))?;
        if entry.columns.iter().any(|c| c.name == new_col) {
            return Err(GqliteError::Other(format!(
                "column '{}' already exists in table '{}'",
                new_col, table_name
            )));
        }
        let col = entry
            .columns
            .iter_mut()
            .find(|c| c.name == old_col)
            .ok_or_else(|| {
                GqliteError::Other(format!(
                    "column '{}' not found in table '{}'",
                    old_col, table_name
                ))
            })?;
        col.name = new_col.to_string();
        Ok(())
    }

    /// Rename a column in a relationship table.
    pub fn rename_column_in_rel_table(
        &mut self,
        table_name: &str,
        old_col: &str,
        new_col: &str,
    ) -> Result<(), GqliteError> {
        let entry = self
            .rel_tables
            .iter_mut()
            .find(|t| t.name == table_name)
            .ok_or_else(|| GqliteError::Other(format!("rel table '{}' not found", table_name)))?;
        if entry.columns.iter().any(|c| c.name == new_col) {
            return Err(GqliteError::Other(format!(
                "column '{}' already exists in table '{}'",
                new_col, table_name
            )));
        }
        let col = entry
            .columns
            .iter_mut()
            .find(|c| c.name == old_col)
            .ok_or_else(|| {
                GqliteError::Other(format!(
                    "column '{}' not found in table '{}'",
                    old_col, table_name
                ))
            })?;
        col.name = new_col.to_string();
        Ok(())
    }

    // ── Plan 007: bincode persistence ───────────────────────────────

    /// Serialize the catalog to bytes using bincode.
    pub fn to_bytes(&self) -> Result<Vec<u8>, GqliteError> {
        bincode::serialize(self)
            .map_err(|e| GqliteError::Storage(format!("catalog serialize error: {}", e)))
    }

    /// Deserialize a catalog from bincode bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, GqliteError> {
        bincode::deserialize(data)
            .map_err(|e| GqliteError::Storage(format!("catalog deserialize error: {}", e)))
    }

    /// Persist the catalog into the pager starting at `start_page`.
    ///
    /// Format: first 8 bytes = total length (u64 LE), then the bincode payload.
    /// If the data exceeds one page, it spans consecutive pages.
    pub fn save_to(
        &self,
        pager: &mut crate::storage::pager::Pager,
        start_page: crate::storage::pager::PageId,
    ) -> Result<(), GqliteError> {
        let payload = self.to_bytes()?;
        let total_len = payload.len() as u64;
        let page_size = pager.page_size() as usize;

        // Build the full byte stream: 8-byte length prefix + payload
        let mut stream = Vec::with_capacity(8 + payload.len());
        stream.extend_from_slice(&total_len.to_le_bytes());
        stream.extend_from_slice(&payload);

        // Calculate how many pages we need
        let pages_needed = (stream.len() + page_size - 1) / page_size;

        // Ensure we have enough pages allocated
        while pager.page_count() < start_page + pages_needed as u64 {
            pager.allocate_page()?;
        }

        // Write page by page
        for i in 0..pages_needed {
            let page_id = start_page + i as u64;
            let start = i * page_size;
            let end = std::cmp::min(start + page_size, stream.len());

            let mut page_buf = vec![0u8; page_size];
            page_buf[..end - start].copy_from_slice(&stream[start..end]);
            pager.write_page(page_id, &page_buf)?;
        }

        Ok(())
    }

    /// Load the catalog from the pager starting at `start_page`.
    pub fn load_from(
        pager: &crate::storage::pager::Pager,
        start_page: crate::storage::pager::PageId,
    ) -> Result<Self, GqliteError> {
        let page_size = pager.page_size() as usize;

        // Read first page to get the total length
        let mut first_page = vec![0u8; page_size];
        pager.read_page(start_page, &mut first_page)?;

        let total_len =
            u64::from_le_bytes(first_page[0..8].try_into().unwrap()) as usize;
        let total_with_header = 8 + total_len;
        let pages_needed = (total_with_header + page_size - 1) / page_size;

        // Accumulate all bytes
        let mut stream = Vec::with_capacity(total_with_header);
        stream.extend_from_slice(&first_page[..std::cmp::min(page_size, total_with_header)]);

        for i in 1..pages_needed {
            let page_id = start_page + i as u64;
            let mut buf = vec![0u8; page_size];
            pager.read_page(page_id, &mut buf)?;
            let remaining = total_with_header - stream.len();
            let take = std::cmp::min(page_size, remaining);
            stream.extend_from_slice(&buf[..take]);
        }

        // Skip the 8-byte length prefix
        let payload = &stream[8..8 + total_len];
        Self::from_bytes(payload)
    }
}

impl Default for Catalog {
    fn default() -> Self {
        Self::new()
    }
}

