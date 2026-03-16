//! Execution engine: interprets a physical plan against storage to produce results.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::Arc;

use crate::catalog::ColumnDef;
use crate::error::GqliteError;
use crate::parser::ast::*;
use crate::planner::logical::BoundSetItem;
use crate::planner::physical::PhysicalPlan;
use crate::storage::table::{NodeTable, RelTable};
use crate::transaction::wal::{WalPayload, WalRecord};
use crate::types::data_type::DataType;
use crate::types::graph::InternalId;
use crate::types::value::Value;
use crate::{ColumnInfo, DatabaseInner, QueryResult, Row};

// ── Intermediate result ─────────────────────────────────────────

/// Intermediate columnar result produced during operator evaluation.
pub(crate) struct Intermediate {
    pub(crate) columns: Vec<String>,
    pub(crate) types: Vec<DataType>,
    pub(crate) rows: Vec<Vec<Value>>,
}

impl Intermediate {
    pub(crate) fn empty() -> Self {
        Self {
            columns: Vec::new(),
            types: Vec::new(),
            rows: Vec::new(),
        }
    }

    pub(crate) fn into_query_result(self) -> QueryResult {
        let columns: Vec<ColumnInfo> = self
            .columns
            .iter()
            .zip(self.types.iter())
            .map(|(name, dt)| ColumnInfo {
                name: name.clone(),
                data_type: dt.clone(),
            })
            .collect();
        let rows: Vec<Row> = self
            .rows
            .into_iter()
            .map(|values| Row { values })
            .collect();
        QueryResult::new(columns, rows)
    }
}

// ── Engine ──────────────────────────────────────────────────────

/// The execution engine interprets a physical plan and produces result rows.
pub struct Engine {
    params: HashMap<String, Value>,
    /// MVCC snapshot timestamp for visibility checks.
    /// Rows with `create_ts <= start_ts` and `delete_ts == 0 || delete_ts > start_ts` are visible.
    start_ts: u64,
}

impl Engine {
    pub fn new() -> Self {
        Self {
            params: HashMap::new(),
            start_ts: u64::MAX, // legacy: see everything
        }
    }

    pub fn with_params(params: HashMap<String, Value>) -> Self {
        Self {
            params,
            start_ts: u64::MAX,
        }
    }

    /// Create an engine with MVCC snapshot and optional parameters.
    pub fn with_snapshot(start_ts: u64, params: HashMap<String, Value>) -> Self {
        Self { params, start_ts }
    }

    /// Execute a physical plan against the database, returning a QueryResult.
    pub fn execute_plan(
        &self,
        plan: &PhysicalPlan,
        db: &Arc<DatabaseInner>,
        txn_id: u64,
    ) -> Result<QueryResult, GqliteError> {
        match plan {
            PhysicalPlan::CreateNodeTable {
                name,
                columns,
                primary_key,
            } => self.exec_create_node_table(db, name, columns, primary_key, txn_id),
            PhysicalPlan::CreateRelTable {
                name,
                from_table,
                to_table,
                columns,
            } => self.exec_create_rel_table(db, name, from_table, to_table, columns, txn_id),
            PhysicalPlan::DropTable { name } => self.exec_drop_table(db, name, txn_id),
            PhysicalPlan::AlterTable { table_name, action } => {
                self.exec_alter_table(db, table_name, action, txn_id)
            }
            PhysicalPlan::CopyFrom {
                table_name,
                file_path,
                header,
                delimiter,
            } => self.exec_copy_from(db, table_name, file_path, *header, *delimiter, txn_id),
            PhysicalPlan::CopyTo {
                source,
                file_path,
                header,
                delimiter,
            } => self.exec_copy_to(db, source, file_path, *header, *delimiter, txn_id),
            PhysicalPlan::EmptyResult => Ok(QueryResult::empty()),
            _ => {
                let intermediate = self.execute_operator(plan, db, txn_id)?;
                Ok(intermediate.into_query_result())
            }
        }
    }

    // ── DDL execution ───────────────────────────────────────────

    fn exec_create_node_table(
        &self,
        db: &Arc<DatabaseInner>,
        name: &str,
        columns: &[(String, DataType)],
        primary_key: &str,
        txn_id: u64,
    ) -> Result<QueryResult, GqliteError> {
        // WAL record
        Self::wal_append(db, txn_id, WalPayload::CreateNodeTable {
            name: name.to_string(),
            columns: columns.to_vec(),
            primary_key: primary_key.to_string(),
        })?;

        let mut catalog = db.catalog.write().unwrap();
        let col_defs: Vec<ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(i, (cname, dtype))| ColumnDef {
                column_id: i as u32,
                name: cname.clone(),
                data_type: dtype.clone(),
                nullable: cname != primary_key,
            })
            .collect();
        let table_id = catalog.create_node_table(name, col_defs, primary_key)?;

        let entry = catalog.get_node_table(name).unwrap().clone();
        let mut storage = db.storage.write().unwrap();
        storage
            .node_tables
            .insert(table_id, NodeTable::new(&entry));
        Ok(QueryResult::empty())
    }

    fn exec_create_rel_table(
        &self,
        db: &Arc<DatabaseInner>,
        name: &str,
        from_table: &str,
        to_table: &str,
        columns: &[(String, DataType)],
        txn_id: u64,
    ) -> Result<QueryResult, GqliteError> {
        Self::wal_append(db, txn_id, WalPayload::CreateRelTable {
            name: name.to_string(),
            from_table: from_table.to_string(),
            to_table: to_table.to_string(),
            columns: columns.to_vec(),
        })?;

        let mut catalog = db.catalog.write().unwrap();
        let col_defs: Vec<ColumnDef> = columns
            .iter()
            .enumerate()
            .map(|(i, (cname, dtype))| ColumnDef {
                column_id: i as u32,
                name: cname.clone(),
                data_type: dtype.clone(),
                nullable: true,
            })
            .collect();
        let table_id = catalog.create_rel_table(name, from_table, to_table, col_defs)?;

        let entry = catalog.get_rel_table(name).unwrap().clone();
        let mut storage = db.storage.write().unwrap();
        storage.rel_tables.insert(table_id, RelTable::new(&entry));
        Ok(QueryResult::empty())
    }

    fn exec_drop_table(
        &self,
        db: &Arc<DatabaseInner>,
        name: &str,
        txn_id: u64,
    ) -> Result<QueryResult, GqliteError> {
        Self::wal_append(db, txn_id, WalPayload::DropTable {
            name: name.to_string(),
        })?;

        let mut catalog = db.catalog.write().unwrap();
        let table_id = catalog
            .get_node_table(name)
            .map(|e| e.table_id)
            .or_else(|| catalog.get_rel_table(name).map(|e| e.table_id));

        catalog.drop_table(name)?;

        if let Some(id) = table_id {
            let mut storage = db.storage.write().unwrap();
            storage.node_tables.remove(&id);
            storage.rel_tables.remove(&id);
        }
        Ok(QueryResult::empty())
    }

    fn exec_alter_table(
        &self,
        db: &Arc<DatabaseInner>,
        table_name: &str,
        action: &AlterTableAction,
        txn_id: u64,
    ) -> Result<QueryResult, GqliteError> {
        match action {
            AlterTableAction::AddColumn { col } => {
                Self::wal_append(
                    db,
                    txn_id,
                    WalPayload::AlterTableAddColumn {
                        table_name: table_name.to_string(),
                        col_name: col.name.clone(),
                        data_type: col.data_type.clone(),
                    },
                )?;

                let mut catalog = db.catalog.write().unwrap();
                let col_id = {
                    // Determine next column_id
                    let is_node = catalog.get_node_table(table_name).is_some();
                    if is_node {
                        let entry = catalog.get_node_table(table_name).unwrap();
                        entry.columns.len() as u32
                    } else {
                        let entry = catalog.get_rel_table(table_name).unwrap();
                        entry.columns.len() as u32
                    }
                };
                let col_def = ColumnDef {
                    column_id: col_id,
                    name: col.name.clone(),
                    data_type: col.data_type.clone(),
                    nullable: true,
                };
                let is_node = catalog.get_node_table(table_name).is_some();
                if is_node {
                    catalog.add_column_to_node_table(table_name, col_def)?;
                    // Add a column chunk to existing storage
                    let table_id = catalog.get_node_table(table_name).unwrap().table_id;
                    let mut storage = db.storage.write().unwrap();
                    if let Some(node_table) = storage.node_tables.get_mut(&table_id) {
                        node_table.add_column(&col.data_type);
                    }
                } else {
                    catalog.add_column_to_rel_table(table_name, col_def)?;
                }
            }
            AlterTableAction::DropColumn { col_name } => {
                Self::wal_append(
                    db,
                    txn_id,
                    WalPayload::AlterTableDropColumn {
                        table_name: table_name.to_string(),
                        col_name: col_name.clone(),
                    },
                )?;

                let mut catalog = db.catalog.write().unwrap();
                let is_node = catalog.get_node_table(table_name).is_some();
                if is_node {
                    // Find column index before dropping
                    let col_idx = catalog
                        .get_node_table(table_name)
                        .unwrap()
                        .columns
                        .iter()
                        .position(|c| c.name == *col_name);
                    catalog.drop_column_from_node_table(table_name, col_name)?;
                    if let Some(idx) = col_idx {
                        let table_id = catalog.get_node_table(table_name).unwrap().table_id;
                        let mut storage = db.storage.write().unwrap();
                        if let Some(node_table) = storage.node_tables.get_mut(&table_id) {
                            node_table.drop_column(idx);
                        }
                    }
                } else {
                    catalog.drop_column_from_rel_table(table_name, col_name)?;
                }
            }
            AlterTableAction::RenameTable { new_name } => {
                Self::wal_append(
                    db,
                    txn_id,
                    WalPayload::AlterTableRenameTable {
                        old_name: table_name.to_string(),
                        new_name: new_name.clone(),
                    },
                )?;
                let mut catalog = db.catalog.write().unwrap();
                catalog.rename_table(table_name, new_name)?;
            }
            AlterTableAction::RenameColumn { old_name, new_name } => {
                Self::wal_append(
                    db,
                    txn_id,
                    WalPayload::AlterTableRenameColumn {
                        table_name: table_name.to_string(),
                        old_col: old_name.clone(),
                        new_col: new_name.clone(),
                    },
                )?;
                let mut catalog = db.catalog.write().unwrap();
                let is_node = catalog.get_node_table(table_name).is_some();
                if is_node {
                    catalog.rename_column_in_node_table(table_name, old_name, new_name)?;
                } else {
                    catalog.rename_column_in_rel_table(table_name, old_name, new_name)?;
                }
            }
        }
        Ok(QueryResult::empty())
    }

    // ── COPY FROM CSV ────────────────────────────────────────────

    fn exec_copy_from(
        &self,
        db: &Arc<DatabaseInner>,
        table_name: &str,
        file_path: &str,
        header: bool,
        delimiter: char,
        txn_id: u64,
    ) -> Result<QueryResult, GqliteError> {
        use std::io::{BufRead, BufReader};

        // Get table schema
        let (table_id, col_types) = {
            let catalog = db.catalog.read().unwrap();
            let entry = catalog.get_node_table(table_name).ok_or_else(|| {
                GqliteError::Other(format!("table '{}' not found for COPY FROM", table_name))
            })?;
            let types: Vec<DataType> = entry.columns.iter().map(|c| c.data_type.clone()).collect();
            (entry.table_id, types)
        };

        let file = std::fs::File::open(file_path)
            .map_err(|e| GqliteError::Other(format!("cannot open '{}': {}", file_path, e)))?;
        let reader = BufReader::new(file);

        let mut row_count = 0u64;
        for (line_idx, line_result) in reader.lines().enumerate() {
            let line = line_result
                .map_err(|e| GqliteError::Other(format!("CSV read error: {}", e)))?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }

            // Skip header row
            if header && line_idx == 0 {
                continue;
            }

            let fields = parse_csv_line(line, delimiter);
            let mut values = Vec::with_capacity(col_types.len());
            for (i, dt) in col_types.iter().enumerate() {
                let field = fields.get(i).map(|s| s.as_str()).unwrap_or("");
                let value = if field.is_empty() || field == "NULL" || field == "\\N" {
                    Value::Null
                } else {
                    parse_csv_value(field, dt)?
                };
                values.push(value);
            }

            // Write WAL record
            Self::wal_append(
                db,
                txn_id,
                WalPayload::InsertNode {
                    table_name: table_name.to_string(),
                    table_id,
                    values: values.clone(),
                },
            )?;

            // Insert into storage
            let mut storage = db.storage.write().unwrap();
            if let Some(node_table) = storage.node_tables.get_mut(&table_id) {
                node_table.insert(&values, txn_id)?;
            }

            row_count += 1;
        }

        // Update catalog row_count
        {
            let mut catalog = db.catalog.write().unwrap();
            if let Some(entry) = catalog.get_node_table_mut_by_id(table_id) {
                entry.row_count += row_count;
            }
        }

        Ok(QueryResult::empty())
    }

    // ── COPY TO CSV ──────────────────────────────────────────────

    fn exec_copy_to(
        &self,
        db: &Arc<DatabaseInner>,
        source: &crate::planner::logical::CopyToSource,
        file_path: &str,
        header: bool,
        delimiter: char,
        txn_id: u64,
    ) -> Result<QueryResult, GqliteError> {
        use crate::planner::logical::CopyToSource;
        use crate::planner::physical::to_physical;
        use std::io::Write;

        let (columns, rows) = match source {
            CopyToSource::Table(table_name) => {
                let catalog = db.catalog.read().unwrap();
                let entry = catalog.get_node_table(table_name).ok_or_else(|| {
                    GqliteError::Other(format!(
                        "table '{}' not found for COPY TO",
                        table_name
                    ))
                })?;
                let col_names: Vec<String> =
                    entry.columns.iter().map(|c| c.name.clone()).collect();
                let tid = entry.table_id;
                drop(catalog);

                let storage = db.storage.read().unwrap();
                let node_table = storage.node_tables.get(&tid).ok_or_else(|| {
                    GqliteError::Other(format!("storage for table '{}' not found", table_name))
                })?;
                let all_rows: Vec<Vec<Value>> =
                    node_table.scan().map(|(_, row)| row).collect();
                (col_names, all_rows)
            }
            CopyToSource::Query(query_plan) => {
                let physical = to_physical(query_plan);
                let intermediate = self.execute_operator(&physical, db, txn_id)?;
                (intermediate.columns, intermediate.rows)
            }
        };

        let mut file = std::fs::File::create(file_path)
            .map_err(|e| GqliteError::Other(format!("cannot create '{}': {}", file_path, e)))?;

        if header {
            let header_line = columns.join(&delimiter.to_string());
            writeln!(file, "{}", header_line)
                .map_err(|e| GqliteError::Other(format!("CSV write error: {}", e)))?;
        }

        for row in &rows {
            let fields: Vec<String> = row.iter().map(|v| value_to_csv_string(v)).collect();
            let line = fields.join(&delimiter.to_string());
            writeln!(file, "{}", line)
                .map_err(|e| GqliteError::Other(format!("CSV write error: {}", e)))?;
        }

        Ok(QueryResult::empty())
    }

    // ── Recursive operator execution ────────────────────────────

    pub(crate) fn execute_operator(
        &self,
        plan: &PhysicalPlan,
        db: &Arc<DatabaseInner>,
        txn_id: u64,
    ) -> Result<Intermediate, GqliteError> {
        match plan {
            PhysicalPlan::SeqScan {
                table_name,
                table_id,
                columns: _,
                alias,
            } => self.exec_seq_scan(db, table_name, *table_id, alias, txn_id),

            PhysicalPlan::Filter { input, predicate } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_filter(input_result, predicate)
            }

            PhysicalPlan::Projection {
                input,
                expressions,
            } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_projection(input_result, expressions)
            }

            PhysicalPlan::ReturnAll { input } => self.execute_operator(input, db, txn_id),

            PhysicalPlan::CsrExpand {
                input,
                rel_table_name,
                rel_table_id,
                direction,
                src_alias,
                dst_alias,
                rel_alias: _,
                dst_table_name: _,
                dst_table_id,
                optional,
            } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_expand(
                    db,
                    input_result,
                    rel_table_name,
                    *rel_table_id,
                    direction,
                    src_alias,
                    dst_alias,
                    dst_table_id,
                    *optional,
                )
            }

            PhysicalPlan::RecursiveExpand {
                input,
                rel_table_name,
                rel_table_id,
                direction,
                src_alias,
                dst_alias,
                dst_table_name: _,
                dst_table_id,
                min_hops,
                max_hops,
            } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_recursive_expand(
                    db,
                    input_result,
                    rel_table_name,
                    *rel_table_id,
                    direction,
                    src_alias,
                    dst_alias,
                    dst_table_id,
                    *min_hops,
                    *max_hops,
                )
            }

            PhysicalPlan::HashJoin { build, probe, .. } => {
                let build_result = self.execute_operator(build, db, txn_id)?;
                let probe_result = self.execute_operator(probe, db, txn_id)?;
                self.exec_cross_join(build_result, probe_result)
            }

            PhysicalPlan::InsertNode {
                table_name,
                table_id,
                values,
            } => self.exec_insert_node(db, table_name, *table_id, values, txn_id),

            PhysicalPlan::InsertRel {
                input,
                rel_table_name,
                rel_table_id,
                src_alias,
                dst_alias,
                properties: _,
            } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_insert_rel(
                    db,
                    input_result,
                    rel_table_name,
                    *rel_table_id,
                    src_alias,
                    dst_alias,
                    txn_id,
                )
            }

            PhysicalPlan::SetProperty { input, items } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_set_property(db, input_result, items, txn_id)
            }

            PhysicalPlan::Delete {
                input,
                detach: _,
                variables,
            } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_delete(db, input_result, variables, txn_id)
            }

            PhysicalPlan::OrderBy { input, items } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_order_by(input_result, items)
            }

            PhysicalPlan::Limit { input, count } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_limit(input_result, count)
            }

            PhysicalPlan::Skip { input, count } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_skip(input_result, count)
            }

            PhysicalPlan::Aggregate {
                input,
                expressions,
            } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_aggregate(input_result, expressions)
            }

            PhysicalPlan::Union { left, right, all } => {
                let left_result = self.execute_operator(left, db, txn_id)?;
                let right_result = self.execute_operator(right, db, txn_id)?;
                self.exec_union(left_result, right_result, *all)
            }

            PhysicalPlan::Unwind {
                input,
                expr,
                alias,
            } => {
                let input_result = self.execute_operator(input, db, txn_id)?;
                self.exec_unwind(input_result, expr, alias)
            }

            PhysicalPlan::Merge {
                table_name,
                table_id,
                properties,
                on_create,
                on_match,
            } => self.exec_merge(db, table_name, *table_id, properties, on_create, on_match, txn_id),

            // DDL handled in execute_plan
            PhysicalPlan::CreateNodeTable { .. }
            | PhysicalPlan::CreateRelTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::AlterTable { .. }
            | PhysicalPlan::CopyFrom { .. }
            | PhysicalPlan::CopyTo { .. }
            | PhysicalPlan::EmptyResult => Ok(Intermediate::empty()),
        }
    }

    // ── SeqScan (Task 024) ──────────────────────────────────────

    fn exec_seq_scan(
        &self,
        db: &Arc<DatabaseInner>,
        table_name: &str,
        table_id: u32,
        alias: &str,
        _txn_id: u64,
    ) -> Result<Intermediate, GqliteError> {
        // Handle unlabeled node scan (no table specified)
        if table_name.is_empty() {
            return Ok(Intermediate::empty());
        }

        let catalog = db.catalog.read().unwrap();
        let entry = catalog
            .get_node_table_by_id(table_id)
            .or_else(|| catalog.get_node_table(table_name))
            .ok_or_else(|| {
                GqliteError::Execution(format!("table '{}' not found", table_name))
            })?;
        let schema = entry.columns.clone();
        drop(catalog);

        let storage = db.storage.read().unwrap();
        let node_table = storage.node_tables.get(&table_id).ok_or_else(|| {
            GqliteError::Execution(format!("storage for table '{}' not found", table_name))
        })?;

        // Columns: alias (InternalId), alias.col1, alias.col2, ...
        let mut col_names = vec![alias.to_string()];
        let mut col_types = vec![DataType::InternalId];
        for col in &schema {
            col_names.push(format!("{}.{}", alias, col.name));
            col_types.push(col.data_type.clone());
        }

        // Use MVCC-aware scan with the engine's snapshot timestamp
        let mut rows = Vec::new();
        for (offset, values) in node_table.scan_mvcc(self.start_ts) {
            let mut row = vec![Value::InternalId(InternalId::new(table_id, offset))];
            row.extend(values);
            rows.push(row);
        }

        Ok(Intermediate {
            columns: col_names,
            types: col_types,
            rows,
        })
    }

    // ── Filter (Task 025) ───────────────────────────────────────

    pub(crate) fn exec_filter(
        &self,
        input: Intermediate,
        predicate: &Expr,
    ) -> Result<Intermediate, GqliteError> {
        let mut filtered = Vec::new();
        for row in &input.rows {
            let val = self.eval_expr(predicate, &input.columns, row)?;
            if matches!(val, Value::Bool(true)) {
                filtered.push(row.clone());
            }
        }
        Ok(Intermediate {
            columns: input.columns,
            types: input.types,
            rows: filtered,
        })
    }

    // ── Projection (Task 025) ───────────────────────────────────

    pub(crate) fn exec_projection(
        &self,
        input: Intermediate,
        expressions: &[(Expr, Option<String>)],
    ) -> Result<Intermediate, GqliteError> {
        let mut col_names = Vec::new();
        let mut col_types = Vec::new();
        for (expr, alias) in expressions {
            col_names.push(alias.clone().unwrap_or_else(|| expr_display_name(expr)));
            col_types.push(DataType::String); // placeholder, inferred below
        }

        let mut rows = Vec::new();
        for row in &input.rows {
            let mut out = Vec::new();
            for (expr, _) in expressions {
                out.push(self.eval_expr(expr, &input.columns, row)?);
            }
            rows.push(out);
        }

        // Infer types from first result row
        if let Some(first) = rows.first() {
            for (i, val) in first.iter().enumerate() {
                if i < col_types.len() {
                    if let Some(dt) = val.data_type() {
                        col_types[i] = dt;
                    }
                }
            }
        }

        Ok(Intermediate {
            columns: col_names,
            types: col_types,
            rows,
        })
    }

    // ── CsrExpand ───────────────────────────────────────────────

    pub(crate) fn exec_expand(
        &self,
        db: &Arc<DatabaseInner>,
        input: Intermediate,
        rel_table_name: &str,
        rel_table_id: u32,
        direction: &Direction,
        src_alias: &str,
        dst_alias: &str,
        dst_table_id: &Option<u32>,
        optional: bool,
    ) -> Result<Intermediate, GqliteError> {
        let storage = db.storage.read().unwrap();
        let catalog = db.catalog.read().unwrap();

        let rel_table = storage.rel_tables.get(&rel_table_id).ok_or_else(|| {
            GqliteError::Execution(format!("rel table '{}' not found", rel_table_name))
        })?;

        let dst_tid = dst_table_id.or_else(|| match direction {
            Direction::Right => Some(rel_table.dst_table_id()),
            Direction::Left => Some(rel_table.src_table_id()),
            Direction::Both => Some(rel_table.dst_table_id()),
        });

        let dst_entry = dst_tid.and_then(|id| catalog.get_node_table_by_id(id));

        // Build output columns: input + destination node columns
        let mut out_cols = input.columns.clone();
        let mut out_types = input.types.clone();
        out_cols.push(dst_alias.to_string());
        out_types.push(DataType::InternalId);
        if let Some(entry) = &dst_entry {
            for col in &entry.columns {
                out_cols.push(format!("{}.{}", dst_alias, col.name));
                out_types.push(col.data_type.clone());
            }
        }

        let src_col = input
            .columns
            .iter()
            .position(|c| c == src_alias)
            .ok_or_else(|| {
                GqliteError::Execution(format!("source alias '{}' not found", src_alias))
            })?;

        let pk_idx = dst_entry.as_ref().map(|e| e.primary_key_idx).unwrap_or(0);
        let mut out_rows = Vec::new();

        for row in &input.rows {
            let src_id = match &row[src_col] {
                Value::InternalId(id) => *id,
                _ => continue,
            };

            let neighbors = match direction {
                Direction::Right => rel_table.get_rels_from(src_id.offset),
                Direction::Left => rel_table.get_rels_to(src_id.offset),
                Direction::Both => {
                    let mut all = rel_table.get_rels_from(src_id.offset);
                    all.extend(rel_table.get_rels_to(src_id.offset));
                    all
                }
            };

            let mut matched = false;
            for (neighbor_offset, _rel_id) in &neighbors {
                let dst_id = dst_tid
                    .map(|tid| InternalId::new(tid, *neighbor_offset))
                    .unwrap_or_else(|| InternalId::new(0, *neighbor_offset));

                let mut new_row = row.clone();
                new_row.push(Value::InternalId(dst_id));

                if let Some(tid) = dst_tid {
                    if let Some(dst_table) = storage.node_tables.get(&tid) {
                        if let Ok(dst_vals) = dst_table.read(*neighbor_offset) {
                            // Skip deleted nodes
                            if dst_vals.get(pk_idx).map_or(true, |v| v.is_null()) {
                                continue;
                            }
                            new_row.extend(dst_vals);
                        } else {
                            continue;
                        }
                    }
                }

                matched = true;
                out_rows.push(new_row);
            }

            // OPTIONAL MATCH: if no neighbors found, fill with NULLs
            if optional && !matched {
                let mut new_row = row.clone();
                let null_count = out_cols.len() - input.columns.len();
                for _ in 0..null_count {
                    new_row.push(Value::Null);
                }
                out_rows.push(new_row);
            }
        }

        Ok(Intermediate {
            columns: out_cols,
            types: out_types,
            rows: out_rows,
        })
    }

    // ── RecursiveExpand (BFS variable-length paths) ─────────────

    pub(crate) fn exec_recursive_expand(
        &self,
        db: &Arc<DatabaseInner>,
        input: Intermediate,
        rel_table_name: &str,
        rel_table_id: u32,
        direction: &Direction,
        src_alias: &str,
        dst_alias: &str,
        dst_table_id: &Option<u32>,
        min_hops: u32,
        max_hops: u32,
    ) -> Result<Intermediate, GqliteError> {
        use std::collections::VecDeque;

        let storage = db.storage.read().unwrap();
        let catalog = db.catalog.read().unwrap();

        let rel_table = storage.rel_tables.get(&rel_table_id).ok_or_else(|| {
            GqliteError::Execution(format!("rel table '{}' not found", rel_table_name))
        })?;

        let dst_tid = dst_table_id.or_else(|| match direction {
            Direction::Right => Some(rel_table.dst_table_id()),
            Direction::Left => Some(rel_table.src_table_id()),
            Direction::Both => Some(rel_table.dst_table_id()),
        });

        let dst_entry = dst_tid.and_then(|id| catalog.get_node_table_by_id(id));
        let pk_idx = dst_entry.as_ref().map(|e| e.primary_key_idx).unwrap_or(0);

        // Build output columns: input cols + dst InternalId + dst properties
        let mut out_cols = input.columns.clone();
        let mut out_types = input.types.clone();
        out_cols.push(dst_alias.to_string());
        out_types.push(DataType::InternalId);
        if let Some(entry) = &dst_entry {
            for col in &entry.columns {
                out_cols.push(format!("{}.{}", dst_alias, col.name));
                out_types.push(col.data_type.clone());
            }
        }

        let src_col = input
            .columns
            .iter()
            .position(|c| c == src_alias)
            .ok_or_else(|| {
                GqliteError::Execution(format!("source alias '{}' not found", src_alias))
            })?;

        let mut out_rows = Vec::new();

        for row in &input.rows {
            let src_id = match &row[src_col] {
                Value::InternalId(id) => *id,
                _ => continue,
            };

            // BFS from src_id with depth tracking.
            // frontier: (node_offset, current_depth)
            let mut frontier: VecDeque<(u64, u32)> = VecDeque::new();
            frontier.push_back((src_id.offset, 0));
            let mut visited = std::collections::HashSet::new();
            visited.insert(src_id.offset);

            while let Some((current_offset, depth)) = frontier.pop_front() {
                if depth >= max_hops {
                    continue;
                }

                let neighbors = match direction {
                    Direction::Right => rel_table.get_rels_from(current_offset),
                    Direction::Left => rel_table.get_rels_to(current_offset),
                    Direction::Both => {
                        let mut all = rel_table.get_rels_from(current_offset);
                        all.extend(rel_table.get_rels_to(current_offset));
                        all
                    }
                };

                let next_depth = depth + 1;
                for (neighbor_offset, _rel_id) in &neighbors {
                    if !visited.insert(*neighbor_offset) {
                        continue; // cycle avoidance
                    }

                    // Verify the destination node exists and is not deleted
                    if let Some(tid) = dst_tid {
                        if let Some(dst_table) = storage.node_tables.get(&tid) {
                            if let Ok(dst_vals) = dst_table.read(*neighbor_offset) {
                                if dst_vals.get(pk_idx).map_or(true, |v| v.is_null()) {
                                    continue; // deleted node
                                }

                                // Emit row if depth is within [min_hops, max_hops]
                                if next_depth >= min_hops {
                                    let dst_id = InternalId::new(tid, *neighbor_offset);
                                    let mut new_row = row.clone();
                                    new_row.push(Value::InternalId(dst_id));
                                    new_row.extend(dst_vals);
                                    out_rows.push(new_row);
                                }
                            } else {
                                continue;
                            }
                        }
                    } else if next_depth >= min_hops {
                        let dst_id = InternalId::new(0, *neighbor_offset);
                        let mut new_row = row.clone();
                        new_row.push(Value::InternalId(dst_id));
                        out_rows.push(new_row);
                    }

                    // Continue BFS if we haven't reached max hops
                    if next_depth < max_hops {
                        frontier.push_back((*neighbor_offset, next_depth));
                    }
                }
            }
        }

        Ok(Intermediate {
            columns: out_cols,
            types: out_types,
            rows: out_rows,
        })
    }

    // ── Cross Join (Task 026 — simplified) ──────────────────────

    pub(crate) fn exec_cross_join(
        &self,
        build: Intermediate,
        probe: Intermediate,
    ) -> Result<Intermediate, GqliteError> {
        let mut out_cols = build.columns;
        out_cols.extend(probe.columns);
        let mut out_types = build.types;
        out_types.extend(probe.types);

        let mut rows = Vec::new();
        for b_row in &build.rows {
            for p_row in &probe.rows {
                let mut combined = b_row.clone();
                combined.extend(p_row.clone());
                rows.push(combined);
            }
        }

        Ok(Intermediate {
            columns: out_cols,
            types: out_types,
            rows,
        })
    }

    // ── InsertNode ──────────────────────────────────────────────

    fn exec_insert_node(
        &self,
        db: &Arc<DatabaseInner>,
        table_name: &str,
        table_id: u32,
        values: &[(usize, Expr)],
        txn_id: u64,
    ) -> Result<Intermediate, GqliteError> {
        let catalog = db.catalog.read().unwrap();
        let entry = catalog
            .get_node_table_by_id(table_id)
            .ok_or_else(|| {
                GqliteError::Execution(format!("table '{}' not found", table_name))
            })?;
        let num_cols = entry.columns.len();
        // Identify SERIAL columns
        let serial_cols: Vec<usize> = entry
            .columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.data_type == DataType::Serial)
            .map(|(i, _)| i)
            .collect();
        drop(catalog);

        let mut row = vec![Value::Null; num_cols];
        for (col_idx, expr) in values {
            let val = eval_literal(expr)?;
            if *col_idx < num_cols {
                row[*col_idx] = val;
            }
        }

        // Auto-assign SERIAL values for columns that weren't explicitly set
        if !serial_cols.is_empty() {
            let mut catalog = db.catalog.write().unwrap();
            if let Some(entry) = catalog.get_node_table_mut_by_id(table_id) {
                for &col_idx in &serial_cols {
                    if row[col_idx].is_null() {
                        row[col_idx] = Value::Int(entry.next_serial as i64);
                        entry.next_serial += 1;
                    }
                }
            }
        }

        // WAL record (before applying)
        Self::wal_append(db, txn_id, WalPayload::InsertNode {
            table_name: table_name.to_string(),
            table_id,
            values: row.clone(),
        })?;

        let mut storage = db.storage.write().unwrap();
        let node_table = storage.node_tables.get_mut(&table_id).ok_or_else(|| {
            GqliteError::Execution(format!("storage for table '{}' not found", table_name))
        })?;
        node_table.insert(&row, txn_id)?;

        Ok(Intermediate::empty())
    }

    // ── InsertRel ───────────────────────────────────────────────

    fn exec_insert_rel(
        &self,
        db: &Arc<DatabaseInner>,
        input: Intermediate,
        rel_table_name: &str,
        rel_table_id: u32,
        src_alias: &str,
        dst_alias: &str,
        txn_id: u64,
    ) -> Result<Intermediate, GqliteError> {
        let src_col = input
            .columns
            .iter()
            .position(|c| c == src_alias)
            .ok_or_else(|| {
                GqliteError::Execution(format!("source alias '{}' not found", src_alias))
            })?;
        let dst_col = input
            .columns
            .iter()
            .position(|c| c == dst_alias)
            .ok_or_else(|| {
                GqliteError::Execution(format!("dest alias '{}' not found", dst_alias))
            })?;

        let mut storage = db.storage.write().unwrap();
        let rel_table = storage.rel_tables.get_mut(&rel_table_id).ok_or_else(|| {
            GqliteError::Execution(format!("rel table '{}' not found", rel_table_name))
        })?;

        for row in &input.rows {
            let src_id = match &row[src_col] {
                Value::InternalId(id) => *id,
                _ => {
                    return Err(GqliteError::Execution(
                        "source is not an InternalId".into(),
                    ))
                }
            };
            let dst_id = match &row[dst_col] {
                Value::InternalId(id) => *id,
                _ => {
                    return Err(GqliteError::Execution(
                        "destination is not an InternalId".into(),
                    ))
                }
            };

            Self::wal_append(db, txn_id, WalPayload::InsertRel {
                rel_table_name: rel_table_name.to_string(),
                rel_table_id,
                src: src_id,
                dst: dst_id,
                properties: vec![],
            })?;

            rel_table.insert_rel(src_id, dst_id, &[])?;
        }
        rel_table.compact();

        Ok(Intermediate::empty())
    }

    // ── SetProperty ─────────────────────────────────────────────

    fn exec_set_property(
        &self,
        db: &Arc<DatabaseInner>,
        input: Intermediate,
        items: &[BoundSetItem],
        txn_id: u64,
    ) -> Result<Intermediate, GqliteError> {
        let catalog = db.catalog.read().unwrap();
        let mut storage = db.storage.write().unwrap();

        for item in items {
            let var_col = input
                .columns
                .iter()
                .position(|c| c == &item.variable)
                .ok_or_else(|| {
                    GqliteError::Execution(format!(
                        "variable '{}' not found",
                        item.variable
                    ))
                })?;

            for row in &input.rows {
                let id = match &row[var_col] {
                    Value::InternalId(id) => *id,
                    _ => continue,
                };

                let val = self.eval_expr(&item.value, &input.columns, row)?;

                if let Some(entry) = catalog.get_node_table_by_id(id.table_id) {
                    let col_idx = entry
                        .columns
                        .iter()
                        .position(|c| c.name == item.field)
                        .ok_or_else(|| {
                            GqliteError::Execution(format!(
                                "column '{}' not found",
                                item.field
                            ))
                        })?;
                    if let Some(node_table) = storage.node_tables.get_mut(&id.table_id) {
                        Self::wal_append(db, txn_id, WalPayload::UpdateProperty {
                            table_id: id.table_id,
                            node_offset: id.offset,
                            col_idx,
                            new_value: val.clone(),
                        })?;
                        node_table.update(id.offset, col_idx, val)?;
                    }
                }
            }
        }

        Ok(Intermediate::empty())
    }

    // ── Delete ──────────────────────────────────────────────────

    fn exec_delete(
        &self,
        db: &Arc<DatabaseInner>,
        input: Intermediate,
        variables: &[String],
        txn_id: u64,
    ) -> Result<Intermediate, GqliteError> {
        let mut storage = db.storage.write().unwrap();

        for var in variables {
            let var_col =
                input.columns.iter().position(|c| c == var).ok_or_else(|| {
                    GqliteError::Execution(format!("variable '{}' not found", var))
                })?;

            for row in &input.rows {
                let id = match &row[var_col] {
                    Value::InternalId(id) => *id,
                    _ => continue,
                };
                if let Some(node_table) = storage.node_tables.get_mut(&id.table_id) {
                    Self::wal_append(db, txn_id, WalPayload::DeleteNode {
                        table_id: id.table_id,
                        node_offset: id.offset,
                    })?;
                    node_table.delete(id.offset, txn_id)?;
                }
            }
        }

        Ok(Intermediate::empty())
    }

    // ── OrderBy (Task 041) ─────────────────────────────────────

    pub(crate) fn exec_order_by(
        &self,
        input: Intermediate,
        items: &[OrderByItem],
    ) -> Result<Intermediate, GqliteError> {
        let mut rows = input.rows;
        let columns = &input.columns;

        rows.sort_by(|a, b| {
            for item in items {
                let va = self.eval_expr(&item.expr, columns, a).unwrap_or(Value::Null);
                let vb = self.eval_expr(&item.expr, columns, b).unwrap_or(Value::Null);

                let ord = compare_values(&va, &vb).unwrap_or(Ordering::Equal);
                let ord = if item.descending { ord.reverse() } else { ord };
                if ord != Ordering::Equal {
                    return ord;
                }
            }
            Ordering::Equal
        });

        Ok(Intermediate {
            columns: input.columns,
            types: input.types,
            rows,
        })
    }

    // ── Limit (Task 041) ───────────────────────────────────────

    pub(crate) fn exec_limit(
        &self,
        input: Intermediate,
        count: &Expr,
    ) -> Result<Intermediate, GqliteError> {
        let n = match eval_literal(count)? {
            Value::Int(i) => i.max(0) as usize,
            _ => {
                return Err(GqliteError::Execution(
                    "LIMIT requires an integer".into(),
                ))
            }
        };

        let rows = input.rows.into_iter().take(n).collect();
        Ok(Intermediate {
            columns: input.columns,
            types: input.types,
            rows,
        })
    }

    // ── Skip (Task 041) ────────────────────────────────────────

    pub(crate) fn exec_skip(
        &self,
        input: Intermediate,
        count: &Expr,
    ) -> Result<Intermediate, GqliteError> {
        let n = match eval_literal(count)? {
            Value::Int(i) => i.max(0) as usize,
            _ => {
                return Err(GqliteError::Execution(
                    "SKIP requires an integer".into(),
                ))
            }
        };

        let rows = input.rows.into_iter().skip(n).collect();
        Ok(Intermediate {
            columns: input.columns,
            types: input.types,
            rows,
        })
    }

    // ── Aggregate (Task 040) ───────────────────────────────────

    pub(crate) fn exec_aggregate(
        &self,
        input: Intermediate,
        expressions: &[(Expr, Option<String>)],
    ) -> Result<Intermediate, GqliteError> {
        use std::collections::HashMap;

        // Separate group-by expressions from aggregate function calls
        let mut group_indices: Vec<usize> = Vec::new();
        let mut agg_indices: Vec<usize> = Vec::new();

        for (i, (expr, _)) in expressions.iter().enumerate() {
            if is_aggregate_call(expr) {
                agg_indices.push(i);
            } else {
                group_indices.push(i);
            }
        }

        // Group rows by key values
        let mut groups: HashMap<Vec<Value>, Vec<Vec<Value>>> = HashMap::new();
        for row in &input.rows {
            let key: Vec<Value> = group_indices
                .iter()
                .map(|&i| self.eval_expr(&expressions[i].0, &input.columns, row))
                .collect::<Result<Vec<_>, _>>()?;
            groups.entry(key).or_default().push(row.clone());
        }

        // If no groups and no group keys, return one row with aggregate defaults
        if groups.is_empty() && group_indices.is_empty() {
            groups.insert(Vec::new(), Vec::new());
        }

        // Build output column names and types
        let mut col_names = Vec::new();
        let mut col_types = Vec::new();
        for (expr, alias) in expressions {
            col_names.push(alias.clone().unwrap_or_else(|| expr_display_name(expr)));
            col_types.push(DataType::String); // placeholder, inferred below
        }

        let mut out_rows = Vec::new();
        for (key, group_rows) in &groups {
            let mut out_row = Vec::new();
            let mut group_key_idx = 0;

            for (i, (expr, _)) in expressions.iter().enumerate() {
                if group_indices.contains(&i) {
                    out_row.push(key[group_key_idx].clone());
                    group_key_idx += 1;
                } else {
                    let val =
                        self.eval_aggregate(expr, &input.columns, group_rows)?;
                    out_row.push(val);
                }
            }
            out_rows.push(out_row);
        }

        // Infer types from first result row
        if let Some(first) = out_rows.first() {
            for (i, val) in first.iter().enumerate() {
                if i < col_types.len() {
                    if let Some(dt) = val.data_type() {
                        col_types[i] = dt;
                    }
                }
            }
        }

        Ok(Intermediate {
            columns: col_names,
            types: col_types,
            rows: out_rows,
        })
    }

    fn eval_aggregate(
        &self,
        expr: &Expr,
        columns: &[String],
        rows: &[Vec<Value>],
    ) -> Result<Value, GqliteError> {
        match expr {
            Expr::FunctionCall { name, args, .. } => {
                let func_name = name.to_lowercase();
                let is_count_star = func_name == "count"
                    && (args.is_empty()
                        || matches!(args.first(), Some(Expr::Star)));

                let mut accumulator = if is_count_star {
                    Box::new(
                        crate::functions::aggregate::CountAccumulator::new_star(),
                    ) as Box<dyn crate::functions::registry::AggregateAccumulator>
                } else {
                    crate::functions::registry::create_accumulator(&func_name)
                        .ok_or_else(|| {
                            GqliteError::Execution(format!(
                                "unknown aggregate '{}'",
                                name
                            ))
                        })?
                };

                for row in rows {
                    let val = if is_count_star {
                        Value::Int(1)
                    } else if args.is_empty() {
                        Value::Null
                    } else {
                        self.eval_expr(&args[0], columns, row)?
                    };
                    accumulator.accumulate(&val);
                }

                Ok(accumulator.finalize())
            }
            _ => Err(GqliteError::Execution(
                "expected aggregate function call".into(),
            )),
        }
    }

    // ── Union ───────────────────────────────────────────────────

    pub(crate) fn exec_union(
        &self,
        left: Intermediate,
        right: Intermediate,
        all: bool,
    ) -> Result<Intermediate, GqliteError> {
        // Use left's schema as the output schema
        let columns = left.columns;
        let types = left.types;
        let mut rows: Vec<Vec<Value>> = left.rows;
        rows.extend(right.rows);

        if !all {
            // Deduplicate: keep first occurrence
            let mut seen = std::collections::HashSet::new();
            rows.retain(|row| {
                let key: Vec<Value> = row.clone();
                seen.insert(key)
            });
        }

        Ok(Intermediate {
            columns,
            types,
            rows,
        })
    }

    // ── Unwind ──────────────────────────────────────────────────

    pub(crate) fn exec_unwind(
        &self,
        input: Intermediate,
        expr: &Expr,
        alias: &str,
    ) -> Result<Intermediate, GqliteError> {
        let mut out_cols = input.columns.clone();
        let mut out_types = input.types.clone();
        out_cols.push(alias.to_string());
        out_types.push(DataType::String); // generic, will match actual value

        let mut out_rows = Vec::new();

        if input.rows.is_empty() {
            // Standalone UNWIND (no previous MATCH)
            let val = self.eval_expr(expr, &[], &[])?;
            if let Value::List(items) = val {
                for item in items {
                    out_rows.push(vec![item]);
                }
            }
        } else {
            for row in &input.rows {
                let val = self.eval_expr(expr, &input.columns, row)?;
                if let Value::List(items) = val {
                    for item in items {
                        let mut new_row = row.clone();
                        new_row.push(item);
                        out_rows.push(new_row);
                    }
                }
            }
        }

        Ok(Intermediate {
            columns: out_cols,
            types: out_types,
            rows: out_rows,
        })
    }

    // ── Merge (upsert) ─────────────────────────────────────────

    fn exec_merge(
        &self,
        db: &Arc<DatabaseInner>,
        table_name: &str,
        table_id: u32,
        properties: &[(usize, Expr)],
        on_create: &[(usize, Expr)],
        on_match: &[(usize, Expr)],
        txn_id: u64,
    ) -> Result<Intermediate, GqliteError> {
        let catalog = db.catalog.read().unwrap();
        let entry = catalog.get_node_table(table_name).ok_or_else(|| {
            GqliteError::Execution(format!("table '{}' not found", table_name))
        })?;

        let pk_idx = entry.primary_key_idx;
        let num_cols = entry.columns.len();

        // Evaluate property values for the match criteria
        let mut match_props: Vec<(usize, Value)> = Vec::new();
        for (idx, expr) in properties {
            let val = self.eval_expr(expr, &[], &[])?;
            match_props.push((*idx, val));
        }

        drop(catalog);

        let storage = db.storage.read().unwrap();
        let node_table = storage.node_tables.get(&table_id).ok_or_else(|| {
            GqliteError::Execution(format!("storage for table '{}' not found", table_name))
        })?;

        // Scan for existing matching node
        let total_rows = node_table.row_count();
        let mut found_offset = None;
        for offset in 0..total_rows {
            if let Ok(vals) = node_table.read(offset as u64) {
                if vals.get(pk_idx).map_or(true, |v| v.is_null()) {
                    continue; // deleted
                }
                let matches = match_props.iter().all(|(idx, val)| {
                    vals.get(*idx).map_or(false, |v| v == val)
                });
                if matches {
                    found_offset = Some(offset as u64);
                    break;
                }
            }
        }

        drop(storage);

        if let Some(offset) = found_offset {
            // Node exists — apply ON MATCH SET
            if !on_match.is_empty() {
                let mut storage = db.storage.write().unwrap();
                let node_table = storage.node_tables.get_mut(&table_id).unwrap();
                for (idx, expr) in on_match {
                    let val = self.eval_expr(expr, &[], &[])?;
                    node_table.update(offset, *idx, val)?;
                }
            }
        } else {
            // Node does not exist — create it
            let mut values: Vec<Value> = vec![Value::Null; num_cols];
            for (idx, val) in &match_props {
                values[*idx] = val.clone();
            }
            // Apply ON CREATE SET values
            for (idx, expr) in on_create {
                let val = self.eval_expr(expr, &[], &[])?;
                values[*idx] = val;
            }

            // WAL record
            Self::wal_append(
                db,
                txn_id,
                WalPayload::InsertNode {
                    table_name: table_name.to_string(),
                    table_id,
                    values: values.clone(),
                },
            )?;

            let mut storage = db.storage.write().unwrap();
            let node_table = storage.node_tables.get_mut(&table_id).unwrap();
            node_table.insert(&values, txn_id)?;
        }

        Ok(Intermediate::empty())
    }

    // ── Expression evaluator ────────────────────────────────────

    fn eval_expr(
        &self,
        expr: &Expr,
        columns: &[String],
        row: &[Value],
    ) -> Result<Value, GqliteError> {
        match expr {
            Expr::IntLit(i) => Ok(Value::Int(*i)),
            Expr::FloatLit(f) => Ok(Value::Float(*f)),
            Expr::StringLit(s) => Ok(Value::String(s.clone())),
            Expr::BoolLit(b) => Ok(Value::Bool(*b)),
            Expr::NullLit => Ok(Value::Null),

            Expr::Ident(name) => columns
                .iter()
                .position(|c| c == name)
                .map(|idx| row[idx].clone())
                .ok_or_else(|| {
                    GqliteError::Execution(format!("variable '{}' not found", name))
                }),

            Expr::Property(base, field) => {
                let col_name = match base.as_ref() {
                    Expr::Ident(var) => format!("{}.{}", var, field),
                    _ => {
                        return Err(GqliteError::Execution(
                            "invalid property access".into(),
                        ))
                    }
                };
                columns
                    .iter()
                    .position(|c| c == &col_name)
                    .map(|idx| row[idx].clone())
                    .ok_or_else(|| {
                        GqliteError::Execution(format!(
                            "property '{}' not found",
                            col_name
                        ))
                    })
            }

            Expr::BinaryOp { left, op, right } => {
                let lv = self.eval_expr(left, columns, row)?;
                let rv = self.eval_expr(right, columns, row)?;
                eval_binary_op(&lv, op, &rv)
            }

            Expr::UnaryOp { op, expr: inner } => {
                let v = self.eval_expr(inner, columns, row)?;
                match op {
                    UnaryOp::Neg => match v {
                        Value::Int(i) => Ok(Value::Int(-i)),
                        Value::Float(f) => Ok(Value::Float(-f)),
                        _ => Ok(Value::Null),
                    },
                    UnaryOp::Not => match v {
                        Value::Bool(b) => Ok(Value::Bool(!b)),
                        Value::Null => Ok(Value::Null),
                        _ => Ok(Value::Null),
                    },
                }
            }

            Expr::IsNull { expr: inner, negated } => {
                let v = self.eval_expr(inner, columns, row)?;
                let is_null = v.is_null();
                Ok(Value::Bool(if *negated { !is_null } else { is_null }))
            }

            Expr::FunctionCall { name, args, .. } => {
                let mut arg_vals = Vec::new();
                for arg in args {
                    arg_vals.push(self.eval_expr(arg, columns, row)?);
                }
                let registry = crate::functions::registry::FunctionRegistry::new();
                if let Some(func) = registry.get_scalar(name) {
                    func(&arg_vals)
                } else {
                    Err(GqliteError::Execution(format!(
                        "unknown function '{}'",
                        name
                    )))
                }
            }

            Expr::ListLit(items) => {
                let values: Vec<Value> = items
                    .iter()
                    .map(|item| self.eval_expr(item, columns, row))
                    .collect::<Result<_, _>>()?;
                Ok(Value::List(values))
            }

            Expr::Cast { expr: inner, target_type } => {
                let v = self.eval_expr(inner, columns, row)?;
                cast_value(v, target_type)
            }

            Expr::Case { operand, when_clauses, else_result } => {
                match operand {
                    Some(op) => {
                        // Simple form: CASE operand WHEN value THEN result ...
                        let op_val = self.eval_expr(op, columns, row)?;
                        for (when_expr, then_expr) in when_clauses {
                            let when_val = self.eval_expr(when_expr, columns, row)?;
                            let eq = eval_binary_op(&op_val, &BinOp::Eq, &when_val)?;
                            if eq == Value::Bool(true) {
                                return self.eval_expr(then_expr, columns, row);
                            }
                        }
                    }
                    None => {
                        // Searched form: CASE WHEN condition THEN result ...
                        for (cond_expr, then_expr) in when_clauses {
                            let cond_val = self.eval_expr(cond_expr, columns, row)?;
                            if cond_val == Value::Bool(true) {
                                return self.eval_expr(then_expr, columns, row);
                            }
                        }
                    }
                }
                // No WHEN matched — evaluate ELSE or return NULL
                match else_result {
                    Some(el) => self.eval_expr(el, columns, row),
                    None => Ok(Value::Null),
                }
            }

            Expr::Star => Ok(Value::Null),
            Expr::Param(name) => {
                Ok(self.params.get(name).cloned().unwrap_or(Value::Null))
            }

            Expr::In { expr, list, negated } => {
                let val = self.eval_expr(expr, columns, row)?;
                let list_val = self.eval_expr(list, columns, row)?;
                match list_val {
                    Value::List(items) => {
                        let found = items.iter().any(|item| {
                            eval_binary_op(&val, &BinOp::Eq, item)
                                .map(|v| v == Value::Bool(true))
                                .unwrap_or(false)
                        });
                        Ok(Value::Bool(if *negated { !found } else { found }))
                    }
                    Value::Null => Ok(Value::Null),
                    _ => Err(GqliteError::Execution("IN requires a list".into())),
                }
            }
        }
    }
}

impl Default for Engine {
    fn default() -> Self {
        Self::new()
    }
}

impl Engine {
    /// Helper: append a WAL record if the database has a WAL writer.
    fn wal_append(
        db: &Arc<DatabaseInner>,
        txn_id: u64,
        payload: WalPayload,
    ) -> Result<(), GqliteError> {
        let mut wal_guard = db.wal.lock();
        if let Some(wal) = wal_guard.as_mut() {
            wal.append(&WalRecord { txn_id, payload })?;
        }
        Ok(())
    }
}

// ── Free functions ──────────────────────────────────────────────

/// Evaluate a literal expression (no row context).
fn eval_literal(expr: &Expr) -> Result<Value, GqliteError> {
    match expr {
        Expr::IntLit(i) => Ok(Value::Int(*i)),
        Expr::FloatLit(f) => Ok(Value::Float(*f)),
        Expr::StringLit(s) => Ok(Value::String(s.clone())),
        Expr::BoolLit(b) => Ok(Value::Bool(*b)),
        Expr::NullLit => Ok(Value::Null),
        _ => Err(GqliteError::Execution(
            "expression requires row context".into(),
        )),
    }
}

/// Cast a value to a target data type.
fn cast_value(v: Value, target: &DataType) -> Result<Value, GqliteError> {
    if v.is_null() {
        return Ok(Value::Null);
    }
    match target {
        DataType::Int64 | DataType::Serial => match &v {
            Value::Int(_) => Ok(v),
            Value::Float(f) => Ok(Value::Int(*f as i64)),
            Value::Bool(b) => Ok(Value::Int(if *b { 1 } else { 0 })),
            Value::String(s) => s
                .trim()
                .parse::<i64>()
                .map(Value::Int)
                .map_err(|_| GqliteError::Execution(format!("cannot cast '{}' to INT64", s))),
            _ => Err(GqliteError::Execution(format!(
                "cannot cast {} to INT64",
                v
            ))),
        },
        DataType::Double => match &v {
            Value::Float(_) => Ok(v),
            Value::Int(i) => Ok(Value::Float(*i as f64)),
            Value::Bool(b) => Ok(Value::Float(if *b { 1.0 } else { 0.0 })),
            Value::String(s) => s
                .trim()
                .parse::<f64>()
                .map(Value::Float)
                .map_err(|_| GqliteError::Execution(format!("cannot cast '{}' to DOUBLE", s))),
            _ => Err(GqliteError::Execution(format!(
                "cannot cast {} to DOUBLE",
                v
            ))),
        },
        DataType::String => Ok(Value::String(v.to_string())),
        DataType::Bool => match &v {
            Value::Bool(_) => Ok(v),
            Value::Int(i) => Ok(Value::Bool(*i != 0)),
            Value::String(s) => match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Bool(true)),
                "false" | "0" | "no" => Ok(Value::Bool(false)),
                _ => Err(GqliteError::Execution(format!(
                    "cannot cast '{}' to BOOL",
                    s
                ))),
            },
            _ => Err(GqliteError::Execution(format!(
                "cannot cast {} to BOOL",
                v
            ))),
        },
        DataType::InternalId => Err(GqliteError::Execution(
            "cannot cast to InternalId".into(),
        )),
    }
}

/// Evaluate a binary operation on two values.
fn eval_binary_op(left: &Value, op: &BinOp, right: &Value) -> Result<Value, GqliteError> {
    // NULL propagation (with special cases for AND/OR)
    if left.is_null() || right.is_null() {
        return match op {
            BinOp::And => match (left, right) {
                (Value::Bool(false), _) | (_, Value::Bool(false)) => Ok(Value::Bool(false)),
                _ => Ok(Value::Null),
            },
            BinOp::Or => match (left, right) {
                (Value::Bool(true), _) | (_, Value::Bool(true)) => Ok(Value::Bool(true)),
                _ => Ok(Value::Null),
            },
            _ => Ok(Value::Null),
        };
    }

    match op {
        BinOp::Add => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a + b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a + b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 + b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a + *b as f64)),
            (Value::String(a), Value::String(b)) => {
                Ok(Value::String(format!("{}{}", a, b)))
            }
            _ => Ok(Value::Null),
        },
        BinOp::Sub => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a - b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a - b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 - b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a - *b as f64)),
            _ => Ok(Value::Null),
        },
        BinOp::Mul => match (left, right) {
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a * b)),
            (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a * b)),
            (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 * b)),
            (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a * *b as f64)),
            _ => Ok(Value::Null),
        },
        BinOp::Div => {
            match (left, right) {
                (Value::Int(_), Value::Int(0)) => {
                    Err(GqliteError::Execution("division by zero".into()))
                }
                (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a / b)),
                (Value::Float(a), Value::Float(b)) if *b == 0.0 => {
                    Err(GqliteError::Execution("division by zero".into()))
                }
                (Value::Float(a), Value::Float(b)) => Ok(Value::Float(a / b)),
                (Value::Int(a), Value::Float(b)) if *b == 0.0 => {
                    Err(GqliteError::Execution("division by zero".into()))
                }
                (Value::Int(a), Value::Float(b)) => Ok(Value::Float(*a as f64 / b)),
                (Value::Float(_), Value::Int(0)) => {
                    Err(GqliteError::Execution("division by zero".into()))
                }
                (Value::Float(a), Value::Int(b)) => Ok(Value::Float(a / *b as f64)),
                _ => Ok(Value::Null),
            }
        }
        BinOp::Mod => match (left, right) {
            (Value::Int(_), Value::Int(0)) => {
                Err(GqliteError::Execution("modulo by zero".into()))
            }
            (Value::Int(a), Value::Int(b)) => Ok(Value::Int(a % b)),
            _ => Ok(Value::Null),
        },
        BinOp::Eq => Ok(Value::Bool(left == right)),
        BinOp::Neq => Ok(Value::Bool(left != right)),
        BinOp::Lt => Ok(Value::Bool(
            compare_values(left, right) == Some(Ordering::Less),
        )),
        BinOp::Gt => Ok(Value::Bool(
            compare_values(left, right) == Some(Ordering::Greater),
        )),
        BinOp::Le => Ok(Value::Bool(matches!(
            compare_values(left, right),
            Some(Ordering::Less | Ordering::Equal)
        ))),
        BinOp::Ge => Ok(Value::Bool(matches!(
            compare_values(left, right),
            Some(Ordering::Greater | Ordering::Equal)
        ))),
        BinOp::And => match (left, right) {
            (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a && *b)),
            _ => Ok(Value::Null),
        },
        BinOp::Or => match (left, right) {
            (Value::Bool(a), Value::Bool(b)) => Ok(Value::Bool(*a || *b)),
            _ => Ok(Value::Null),
        },
    }
}

/// Compare two values, returning their ordering if comparable.
fn compare_values(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a.partial_cmp(b),
        (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
        (Value::Int(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
        (Value::Float(a), Value::Int(b)) => a.partial_cmp(&(*b as f64)),
        (Value::String(a), Value::String(b)) => Some(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => a.partial_cmp(b),
        _ => None,
    }
}

/// Check if an expression is a top-level aggregate function call.
fn is_aggregate_call(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } => matches!(
            name.to_lowercase().as_str(),
            "count" | "sum" | "avg" | "min" | "max" | "collect"
        ),
        _ => false,
    }
}

/// Generate a display name for a projection expression.
fn expr_display_name(expr: &Expr) -> String {
    match expr {
        Expr::Ident(name) => name.clone(),
        Expr::Property(base, field) => match base.as_ref() {
            Expr::Ident(var) => format!("{}.{}", var, field),
            _ => field.clone(),
        },
        Expr::FunctionCall { name, .. } => name.clone(),
        Expr::Star => "*".to_string(),
        _ => "?column?".to_string(),
    }
}

/// Parse a CSV line into fields, handling quoted fields.
fn parse_csv_line(line: &str, delimiter: char) -> Vec<String> {
    let mut fields = Vec::new();
    let mut current = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();

    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    // Escaped quote
                    current.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                current.push(c);
            }
        } else if c == '"' {
            in_quotes = true;
        } else if c == delimiter {
            fields.push(current.clone());
            current.clear();
        } else {
            current.push(c);
        }
    }
    fields.push(current);
    fields
}

/// Parse a CSV field string into a typed Value.
fn parse_csv_value(field: &str, dt: &DataType) -> Result<Value, GqliteError> {
    match dt {
        DataType::Bool => match field.to_lowercase().as_str() {
            "true" | "1" | "t" | "yes" => Ok(Value::Bool(true)),
            "false" | "0" | "f" | "no" => Ok(Value::Bool(false)),
            _ => Err(GqliteError::Other(format!(
                "cannot parse '{}' as BOOL",
                field
            ))),
        },
        DataType::Int64 | DataType::Serial => {
            let i: i64 = field.parse().map_err(|_| {
                GqliteError::Other(format!("cannot parse '{}' as INT64", field))
            })?;
            Ok(Value::Int(i))
        }
        DataType::Double => {
            let f: f64 = field.parse().map_err(|_| {
                GqliteError::Other(format!("cannot parse '{}' as DOUBLE", field))
            })?;
            Ok(Value::Float(f))
        }
        DataType::String => Ok(Value::String(field.to_string())),
        DataType::InternalId => Err(GqliteError::Other(
            "cannot import InternalId from CSV".into(),
        )),
    }
}

/// Convert a Value to its CSV string representation.
fn value_to_csv_string(v: &Value) -> String {
    match v {
        Value::Null => String::new(),
        Value::Bool(b) => b.to_string(),
        Value::Int(i) => i.to_string(),
        Value::Float(f) => f.to_string(),
        Value::String(s) => s.clone(),
        Value::InternalId(id) => format!("{}:{}", id.table_id, id.offset),
        Value::List(items) => {
            let parts: Vec<String> = items.iter().map(|v| value_to_csv_string(v)).collect();
            format!("[{}]", parts.join(","))
        }
        Value::Bytes(b) => format!("0x{}", b.iter().map(|byte| format!("{:02x}", byte)).collect::<String>()),
    }
}

