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
    pub(crate) fn execute_plan(
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

            Expr::Star => Ok(Value::Null),
            Expr::Param(name) => {
                Ok(self.params.get(name).cloned().unwrap_or(Value::Null))
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::Database;
    use crate::types::value::Value;
    use super::Engine;

    #[test]
    fn ddl_create_and_drop() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        let result = db
            .execute("CREATE NODE TABLE Movie (id INT64, title STRING, PRIMARY KEY (id))")
            .unwrap();
        assert!(result.is_empty());

        db.execute("DROP TABLE Movie").unwrap();
    }

    #[test]
    fn insert_and_scan() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})")
            .unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result.num_rows(), 2);
        let names: Vec<&str> = result
            .rows()
            .iter()
            .map(|r| r.get_string(0).unwrap())
            .collect();
        assert!(names.contains(&"Alice"));
        assert!(names.contains(&"Bob"));
    }

    #[test]
    fn filter_predicate() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob', age: 25})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 3, name: 'Charlie', age: 35})")
            .unwrap();

        let result = db
            .query("MATCH (n:Person) WHERE n.age > 28 RETURN n.name")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn return_all() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
            .unwrap();

        let result = db.query("MATCH (n:Person) RETURN *").unwrap();
        assert_eq!(result.num_rows(), 1);
        // Should include all columns: n, n.id, n.name
        assert!(result.column_names().len() >= 3);
    }

    #[test]
    fn relationship_expand() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE REL TABLE KNOWS (FROM Person TO Person)")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})")
            .unwrap();

        // Create relationship
        db.execute(
            "MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[r:KNOWS]->(b)",
        )
        .unwrap();

        // Query relationships
        let result = db
            .query("MATCH (a:Person)-[r:KNOWS]->(b:Person) RETURN a.name, b.name")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0), Some("Alice"));
        assert_eq!(result.rows()[0].get_string(1), Some("Bob"));
    }

    #[test]
    fn set_property() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
            .unwrap();

        db.execute("MATCH (n:Person) WHERE n.id = 1 SET n.name = 'Updated'")
            .unwrap();

        let result = db
            .query("MATCH (n:Person) WHERE n.id = 1 RETURN n.name")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0), Some("Updated"));
    }

    #[test]
    fn delete_node() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})")
            .unwrap();

        db.execute("MATCH (n:Person) WHERE n.id = 1 DELETE n")
            .unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0), Some("Bob"));
    }

    #[test]
    fn expression_arithmetic() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Num (id INT64, val INT64, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Num {id: 1, val: 10})").unwrap();

        let result = db
            .query("MATCH (n:Num) RETURN n.val + 5")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_int(0), Some(15));
    }

    #[test]
    fn scalar_function_in_projection() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})")
            .unwrap();

        let result = db
            .query("MATCH (n:Person) RETURN upper(n.name)")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0), Some("ALICE"));
    }

    // ── ORDER BY tests (Task 041) ──────────────────────────────

    fn setup_persons(db: &Database) {
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob', age: 25})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 3, name: 'Charlie', age: 35})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 4, name: 'Diana', age: 28})")
            .unwrap();
    }

    #[test]
    fn order_by_asc() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN n.name ORDER BY n.age")
            .unwrap();
        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.rows()[0].get_string(0), Some("Bob"));
        assert_eq!(result.rows()[1].get_string(0), Some("Diana"));
        assert_eq!(result.rows()[2].get_string(0), Some("Alice"));
        assert_eq!(result.rows()[3].get_string(0), Some("Charlie"));
    }

    #[test]
    fn order_by_desc() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN n.name ORDER BY n.age DESC")
            .unwrap();
        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.rows()[0].get_string(0), Some("Charlie"));
        assert_eq!(result.rows()[1].get_string(0), Some("Alice"));
        assert_eq!(result.rows()[2].get_string(0), Some("Diana"));
        assert_eq!(result.rows()[3].get_string(0), Some("Bob"));
    }

    #[test]
    fn order_by_string() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN n.name ORDER BY n.name")
            .unwrap();
        assert_eq!(result.num_rows(), 4);
        assert_eq!(result.rows()[0].get_string(0), Some("Alice"));
        assert_eq!(result.rows()[1].get_string(0), Some("Bob"));
        assert_eq!(result.rows()[2].get_string(0), Some("Charlie"));
        assert_eq!(result.rows()[3].get_string(0), Some("Diana"));
    }

    // ── LIMIT tests (Task 041) ─────────────────────────────────

    #[test]
    fn limit_results() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN n.name LIMIT 2")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn limit_larger_than_result() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN n.name LIMIT 100")
            .unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    // ── SKIP tests (Task 041) ──────────────────────────────────

    #[test]
    fn skip_results() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 2")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.rows()[0].get_string(0), Some("Alice"));
        assert_eq!(result.rows()[1].get_string(0), Some("Charlie"));
    }

    #[test]
    fn skip_and_limit() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN n.name ORDER BY n.age SKIP 1 LIMIT 2")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.rows()[0].get_string(0), Some("Diana"));
        assert_eq!(result.rows()[1].get_string(0), Some("Alice"));
    }

    // ── Aggregate tests (Task 040) ─────────────────────────────

    #[test]
    fn count_star() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN count(*)")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_int(0), Some(4));
    }

    #[test]
    fn count_expression() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN count(n)")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_int(0), Some(4));
    }

    #[test]
    fn sum_and_avg() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN sum(n.age), avg(n.age)")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        // sum = 30 + 25 + 35 + 28 = 118
        assert_eq!(result.rows()[0].get_int(0), Some(118));
        // avg = 118 / 4 = 29.5
        assert_eq!(result.rows()[0].get_float(1), Some(29.5));
    }

    #[test]
    fn min_and_max() {
        let db = Database::in_memory();
        setup_persons(&db);

        let result = db
            .query("MATCH (n:Person) RETURN min(n.age), max(n.age)")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_int(0), Some(25));
        assert_eq!(result.rows()[0].get_int(1), Some(35));
    }

    #[test]
    fn group_by_with_count() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, city STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice', city: 'NYC'})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob', city: 'LA'})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 3, name: 'Charlie', city: 'NYC'})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 4, name: 'Diana', city: 'LA'})")
            .unwrap();
        db.execute("CREATE (n:Person {id: 5, name: 'Eve', city: 'NYC'})")
            .unwrap();

        let result = db
            .query("MATCH (n:Person) RETURN n.city, count(n)")
            .unwrap();
        assert_eq!(result.num_rows(), 2);

        // Find which row is NYC and which is LA
        let rows = result.rows();
        for row in rows {
            let city = row.get_string(0).unwrap();
            let count = row.get_int(1).unwrap();
            match city {
                "NYC" => assert_eq!(count, 3),
                "LA" => assert_eq!(count, 2),
                _ => panic!("unexpected city: {}", city),
            }
        }
    }

    #[test]
    fn collect_aggregate() {
        let db = Database::in_memory();
        db.execute(
            "CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))",
        )
        .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

        let result = db
            .query("MATCH (n:Person) RETURN collect(n.name)")
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        // The result should be a list
        let val = &result.rows()[0].values[0];
        match val {
            crate::types::value::Value::List(items) => {
                assert_eq!(items.len(), 2);
            }
            _ => panic!("expected List, got {:?}", val),
        }
    }

    // ── OPTIONAL MATCH / UNION / UNWIND / MERGE tests (Plan 038/039) ──

    #[test]
    fn optional_match_with_no_relationship() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE REL TABLE Knows (FROM Person TO Person)")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();
        // Only Alice knows Bob, not vice versa
        db.execute("MATCH (a:Person), (b:Person) WHERE a.id = 1 AND b.id = 2 CREATE (a)-[:Knows]->(b)")
            .unwrap();

        // OPTIONAL MATCH: Bob has no outgoing KNOWS, should still appear with NULLs
        let result = db
            .query("MATCH (a:Person) OPTIONAL MATCH (a)-[:Knows]->(b:Person) RETURN a.name, b.name ORDER BY a.name")
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        // Alice -> Bob
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
        assert_eq!(result.rows()[0].get_string(1).unwrap(), "Bob");
        // Bob -> NULL
        assert_eq!(result.rows()[1].get_string(0).unwrap(), "Bob");
        assert!(result.rows()[1].values[1].is_null());
    }

    #[test]
    fn union_all_combines_results() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

        let result = db
            .query("MATCH (a:Person) RETURN a.name UNION ALL MATCH (b:Person) RETURN b.name")
            .unwrap();
        // 2 + 2 = 4 rows (duplicates preserved)
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn union_distinct_deduplicates() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

        let result = db
            .query("MATCH (a:Person) RETURN a.name UNION MATCH (b:Person) RETURN b.name")
            .unwrap();
        // Deduplicated: 2 unique names
        assert_eq!(result.num_rows(), 2);
    }

    #[test]
    fn unwind_list_literal() {
        let db = Database::in_memory();
        let result = db.query("UNWIND [1, 2, 3] AS x RETURN x").unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
        assert_eq!(result.rows()[1].get_int(0).unwrap(), 2);
        assert_eq!(result.rows()[2].get_int(0).unwrap(), 3);
    }

    #[test]
    fn merge_creates_when_not_exists() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();

        db.execute("MERGE (n:Person {id: 1, name: 'Alice'}) ON CREATE SET n.age = 25")
            .unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.name, n.age").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
        assert_eq!(result.rows()[0].get_int(1).unwrap(), 25);
    }

    #[test]
    fn merge_matches_when_exists() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 25})").unwrap();

        db.execute("MERGE (n:Person {id: 1, name: 'Alice'}) ON MATCH SET n.age = 30")
            .unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.name, n.age").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
        assert_eq!(result.rows()[0].get_int(1).unwrap(), 30);
    }

    #[test]
    fn serial_auto_increment() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id SERIAL, name STRING, PRIMARY KEY (id))")
            .unwrap();

        db.execute("CREATE (n:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();
        db.execute("CREATE (n:Person {name: 'Charlie'})").unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.id, n.name ORDER BY n.id ASC").unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.rows()[0].get_int(0).unwrap(), 0);
        assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
        assert_eq!(result.rows()[1].get_int(0).unwrap(), 1);
        assert_eq!(result.rows()[1].get_string(1).unwrap(), "Bob");
        assert_eq!(result.rows()[2].get_int(0).unwrap(), 2);
        assert_eq!(result.rows()[2].get_string(1).unwrap(), "Charlie");
    }

    #[test]
    fn serial_with_explicit_value() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id SERIAL, name STRING, PRIMARY KEY (id))")
            .unwrap();

        // Explicitly provide id — should use the provided value
        db.execute("CREATE (n:Person {id: 100, name: 'Alice'})").unwrap();
        // Next auto should still start from counter (0), not from 100
        db.execute("CREATE (n:Person {name: 'Bob'})").unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.id, n.name ORDER BY n.id ASC").unwrap();
        assert_eq!(result.num_rows(), 2);
        // Bob gets id=0 (auto), Alice has id=100 (explicit)
        assert_eq!(result.rows()[0].get_int(0).unwrap(), 0);
        assert_eq!(result.rows()[0].get_string(1).unwrap(), "Bob");
        assert_eq!(result.rows()[1].get_int(0).unwrap(), 100);
        assert_eq!(result.rows()[1].get_string(1).unwrap(), "Alice");
    }

    #[test]
    fn alter_table_add_column() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();

        // Add a new column
        db.execute("ALTER TABLE Person ADD age INT64").unwrap();

        // New column should be NULL for existing rows
        let result = db.query("MATCH (n:Person) RETURN n.id, n.name, n.age").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
        assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
        assert!(result.rows()[0].values[2].is_null());
    }

    #[test]
    fn alter_table_drop_column() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice', age: 30})").unwrap();

        // Drop the age column
        db.execute("ALTER TABLE Person DROP COLUMN age").unwrap();

        // Should still be able to query remaining columns
        let result = db.query("MATCH (n:Person) RETURN n.id, n.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
        assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
    }

    #[test]
    fn alter_table_rename_table() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();

        db.execute("ALTER TABLE Person RENAME TO People").unwrap();

        // Old name should fail
        let result = db.query("MATCH (n:Person) RETURN n.name");
        assert!(result.is_err());

        // New name should work
        let result = db.query("MATCH (n:People) RETURN n.name").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    }

    #[test]
    fn alter_table_rename_column() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();

        db.execute("ALTER TABLE Person RENAME COLUMN name TO fullname").unwrap();

        // Old column name should not return data
        let result = db.query("MATCH (n:Person) RETURN n.fullname").unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");
    }

    #[test]
    fn alter_table_drop_pk_column_fails() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        let result = db.execute("ALTER TABLE Person DROP COLUMN id");
        assert!(result.is_err());
    }

    #[test]
    fn copy_from_csv() {
        use std::io::Write;

        let dir = std::env::temp_dir().join("gqlite_test_csv");
        std::fs::create_dir_all(&dir).ok();
        let csv_path = dir.join("persons.csv");

        // Write a test CSV file
        {
            let mut f = std::fs::File::create(&csv_path).unwrap();
            writeln!(f, "id,name,age").unwrap();
            writeln!(f, "1,Alice,30").unwrap();
            writeln!(f, "2,Bob,25").unwrap();
            writeln!(f, "3,Charlie,35").unwrap();
        }

        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();

        let csv_str = csv_path.to_str().unwrap();
        db.execute(&format!("COPY Person FROM '{}'", csv_str)).unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.id, n.name, n.age ORDER BY n.id ASC").unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
        assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");
        assert_eq!(result.rows()[0].get_int(2).unwrap(), 30);
        assert_eq!(result.rows()[2].get_int(0).unwrap(), 3);
        assert_eq!(result.rows()[2].get_string(1).unwrap(), "Charlie");

        std::fs::remove_file(&csv_path).ok();
    }

    #[test]
    fn copy_to_csv_table() {
        let dir = std::env::temp_dir().join("gqlite_test_csv");
        std::fs::create_dir_all(&dir).ok();
        let csv_path = dir.join("export.csv");

        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

        let csv_str = csv_path.to_str().unwrap();
        db.execute(&format!("COPY Person TO '{}'", csv_str)).unwrap();

        let content = std::fs::read_to_string(&csv_path).unwrap();
        let lines: Vec<&str> = content.trim().lines().collect();
        assert_eq!(lines[0], "id,name");
        assert!(lines.len() >= 3); // header + 2 rows

        std::fs::remove_file(&csv_path).ok();
    }

    #[test]
    fn copy_from_csv_with_nulls() {
        use std::io::Write;

        let dir = std::env::temp_dir().join("gqlite_test_csv");
        std::fs::create_dir_all(&dir).ok();
        let csv_path = dir.join("nulls.csv");

        {
            let mut f = std::fs::File::create(&csv_path).unwrap();
            writeln!(f, "id,name,age").unwrap();
            writeln!(f, "1,Alice,30").unwrap();
            writeln!(f, "2,,NULL").unwrap(); // empty name, NULL age
        }

        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))")
            .unwrap();

        let csv_str = csv_path.to_str().unwrap();
        db.execute(&format!("COPY Person FROM '{}'", csv_str)).unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.id, n.name, n.age ORDER BY n.id ASC").unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.rows()[1].get_int(0).unwrap(), 2);
        assert!(result.rows()[1].values[1].is_null()); // empty name → NULL
        assert!(result.rows()[1].values[2].is_null()); // "NULL" → NULL

        std::fs::remove_file(&csv_path).ok();
    }

    #[test]
    fn copy_from_tsv() {
        use std::io::Write;

        let dir = std::env::temp_dir().join("gqlite_test_csv");
        std::fs::create_dir_all(&dir).ok();
        let tsv_path = dir.join("persons.tsv");

        {
            let mut f = std::fs::File::create(&tsv_path).unwrap();
            writeln!(f, "id\tname").unwrap();
            writeln!(f, "1\tAlice").unwrap();
            writeln!(f, "2\tBob").unwrap();
        }

        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))")
            .unwrap();

        let tsv_str = tsv_path.to_str().unwrap();
        db.execute(&format!("COPY Person FROM '{}' (DELIMITER '\t')", tsv_str)).unwrap();

        let result = db.query("MATCH (n:Person) RETURN n.id, n.name ORDER BY n.id ASC").unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.rows()[0].get_int(0).unwrap(), 1);
        assert_eq!(result.rows()[0].get_string(1).unwrap(), "Alice");

        std::fs::remove_file(&tsv_path).ok();
    }

    #[test]
    fn prepared_statement_with_params() {
        let db = crate::Database::in_memory();
        db.execute("CREATE NODE TABLE Person (id INT64, name STRING, PRIMARY KEY (id))").unwrap();
        db.execute("CREATE (n:Person {id: 1, name: 'Alice'})").unwrap();
        db.execute("CREATE (n:Person {id: 2, name: 'Bob'})").unwrap();

        let conn = db.connect();
        let stmt = conn.prepare("MATCH (n:Person) WHERE n.id = $id RETURN n.name").unwrap();

        let mut params = std::collections::HashMap::new();
        params.insert("id".to_string(), Value::Int(1));
        let result = stmt.execute(params).unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "Alice");

        let mut params2 = std::collections::HashMap::new();
        params2.insert("id".to_string(), Value::Int(2));
        let result2 = stmt.execute(params2).unwrap();
        assert_eq!(result2.num_rows(), 1);
        assert_eq!(result2.rows()[0].get_string(0).unwrap(), "Bob");
    }

    #[test]
    fn recursive_expand_variable_length() {
        // Build a chain: A -> B -> C -> D
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (name STRING, PRIMARY KEY(name))")
            .unwrap();
        db.execute("CREATE REL TABLE KNOWS (FROM Person TO Person)")
            .unwrap();
        db.execute("CREATE (p:Person {name: 'A'})").unwrap();
        db.execute("CREATE (p:Person {name: 'B'})").unwrap();
        db.execute("CREATE (p:Person {name: 'C'})").unwrap();
        db.execute("CREATE (p:Person {name: 'D'})").unwrap();
        db.execute(
            "MATCH (a:Person), (b:Person) WHERE a.name = 'A' AND b.name = 'B' \
             CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();
        db.execute(
            "MATCH (a:Person), (b:Person) WHERE a.name = 'B' AND b.name = 'C' \
             CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();
        db.execute(
            "MATCH (a:Person), (b:Person) WHERE a.name = 'C' AND b.name = 'D' \
             CREATE (a)-[:KNOWS]->(b)",
        )
        .unwrap();

        // 1 hop from A: should get B
        let result = db
            .execute(
                "MATCH (a:Person)-[:KNOWS*1..1]->(b:Person) \
                 WHERE a.name = 'A' RETURN b.name",
            )
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "B");

        // 1..2 hops from A: should get B, C
        let result = db
            .execute(
                "MATCH (a:Person)-[:KNOWS*1..2]->(b:Person) \
                 WHERE a.name = 'A' RETURN b.name ORDER BY b.name",
            )
            .unwrap();
        assert_eq!(result.num_rows(), 2);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "B");
        assert_eq!(result.rows()[1].get_string(0).unwrap(), "C");

        // 1..3 hops from A: should get B, C, D
        let result = db
            .execute(
                "MATCH (a:Person)-[:KNOWS*1..3]->(b:Person) \
                 WHERE a.name = 'A' RETURN b.name ORDER BY b.name",
            )
            .unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "B");
        assert_eq!(result.rows()[1].get_string(0).unwrap(), "C");
        assert_eq!(result.rows()[2].get_string(0).unwrap(), "D");

        // Exactly 2 hops from A: should get C only
        let result = db
            .execute(
                "MATCH (a:Person)-[:KNOWS*2..2]->(b:Person) \
                 WHERE a.name = 'A' RETURN b.name",
            )
            .unwrap();
        assert_eq!(result.num_rows(), 1);
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "C");
    }

    // ── MVCC integration tests ──────────────────────────────────

    #[test]
    fn mvcc_write_invisible_to_concurrent_read() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Item (id INT64, val STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE (i:Item {id: 1, val: 'original'})").unwrap();

        // Phase 1: verify the row exists
        let r1 = db.execute("MATCH (i:Item) RETURN i.val").unwrap();
        assert_eq!(r1.num_rows(), 1);

        // Phase 2: use low-level API to test snapshot isolation
        // Start a read-only transaction (captures snapshot BEFORE the write)
        let db_inner = db.inner.clone();
        let mut read_txn = db_inner.txn_manager.begin_read_only();
        let read_start_ts = read_txn.start_ts;

        // Now do a write that inserts a new row
        db.execute("CREATE (i:Item {id: 2, val: 'new_item'})").unwrap();

        // The read transaction should NOT see the new row
        let engine = Engine::with_snapshot(read_start_ts, HashMap::new());
        let physical = {
            let catalog = db_inner.catalog.read().unwrap();
            let mut binder = crate::binder::Binder::new(&catalog);
            let stmt = crate::parser::parser::Parser::parse_query(
                "MATCH (i:Item) RETURN i.val"
            ).unwrap();
            let bound = binder.bind(&stmt).unwrap();
            let planner = crate::planner::logical::Planner::new(&catalog);
            let logical = planner.plan(&bound).unwrap();
            let logical = crate::planner::optimizer::optimize(logical);
            crate::planner::physical::to_physical(&logical)
        };
        let result = engine.execute_plan_parallel(&physical, &db_inner, read_txn.id).unwrap();
        assert_eq!(result.num_rows(), 1, "concurrent read should only see 1 row");
        assert_eq!(result.rows()[0].get_string(0).unwrap(), "original");

        db_inner.txn_manager.commit(&mut read_txn);

        // Phase 3: a NEW read after commit should see both rows
        let r3 = db.execute("MATCH (i:Item) RETURN i.val ORDER BY i.val").unwrap();
        assert_eq!(r3.num_rows(), 2);
    }

    #[test]
    fn mvcc_write_visible_after_commit() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Thing (id INT64, name STRING, PRIMARY KEY(id))")
            .unwrap();

        // Before any data insert, read sees nothing
        let r1 = db.execute("MATCH (t:Thing) RETURN t.name").unwrap();
        assert_eq!(r1.num_rows(), 0);

        // Write and commit
        db.execute("CREATE (t:Thing {id: 1, name: 'alpha'})").unwrap();

        // New read after commit sees the data
        let r2 = db.execute("MATCH (t:Thing) RETURN t.name").unwrap();
        assert_eq!(r2.num_rows(), 1);
        assert_eq!(r2.rows()[0].get_string(0).unwrap(), "alpha");
    }

    #[test]
    fn mvcc_delete_invisible_to_concurrent_read() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Item (id INT64, val STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE (i:Item {id: 1, val: 'keep'})").unwrap();
        db.execute("CREATE (i:Item {id: 2, val: 'delete_me'})").unwrap();

        // Start a read-only transaction (captures snapshot)
        let db_inner = db.inner.clone();
        let mut read_txn = db_inner.txn_manager.begin_read_only();
        let read_start_ts = read_txn.start_ts;

        // Delete a row in a write transaction
        db.execute("MATCH (i:Item) WHERE i.id = 2 DELETE i").unwrap();

        // The read transaction should still see both rows (delete happened after snapshot)
        let engine = Engine::with_snapshot(read_start_ts, HashMap::new());
        let physical = {
            let catalog = db_inner.catalog.read().unwrap();
            let mut binder = crate::binder::Binder::new(&catalog);
            let stmt = crate::parser::parser::Parser::parse_query(
                "MATCH (i:Item) RETURN i.val ORDER BY i.val"
            ).unwrap();
            let bound = binder.bind(&stmt).unwrap();
            let planner = crate::planner::logical::Planner::new(&catalog);
            let logical = planner.plan(&bound).unwrap();
            let logical = crate::planner::optimizer::optimize(logical);
            crate::planner::physical::to_physical(&logical)
        };
        let result = engine.execute_plan_parallel(&physical, &db_inner, read_txn.id).unwrap();
        assert_eq!(result.num_rows(), 2, "concurrent read should still see 2 rows");
        db_inner.txn_manager.commit(&mut read_txn);

        // After committing the read txn, new reads should see only 1 row
        let r3 = db.execute("MATCH (i:Item) RETURN i.val").unwrap();
        assert_eq!(r3.num_rows(), 1);
        assert_eq!(r3.rows()[0].get_string(0).unwrap(), "keep");
    }

    #[test]
    fn mvcc_gc_old_versions() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Item (id INT64, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE (i:Item {id: 1})").unwrap();
        db.execute("CREATE (i:Item {id: 2})").unwrap();

        // Delete id=1
        db.execute("MATCH (i:Item) WHERE i.id = 1 DELETE i").unwrap();

        // Run GC with a safe timestamp beyond all transactions
        let last_committed = db.inner.txn_manager.last_committed_id();
        let mut storage = db.inner.storage.write().unwrap();
        let mut total_purged = 0u64;
        for nt in storage.node_tables.values_mut() {
            total_purged += nt.gc(last_committed + 1);
        }
        assert!(total_purged > 0, "GC should purge at least one deleted row");
    }
}
