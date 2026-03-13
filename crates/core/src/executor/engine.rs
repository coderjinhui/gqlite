//! Execution engine: interprets a physical plan against storage to produce results.

use std::cmp::Ordering;
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
struct Intermediate {
    columns: Vec<String>,
    types: Vec<DataType>,
    rows: Vec<Vec<Value>>,
}

impl Intermediate {
    fn empty() -> Self {
        Self {
            columns: Vec::new(),
            types: Vec::new(),
            rows: Vec::new(),
        }
    }

    fn into_query_result(self) -> QueryResult {
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
pub struct Engine;

impl Engine {
    pub fn new() -> Self {
        Self
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

    // ── Recursive operator execution ────────────────────────────

    fn execute_operator(
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
            } => self.exec_seq_scan(db, table_name, *table_id, alias),

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

            // DDL handled in execute_plan
            PhysicalPlan::CreateNodeTable { .. }
            | PhysicalPlan::CreateRelTable { .. }
            | PhysicalPlan::DropTable { .. }
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

        let mut rows = Vec::new();
        for (offset, values) in node_table.scan() {
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

    fn exec_filter(
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

    fn exec_projection(
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

    fn exec_expand(
        &self,
        db: &Arc<DatabaseInner>,
        input: Intermediate,
        rel_table_name: &str,
        rel_table_id: u32,
        direction: &Direction,
        src_alias: &str,
        dst_alias: &str,
        dst_table_id: &Option<u32>,
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

                out_rows.push(new_row);
            }
        }

        Ok(Intermediate {
            columns: out_cols,
            types: out_types,
            rows: out_rows,
        })
    }

    // ── Cross Join (Task 026 — simplified) ──────────────────────

    fn exec_cross_join(
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
        drop(catalog);

        let mut row = vec![Value::Null; num_cols];
        for (col_idx, expr) in values {
            let val = eval_literal(expr)?;
            if *col_idx < num_cols {
                row[*col_idx] = val;
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
        node_table.insert(&row)?;

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
                    node_table.delete(id.offset)?;
                }
            }
        }

        Ok(Intermediate::empty())
    }

    // ── OrderBy (Task 041) ─────────────────────────────────────

    fn exec_order_by(
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

    fn exec_limit(
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

    fn exec_skip(
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

    fn exec_aggregate(
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

            Expr::Star | Expr::Param(_) => Ok(Value::Null),
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

#[cfg(test)]
mod tests {
    use crate::Database;

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
}
