//! Physical plan operators, mapping 1:1 to executable steps.
//!
//! The physical plan specifies concrete algorithms (e.g., sequential scan,
//! hash join) that the execution engine will run.

use crate::parser::ast::{Direction, Expr};
use crate::planner::logical::{BoundSetItem, JoinKey};
use crate::types::data_type::DataType;

/// A physical plan operator tree.
#[derive(Debug, Clone)]
pub enum PhysicalPlan {
    /// Sequential scan over stored nodes.
    SeqScan {
        table_name: String,
        table_id: u32,
        columns: Vec<usize>,
        alias: String,
    },

    /// Expand from source nodes to neighbors via relationships.
    CsrExpand {
        input: Box<PhysicalPlan>,
        rel_table_name: String,
        rel_table_id: u32,
        direction: Direction,
        src_alias: String,
        dst_alias: String,
        rel_alias: Option<String>,
        dst_table_name: Option<String>,
        dst_table_id: Option<u32>,
    },

    /// Apply a filter expression.
    Filter {
        input: Box<PhysicalPlan>,
        predicate: Expr,
    },

    /// Emit selected expressions.
    Projection {
        input: Box<PhysicalPlan>,
        expressions: Vec<(Expr, Option<String>)>,
    },

    /// Return all columns from input.
    ReturnAll { input: Box<PhysicalPlan> },

    /// Hash join two inputs.
    HashJoin {
        build: Box<PhysicalPlan>,
        probe: Box<PhysicalPlan>,
        build_key: JoinKey,
        probe_key: JoinKey,
    },

    /// Insert a new node row.
    InsertNode {
        table_name: String,
        table_id: u32,
        values: Vec<(usize, Expr)>,
    },

    /// Insert a new relationship.
    InsertRel {
        input: Box<PhysicalPlan>,
        rel_table_name: String,
        rel_table_id: u32,
        src_alias: String,
        dst_alias: String,
        properties: Vec<(usize, Expr)>,
    },

    /// Set properties on matched nodes/rels.
    SetProperty {
        input: Box<PhysicalPlan>,
        items: Vec<BoundSetItem>,
    },

    /// Delete matched nodes/rels.
    Delete {
        input: Box<PhysicalPlan>,
        detach: bool,
        variables: Vec<String>,
    },

    /// DDL: Create a node table.
    CreateNodeTable {
        name: String,
        columns: Vec<(String, DataType)>,
        primary_key: String,
    },

    /// DDL: Create a relationship table.
    CreateRelTable {
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<(String, DataType)>,
    },

    /// DDL: Drop a table.
    DropTable { name: String },

    /// Empty result (no-op).
    EmptyResult,
}

impl PhysicalPlan {
    /// Returns `true` if this plan only reads data (no mutations, no DDL).
    pub fn is_read_only(&self) -> bool {
        match self {
            PhysicalPlan::SeqScan { .. }
            | PhysicalPlan::CsrExpand { .. }
            | PhysicalPlan::Filter { .. }
            | PhysicalPlan::Projection { .. }
            | PhysicalPlan::ReturnAll { .. }
            | PhysicalPlan::HashJoin { .. }
            | PhysicalPlan::EmptyResult => true,

            PhysicalPlan::InsertNode { .. }
            | PhysicalPlan::InsertRel { .. }
            | PhysicalPlan::SetProperty { .. }
            | PhysicalPlan::Delete { .. }
            | PhysicalPlan::CreateNodeTable { .. }
            | PhysicalPlan::CreateRelTable { .. }
            | PhysicalPlan::DropTable { .. } => false,
        }
    }
}

/// Translate a logical plan into a physical plan (1:1 mapping for now).
pub fn to_physical(
    logical: &crate::planner::logical::LogicalOperator,
) -> PhysicalPlan {
    use crate::planner::logical::LogicalOperator;

    match logical {
        LogicalOperator::ScanNode {
            table_name,
            table_id,
            columns,
            alias,
        } => PhysicalPlan::SeqScan {
            table_name: table_name.clone(),
            table_id: *table_id,
            columns: columns.clone(),
            alias: alias.clone(),
        },

        LogicalOperator::Filter { input, predicate } => PhysicalPlan::Filter {
            input: Box::new(to_physical(input)),
            predicate: predicate.clone(),
        },

        LogicalOperator::Projection {
            input,
            expressions,
        } => PhysicalPlan::Projection {
            input: Box::new(to_physical(input)),
            expressions: expressions.clone(),
        },

        LogicalOperator::ReturnAll { input } => PhysicalPlan::ReturnAll {
            input: Box::new(to_physical(input)),
        },

        LogicalOperator::HashJoin {
            build,
            probe,
            build_key,
            probe_key,
        } => PhysicalPlan::HashJoin {
            build: Box::new(to_physical(build)),
            probe: Box::new(to_physical(probe)),
            build_key: build_key.clone(),
            probe_key: probe_key.clone(),
        },

        LogicalOperator::Expand {
            input,
            rel_table_name,
            rel_table_id,
            direction,
            src_alias,
            dst_alias,
            rel_alias,
            dst_table_name,
            dst_table_id,
        } => PhysicalPlan::CsrExpand {
            input: Box::new(to_physical(input)),
            rel_table_name: rel_table_name.clone(),
            rel_table_id: *rel_table_id,
            direction: *direction,
            src_alias: src_alias.clone(),
            dst_alias: dst_alias.clone(),
            rel_alias: rel_alias.clone(),
            dst_table_name: dst_table_name.clone(),
            dst_table_id: *dst_table_id,
        },

        LogicalOperator::InsertNode {
            table_name,
            table_id,
            values,
        } => PhysicalPlan::InsertNode {
            table_name: table_name.clone(),
            table_id: *table_id,
            values: values.clone(),
        },

        LogicalOperator::InsertRel {
            input,
            rel_table_name,
            rel_table_id,
            src_alias,
            dst_alias,
            properties,
        } => PhysicalPlan::InsertRel {
            input: Box::new(to_physical(input)),
            rel_table_name: rel_table_name.clone(),
            rel_table_id: *rel_table_id,
            src_alias: src_alias.clone(),
            dst_alias: dst_alias.clone(),
            properties: properties.clone(),
        },

        LogicalOperator::SetProperty { input, items } => PhysicalPlan::SetProperty {
            input: Box::new(to_physical(input)),
            items: items.clone(),
        },

        LogicalOperator::Delete {
            input,
            detach,
            variables,
        } => PhysicalPlan::Delete {
            input: Box::new(to_physical(input)),
            detach: *detach,
            variables: variables.clone(),
        },

        LogicalOperator::CreateNodeTable {
            name,
            columns,
            primary_key,
        } => PhysicalPlan::CreateNodeTable {
            name: name.clone(),
            columns: columns.clone(),
            primary_key: primary_key.clone(),
        },

        LogicalOperator::CreateRelTable {
            name,
            from_table,
            to_table,
            columns,
        } => PhysicalPlan::CreateRelTable {
            name: name.clone(),
            from_table: from_table.clone(),
            to_table: to_table.clone(),
            columns: columns.clone(),
        },

        LogicalOperator::DropTable { name } => PhysicalPlan::DropTable {
            name: name.clone(),
        },

        LogicalOperator::EmptyResult => PhysicalPlan::EmptyResult,
    }
}
