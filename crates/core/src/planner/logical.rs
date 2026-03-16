//! Logical plan operators for query execution.
//!
//! The logical plan is an operator tree that describes *what* to compute
//! without specifying *how*. It is produced by the `Planner` from a `BoundStatement`.

use crate::binder::*;
use crate::error::GqliteError;
use crate::parser::ast::*;
use crate::types::data_type::DataType;

/// A node in the logical operator tree.
#[derive(Debug, Clone)]
pub enum LogicalOperator {
    /// Scan all rows from a node table.
    ScanNode {
        table_name: String,
        table_id: u32,
        /// Column indices to read (all columns if empty).
        columns: Vec<usize>,
        /// Variable name bound to this scan.
        alias: String,
    },

    /// Filter rows by a predicate expression.
    Filter {
        input: Box<LogicalOperator>,
        predicate: Expr,
    },

    /// Project specific expressions from input.
    Projection {
        input: Box<LogicalOperator>,
        expressions: Vec<(Expr, Option<String>)>, // (expr, optional alias)
    },

    /// Hash join two inputs on a key.
    HashJoin {
        build: Box<LogicalOperator>,
        probe: Box<LogicalOperator>,
        /// Column index in build output to join on.
        build_key: JoinKey,
        /// Column index in probe output to join on.
        probe_key: JoinKey,
    },

    /// Expand relationships from a source node scan.
    Expand {
        input: Box<LogicalOperator>,
        rel_table_name: String,
        rel_table_id: u32,
        direction: Direction,
        src_alias: String,
        dst_alias: String,
        rel_alias: Option<String>,
        /// Target node table for the destination.
        dst_table_name: Option<String>,
        dst_table_id: Option<u32>,
        /// If true, produce NULLs for unmatched rows (left outer join).
        optional: bool,
    },

    /// Variable-length recursive expand (BFS): `(a)-[*min..max]->(b)`.
    RecursiveExpand {
        input: Box<LogicalOperator>,
        rel_table_name: String,
        rel_table_id: u32,
        direction: Direction,
        src_alias: String,
        dst_alias: String,
        dst_table_name: Option<String>,
        dst_table_id: Option<u32>,
        min_hops: u32,
        max_hops: u32,
    },

    /// Shortest-path BFS: `p = shortestPath((a)-[:R*..N]->(b))`.
    ShortestPath {
        input: Box<LogicalOperator>,
        rel_table_name: String,
        rel_table_id: u32,
        direction: Direction,
        src_alias: String,
        dst_alias: String,
        path_alias: String,
        dst_table_id: Option<u32>,
        max_hops: u32,
        all_paths: bool,
    },

    /// Insert a new node.
    InsertNode {
        table_name: String,
        table_id: u32,
        /// (column_index, value expression)
        values: Vec<(usize, Expr)>,
    },

    /// Insert a new relationship.
    InsertRel {
        /// We need a MATCH first to find the endpoints.
        input: Box<LogicalOperator>,
        rel_table_name: String,
        rel_table_id: u32,
        src_alias: String,
        dst_alias: String,
        /// (column_index, value expression)
        properties: Vec<(usize, Expr)>,
    },

    /// Update node/rel properties.
    SetProperty {
        input: Box<LogicalOperator>,
        items: Vec<BoundSetItem>,
    },

    /// Delete nodes/relationships.
    Delete {
        input: Box<LogicalOperator>,
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

    /// Return all results (identity operator for top-level queries without projection).
    ReturnAll { input: Box<LogicalOperator> },

    /// Sort rows by given expressions.
    OrderBy {
        input: Box<LogicalOperator>,
        items: Vec<OrderByItem>,
    },

    /// Limit output to N rows.
    Limit {
        input: Box<LogicalOperator>,
        count: Expr,
    },

    /// Skip the first N rows.
    Skip {
        input: Box<LogicalOperator>,
        count: Expr,
    },

    /// Aggregate with implicit GROUP BY from non-aggregate expressions.
    Aggregate {
        input: Box<LogicalOperator>,
        expressions: Vec<(Expr, Option<String>)>,
    },

    /// Empty result (for standalone CREATE/INSERT).
    EmptyResult,

    /// Combine two query results (UNION / UNION ALL).
    Union {
        left: Box<LogicalOperator>,
        right: Box<LogicalOperator>,
        all: bool,
    },

    /// Expand a list expression into multiple rows.
    Unwind {
        input: Box<LogicalOperator>,
        expr: Expr,
        alias: String,
    },

    /// Upsert: match or create a pattern.
    Merge {
        table_name: String,
        table_id: u32,
        properties: Vec<(usize, Expr)>,
        on_create: Vec<(usize, Expr)>,
        on_match: Vec<(usize, Expr)>,
    },

    /// DDL: Alter a table.
    AlterTable {
        table_name: String,
        action: AlterTableAction,
    },

    /// COPY FROM: import CSV into a table.
    CopyFrom {
        table_name: String,
        file_path: String,
        header: bool,
        delimiter: char,
    },

    /// COPY TO: export data to CSV.
    CopyTo {
        source: CopyToSource,
        file_path: String,
        header: bool,
        delimiter: char,
    },
}

/// Source for COPY TO operation.
#[derive(Debug, Clone)]
pub enum CopyToSource {
    Table(String),
    Query(Box<LogicalOperator>),
}

/// Join key reference.
#[derive(Debug, Clone)]
pub struct JoinKey {
    /// Which alias this key comes from.
    pub alias: String,
    /// Which column (by name) for the key. For node joins, this is the internal ID.
    pub column: String,
}

/// A resolved SET item with table/column info.
#[derive(Debug, Clone)]
pub struct BoundSetItem {
    pub variable: String,
    pub field: String,
    pub value: Expr,
}

/// Converts a `BoundStatement` into a `LogicalOperator` tree.
pub struct Planner<'a> {
    catalog: &'a crate::catalog::Catalog,
}

impl<'a> Planner<'a> {
    pub fn new(catalog: &'a crate::catalog::Catalog) -> Self {
        Self { catalog }
    }

    /// Generate a logical plan from a bound statement.
    pub fn plan(&self, stmt: &BoundStatement) -> Result<LogicalOperator, GqliteError> {
        match stmt {
            BoundStatement::Query(q) => self.plan_query(q),
            BoundStatement::CreateNodeTable {
                name,
                columns,
                primary_key,
            } => Ok(LogicalOperator::CreateNodeTable {
                name: name.clone(),
                columns: columns.clone(),
                primary_key: primary_key.clone(),
            }),
            BoundStatement::CreateRelTable {
                name,
                from_table,
                to_table,
                columns,
            } => Ok(LogicalOperator::CreateRelTable {
                name: name.clone(),
                from_table: from_table.clone(),
                to_table: to_table.clone(),
                columns: columns.clone(),
            }),
            BoundStatement::DropTable { name } => {
                Ok(LogicalOperator::DropTable { name: name.clone() })
            }
            BoundStatement::AlterTable { table_name, action } => {
                Ok(LogicalOperator::AlterTable {
                    table_name: table_name.clone(),
                    action: action.clone(),
                })
            }
            BoundStatement::CopyFrom {
                table_name,
                file_path,
                header,
                delimiter,
            } => Ok(LogicalOperator::CopyFrom {
                table_name: table_name.clone(),
                file_path: file_path.clone(),
                header: *header,
                delimiter: *delimiter,
            }),
            BoundStatement::CopyTo {
                source,
                file_path,
                header,
                delimiter,
            } => {
                let copy_source = match source {
                    crate::binder::BoundCopySource::Table(name) => {
                        CopyToSource::Table(name.clone())
                    }
                    crate::binder::BoundCopySource::Query(bound_stmt) => {
                        let plan = self.plan(bound_stmt)?;
                        CopyToSource::Query(Box::new(plan))
                    }
                };
                Ok(LogicalOperator::CopyTo {
                    source: copy_source,
                    file_path: file_path.clone(),
                    header: *header,
                    delimiter: *delimiter,
                })
            }
            BoundStatement::Union { left, right, all } => {
                let left_plan = self.plan(left)?;
                let right_plan = self.plan(right)?;
                Ok(LogicalOperator::Union {
                    left: Box::new(left_plan),
                    right: Box::new(right_plan),
                    all: *all,
                })
            }
        }
    }

    fn plan_query(&self, q: &BoundQuery) -> Result<LogicalOperator, GqliteError> {
        let mut current_plan: Option<LogicalOperator> = None;
        let mut pending_filter: Option<Expr> = None;
        let mut pending_create: Option<&GraphPattern> = None;
        let mut pending_set: Vec<BoundSetItem> = Vec::new();
        let mut pending_delete: Option<(bool, Vec<String>)> = None;

        for clause in &q.clauses {
            match clause {
                BoundClause::Match(m) => {
                    if m.optional {
                        if let Some(existing) = current_plan.take() {
                            // OPTIONAL MATCH after a previous MATCH:
                            // Feed existing rows into the expand with optional=true.
                            let expanded =
                                self.plan_optional_match_expand(&m.pattern, existing)?;
                            current_plan = Some(expanded);
                        } else {
                            // OPTIONAL MATCH without prior MATCH — treat as regular match
                            let match_plan = self.plan_match_pattern(&m.pattern, true)?;
                            current_plan = match_plan;
                        }
                    } else {
                        let match_plan = self.plan_match_pattern(&m.pattern, false)?;
                        current_plan = Some(match match_plan {
                            Some(plan) => {
                                if let Some(existing) = current_plan {
                                    // Multiple MATCH → cross product (simplified as nested)
                                    LogicalOperator::HashJoin {
                                        build: Box::new(existing),
                                        probe: Box::new(plan),
                                        build_key: JoinKey {
                                            alias: String::new(),
                                            column: String::new(),
                                        },
                                        probe_key: JoinKey {
                                            alias: String::new(),
                                            column: String::new(),
                                        },
                                    }
                                } else {
                                    plan
                                }
                            }
                            None => {
                                return Err(GqliteError::Other(
                                    "empty MATCH pattern".into(),
                                ))
                            }
                        });
                    }
                }
                BoundClause::Where(expr) => {
                    pending_filter = Some(expr.clone());
                }
                BoundClause::Return(ret) => {
                    // Apply pending filter first
                    if let Some(predicate) = pending_filter.take() {
                        if let Some(input) = current_plan.take() {
                            current_plan = Some(LogicalOperator::Filter {
                                input: Box::new(input),
                                predicate,
                            });
                        }
                    }

                    // Apply pending SET
                    if !pending_set.is_empty() {
                        if let Some(input) = current_plan.take() {
                            current_plan = Some(LogicalOperator::SetProperty {
                                input: Box::new(input),
                                items: std::mem::take(&mut pending_set),
                            });
                        }
                    }

                    // Apply pending DELETE
                    if let Some((detach, vars)) = pending_delete.take() {
                        if let Some(input) = current_plan.take() {
                            current_plan = Some(LogicalOperator::Delete {
                                input: Box::new(input),
                                detach,
                                variables: vars,
                            });
                        }
                    }

                    // Build projection or aggregate
                    if ret.return_all {
                        if let Some(input) = current_plan.take() {
                            current_plan = Some(LogicalOperator::ReturnAll {
                                input: Box::new(input),
                            });
                        }
                    } else {
                        let expressions: Vec<(Expr, Option<String>)> = ret
                            .items
                            .iter()
                            .map(|item| (item.expr.clone(), item.alias.clone()))
                            .collect();
                        let has_agg = ret
                            .items
                            .iter()
                            .any(|item| contains_aggregate(&item.expr));
                        if let Some(input) = current_plan.take() {
                            if has_agg {
                                current_plan = Some(LogicalOperator::Aggregate {
                                    input: Box::new(input),
                                    expressions,
                                });
                            } else {
                                current_plan = Some(LogicalOperator::Projection {
                                    input: Box::new(input),
                                    expressions,
                                });
                            }
                        }
                    }
                }
                BoundClause::Create(pattern) => {
                    pending_create = Some(pattern);
                }
                BoundClause::Set(items) => {
                    for item in items {
                        pending_set.push(BoundSetItem {
                            variable: item.property.variable.clone(),
                            field: item.property.field.clone(),
                            value: item.value.clone(),
                        });
                    }
                }
                BoundClause::Delete(d) => {
                    let vars: Vec<String> = d
                        .exprs
                        .iter()
                        .filter_map(|e| {
                            if let Expr::Ident(name) = e {
                                Some(name.clone())
                            } else {
                                None
                            }
                        })
                        .collect();
                    pending_delete = Some((d.detach, vars));
                }
                BoundClause::With(items) => {
                    // Apply pending filter first
                    if let Some(predicate) = pending_filter.take() {
                        if let Some(input) = current_plan.take() {
                            current_plan = Some(LogicalOperator::Filter {
                                input: Box::new(input),
                                predicate,
                            });
                        }
                    }
                    // WITH acts like RETURN but feeds into next clause
                    let expressions: Vec<(Expr, Option<String>)> = items
                        .iter()
                        .map(|item| (item.expr.clone(), item.alias.clone()))
                        .collect();
                    let has_agg = items
                        .iter()
                        .any(|item| contains_aggregate(&item.expr));
                    if let Some(input) = current_plan.take() {
                        if has_agg {
                            current_plan = Some(LogicalOperator::Aggregate {
                                input: Box::new(input),
                                expressions,
                            });
                        } else {
                            current_plan = Some(LogicalOperator::Projection {
                                input: Box::new(input),
                                expressions,
                            });
                        }
                    }
                }
                BoundClause::OrderBy(items) => {
                    if let Some(plan) = current_plan.take() {
                        // Insert OrderBy before Projection so sort can
                        // access pre-projection columns (e.g. ORDER BY n.age
                        // when only RETURN n.name).
                        match plan {
                            LogicalOperator::Projection {
                                input,
                                expressions,
                            } => {
                                let sorted = LogicalOperator::OrderBy {
                                    input,
                                    items: items.clone(),
                                };
                                current_plan =
                                    Some(LogicalOperator::Projection {
                                        input: Box::new(sorted),
                                        expressions,
                                    });
                            }
                            other => {
                                current_plan = Some(LogicalOperator::OrderBy {
                                    input: Box::new(other),
                                    items: items.clone(),
                                });
                            }
                        }
                    }
                }
                BoundClause::Limit(expr) => {
                    if let Some(input) = current_plan.take() {
                        current_plan = Some(LogicalOperator::Limit {
                            input: Box::new(input),
                            count: expr.clone(),
                        });
                    }
                }
                BoundClause::Skip(expr) => {
                    if let Some(input) = current_plan.take() {
                        current_plan = Some(LogicalOperator::Skip {
                            input: Box::new(input),
                            count: expr.clone(),
                        });
                    }
                }
                BoundClause::Unwind { expr, alias } => {
                    let input = current_plan
                        .take()
                        .unwrap_or(LogicalOperator::EmptyResult);
                    current_plan = Some(LogicalOperator::Unwind {
                        input: Box::new(input),
                        expr: expr.clone(),
                        alias: alias.clone(),
                    });
                }
                BoundClause::Merge(m) => {
                    // MERGE on a single node pattern
                    if let Some(path) = m.pattern.paths.first() {
                        if let Some(PatternElement::Node(n)) = path.elements.first() {
                            if let Some(ref label) = n.label {
                                let entry =
                                    self.catalog.get_node_table(label).ok_or_else(|| {
                                        GqliteError::Other(format!(
                                            "table '{}' not found",
                                            label
                                        ))
                                    })?;
                                let properties: Vec<(usize, Expr)> = n
                                    .properties
                                    .iter()
                                    .filter_map(|(name, expr)| {
                                        entry
                                            .columns
                                            .iter()
                                            .position(|c| c.name == *name)
                                            .map(|idx| (idx, expr.clone()))
                                    })
                                    .collect();
                                let on_create: Vec<(usize, Expr)> = m
                                    .on_create
                                    .iter()
                                    .filter_map(|item| {
                                        entry
                                            .columns
                                            .iter()
                                            .position(|c| c.name == item.property.field)
                                            .map(|idx| (idx, item.value.clone()))
                                    })
                                    .collect();
                                let on_match: Vec<(usize, Expr)> = m
                                    .on_match
                                    .iter()
                                    .filter_map(|item| {
                                        entry
                                            .columns
                                            .iter()
                                            .position(|c| c.name == item.property.field)
                                            .map(|idx| (idx, item.value.clone()))
                                    })
                                    .collect();
                                current_plan = Some(LogicalOperator::Merge {
                                    table_name: label.clone(),
                                    table_id: entry.table_id,
                                    properties,
                                    on_create,
                                    on_match,
                                });
                            }
                        }
                    }
                }
            }
        }

        // Handle standalone CREATE (no RETURN)
        if let Some(pattern) = pending_create {
            // Apply pending filter before CREATE
            if let Some(predicate) = pending_filter.take() {
                if let Some(input) = current_plan.take() {
                    current_plan = Some(LogicalOperator::Filter {
                        input: Box::new(input),
                        predicate,
                    });
                }
            }
            let create_plan = self.plan_create(current_plan, pattern)?;
            return Ok(create_plan);
        }

        // Apply any remaining filter BEFORE SET/DELETE
        if let Some(predicate) = pending_filter {
            if let Some(input) = current_plan.take() {
                current_plan = Some(LogicalOperator::Filter {
                    input: Box::new(input),
                    predicate,
                });
            }
        }

        // Handle SET/DELETE without RETURN
        if !pending_set.is_empty() {
            if let Some(input) = current_plan.take() {
                current_plan = Some(LogicalOperator::SetProperty {
                    input: Box::new(input),
                    items: pending_set,
                });
            }
        }
        if let Some((detach, vars)) = pending_delete {
            if let Some(input) = current_plan.take() {
                current_plan = Some(LogicalOperator::Delete {
                    input: Box::new(input),
                    detach,
                    variables: vars,
                });
            }
        }

        current_plan.ok_or_else(|| GqliteError::Other("empty query".into()))
    }

    /// Plan a MATCH graph pattern → ScanNode [→ Expand]*
    fn plan_match_pattern(
        &self,
        pattern: &GraphPattern,
        optional: bool,
    ) -> Result<Option<LogicalOperator>, GqliteError> {
        let mut result: Option<LogicalOperator> = None;

        for path in &pattern.paths {
            let plan = self.plan_path_pattern(path, optional)?;
            if let Some(plan) = plan {
                result = Some(if let Some(existing) = result {
                    // Multiple comma-separated patterns → cross join
                    LogicalOperator::HashJoin {
                        build: Box::new(existing),
                        probe: Box::new(plan),
                        build_key: JoinKey {
                            alias: String::new(),
                            column: String::new(),
                        },
                        probe_key: JoinKey {
                            alias: String::new(),
                            column: String::new(),
                        },
                    }
                } else {
                    plan
                });
            }
        }

        // Plan shortest-path patterns on top of the existing scan/expand plan.
        for sp in &pattern.shortest_paths {
            if let Some(input) = result.take() {
                result = Some(self.plan_shortest_path(input, sp)?);
            }
        }

        Ok(result)
    }

    /// Plan an OPTIONAL MATCH by appending Expand operators (with optional=true)
    /// on top of the existing plan. The first node in each path is assumed to
    /// already exist in the existing plan's output, so we skip creating a ScanNode
    /// for it and feed the existing plan into the Expand instead.
    fn plan_optional_match_expand(
        &self,
        pattern: &GraphPattern,
        existing: LogicalOperator,
    ) -> Result<LogicalOperator, GqliteError> {
        let mut current = existing;

        for path in &pattern.paths {
            let mut first_node = true;
            let mut last_alias = String::new();

            for elem in &path.elements {
                match elem {
                    PatternElement::Node(n) => {
                        let alias = n.alias.clone().unwrap_or_default();
                        if first_node {
                            // Skip ScanNode — this alias already exists in the existing plan
                            first_node = false;
                        }
                        last_alias = alias;
                    }
                    PatternElement::Rel(r) => {
                        let src_alias = last_alias.clone();
                        let dst_alias = self.next_node_alias_from_path(path, r)?;

                        let (rel_table_name, rel_table_id) =
                            if let Some(ref label) = r.label {
                                let entry =
                                    self.catalog.get_rel_table(label).ok_or_else(|| {
                                        GqliteError::Other(format!(
                                            "rel table '{}' not found",
                                            label
                                        ))
                                    })?;
                                (label.clone(), entry.table_id)
                            } else {
                                (String::new(), 0)
                            };

                        let (dst_table_name, dst_table_id) =
                            self.resolve_dst_table(path, r);

                        current = LogicalOperator::Expand {
                            input: Box::new(current),
                            rel_table_name,
                            rel_table_id,
                            direction: r.direction,
                            src_alias,
                            dst_alias: dst_alias.clone(),
                            rel_alias: r.alias.clone(),
                            dst_table_name,
                            dst_table_id,
                            optional: true,
                        };
                        last_alias = dst_alias;
                    }
                }
            }
        }

        Ok(current)
    }

    /// Plan a single path pattern: (a:Label)-[r:Rel]->(b:Label)
    fn plan_path_pattern(
        &self,
        path: &PathPattern,
        optional: bool,
    ) -> Result<Option<LogicalOperator>, GqliteError> {
        let mut current: Option<LogicalOperator> = None;
        let mut last_alias = String::new();

        for elem in &path.elements {
            match elem {
                PatternElement::Node(n) => {
                    let alias = n.alias.clone().unwrap_or_default();
                    if current.is_none() {
                        // First node → ScanNode
                        if let Some(ref label) = n.label {
                            let entry = self.catalog.get_node_table(label).ok_or_else(|| {
                                GqliteError::Other(format!("table '{}' not found", label))
                            })?;
                            current = Some(LogicalOperator::ScanNode {
                                table_name: label.clone(),
                                table_id: entry.table_id,
                                columns: vec![], // all columns
                                alias: alias.clone(),
                            });
                        } else {
                            // Unlabeled node scan — scan all node tables
                            // For now, treat as error for simplicity
                            current = Some(LogicalOperator::ScanNode {
                                table_name: String::new(),
                                table_id: 0,
                                columns: vec![],
                                alias: alias.clone(),
                            });
                        }
                    }
                    // If there's already a current plan and this is the destination after Expand,
                    // the alias is already set in Expand
                    last_alias = alias;
                }
                PatternElement::Rel(r) => {
                    let input = current.take().ok_or_else(|| {
                        GqliteError::Other("relationship without source node".into())
                    })?;
                    let src_alias = last_alias.clone();
                    let dst_alias = self.next_node_alias_from_path(path, r)?;

                    let (rel_table_name, rel_table_id) = if let Some(ref label) = r.label {
                        let entry = self.catalog.get_rel_table(label).ok_or_else(|| {
                            GqliteError::Other(format!("rel table '{}' not found", label))
                        })?;
                        (label.clone(), entry.table_id)
                    } else {
                        (String::new(), 0)
                    };

                    let (dst_table_name, dst_table_id) = self.resolve_dst_table(path, r);

                    if let Some((min_hops, max_hops)) = r.var_length {
                        current = Some(LogicalOperator::RecursiveExpand {
                            input: Box::new(input),
                            rel_table_name,
                            rel_table_id,
                            direction: r.direction,
                            src_alias,
                            dst_alias: dst_alias.clone(),
                            dst_table_name,
                            dst_table_id,
                            min_hops,
                            max_hops,
                        });
                    } else {
                        current = Some(LogicalOperator::Expand {
                            input: Box::new(input),
                            rel_table_name,
                            rel_table_id,
                            direction: r.direction,
                            src_alias,
                            dst_alias: dst_alias.clone(),
                            rel_alias: r.alias.clone(),
                            dst_table_name,
                            dst_table_id,
                            optional,
                        });
                    }
                    last_alias = dst_alias;
                }
            }
        }

        Ok(current)
    }

    /// Plan a CREATE pattern.
    fn plan_create(
        &self,
        input: Option<LogicalOperator>,
        pattern: &GraphPattern,
    ) -> Result<LogicalOperator, GqliteError> {
        // For each node in the pattern, generate InsertNode
        // For each rel in the pattern, generate InsertRel
        for path in &pattern.paths {
            let mut has_rel = false;
            for elem in &path.elements {
                if let PatternElement::Rel(_) = elem {
                    has_rel = true;
                    break;
                }
            }

            if !has_rel {
                // Simple node creation
                for elem in &path.elements {
                    if let PatternElement::Node(n) = elem {
                        if let Some(ref label) = n.label {
                            let entry =
                                self.catalog.get_node_table(label).ok_or_else(|| {
                                    GqliteError::Other(format!(
                                        "table '{}' not found",
                                        label
                                    ))
                                })?;
                            let values: Vec<(usize, Expr)> = n
                                .properties
                                .iter()
                                .filter_map(|(name, expr)| {
                                    entry
                                        .columns
                                        .iter()
                                        .position(|c| c.name == *name)
                                        .map(|idx| (idx, expr.clone()))
                                })
                                .collect();
                            return Ok(LogicalOperator::InsertNode {
                                table_name: label.clone(),
                                table_id: entry.table_id,
                                values,
                            });
                        }
                    }
                }
            } else if input.is_some() {
                // Relationship creation requires existing nodes (from MATCH)
                // Find the rel element
                for elem in &path.elements {
                    if let PatternElement::Rel(r) = elem {
                        if let Some(ref label) = r.label {
                            let entry =
                                self.catalog.get_rel_table(label).ok_or_else(|| {
                                    GqliteError::Other(format!(
                                        "rel table '{}' not found",
                                        label
                                    ))
                                })?;

                            // Find src and dst aliases from surrounding nodes
                            let (src_alias, dst_alias) =
                                self.extract_rel_endpoints(path, r);

                            return Ok(LogicalOperator::InsertRel {
                                input: Box::new(input.unwrap()),
                                rel_table_name: label.clone(),
                                rel_table_id: entry.table_id,
                                src_alias,
                                dst_alias,
                                properties: vec![],
                            });
                        }
                    }
                }
            }
        }

        Ok(LogicalOperator::EmptyResult)
    }

    /// Extract the alias of the destination node following a relationship in the path.
    fn next_node_alias_from_path(
        &self,
        path: &PathPattern,
        rel: &RelPattern,
    ) -> Result<String, GqliteError> {
        let mut found_rel = false;
        for elem in &path.elements {
            if found_rel {
                if let PatternElement::Node(n) = elem {
                    return Ok(n.alias.clone().unwrap_or_default());
                }
            }
            if let PatternElement::Rel(r) = elem {
                if std::ptr::eq(r, rel) {
                    found_rel = true;
                }
            }
        }
        Ok(String::new())
    }

    /// Resolve the destination node table from the path context.
    fn resolve_dst_table(
        &self,
        path: &PathPattern,
        rel: &RelPattern,
    ) -> (Option<String>, Option<u32>) {
        let mut found_rel = false;
        for elem in &path.elements {
            if found_rel {
                if let PatternElement::Node(n) = elem {
                    if let Some(ref label) = n.label {
                        if let Some(entry) = self.catalog.get_node_table(label) {
                            return (Some(label.clone()), Some(entry.table_id));
                        }
                    }
                    return (None, None);
                }
            }
            if let PatternElement::Rel(r) = elem {
                if std::ptr::eq(r, rel) {
                    found_rel = true;
                }
            }
        }
        (None, None)
    }

    /// Extract source and destination aliases from nodes surrounding a relationship.
    fn extract_rel_endpoints(&self, path: &PathPattern, rel: &RelPattern) -> (String, String) {
        let mut src = String::new();
        let mut dst = String::new();
        let mut prev_node_alias = String::new();
        let mut found_rel = false;

        for elem in &path.elements {
            match elem {
                PatternElement::Node(n) => {
                    let alias = n.alias.clone().unwrap_or_default();
                    if found_rel {
                        dst = alias;
                        break;
                    }
                    prev_node_alias = alias;
                }
                PatternElement::Rel(r) => {
                    if std::ptr::eq(r, rel) {
                        src = prev_node_alias.clone();
                        found_rel = true;
                    }
                }
            }
        }
        (src, dst)
    }

    /// Plan a `shortestPath(...)` / `allShortestPaths(...)` pattern.
    fn plan_shortest_path(
        &self,
        input: LogicalOperator,
        sp: &ShortestPathPattern,
    ) -> Result<LogicalOperator, GqliteError> {
        // Extract source alias (first node) and destination alias (last node)
        let src_alias = sp
            .pattern
            .elements
            .first()
            .and_then(|e| {
                if let PatternElement::Node(n) = e {
                    n.alias.clone()
                } else {
                    None
                }
            })
            .unwrap_or_default();

        let dst_alias = sp
            .pattern
            .elements
            .last()
            .and_then(|e| {
                if let PatternElement::Node(n) = e {
                    n.alias.clone()
                } else {
                    None
                }
            })
            .unwrap_or_default();

        // Extract rel info from the pattern (the first Rel element)
        let rel = sp.pattern.elements.iter().find_map(|e| {
            if let PatternElement::Rel(r) = e {
                Some(r)
            } else {
                None
            }
        });

        let rel = rel.ok_or_else(|| {
            GqliteError::Other(
                "shortestPath requires a relationship pattern".into(),
            )
        })?;

        let (rel_table_name, rel_table_id) = if let Some(ref label) = rel.label {
            let entry = self.catalog.get_rel_table(label).ok_or_else(|| {
                GqliteError::Other(format!("rel table '{}' not found", label))
            })?;
            (label.clone(), entry.table_id)
        } else {
            return Err(GqliteError::Other(
                "shortestPath requires a typed relationship".into(),
            ));
        };

        // Resolve destination table id from the last node's label
        let dst_table_id = sp
            .pattern
            .elements
            .last()
            .and_then(|e| {
                if let PatternElement::Node(n) = e {
                    n.label
                        .as_ref()
                        .and_then(|l| self.catalog.get_node_table(l))
                        .map(|entry| entry.table_id)
                } else {
                    None
                }
            });

        let max_hops = rel
            .var_length
            .map(|(_, max)| max)
            .unwrap_or(u32::MAX);

        Ok(LogicalOperator::ShortestPath {
            input: Box::new(input),
            rel_table_name,
            rel_table_id,
            direction: rel.direction,
            src_alias,
            dst_alias,
            path_alias: sp.path_variable.clone(),
            dst_table_id,
            max_hops,
            all_paths: sp.all_paths,
        })
    }
}

/// Check if an expression contains an aggregate function call.
fn contains_aggregate(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, args, .. } => {
            if matches!(
                name.to_lowercase().as_str(),
                "count" | "sum" | "avg" | "min" | "max" | "collect"
            ) {
                return true;
            }
            args.iter().any(contains_aggregate)
        }
        Expr::Property(base, _) => contains_aggregate(base),
        Expr::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expr::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expr::IsNull { expr, .. } => contains_aggregate(expr),
        Expr::Cast { expr, .. } => contains_aggregate(expr),
        Expr::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                if contains_aggregate(op) {
                    return true;
                }
            }
            for (cond, result) in when_clauses {
                if contains_aggregate(cond) || contains_aggregate(result) {
                    return true;
                }
            }
            if let Some(el) = else_result {
                if contains_aggregate(el) {
                    return true;
                }
            }
            false
        }
        Expr::In { expr, list, .. } => {
            contains_aggregate(expr) || contains_aggregate(list)
        }
        Expr::Exists(_) => false,
        Expr::ListComprehension { list, filter, map_expr, .. } => {
            if contains_aggregate(list) {
                return true;
            }
            if let Some(f) = filter {
                if contains_aggregate(f) {
                    return true;
                }
            }
            if let Some(m) = map_expr {
                if contains_aggregate(m) {
                    return true;
                }
            }
            false
        }
        _ => false,
    }
}

