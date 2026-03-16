use std::collections::HashMap;

use crate::catalog::Catalog;
use crate::error::GqliteError;
use crate::parser::ast::*;
use crate::types::data_type::DataType;

/// A bound variable with its resolved type and table info.
#[derive(Debug, Clone)]
pub struct BoundVariable {
    pub name: String,
    pub table_id: Option<u32>,
    pub var_type: BoundVarType,
}

#[derive(Debug, Clone)]
pub enum BoundVarType {
    Node { label: Option<String> },
    Rel { label: Option<String> },
}

/// Scope tracking for bound variables.
#[derive(Debug, Clone, Default)]
pub struct BindingScope {
    variables: HashMap<String, BoundVariable>,
}

impl BindingScope {
    pub fn define(&mut self, var: BoundVariable) {
        self.variables.insert(var.name.clone(), var);
    }

    pub fn lookup(&self, name: &str) -> Option<&BoundVariable> {
        self.variables.get(name)
    }

    pub fn has(&self, name: &str) -> bool {
        self.variables.contains_key(name)
    }
}

/// Semantic binder: resolves names, checks types, validates references.
pub struct Binder<'a> {
    catalog: &'a Catalog,
    scope: BindingScope,
}

impl<'a> Binder<'a> {
    pub fn new(catalog: &'a Catalog) -> Self {
        Self {
            catalog,
            scope: BindingScope::default(),
        }
    }

    /// Bind a statement, performing semantic validation.
    pub fn bind(&mut self, stmt: &Statement) -> Result<BoundStatement, GqliteError> {
        match stmt {
            Statement::Query(q) => self.bind_query(q),
            Statement::CreateNodeTable(s) => self.bind_create_node_table(s),
            Statement::CreateRelTable(s) => self.bind_create_rel_table(s),
            Statement::DropTable(s) => Ok(BoundStatement::DropTable {
                name: s.name.clone(),
            }),
            Statement::AlterTable(s) => Ok(BoundStatement::AlterTable {
                table_name: s.table_name.clone(),
                action: s.action.clone(),
            }),
            Statement::CopyFrom(s) => Ok(BoundStatement::CopyFrom {
                table_name: s.table_name.clone(),
                file_path: s.file_path.clone(),
                header: s.header,
                delimiter: s.delimiter,
            }),
            Statement::CopyTo(s) => {
                let source = match &s.source {
                    CopySource::Table(name) => BoundCopySource::Table(name.clone()),
                    CopySource::Query(q) => {
                        let bound = self.bind(&Statement::Query(q.as_ref().clone()))?;
                        BoundCopySource::Query(Box::new(bound))
                    }
                };
                Ok(BoundStatement::CopyTo {
                    source,
                    file_path: s.file_path.clone(),
                    header: s.header,
                    delimiter: s.delimiter,
                })
            }
            Statement::Union { left, right, all } => {
                let bound_left = self.bind(left)?;
                let bound_right = self.bind(right)?;
                Ok(BoundStatement::Union {
                    left: Box::new(bound_left),
                    right: Box::new(bound_right),
                    all: *all,
                })
            }
            Statement::Call { .. } => {
                // CALL statements are handled directly in Connection::execute_with_params,
                // they should never reach the binder.
                Err(GqliteError::Parse(
                    "CALL statements should be handled before binding".into(),
                ))
            }
        }
    }

    fn bind_query(&mut self, q: &QueryStatement) -> Result<BoundStatement, GqliteError> {
        let mut bound_clauses = Vec::new();

        for clause in &q.clauses {
            match clause {
                Clause::Match(m) => {
                    self.bind_match_pattern(&m.pattern)?;
                    bound_clauses.push(BoundClause::Match(BoundMatch {
                        optional: m.optional,
                        pattern: m.pattern.clone(),
                    }));
                }
                Clause::Where(w) => {
                    self.validate_expr(&w.expr)?;
                    bound_clauses.push(BoundClause::Where(w.expr.clone()));
                }
                Clause::Return(r) => {
                    if !r.return_all {
                        for item in &r.items {
                            self.validate_expr(&item.expr)?;
                        }
                    }
                    bound_clauses.push(BoundClause::Return(BoundReturn {
                        distinct: r.distinct,
                        items: r.items.clone(),
                        return_all: r.return_all,
                    }));
                }
                Clause::Create(c) => {
                    // For CREATE, variables may reference already-bound nodes
                    self.bind_create_pattern(&c.pattern)?;
                    bound_clauses.push(BoundClause::Create(c.pattern.clone()));
                }
                Clause::Set(s) => {
                    for item in &s.items {
                        if !self.scope.has(&item.property.variable) {
                            return Err(GqliteError::Parse(format!(
                                "undefined variable '{}'",
                                item.property.variable
                            )));
                        }
                        self.validate_expr(&item.value)?;
                    }
                    bound_clauses.push(BoundClause::Set(s.items.clone()));
                }
                Clause::Delete(d) => {
                    for expr in &d.exprs {
                        self.validate_expr(expr)?;
                    }
                    bound_clauses.push(BoundClause::Delete(BoundDelete {
                        detach: d.detach,
                        exprs: d.exprs.clone(),
                    }));
                }
                Clause::With(w) => {
                    for item in &w.items {
                        self.validate_expr(&item.expr)?;
                    }
                    bound_clauses.push(BoundClause::With(w.items.clone()));
                }
                Clause::OrderBy(o) => {
                    for item in &o.items {
                        self.validate_expr(&item.expr)?;
                    }
                    bound_clauses.push(BoundClause::OrderBy(o.items.clone()));
                }
                Clause::Limit(l) => {
                    bound_clauses.push(BoundClause::Limit(l.count.clone()));
                }
                Clause::Skip(s) => {
                    bound_clauses.push(BoundClause::Skip(s.count.clone()));
                }
                Clause::Unwind(u) => {
                    self.validate_expr(&u.expr)?;
                    // Register the unwound variable in scope
                    self.scope.define(BoundVariable {
                        name: u.alias.clone(),
                        table_id: None,
                        var_type: BoundVarType::Node { label: None },
                    });
                    bound_clauses.push(BoundClause::Unwind {
                        expr: u.expr.clone(),
                        alias: u.alias.clone(),
                    });
                }
                Clause::Merge(m) => {
                    self.bind_match_pattern(&m.pattern)?;
                    bound_clauses.push(BoundClause::Merge(BoundMerge {
                        pattern: m.pattern.clone(),
                        on_create: m.on_create.clone(),
                        on_match: m.on_match.clone(),
                    }));
                }
                Clause::CallSubquery(sub_query) => {
                    // The inner subquery has its own scope; pass through for
                    // binding at execution time (like EXISTS).
                    // But we need to register output columns in the outer scope
                    // so that subsequent clauses can reference them.
                    for clause in &sub_query.clauses {
                        if let Clause::Return(ret) = clause {
                            for item in &ret.items {
                                // Use alias if present, otherwise derive from expr
                                let col_name = if let Some(ref alias) = item.alias {
                                    alias.clone()
                                } else {
                                    match &item.expr {
                                        Expr::Ident(name) => name.clone(),
                                        Expr::Property(_, field) => field.clone(),
                                        Expr::FunctionCall { name, .. } => name.clone(),
                                        _ => continue,
                                    }
                                };
                                self.scope.define(BoundVariable {
                                    name: col_name,
                                    table_id: None,
                                    var_type: BoundVarType::Node { label: None },
                                });
                            }
                        }
                    }
                    bound_clauses.push(BoundClause::CallSubquery(sub_query.clone()));
                }
            }
        }

        Ok(BoundStatement::Query(BoundQuery {
            clauses: bound_clauses,
        }))
    }

    fn bind_match_pattern(&mut self, pattern: &GraphPattern) -> Result<(), GqliteError> {
        for path in &pattern.paths {
            for elem in &path.elements {
                match elem {
                    PatternElement::Node(n) => {
                        // Validate label exists if specified
                        if let Some(ref label) = n.label {
                            if self.catalog.get_node_table(label).is_none() {
                                return Err(GqliteError::Parse(format!(
                                    "node table '{}' not found",
                                    label
                                )));
                            }
                        }
                        // Register variable
                        if let Some(ref alias) = n.alias {
                            let table_id = n
                                .label
                                .as_ref()
                                .and_then(|l| self.catalog.get_node_table(l))
                                .map(|t| t.table_id);
                            self.scope.define(BoundVariable {
                                name: alias.clone(),
                                table_id,
                                var_type: BoundVarType::Node {
                                    label: n.label.clone(),
                                },
                            });
                        }
                    }
                    PatternElement::Rel(r) => {
                        if let Some(ref label) = r.label {
                            if self.catalog.get_rel_table(label).is_none() {
                                return Err(GqliteError::Parse(format!(
                                    "relationship table '{}' not found",
                                    label
                                )));
                            }
                        }
                        if let Some(ref alias) = r.alias {
                            self.scope.define(BoundVariable {
                                name: alias.clone(),
                                table_id: r
                                    .label
                                    .as_ref()
                                    .and_then(|l| self.catalog.get_rel_table(l))
                                    .map(|t| t.table_id),
                                var_type: BoundVarType::Rel {
                                    label: r.label.clone(),
                                },
                            });
                        }
                    }
                }
            }
        }

        // Bind shortest-path patterns: validate inner elements and register path variable
        for sp in &pattern.shortest_paths {
            for elem in &sp.pattern.elements {
                match elem {
                    PatternElement::Node(n) => {
                        if let Some(ref label) = n.label {
                            if self.catalog.get_node_table(label).is_none() {
                                return Err(GqliteError::Parse(format!(
                                    "node table '{}' not found",
                                    label
                                )));
                            }
                        }
                        if let Some(ref alias) = n.alias {
                            let table_id = n
                                .label
                                .as_ref()
                                .and_then(|l| self.catalog.get_node_table(l))
                                .map(|t| t.table_id);
                            // Only define if not already in scope (node may be
                            // defined in a preceding regular path pattern).
                            if !self.scope.has(alias) {
                                self.scope.define(BoundVariable {
                                    name: alias.clone(),
                                    table_id,
                                    var_type: BoundVarType::Node {
                                        label: n.label.clone(),
                                    },
                                });
                            }
                        }
                    }
                    PatternElement::Rel(r) => {
                        if let Some(ref label) = r.label {
                            if self.catalog.get_rel_table(label).is_none() {
                                return Err(GqliteError::Parse(format!(
                                    "relationship table '{}' not found",
                                    label
                                )));
                            }
                        }
                    }
                }
            }
            // Register the path variable (the `p` in `p = shortestPath(...)`)
            self.scope.define(BoundVariable {
                name: sp.path_variable.clone(),
                table_id: None,
                var_type: BoundVarType::Node { label: None }, // path variable, generic
            });
        }

        Ok(())
    }

    fn bind_create_pattern(&mut self, pattern: &GraphPattern) -> Result<(), GqliteError> {
        for path in &pattern.paths {
            for elem in &path.elements {
                if let PatternElement::Node(n) = elem {
                    if let Some(ref alias) = n.alias {
                        if !self.scope.has(alias) {
                            // New node being created
                            self.scope.define(BoundVariable {
                                name: alias.clone(),
                                table_id: None,
                                var_type: BoundVarType::Node {
                                    label: n.label.clone(),
                                },
                            });
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn validate_expr(&self, expr: &Expr) -> Result<(), GqliteError> {
        match expr {
            Expr::Ident(name) => {
                if !self.scope.has(name) {
                    return Err(GqliteError::Parse(format!(
                        "undefined variable '{}'",
                        name
                    )));
                }
                Ok(())
            }
            Expr::Property(base, _field) => self.validate_expr(base),
            Expr::BinaryOp { left, right, .. } => {
                self.validate_expr(left)?;
                self.validate_expr(right)
            }
            Expr::UnaryOp { expr, .. } => self.validate_expr(expr),
            Expr::IsNull { expr, .. } => self.validate_expr(expr),
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.validate_expr(arg)?;
                }
                Ok(())
            }
            Expr::IntLit(_)
            | Expr::FloatLit(_)
            | Expr::StringLit(_)
            | Expr::BoolLit(_)
            | Expr::NullLit
            | Expr::Param(_)
            | Expr::Star => Ok(()),
            Expr::ListLit(items) => {
                for item in items {
                    self.validate_expr(item)?;
                }
                Ok(())
            }
            Expr::Cast { expr, .. } => self.validate_expr(expr),
            Expr::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    self.validate_expr(op)?;
                }
                for (cond, result) in when_clauses {
                    self.validate_expr(cond)?;
                    self.validate_expr(result)?;
                }
                if let Some(el) = else_result {
                    self.validate_expr(el)?;
                }
                Ok(())
            }
            Expr::In { expr, list, .. } => {
                self.validate_expr(expr)?;
                self.validate_expr(list)
            }
            Expr::Exists(_) => {
                // The inner query is validated when it gets bound during execution.
                // At bind time we just accept it — the inner scope is separate.
                Ok(())
            }
            Expr::ListComprehension { list, filter, map_expr, .. } => {
                // Validate the list expression (it may reference bound variables).
                // The comprehension variable is local; filter/map_expr may reference it,
                // so we skip deep validation of those sub-expressions at bind time.
                self.validate_expr(list)?;
                // filter and map_expr may reference the comprehension variable which
                // is not in scope at bind time, so we don't validate them here.
                let _ = filter;
                let _ = map_expr;
                Ok(())
            }
        }
    }

    fn bind_create_node_table(
        &self,
        s: &CreateNodeTableStmt,
    ) -> Result<BoundStatement, GqliteError> {
        // Validate: PK column must exist in column list
        if !s.columns.iter().any(|c| c.name == s.primary_key) {
            return Err(GqliteError::Parse(format!(
                "primary key column '{}' not found in column definitions",
                s.primary_key
            )));
        }
        Ok(BoundStatement::CreateNodeTable {
            name: s.name.clone(),
            columns: s
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect(),
            primary_key: s.primary_key.clone(),
        })
    }

    fn bind_create_rel_table(
        &self,
        s: &CreateRelTableStmt,
    ) -> Result<BoundStatement, GqliteError> {
        // Validate: FROM and TO tables must exist
        if self.catalog.get_node_table(&s.from_table).is_none() {
            return Err(GqliteError::Parse(format!(
                "source table '{}' not found",
                s.from_table
            )));
        }
        if self.catalog.get_node_table(&s.to_table).is_none() {
            return Err(GqliteError::Parse(format!(
                "destination table '{}' not found",
                s.to_table
            )));
        }
        Ok(BoundStatement::CreateRelTable {
            name: s.name.clone(),
            from_table: s.from_table.clone(),
            to_table: s.to_table.clone(),
            columns: s
                .columns
                .iter()
                .map(|c| (c.name.clone(), c.data_type.clone()))
                .collect(),
        })
    }
}

// ── Bound AST types ─────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum BoundStatement {
    Query(BoundQuery),
    CreateNodeTable {
        name: String,
        columns: Vec<(String, DataType)>,
        primary_key: String,
    },
    CreateRelTable {
        name: String,
        from_table: String,
        to_table: String,
        columns: Vec<(String, DataType)>,
    },
    DropTable {
        name: String,
    },
    AlterTable {
        table_name: String,
        action: AlterTableAction,
    },
    CopyFrom {
        table_name: String,
        file_path: String,
        header: bool,
        delimiter: char,
    },
    CopyTo {
        source: BoundCopySource,
        file_path: String,
        header: bool,
        delimiter: char,
    },
    Union {
        left: Box<BoundStatement>,
        right: Box<BoundStatement>,
        all: bool,
    },
}

#[derive(Debug, Clone)]
pub enum BoundCopySource {
    Table(String),
    Query(Box<BoundStatement>),
}

#[derive(Debug, Clone)]
pub struct BoundQuery {
    pub clauses: Vec<BoundClause>,
}

#[derive(Debug, Clone)]
pub enum BoundClause {
    Match(BoundMatch),
    Where(Expr),
    Return(BoundReturn),
    Create(GraphPattern),
    Set(Vec<SetItem>),
    Delete(BoundDelete),
    With(Vec<ReturnItem>),
    OrderBy(Vec<OrderByItem>),
    Limit(Expr),
    Skip(Expr),
    Unwind { expr: Expr, alias: String },
    Merge(BoundMerge),
    CallSubquery(QueryStatement),
}

#[derive(Debug, Clone)]
pub struct BoundMatch {
    pub optional: bool,
    pub pattern: GraphPattern,
}

#[derive(Debug, Clone)]
pub struct BoundReturn {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    pub return_all: bool,
}

#[derive(Debug, Clone)]
pub struct BoundDelete {
    pub detach: bool,
    pub exprs: Vec<Expr>,
}

#[derive(Debug, Clone)]
pub struct BoundMerge {
    pub pattern: GraphPattern,
    pub on_create: Vec<SetItem>,
    pub on_match: Vec<SetItem>,
}

