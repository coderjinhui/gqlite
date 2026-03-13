//! Abstract syntax tree nodes for the gqlite Cypher subset.

use crate::types::data_type::DataType;

// ── Top-level Statement ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Statement {
    Query(QueryStatement),
    CreateNodeTable(CreateNodeTableStmt),
    CreateRelTable(CreateRelTableStmt),
    DropTable(DropTableStmt),
}

// ── Query Statement ─────────────────────────────────────────────

/// A query is a sequence of clauses.
#[derive(Debug, Clone)]
pub struct QueryStatement {
    pub clauses: Vec<Clause>,
}

#[derive(Debug, Clone)]
pub enum Clause {
    Match(MatchClause),
    Where(WhereClause),
    Return(ReturnClause),
    With(WithClause),
    OrderBy(OrderByClause),
    Limit(LimitClause),
    Skip(SkipClause),
    Create(CreateClause),
    Set(SetClause),
    Delete(DeleteClause),
}

// ── MATCH ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MatchClause {
    pub optional: bool,
    pub pattern: GraphPattern,
}

/// Comma-separated list of path patterns.
#[derive(Debug, Clone)]
pub struct GraphPattern {
    pub paths: Vec<PathPattern>,
}

/// A chain of alternating node and relationship patterns.
#[derive(Debug, Clone)]
pub struct PathPattern {
    pub elements: Vec<PatternElement>,
}

#[derive(Debug, Clone)]
pub enum PatternElement {
    Node(NodePattern),
    Rel(RelPattern),
}

#[derive(Debug, Clone)]
pub struct NodePattern {
    pub alias: Option<String>,
    pub label: Option<String>,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone)]
pub struct RelPattern {
    pub alias: Option<String>,
    pub label: Option<String>,
    pub direction: Direction,
    pub properties: Vec<(String, Expr)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Right,  // -[]->(
    Left,   // <-[]-
    Both,   // -[]-
}

// ── WHERE ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WhereClause {
    pub expr: Expr,
}

// ── RETURN ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct ReturnClause {
    pub distinct: bool,
    pub items: Vec<ReturnItem>,
    /// True if RETURN * was used.
    pub return_all: bool,
}

#[derive(Debug, Clone)]
pub struct ReturnItem {
    pub expr: Expr,
    pub alias: Option<String>,
}

// ── WITH ────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct WithClause {
    pub items: Vec<ReturnItem>,
}

// ── ORDER BY ────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub items: Vec<OrderByItem>,
}

#[derive(Debug, Clone)]
pub struct OrderByItem {
    pub expr: Expr,
    pub descending: bool,
}

// ── LIMIT / SKIP ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct LimitClause {
    pub count: Expr,
}

#[derive(Debug, Clone)]
pub struct SkipClause {
    pub count: Expr,
}

// ── CREATE (DML) ────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateClause {
    pub pattern: GraphPattern,
}

// ── SET ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct SetClause {
    pub items: Vec<SetItem>,
}

#[derive(Debug, Clone)]
pub struct SetItem {
    pub property: PropertyRef,
    pub value: Expr,
}

#[derive(Debug, Clone)]
pub struct PropertyRef {
    pub variable: String,
    pub field: String,
}

// ── DELETE ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct DeleteClause {
    pub detach: bool,
    pub exprs: Vec<Expr>,
}

// ── DDL Statements ──────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct CreateNodeTableStmt {
    pub name: String,
    pub columns: Vec<ColumnDefAst>,
    pub primary_key: String,
}

#[derive(Debug, Clone)]
pub struct CreateRelTableStmt {
    pub name: String,
    pub from_table: String,
    pub to_table: String,
    pub columns: Vec<ColumnDefAst>,
}

#[derive(Debug, Clone)]
pub struct DropTableStmt {
    pub name: String,
}

#[derive(Debug, Clone)]
pub struct ColumnDefAst {
    pub name: String,
    pub data_type: DataType,
}

// ── Expressions ─────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Expr {
    /// An identifier (variable name).
    Ident(String),
    /// Property access: expr.field
    Property(Box<Expr>, String),
    /// Integer literal.
    IntLit(i64),
    /// Float literal.
    FloatLit(f64),
    /// String literal.
    StringLit(String),
    /// Boolean literal (TRUE / FALSE).
    BoolLit(bool),
    /// NULL literal.
    NullLit,
    /// Parameter reference: $name
    Param(String),
    /// Binary operation.
    BinaryOp {
        left: Box<Expr>,
        op: BinOp,
        right: Box<Expr>,
    },
    /// Unary operation (-expr, NOT expr).
    UnaryOp {
        op: UnaryOp,
        expr: Box<Expr>,
    },
    /// IS NULL / IS NOT NULL.
    IsNull {
        expr: Box<Expr>,
        negated: bool,
    },
    /// Function call: name(DISTINCT? args...).
    FunctionCall {
        name: String,
        distinct: bool,
        args: Vec<Expr>,
    },
    /// Star expression (*) — used in RETURN * or count(*).
    Star,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BinOp {
    Eq,
    Neq,
    Lt,
    Gt,
    Le,
    Ge,
    Add,
    Sub,
    Mul,
    Div,
    Mod,
    And,
    Or,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UnaryOp {
    Neg,
    Not,
}
