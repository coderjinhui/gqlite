//! Abstract syntax tree nodes for the gqlite Cypher subset.

use crate::data_type::DataType;

// ── Top-level Statement ─────────────────────────────────────────

#[derive(Debug, Clone)]
pub enum Statement {
    Query(QueryStatement),
    CreateNodeTable(CreateNodeTableStmt),
    CreateRelTable(CreateRelTableStmt),
    DropTable(DropTableStmt),
    AlterTable(AlterTableStmt),
    CopyFrom(CopyFromStmt),
    CopyTo(CopyToStmt),
    Union {
        left: Box<Statement>,
        right: Box<Statement>,
        all: bool,
    },
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
    Unwind(UnwindClause),
    Merge(MergeClause),
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
    /// `p = shortestPath((a)-[:REL*..N]->(b))` assignments in MATCH.
    pub shortest_paths: Vec<ShortestPathPattern>,
}

/// A `shortestPath(...)` or `allShortestPaths(...)` pattern assignment.
#[derive(Debug, Clone)]
pub struct ShortestPathPattern {
    /// The variable name bound to the path (e.g., `p` in `p = shortestPath(...)`).
    pub path_variable: String,
    /// The inner path pattern describing the traversal.
    pub pattern: PathPattern,
    /// `false` for `shortestPath`, `true` for `allShortestPaths`.
    pub all_paths: bool,
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
    /// Variable-length path: Some((min_hops, max_hops)). `*1..3` → (1,3).
    /// `*` alone → (1, u32::MAX). `*..5` → (1, 5). `*3..` → (3, u32::MAX).
    pub var_length: Option<(u32, u32)>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Right,  // -[]->
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

// ── UNWIND ──────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct UnwindClause {
    pub expr: Expr,
    pub alias: String,
}

// ── MERGE ───────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct MergeClause {
    pub pattern: GraphPattern,
    pub on_create: Vec<SetItem>,
    pub on_match: Vec<SetItem>,
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
pub struct AlterTableStmt {
    pub table_name: String,
    pub action: AlterTableAction,
}

#[derive(Debug, Clone)]
pub enum AlterTableAction {
    AddColumn { col: ColumnDefAst },
    DropColumn { col_name: String },
    RenameTable { new_name: String },
    RenameColumn { old_name: String, new_name: String },
}

#[derive(Debug, Clone)]
pub struct ColumnDefAst {
    pub name: String,
    pub data_type: DataType,
}

/// COPY <table> FROM '<path>' [WITH (...)]
#[derive(Debug, Clone)]
pub struct CopyFromStmt {
    pub table_name: String,
    pub file_path: String,
    pub header: bool,
    pub delimiter: char,
}

/// COPY <table_or_query> TO '<path>' [WITH (...)]
#[derive(Debug, Clone)]
pub struct CopyToStmt {
    pub source: CopySource,
    pub file_path: String,
    pub header: bool,
    pub delimiter: char,
}

#[derive(Debug, Clone)]
pub enum CopySource {
    Table(String),
    Query(Box<QueryStatement>),
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
    /// List literal: [expr, expr, ...]
    ListLit(Vec<Expr>),
    /// CAST(expr AS type)
    Cast {
        expr: Box<Expr>,
        target_type: DataType,
    },
    /// CASE expression (searched and simple forms).
    /// - Searched: `CASE WHEN cond THEN result [ELSE default] END`
    /// - Simple:   `CASE operand WHEN value THEN result [ELSE default] END`
    Case {
        operand: Option<Box<Expr>>,
        when_clauses: Vec<(Expr, Expr)>,
        else_result: Option<Box<Expr>>,
    },
    /// IN list expression: expr [NOT] IN [list]
    In {
        expr: Box<Expr>,
        list: Box<Expr>,
        negated: bool,
    },
    /// EXISTS { subquery } — evaluates to true if the subquery returns at least one row.
    Exists(Box<QueryStatement>),
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
