use crate::error::GqliteError;
use crate::types::data_type::DataType;

use super::ast::*;
use super::token::Token;

/// Recursive descent parser for the gqlite Cypher subset.
pub struct Parser {
    tokens: Vec<Token>,
    pos: usize,
}

impl Parser {
    pub fn new(tokens: Vec<Token>) -> Self {
        Self { tokens, pos: 0 }
    }

    /// Parse from a raw query string (tokenize + parse).
    pub fn parse_query(input: &str) -> Result<Statement, GqliteError> {
        let tokens = super::token::tokenize(input)
            .map_err(|e| GqliteError::Parse(e))?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    /// Parse the token stream into a Statement.
    pub fn parse(&mut self) -> Result<Statement, GqliteError> {
        // Skip leading semicolons
        while self.check(&Token::Semicolon) {
            self.advance();
        }

        let stmt = match self.peek() {
            Token::Create => {
                // Peek ahead: CREATE NODE TABLE / CREATE REL TABLE / CREATE (pattern)
                if self.peek_at(1) == &Token::Node && self.peek_at(2) == &Token::Table {
                    self.parse_create_node_table()
                } else if self.peek_at(1) == &Token::Rel && self.peek_at(2) == &Token::Table {
                    self.parse_create_rel_table()
                } else {
                    self.parse_query_statement()
                }
            }
            Token::Drop => self.parse_drop_table(),
            _ => self.parse_query_statement(),
        }?;

        // Consume optional trailing semicolons
        while self.check(&Token::Semicolon) {
            self.advance();
        }

        Ok(stmt)
    }

    // ── Query Statement ─────────────────────────────────────────

    fn parse_query_statement(&mut self) -> Result<Statement, GqliteError> {
        let mut clauses = Vec::new();

        loop {
            match self.peek() {
                Token::Match | Token::Optional => clauses.push(self.parse_match_clause()?),
                Token::Where => clauses.push(self.parse_where_clause()?),
                Token::Return => clauses.push(self.parse_return_clause()?),
                Token::With => clauses.push(self.parse_with_clause()?),
                Token::Order => clauses.push(self.parse_order_by_clause()?),
                Token::Limit => clauses.push(self.parse_limit_clause()?),
                Token::Skip => clauses.push(self.parse_skip_clause()?),
                Token::Create => clauses.push(self.parse_create_clause()?),
                Token::Set => clauses.push(self.parse_set_clause()?),
                Token::Delete | Token::Detach => clauses.push(self.parse_delete_clause()?),
                _ => break,
            }
        }

        if clauses.is_empty() {
            return Err(self.error("expected a query clause"));
        }

        Ok(Statement::Query(QueryStatement { clauses }))
    }

    // ── MATCH ───────────────────────────────────────────────────

    fn parse_match_clause(&mut self) -> Result<Clause, GqliteError> {
        let optional = self.check(&Token::Optional);
        if optional {
            self.advance();
        }
        self.expect(&Token::Match)?;

        let pattern = self.parse_graph_pattern()?;
        Ok(Clause::Match(MatchClause { optional, pattern }))
    }

    fn parse_graph_pattern(&mut self) -> Result<GraphPattern, GqliteError> {
        let mut paths = vec![self.parse_path_pattern()?];
        while self.check(&Token::Comma) {
            self.advance();
            paths.push(self.parse_path_pattern()?);
        }
        Ok(GraphPattern { paths })
    }

    fn parse_path_pattern(&mut self) -> Result<PathPattern, GqliteError> {
        let mut elements = vec![PatternElement::Node(self.parse_node_pattern()?)];

        // rel + node pairs
        while self.is_rel_start() {
            elements.push(PatternElement::Rel(self.parse_rel_pattern()?));
            elements.push(PatternElement::Node(self.parse_node_pattern()?));
        }

        Ok(PathPattern { elements })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, GqliteError> {
        self.expect(&Token::LParen)?;
        let mut alias = None;
        let mut label = None;
        let mut properties = Vec::new();

        // optional alias
        if let Token::Ident(_) = self.peek() {
            alias = Some(self.expect_ident()?);
        }

        // optional :Label
        if self.check(&Token::Colon) {
            self.advance();
            label = Some(self.expect_ident()?);
        }

        // optional {props}
        if self.check(&Token::LBrace) {
            properties = self.parse_property_map()?;
        }

        self.expect(&Token::RParen)?;
        Ok(NodePattern {
            alias,
            label,
            properties,
        })
    }

    fn is_rel_start(&self) -> bool {
        matches!(self.peek(), Token::Dash | Token::LeftArrow)
    }

    fn parse_rel_pattern(&mut self) -> Result<RelPattern, GqliteError> {
        let direction;
        let alias;
        let label;
        let properties;

        if self.check(&Token::LeftArrow) {
            // <-[...]-
            self.advance(); // <-
            self.expect(&Token::LBracket)?;
            let (a, l, p) = self.parse_rel_inner()?;
            alias = a;
            label = l;
            properties = p;
            self.expect(&Token::RBracket)?;
            self.expect(&Token::Dash)?;
            direction = Direction::Left;
        } else {
            // -[...]-> or -[...]-
            self.expect(&Token::Dash)?;
            self.expect(&Token::LBracket)?;
            let (a, l, p) = self.parse_rel_inner()?;
            alias = a;
            label = l;
            properties = p;
            self.expect(&Token::RBracket)?;

            if self.check(&Token::Arrow) {
                self.advance(); // ->
                direction = Direction::Right;
            } else {
                self.expect(&Token::Dash)?;
                direction = Direction::Both;
            }
        }

        Ok(RelPattern {
            alias,
            label,
            direction,
            properties,
        })
    }

    fn parse_rel_inner(
        &mut self,
    ) -> Result<(Option<String>, Option<String>, Vec<(String, Expr)>), GqliteError> {
        let mut alias = None;
        let mut label = None;
        let mut properties = Vec::new();

        if let Token::Ident(_) = self.peek() {
            alias = Some(self.expect_ident()?);
        }
        if self.check(&Token::Colon) {
            self.advance();
            label = Some(self.expect_ident()?);
        }
        if self.check(&Token::LBrace) {
            properties = self.parse_property_map()?;
        }

        Ok((alias, label, properties))
    }

    fn parse_property_map(&mut self) -> Result<Vec<(String, Expr)>, GqliteError> {
        self.expect(&Token::LBrace)?;
        let mut props = Vec::new();
        if !self.check(&Token::RBrace) {
            let key = self.expect_ident()?;
            self.expect(&Token::Colon)?;
            let val = self.parse_expr()?;
            props.push((key, val));

            while self.check(&Token::Comma) {
                self.advance();
                let key = self.expect_ident()?;
                self.expect(&Token::Colon)?;
                let val = self.parse_expr()?;
                props.push((key, val));
            }
        }
        self.expect(&Token::RBrace)?;
        Ok(props)
    }

    // ── WHERE ───────────────────────────────────────────────────

    fn parse_where_clause(&mut self) -> Result<Clause, GqliteError> {
        self.expect(&Token::Where)?;
        let expr = self.parse_expr()?;
        Ok(Clause::Where(WhereClause { expr }))
    }

    // ── RETURN ──────────────────────────────────────────────────

    fn parse_return_clause(&mut self) -> Result<Clause, GqliteError> {
        self.expect(&Token::Return)?;

        let distinct = self.check(&Token::Distinct);
        if distinct {
            self.advance();
        }

        // RETURN *
        if self.check(&Token::Star) {
            self.advance();
            return Ok(Clause::Return(ReturnClause {
                distinct,
                items: Vec::new(),
                return_all: true,
            }));
        }

        let items = self.parse_return_items()?;
        Ok(Clause::Return(ReturnClause {
            distinct,
            items,
            return_all: false,
        }))
    }

    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>, GqliteError> {
        let mut items = vec![self.parse_return_item()?];
        while self.check(&Token::Comma) {
            self.advance();
            items.push(self.parse_return_item()?);
        }
        Ok(items)
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, GqliteError> {
        let expr = self.parse_expr()?;
        let alias = if self.check(&Token::As) {
            self.advance();
            Some(self.expect_ident()?)
        } else {
            None
        };
        Ok(ReturnItem { expr, alias })
    }

    // ── WITH ────────────────────────────────────────────────────

    fn parse_with_clause(&mut self) -> Result<Clause, GqliteError> {
        self.expect(&Token::With)?;
        let items = self.parse_return_items()?;
        Ok(Clause::With(WithClause { items }))
    }

    // ── ORDER BY ────────────────────────────────────────────────

    fn parse_order_by_clause(&mut self) -> Result<Clause, GqliteError> {
        self.expect(&Token::Order)?;
        self.expect(&Token::By)?;

        let mut items = vec![self.parse_order_by_item()?];
        while self.check(&Token::Comma) {
            self.advance();
            items.push(self.parse_order_by_item()?);
        }
        Ok(Clause::OrderBy(OrderByClause { items }))
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, GqliteError> {
        let expr = self.parse_expr()?;
        let descending = if self.check(&Token::Desc) {
            self.advance();
            true
        } else if self.check(&Token::Asc) {
            self.advance();
            false
        } else {
            false
        };
        Ok(OrderByItem { expr, descending })
    }

    // ── LIMIT / SKIP ────────────────────────────────────────────

    fn parse_limit_clause(&mut self) -> Result<Clause, GqliteError> {
        self.expect(&Token::Limit)?;
        let count = self.parse_expr()?;
        Ok(Clause::Limit(LimitClause { count }))
    }

    fn parse_skip_clause(&mut self) -> Result<Clause, GqliteError> {
        self.expect(&Token::Skip)?;
        let count = self.parse_expr()?;
        Ok(Clause::Skip(SkipClause { count }))
    }

    // ── CREATE (DML) ────────────────────────────────────────────

    fn parse_create_clause(&mut self) -> Result<Clause, GqliteError> {
        self.expect(&Token::Create)?;
        let pattern = self.parse_graph_pattern()?;
        Ok(Clause::Create(CreateClause { pattern }))
    }

    // ── SET ─────────────────────────────────────────────────────

    fn parse_set_clause(&mut self) -> Result<Clause, GqliteError> {
        self.expect(&Token::Set)?;
        let mut items = vec![self.parse_set_item()?];
        while self.check(&Token::Comma) {
            self.advance();
            items.push(self.parse_set_item()?);
        }
        Ok(Clause::Set(SetClause { items }))
    }

    fn parse_set_item(&mut self) -> Result<SetItem, GqliteError> {
        let variable = self.expect_ident()?;
        self.expect(&Token::Dot)?;
        let field = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        let value = self.parse_expr()?;
        Ok(SetItem {
            property: PropertyRef { variable, field },
            value,
        })
    }

    // ── DELETE ───────────────────────────────────────────────────

    fn parse_delete_clause(&mut self) -> Result<Clause, GqliteError> {
        let detach = self.check(&Token::Detach);
        if detach {
            self.advance();
        }
        self.expect(&Token::Delete)?;

        let mut exprs = vec![self.parse_expr()?];
        while self.check(&Token::Comma) {
            self.advance();
            exprs.push(self.parse_expr()?);
        }
        Ok(Clause::Delete(DeleteClause { detach, exprs }))
    }

    // ── DDL ─────────────────────────────────────────────────────

    fn parse_create_node_table(&mut self) -> Result<Statement, GqliteError> {
        self.expect(&Token::Create)?;
        self.expect(&Token::Node)?;
        self.expect(&Token::Table)?;
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;

        let mut columns = Vec::new();
        let mut primary_key = None;

        loop {
            if self.check(&Token::RParen) {
                break;
            }
            if self.check(&Token::Primary) {
                // PRIMARY KEY (col)
                self.advance();
                self.expect(&Token::Key)?;
                self.expect(&Token::LParen)?;
                primary_key = Some(self.expect_ident()?);
                self.expect(&Token::RParen)?;
            } else {
                let col_name = self.expect_ident()?;
                let data_type = self.parse_data_type()?;
                columns.push(ColumnDefAst {
                    name: col_name,
                    data_type,
                });
            }

            if !self.check(&Token::Comma) {
                break;
            }
            self.advance(); // consume comma
        }

        self.expect(&Token::RParen)?;

        let pk = primary_key
            .ok_or_else(|| self.error("CREATE NODE TABLE requires PRIMARY KEY"))?;

        Ok(Statement::CreateNodeTable(CreateNodeTableStmt {
            name,
            columns,
            primary_key: pk,
        }))
    }

    fn parse_create_rel_table(&mut self) -> Result<Statement, GqliteError> {
        self.expect(&Token::Create)?;
        self.expect(&Token::Rel)?;
        self.expect(&Token::Table)?;
        let name = self.expect_ident()?;
        self.expect(&Token::LParen)?;

        // FROM table TO table
        self.expect(&Token::From)?;
        let from_table = self.expect_ident()?;
        self.expect(&Token::To)?;
        let to_table = self.expect_ident()?;

        let mut columns = Vec::new();
        while self.check(&Token::Comma) {
            self.advance();
            if self.check(&Token::RParen) {
                break;
            }
            let col_name = self.expect_ident()?;
            let data_type = self.parse_data_type()?;
            columns.push(ColumnDefAst {
                name: col_name,
                data_type,
            });
        }

        self.expect(&Token::RParen)?;

        Ok(Statement::CreateRelTable(CreateRelTableStmt {
            name,
            from_table,
            to_table,
            columns,
        }))
    }

    fn parse_drop_table(&mut self) -> Result<Statement, GqliteError> {
        self.expect(&Token::Drop)?;
        self.expect(&Token::Table)?;
        let name = self.expect_ident()?;
        Ok(Statement::DropTable(DropTableStmt { name }))
    }

    fn parse_data_type(&mut self) -> Result<DataType, GqliteError> {
        match self.peek() {
            Token::TypeInt64 => {
                self.advance();
                Ok(DataType::Int64)
            }
            Token::TypeDouble => {
                self.advance();
                Ok(DataType::Double)
            }
            Token::TypeString => {
                self.advance();
                Ok(DataType::String)
            }
            Token::TypeBool => {
                self.advance();
                Ok(DataType::Bool)
            }
            _ => Err(self.error("expected type (INT64, DOUBLE, STRING, BOOL)")),
        }
    }

    // ── Expression Parsing (Pratt-style) ────────────────────────

    pub fn parse_expr(&mut self) -> Result<Expr, GqliteError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, GqliteError> {
        let mut left = self.parse_and_expr()?;
        while self.check(&Token::Or) {
            self.advance();
            let right = self.parse_and_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::Or,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_and_expr(&mut self) -> Result<Expr, GqliteError> {
        let mut left = self.parse_not_expr()?;
        while self.check(&Token::And) {
            self.advance();
            let right = self.parse_not_expr()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op: BinOp::And,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_not_expr(&mut self) -> Result<Expr, GqliteError> {
        if self.check(&Token::Not) {
            self.advance();
            let expr = self.parse_comparison()?;
            Ok(Expr::UnaryOp {
                op: UnaryOp::Not,
                expr: Box::new(expr),
            })
        } else {
            self.parse_comparison()
        }
    }

    fn parse_comparison(&mut self) -> Result<Expr, GqliteError> {
        let left = self.parse_addition()?;

        // IS [NOT] NULL
        if self.check(&Token::Is) {
            self.advance();
            let negated = self.check(&Token::Not);
            if negated {
                self.advance();
            }
            self.expect(&Token::Null)?;
            return Ok(Expr::IsNull {
                expr: Box::new(left),
                negated,
            });
        }

        let op = match self.peek() {
            Token::Eq => Some(BinOp::Eq),
            Token::Neq | Token::BangEq => Some(BinOp::Neq),
            Token::Lt => Some(BinOp::Lt),
            Token::Gt => Some(BinOp::Gt),
            Token::Le => Some(BinOp::Le),
            Token::Ge => Some(BinOp::Ge),
            _ => None,
        };

        if let Some(op) = op {
            self.advance();
            let right = self.parse_addition()?;
            Ok(Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            })
        } else {
            Ok(left)
        }
    }

    fn parse_addition(&mut self) -> Result<Expr, GqliteError> {
        let mut left = self.parse_multiplication()?;
        loop {
            let op = match self.peek() {
                Token::Plus => BinOp::Add,
                Token::Dash => BinOp::Sub,
                _ => break,
            };
            self.advance();
            let right = self.parse_multiplication()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_multiplication(&mut self) -> Result<Expr, GqliteError> {
        let mut left = self.parse_unary()?;
        loop {
            let op = match self.peek() {
                Token::Star => BinOp::Mul,
                Token::Slash => BinOp::Div,
                Token::Percent => BinOp::Mod,
                _ => break,
            };
            self.advance();
            let right = self.parse_unary()?;
            left = Expr::BinaryOp {
                left: Box::new(left),
                op,
                right: Box::new(right),
            };
        }
        Ok(left)
    }

    fn parse_unary(&mut self) -> Result<Expr, GqliteError> {
        if self.check(&Token::Dash) {
            self.advance();
            let expr = self.parse_primary()?;
            Ok(Expr::UnaryOp {
                op: UnaryOp::Neg,
                expr: Box::new(expr),
            })
        } else {
            self.parse_primary()
        }
    }

    fn parse_primary(&mut self) -> Result<Expr, GqliteError> {
        match self.peek().clone() {
            Token::IntLit(v) => {
                self.advance();
                Ok(Expr::IntLit(v))
            }
            Token::FloatLit(v) => {
                self.advance();
                Ok(Expr::FloatLit(v))
            }
            Token::StringLit(s) => {
                self.advance();
                Ok(Expr::StringLit(s))
            }
            Token::True => {
                self.advance();
                Ok(Expr::BoolLit(true))
            }
            Token::False => {
                self.advance();
                Ok(Expr::BoolLit(false))
            }
            Token::Null => {
                self.advance();
                Ok(Expr::NullLit)
            }
            Token::Param(name) => {
                self.advance();
                Ok(Expr::Param(name))
            }
            Token::Star => {
                self.advance();
                Ok(Expr::Star)
            }
            Token::Ident(name) => {
                self.advance();
                // Check for function call: name(...)
                if self.check(&Token::LParen) {
                    return self.parse_function_call(name);
                }
                // Property chain: a.b.c
                let mut expr = Expr::Ident(name);
                while self.check(&Token::Dot) {
                    self.advance();
                    let field = self.expect_ident()?;
                    expr = Expr::Property(Box::new(expr), field);
                }
                Ok(expr)
            }
            Token::LParen => {
                self.advance();
                let expr = self.parse_expr()?;
                self.expect(&Token::RParen)?;
                Ok(expr)
            }
            _ => Err(self.error(&format!("unexpected token: {:?}", self.peek()))),
        }
    }

    fn parse_function_call(&mut self, name: String) -> Result<Expr, GqliteError> {
        self.expect(&Token::LParen)?;

        // count(*) special case
        if self.check(&Token::Star) {
            self.advance();
            self.expect(&Token::RParen)?;
            return Ok(Expr::FunctionCall {
                name,
                distinct: false,
                args: vec![Expr::Star],
            });
        }

        if self.check(&Token::RParen) {
            self.advance();
            return Ok(Expr::FunctionCall {
                name,
                distinct: false,
                args: Vec::new(),
            });
        }

        let distinct = self.check(&Token::Distinct);
        if distinct {
            self.advance();
        }

        let mut args = vec![self.parse_expr()?];
        while self.check(&Token::Comma) {
            self.advance();
            args.push(self.parse_expr()?);
        }
        self.expect(&Token::RParen)?;

        Ok(Expr::FunctionCall {
            name,
            distinct,
            args,
        })
    }

    // ── Token helpers ───────────────────────────────────────────

    fn peek(&self) -> &Token {
        self.tokens.get(self.pos).unwrap_or(&Token::Eof)
    }

    fn peek_at(&self, offset: usize) -> &Token {
        self.tokens.get(self.pos + offset).unwrap_or(&Token::Eof)
    }

    fn advance(&mut self) -> &Token {
        let tok = self.tokens.get(self.pos).unwrap_or(&Token::Eof);
        self.pos += 1;
        tok
    }

    fn check(&self, expected: &Token) -> bool {
        std::mem::discriminant(self.peek()) == std::mem::discriminant(expected)
    }

    fn expect(&mut self, expected: &Token) -> Result<(), GqliteError> {
        if self.check(expected) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("expected {:?}, got {:?}", expected, self.peek())))
        }
    }

    fn expect_ident(&mut self) -> Result<String, GqliteError> {
        match self.peek().clone() {
            Token::Ident(name) => {
                self.advance();
                Ok(name)
            }
            _ => Err(self.error(&format!("expected identifier, got {:?}", self.peek()))),
        }
    }

    fn error(&self, msg: &str) -> GqliteError {
        GqliteError::Parse(format!("at position {}: {}", self.pos, msg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(input: &str) -> Statement {
        Parser::parse_query(input).unwrap()
    }

    fn parse_err(input: &str) -> String {
        Parser::parse_query(input).unwrap_err().to_string()
    }

    // ── Expression tests (Plan 016) ─────────────────────────────

    #[test]
    fn expr_literal_types() {
        let stmt = parse("RETURN 42, 3.14, 'hello', true, false, null");
        let Statement::Query(q) = stmt else {
            panic!("expected query");
        };
        let Clause::Return(ret) = &q.clauses[0] else {
            panic!("expected return");
        };
        assert!(matches!(ret.items[0].expr, Expr::IntLit(42)));
        assert!(matches!(ret.items[1].expr, Expr::FloatLit(_)));
        assert!(matches!(ret.items[2].expr, Expr::StringLit(_)));
        assert!(matches!(ret.items[3].expr, Expr::BoolLit(true)));
        assert!(matches!(ret.items[4].expr, Expr::BoolLit(false)));
        assert!(matches!(ret.items[5].expr, Expr::NullLit));
    }

    #[test]
    fn expr_operator_precedence() {
        // 1 + 2 * 3 → 1 + (2 * 3)
        let stmt = parse("RETURN 1 + 2 * 3");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Return(ret) = &q.clauses[0] else {
            panic!();
        };
        let Expr::BinaryOp { op, .. } = &ret.items[0].expr else {
            panic!();
        };
        assert_eq!(*op, BinOp::Add);
    }

    #[test]
    fn expr_and_or_precedence() {
        // a AND b OR c → (a AND b) OR c
        let stmt = parse("MATCH (n) WHERE n.a = 1 AND n.b = 2 OR n.c = 3 RETURN n");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Where(w) = &q.clauses[1] else {
            panic!();
        };
        assert!(matches!(w.expr, Expr::BinaryOp { op: BinOp::Or, .. }));
    }

    #[test]
    fn expr_is_null() {
        let stmt = parse("MATCH (n) WHERE n.value IS NOT NULL RETURN n");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Where(w) = &q.clauses[1] else {
            panic!();
        };
        let Expr::IsNull { negated, .. } = &w.expr else {
            panic!();
        };
        assert!(*negated);
    }

    #[test]
    fn expr_function_call() {
        let stmt = parse("RETURN count(DISTINCT n.name)");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Return(ret) = &q.clauses[0] else {
            panic!();
        };
        let Expr::FunctionCall {
            name, distinct, ..
        } = &ret.items[0].expr
        else {
            panic!();
        };
        assert_eq!(name, "count");
        assert!(*distinct);
    }

    #[test]
    fn expr_count_star() {
        let stmt = parse("RETURN count(*)");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Return(ret) = &q.clauses[0] else {
            panic!();
        };
        let Expr::FunctionCall { args, .. } = &ret.items[0].expr else {
            panic!();
        };
        assert!(matches!(args[0], Expr::Star));
    }

    // ── MATCH tests (Plan 017) ──────────────────────────────────

    #[test]
    fn match_simple_node() {
        let stmt = parse("MATCH (n) RETURN n");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Match(m) = &q.clauses[0] else {
            panic!();
        };
        assert_eq!(m.pattern.paths.len(), 1);
    }

    #[test]
    fn match_labeled_node() {
        let stmt = parse("MATCH (a:Person) RETURN a");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Match(m) = &q.clauses[0] else {
            panic!();
        };
        let PatternElement::Node(n) = &m.pattern.paths[0].elements[0] else {
            panic!();
        };
        assert_eq!(n.alias.as_deref(), Some("a"));
        assert_eq!(n.label.as_deref(), Some("Person"));
    }

    #[test]
    fn match_with_properties() {
        let stmt = parse("MATCH (a:Person {name: 'Alice'}) RETURN a");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Match(m) = &q.clauses[0] else {
            panic!();
        };
        let PatternElement::Node(n) = &m.pattern.paths[0].elements[0] else {
            panic!();
        };
        assert_eq!(n.properties.len(), 1);
        assert_eq!(n.properties[0].0, "name");
    }

    #[test]
    fn match_directed_relationship() {
        let stmt = parse("MATCH (a)-[r:KNOWS]->(b) RETURN a, b");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Match(m) = &q.clauses[0] else {
            panic!();
        };
        let path = &m.pattern.paths[0];
        assert_eq!(path.elements.len(), 3); // node, rel, node
        let PatternElement::Rel(r) = &path.elements[1] else {
            panic!();
        };
        assert_eq!(r.direction, Direction::Right);
        assert_eq!(r.label.as_deref(), Some("KNOWS"));
    }

    #[test]
    fn match_undirected() {
        let stmt = parse("MATCH (a)-[r]-(b) RETURN a");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Match(m) = &q.clauses[0] else {
            panic!();
        };
        let PatternElement::Rel(r) = &m.pattern.paths[0].elements[1] else {
            panic!();
        };
        assert_eq!(r.direction, Direction::Both);
    }

    #[test]
    fn match_multiple_patterns() {
        let stmt =
            parse("MATCH (a)-[:KNOWS]->(b), (b)-[:LIVES_IN]->(c) RETURN a, b, c");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Match(m) = &q.clauses[0] else {
            panic!();
        };
        assert_eq!(m.pattern.paths.len(), 2);
    }

    // ── WHERE / RETURN tests (Plan 018) ─────────────────────────

    #[test]
    fn where_clause() {
        let stmt = parse("MATCH (a:Person) WHERE a.age > 30 AND a.name = 'Alice' RETURN a.name");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        assert_eq!(q.clauses.len(), 3); // MATCH, WHERE, RETURN
        assert!(matches!(q.clauses[1], Clause::Where(_)));
    }

    #[test]
    fn return_with_alias() {
        let stmt = parse("RETURN a.name AS name, count(a) AS cnt");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Return(ret) = &q.clauses[0] else {
            panic!();
        };
        assert_eq!(ret.items[0].alias.as_deref(), Some("name"));
        assert_eq!(ret.items[1].alias.as_deref(), Some("cnt"));
    }

    #[test]
    fn return_star() {
        let stmt = parse("MATCH (n) RETURN *");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Return(ret) = &q.clauses[1] else {
            panic!();
        };
        assert!(ret.return_all);
    }

    #[test]
    fn return_distinct() {
        let stmt = parse("MATCH (n) RETURN DISTINCT n.city");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Return(ret) = &q.clauses[1] else {
            panic!();
        };
        assert!(ret.distinct);
    }

    // ── CREATE / SET / DELETE tests (Plan 019) ──────────────────

    #[test]
    fn create_node() {
        let stmt = parse("CREATE (n:Person {name: 'Alice'})");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Create(c) = &q.clauses[0] else {
            panic!();
        };
        let PatternElement::Node(n) = &c.pattern.paths[0].elements[0] else {
            panic!();
        };
        assert_eq!(n.label.as_deref(), Some("Person"));
    }

    #[test]
    fn create_relationship() {
        let stmt = parse("MATCH (a), (b) CREATE (a)-[:KNOWS]->(b)");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        assert_eq!(q.clauses.len(), 2); // MATCH + CREATE
        assert!(matches!(q.clauses[1], Clause::Create(_)));
    }

    #[test]
    fn set_property() {
        let stmt = parse("MATCH (n) SET n.name = 'Bob'");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Set(s) = &q.clauses[1] else {
            panic!();
        };
        assert_eq!(s.items[0].property.variable, "n");
        assert_eq!(s.items[0].property.field, "name");
    }

    #[test]
    fn detach_delete() {
        let stmt = parse("MATCH (n) DETACH DELETE n");
        let Statement::Query(q) = stmt else {
            panic!();
        };
        let Clause::Delete(d) = &q.clauses[1] else {
            panic!();
        };
        assert!(d.detach);
    }

    // ── DDL tests (Plan 020) ────────────────────────────────────

    #[test]
    fn create_node_table() {
        let stmt = parse(
            "CREATE NODE TABLE Person (id INT64, name STRING, age INT64, PRIMARY KEY (id))",
        );
        let Statement::CreateNodeTable(t) = stmt else {
            panic!("expected CreateNodeTable, got {:?}", stmt);
        };
        assert_eq!(t.name, "Person");
        assert_eq!(t.columns.len(), 3);
        assert_eq!(t.primary_key, "id");
        assert_eq!(t.columns[0].name, "id");
        assert_eq!(t.columns[0].data_type, DataType::Int64);
        assert_eq!(t.columns[1].data_type, DataType::String);
    }

    #[test]
    fn create_rel_table() {
        let stmt = parse("CREATE REL TABLE Knows (FROM Person TO Person, since INT64)");
        let Statement::CreateRelTable(t) = stmt else {
            panic!("expected CreateRelTable, got {:?}", stmt);
        };
        assert_eq!(t.name, "Knows");
        assert_eq!(t.from_table, "Person");
        assert_eq!(t.to_table, "Person");
        assert_eq!(t.columns.len(), 1);
        assert_eq!(t.columns[0].name, "since");
    }

    #[test]
    fn drop_table() {
        let stmt = parse("DROP TABLE Person");
        let Statement::DropTable(t) = stmt else {
            panic!();
        };
        assert_eq!(t.name, "Person");
    }

    #[test]
    fn create_rel_table_no_props() {
        let stmt = parse("CREATE REL TABLE Follows (FROM Person TO Person)");
        let Statement::CreateRelTable(t) = stmt else {
            panic!();
        };
        assert!(t.columns.is_empty());
    }

    // ── Full query integration ──────────────────────────────────

    #[test]
    fn full_query_pipeline() {
        let stmt = parse(
            "MATCH (a:Person) WHERE a.age > 30 RETURN a.name AS name ORDER BY a.name LIMIT 10",
        );
        let Statement::Query(q) = stmt else {
            panic!();
        };
        assert_eq!(q.clauses.len(), 5); // MATCH, WHERE, RETURN, ORDER BY, LIMIT
    }

    #[test]
    fn parse_error_message() {
        let err = parse_err("MATCH");
        assert!(err.contains("parse error"));
    }
}
