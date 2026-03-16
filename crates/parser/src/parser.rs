use crate::ParseError;
use crate::data_type::DataType;

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
    pub fn parse_query(input: &str) -> Result<Statement, ParseError> {
        let tokens = super::token::tokenize(input)
            .map_err(|e| ParseError::Lex(e))?;
        let mut parser = Parser::new(tokens);
        parser.parse()
    }

    /// Parse the token stream into a Statement.
    pub fn parse(&mut self) -> Result<Statement, ParseError> {
        // Skip leading semicolons
        while self.check(&Token::Semicolon) {
            self.advance();
        }

        let stmt = match self.peek() {
            Token::Call => self.parse_call_statement(),
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
            Token::Alter => self.parse_alter_table(),
            Token::Copy => self.parse_copy(),
            _ => self.parse_query_statement(),
        }?;

        // Consume optional trailing semicolons
        while self.check(&Token::Semicolon) {
            self.advance();
        }

        // Check for UNION
        if self.check(&Token::Union) {
            return self.parse_union(stmt);
        }

        Ok(stmt)
    }

    fn parse_union(&mut self, left: Statement) -> Result<Statement, ParseError> {
        self.expect(&Token::Union)?;
        let all = self.check(&Token::All);
        if all {
            self.advance();
        }
        let right = self.parse()?;
        Ok(Statement::Union {
            left: Box::new(left),
            right: Box::new(right),
            all,
        })
    }

    // ── CALL Statement ─────────────────────────────────────────

    fn parse_call_statement(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Call)?;
        // Procedure name (may have dots: dbms.tables)
        let mut name = self.expect_ident()?;
        while self.check(&Token::Dot) {
            self.advance();
            let part = self.expect_ident()?;
            name = format!("{}.{}", name, part);
        }
        // Arguments
        self.expect(&Token::LParen)?;
        let mut args = Vec::new();
        if !self.check(&Token::RParen) {
            args.push(self.parse_expr()?);
            while self.check(&Token::Comma) {
                self.advance();
                args.push(self.parse_expr()?);
            }
        }
        self.expect(&Token::RParen)?;
        // YIELD clause (optional)
        let mut yields = Vec::new();
        if self.check(&Token::Yield) {
            self.advance();
            yields.push(self.expect_ident()?);
            while self.check(&Token::Comma) {
                self.advance();
                yields.push(self.expect_ident()?);
            }
        }
        Ok(Statement::Call { procedure: name, args, yields })
    }

    // ── Query Statement ─────────────────────────────────────────

    fn parse_query_statement(&mut self) -> Result<Statement, ParseError> {
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
                Token::Unwind => clauses.push(self.parse_unwind_clause()?),
                Token::Merge => clauses.push(self.parse_merge_clause()?),
                _ => break,
            }
        }

        if clauses.is_empty() {
            return Err(self.error("expected a query clause"));
        }

        Ok(Statement::Query(QueryStatement { clauses }))
    }

    // ── MATCH ───────────────────────────────────────────────────

    fn parse_match_clause(&mut self) -> Result<Clause, ParseError> {
        let optional = self.check(&Token::Optional);
        if optional {
            self.advance();
        }
        self.expect(&Token::Match)?;

        let pattern = self.parse_graph_pattern()?;
        Ok(Clause::Match(MatchClause { optional, pattern }))
    }

    fn parse_graph_pattern(&mut self) -> Result<GraphPattern, ParseError> {
        let mut paths = Vec::new();
        let mut shortest_paths = Vec::new();

        // Parse first element (path or shortest-path assignment)
        self.parse_graph_pattern_element(&mut paths, &mut shortest_paths)?;

        while self.check(&Token::Comma) {
            self.advance();
            self.parse_graph_pattern_element(&mut paths, &mut shortest_paths)?;
        }

        Ok(GraphPattern { paths, shortest_paths })
    }

    /// Parse a single element in a comma-separated graph pattern.
    /// It can be a regular path pattern `(a)-[:R]->(b)`, or a shortest-path
    /// assignment `p = shortestPath((a)-[:R*..N]->(b))`.
    fn parse_graph_pattern_element(
        &mut self,
        paths: &mut Vec<PathPattern>,
        shortest_paths: &mut Vec<ShortestPathPattern>,
    ) -> Result<(), ParseError> {
        // Check for `ident = shortestPath(...)` or `ident = allShortestPaths(...)`
        if let Token::Ident(_) = self.peek() {
            if self.peek_at(1) == &Token::Eq {
                // Look ahead: is the token after `=` an ident that matches shortestPath/allShortestPaths?
                if let Token::Ident(func_name) = self.peek_at(2) {
                    let lower = func_name.to_lowercase();
                    if lower == "shortestpath" || lower == "allshortestpaths" {
                        let sp = self.parse_shortest_path_pattern()?;
                        shortest_paths.push(sp);
                        return Ok(());
                    }
                }
            }
        }
        paths.push(self.parse_path_pattern()?);
        Ok(())
    }

    /// Parse `variable = shortestPath((pattern))` or `variable = allShortestPaths((pattern))`.
    fn parse_shortest_path_pattern(&mut self) -> Result<ShortestPathPattern, ParseError> {
        let path_variable = self.expect_ident()?;
        self.expect(&Token::Eq)?;
        let func_name = self.expect_ident()?;
        let all_paths = func_name.to_lowercase() == "allshortestpaths";
        self.expect(&Token::LParen)?;
        let pattern = self.parse_path_pattern()?;
        self.expect(&Token::RParen)?;
        Ok(ShortestPathPattern {
            path_variable,
            pattern,
            all_paths,
        })
    }

    fn parse_path_pattern(&mut self) -> Result<PathPattern, ParseError> {
        let mut elements = vec![PatternElement::Node(self.parse_node_pattern()?)];

        // rel + node pairs
        while self.is_rel_start() {
            elements.push(PatternElement::Rel(self.parse_rel_pattern()?));
            elements.push(PatternElement::Node(self.parse_node_pattern()?));
        }

        Ok(PathPattern { elements })
    }

    fn parse_node_pattern(&mut self) -> Result<NodePattern, ParseError> {
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

    fn parse_rel_pattern(&mut self) -> Result<RelPattern, ParseError> {
        let direction;
        let alias;
        let label;
        let properties;
        let var_length;

        if self.check(&Token::LeftArrow) {
            // <-[...]-
            self.advance(); // <-
            self.expect(&Token::LBracket)?;
            let (a, l, p, vl) = self.parse_rel_inner()?;
            alias = a;
            label = l;
            properties = p;
            var_length = vl;
            self.expect(&Token::RBracket)?;
            self.expect(&Token::Dash)?;
            direction = Direction::Left;
        } else {
            // -[...]-> or -[...]-
            self.expect(&Token::Dash)?;
            self.expect(&Token::LBracket)?;
            let (a, l, p, vl) = self.parse_rel_inner()?;
            alias = a;
            label = l;
            properties = p;
            var_length = vl;
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
            var_length,
        })
    }

    fn parse_rel_inner(
        &mut self,
    ) -> Result<(Option<String>, Option<String>, Vec<(String, Expr)>, Option<(u32, u32)>), ParseError> {
        let mut alias = None;
        let mut label = None;
        let mut properties = Vec::new();
        let mut var_length = None;

        if let Token::Ident(_) = self.peek() {
            alias = Some(self.expect_ident()?);
        }
        if self.check(&Token::Colon) {
            self.advance();
            label = Some(self.expect_ident()?);
        }
        // Variable-length: *  or  *min..max  or  *..max  or  *min..
        if self.check(&Token::Star) {
            self.advance(); // consume *
            let mut min_hops: u32 = 1;
            let mut max_hops: u32 = u32::MAX;

            if let Token::IntLit(n) = self.peek() {
                min_hops = *n as u32;
                self.advance();
            }
            if self.check(&Token::DotDot) {
                self.advance(); // consume ..
                if let Token::IntLit(n) = self.peek() {
                    max_hops = *n as u32;
                    self.advance();
                }
            } else {
                // *N means exactly N hops (no ..)
                if min_hops != 1 || !matches!(self.peek(), Token::RBracket) {
                    max_hops = min_hops;
                }
            }
            var_length = Some((min_hops, max_hops));
        }
        if self.check(&Token::LBrace) {
            properties = self.parse_property_map()?;
        }

        Ok((alias, label, properties, var_length))
    }

    fn parse_property_map(&mut self) -> Result<Vec<(String, Expr)>, ParseError> {
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

    fn parse_where_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Where)?;
        let expr = self.parse_expr()?;
        Ok(Clause::Where(WhereClause { expr }))
    }

    // ── RETURN ──────────────────────────────────────────────────

    fn parse_return_clause(&mut self) -> Result<Clause, ParseError> {
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

    fn parse_return_items(&mut self) -> Result<Vec<ReturnItem>, ParseError> {
        let mut items = vec![self.parse_return_item()?];
        while self.check(&Token::Comma) {
            self.advance();
            items.push(self.parse_return_item()?);
        }
        Ok(items)
    }

    fn parse_return_item(&mut self) -> Result<ReturnItem, ParseError> {
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

    fn parse_with_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::With)?;
        let items = self.parse_return_items()?;
        Ok(Clause::With(WithClause { items }))
    }

    // ── ORDER BY ────────────────────────────────────────────────

    fn parse_order_by_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Order)?;
        self.expect(&Token::By)?;

        let mut items = vec![self.parse_order_by_item()?];
        while self.check(&Token::Comma) {
            self.advance();
            items.push(self.parse_order_by_item()?);
        }
        Ok(Clause::OrderBy(OrderByClause { items }))
    }

    fn parse_order_by_item(&mut self) -> Result<OrderByItem, ParseError> {
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

    fn parse_limit_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Limit)?;
        let count = self.parse_expr()?;
        Ok(Clause::Limit(LimitClause { count }))
    }

    fn parse_skip_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Skip)?;
        let count = self.parse_expr()?;
        Ok(Clause::Skip(SkipClause { count }))
    }

    // ── CREATE (DML) ────────────────────────────────────────────

    fn parse_create_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Create)?;
        let pattern = self.parse_graph_pattern()?;
        Ok(Clause::Create(CreateClause { pattern }))
    }

    // ── SET ─────────────────────────────────────────────────────

    fn parse_set_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Set)?;
        let mut items = vec![self.parse_set_item()?];
        while self.check(&Token::Comma) {
            self.advance();
            items.push(self.parse_set_item()?);
        }
        Ok(Clause::Set(SetClause { items }))
    }

    fn parse_set_item(&mut self) -> Result<SetItem, ParseError> {
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

    fn parse_delete_clause(&mut self) -> Result<Clause, ParseError> {
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

    // ── UNWIND ──────────────────────────────────────────────────

    fn parse_unwind_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Unwind)?;
        let expr = self.parse_expr()?;
        self.expect(&Token::As)?;
        let alias = self.expect_ident()?;
        Ok(Clause::Unwind(UnwindClause { expr, alias }))
    }

    // ── MERGE ───────────────────────────────────────────────────

    fn parse_merge_clause(&mut self) -> Result<Clause, ParseError> {
        self.expect(&Token::Merge)?;
        let pattern = self.parse_graph_pattern()?;

        let mut on_create = Vec::new();
        let mut on_match = Vec::new();

        // Parse optional ON CREATE SET / ON MATCH SET clauses
        while self.check(&Token::On) {
            self.advance();
            if self.check(&Token::Create) {
                self.advance();
                self.expect(&Token::Set)?;
                let mut items = vec![self.parse_set_item()?];
                while self.check(&Token::Comma) {
                    self.advance();
                    items.push(self.parse_set_item()?);
                }
                on_create = items;
            } else if self.check(&Token::Match) {
                self.advance();
                self.expect(&Token::Set)?;
                let mut items = vec![self.parse_set_item()?];
                while self.check(&Token::Comma) {
                    self.advance();
                    items.push(self.parse_set_item()?);
                }
                on_match = items;
            } else {
                return Err(self.error("expected CREATE or MATCH after ON"));
            }
        }

        Ok(Clause::Merge(MergeClause {
            pattern,
            on_create,
            on_match,
        }))
    }

    // ── DDL ─────────────────────────────────────────────────────

    fn parse_create_node_table(&mut self) -> Result<Statement, ParseError> {
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

    fn parse_create_rel_table(&mut self) -> Result<Statement, ParseError> {
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

    fn parse_drop_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Drop)?;
        self.expect(&Token::Table)?;
        let name = self.expect_ident()?;
        Ok(Statement::DropTable(DropTableStmt { name }))
    }

    fn parse_alter_table(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Alter)?;
        self.expect(&Token::Table)?;
        let table_name = self.expect_ident()?;

        let action = match self.peek() {
            Token::Add => {
                // ALTER TABLE t ADD col_name TYPE
                self.advance();
                // optional COLUMN keyword
                if self.check(&Token::Column) {
                    self.advance();
                }
                let col_name = self.expect_ident()?;
                let data_type = self.parse_data_type()?;
                AlterTableAction::AddColumn {
                    col: ColumnDefAst {
                        name: col_name,
                        data_type,
                    },
                }
            }
            Token::Drop => {
                // ALTER TABLE t DROP COLUMN col_name
                self.advance();
                // optional COLUMN keyword
                if self.check(&Token::Column) {
                    self.advance();
                }
                let col_name = self.expect_ident()?;
                AlterTableAction::DropColumn { col_name }
            }
            Token::Rename => {
                self.advance();
                if self.check(&Token::Column) {
                    // ALTER TABLE t RENAME COLUMN old TO new
                    self.advance();
                    let old_name = self.expect_ident()?;
                    self.expect(&Token::To)?;
                    let new_name = self.expect_ident()?;
                    AlterTableAction::RenameColumn { old_name, new_name }
                } else if self.check(&Token::To) {
                    // ALTER TABLE t RENAME TO new_name
                    self.advance();
                    let new_name = self.expect_ident()?;
                    AlterTableAction::RenameTable { new_name }
                } else {
                    return Err(self.error("expected COLUMN or TO after RENAME"));
                }
            }
            _ => return Err(self.error("expected ADD, DROP, or RENAME after ALTER TABLE")),
        };

        Ok(Statement::AlterTable(AlterTableStmt {
            table_name,
            action,
        }))
    }

    fn parse_copy(&mut self) -> Result<Statement, ParseError> {
        self.expect(&Token::Copy)?;

        // Check if it's COPY ... TO (export) or COPY ... FROM (import)
        // COPY (query) TO 'path' or COPY table FROM/TO 'path'
        if self.check(&Token::LParen) {
            // COPY (query) TO 'path'
            self.advance(); // consume (
            let query = self.parse_query_body()?;
            self.expect(&Token::RParen)?;
            self.expect(&Token::To)?;
            let file_path = self.expect_string_lit()?;
            let (header, delimiter) = self.parse_copy_options()?;
            return Ok(Statement::CopyTo(CopyToStmt {
                source: CopySource::Query(Box::new(query)),
                file_path,
                header,
                delimiter,
            }));
        }

        let table_name = self.expect_ident()?;

        if self.check(&Token::From) {
            self.advance();
            let file_path = self.expect_string_lit()?;
            let (header, delimiter) = self.parse_copy_options()?;
            Ok(Statement::CopyFrom(CopyFromStmt {
                table_name,
                file_path,
                header,
                delimiter,
            }))
        } else if self.check(&Token::To) {
            self.advance();
            let file_path = self.expect_string_lit()?;
            let (header, delimiter) = self.parse_copy_options()?;
            Ok(Statement::CopyTo(CopyToStmt {
                source: CopySource::Table(table_name),
                file_path,
                header,
                delimiter,
            }))
        } else {
            Err(self.error("expected FROM or TO after COPY <table>"))
        }
    }

    /// Parse optional WITH (HEADER, DELIMITER 'x') options.
    fn parse_copy_options(&mut self) -> Result<(bool, char), ParseError> {
        let mut header = true;
        let mut delimiter = ',';

        // Check for WITH keyword or just (
        if self.check(&Token::With) {
            self.advance();
        }
        if self.check(&Token::LParen) {
            self.advance();
            loop {
                if self.check(&Token::RParen) {
                    self.advance();
                    break;
                }
                if self.check(&Token::Header) {
                    self.advance();
                    // Optionally followed by = true/false
                    if self.check(&Token::Eq) {
                        self.advance();
                        if self.check(&Token::True) {
                            self.advance();
                            header = true;
                        } else if self.check(&Token::False) {
                            self.advance();
                            header = false;
                        }
                    }
                } else if self.check(&Token::Delimiter) {
                    self.advance();
                    // Expect delimiter character as a string literal
                    if self.check(&Token::Eq) {
                        self.advance();
                    }
                    let delim_str = self.expect_string_lit()?;
                    if let Some(c) = delim_str.chars().next() {
                        delimiter = c;
                    }
                } else {
                    self.advance(); // skip unknown options
                }
                if self.check(&Token::Comma) {
                    self.advance();
                }
            }
        }

        Ok((header, delimiter))
    }

    fn parse_data_type(&mut self) -> Result<DataType, ParseError> {
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
            Token::TypeSerial => {
                self.advance();
                Ok(DataType::Serial)
            }
            _ => Err(self.error("expected type (INT64, DOUBLE, STRING, BOOL, SERIAL)")),
        }
    }

    // ── Expression Parsing (Pratt-style) ────────────────────────

    pub fn parse_expr(&mut self) -> Result<Expr, ParseError> {
        self.parse_or_expr()
    }

    fn parse_or_expr(&mut self) -> Result<Expr, ParseError> {
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

    fn parse_and_expr(&mut self) -> Result<Expr, ParseError> {
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

    fn parse_not_expr(&mut self) -> Result<Expr, ParseError> {
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

    fn parse_comparison(&mut self) -> Result<Expr, ParseError> {
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

        // [NOT] IN [list]
        if self.check(&Token::In) {
            self.advance();
            let list = self.parse_addition()?;
            return Ok(Expr::In {
                expr: Box::new(left),
                list: Box::new(list),
                negated: false,
            });
        }
        if self.check(&Token::Not) && self.peek_at(1) == &Token::In {
            self.advance(); // consume NOT
            self.advance(); // consume IN
            let list = self.parse_addition()?;
            return Ok(Expr::In {
                expr: Box::new(left),
                list: Box::new(list),
                negated: true,
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

    fn parse_addition(&mut self) -> Result<Expr, ParseError> {
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

    fn parse_multiplication(&mut self) -> Result<Expr, ParseError> {
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

    fn parse_unary(&mut self) -> Result<Expr, ParseError> {
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

    fn parse_primary(&mut self) -> Result<Expr, ParseError> {
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
            Token::LBracket => self.parse_list_literal(),
            Token::Cast => {
                self.advance();
                self.expect(&Token::LParen)?;
                let expr = self.parse_expr()?;
                self.expect(&Token::As)?;
                let target_type = self.parse_data_type()?;
                self.expect(&Token::RParen)?;
                Ok(Expr::Cast {
                    expr: Box::new(expr),
                    target_type,
                })
            }
            Token::Case => self.parse_case_expr(),
            Token::Exists => self.parse_exists_expr(),
            _ => Err(self.error(&format!("unexpected token: {:?}", self.peek()))),
        }
    }

    fn parse_list_literal(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::LBracket)?;
        let mut items = Vec::new();
        if !self.check(&Token::RBracket) {
            items.push(self.parse_expr()?);
            while self.check(&Token::Comma) {
                self.advance();
                items.push(self.parse_expr()?);
            }
        }
        self.expect(&Token::RBracket)?;
        Ok(Expr::ListLit(items))
    }

    fn parse_function_call(&mut self, name: String) -> Result<Expr, ParseError> {
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

    fn parse_case_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::Case)?;

        // Simple form: CASE <operand> WHEN ...
        // Searched form: CASE WHEN ...
        let operand = if !self.check(&Token::When) {
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        let mut when_clauses = Vec::new();
        while self.check(&Token::When) {
            self.advance();
            let condition = self.parse_expr()?;
            self.expect(&Token::Then)?;
            let result = self.parse_expr()?;
            when_clauses.push((condition, result));
        }

        if when_clauses.is_empty() {
            return Err(self.error("CASE requires at least one WHEN clause"));
        }

        let else_result = if self.check(&Token::Else) {
            self.advance();
            Some(Box::new(self.parse_expr()?))
        } else {
            None
        };

        self.expect(&Token::End)?;

        Ok(Expr::Case {
            operand,
            when_clauses,
            else_result,
        })
    }

    /// Parse `EXISTS { <query-body> }`.
    fn parse_exists_expr(&mut self) -> Result<Expr, ParseError> {
        self.expect(&Token::Exists)?;
        self.expect(&Token::LBrace)?;
        let query = self.parse_query_body()?;
        self.expect(&Token::RBrace)?;
        Ok(Expr::Exists(Box::new(query)))
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

    fn expect(&mut self, expected: &Token) -> Result<(), ParseError> {
        if self.check(expected) {
            self.advance();
            Ok(())
        } else {
            Err(self.error(&format!("expected {:?}, got {:?}", expected, self.peek())))
        }
    }

    fn expect_ident(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            Token::Ident(name) => {
                self.advance();
                Ok(name)
            }
            _ => Err(self.error(&format!("expected identifier, got {:?}", self.peek()))),
        }
    }

    fn expect_string_lit(&mut self) -> Result<String, ParseError> {
        match self.peek().clone() {
            Token::StringLit(s) => {
                self.advance();
                Ok(s)
            }
            _ => Err(self.error(&format!("expected string literal, got {:?}", self.peek()))),
        }
    }

    /// Parse a query body (clauses only, no Statement wrapper).
    fn parse_query_body(&mut self) -> Result<QueryStatement, ParseError> {
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
                Token::Unwind => clauses.push(self.parse_unwind_clause()?),
                Token::Merge => clauses.push(self.parse_merge_clause()?),
                _ => break,
            }
        }
        if clauses.is_empty() {
            return Err(self.error("expected a query clause"));
        }
        Ok(QueryStatement { clauses })
    }

    fn error(&self, msg: &str) -> ParseError {
        ParseError::Parse(format!("at position {}: {}", self.pos, msg))
    }
}
