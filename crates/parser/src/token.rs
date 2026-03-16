use logos::Logos;

/// Helper callback: if a regex-matched identifier is actually a keyword, skip it
/// (logos will match the keyword variant instead). Otherwise emit as Ident.
fn parse_ident_or_keyword(lex: &mut logos::Lexer<Token>) -> logos::FilterResult<String, ()> {
    let slice = lex.slice();
    match slice.to_uppercase().as_str() {
        "MATCH" | "OPTIONAL" | "WHERE" | "RETURN" | "WITH" | "ORDER" | "BY" | "ASC"
        | "DESC" | "LIMIT" | "SKIP" | "CREATE" | "SET" | "DELETE" | "DETACH" | "MERGE"
        | "NODE" | "REL" | "TABLE" | "DROP" | "PRIMARY" | "KEY" | "AND" | "OR" | "NOT"
        | "IS" | "NULL" | "TRUE" | "FALSE" | "AS" | "IN" | "EXISTS" | "DISTINCT"
        | "UNION" | "ALL" | "UNWIND" | "ON" | "BEGIN" | "COMMIT" | "ROLLBACK" | "CASE"
        | "WHEN" | "THEN" | "ELSE" | "END" | "FROM" | "TO" | "INT64" | "DOUBLE"
        | "STRING" | "BOOL" | "SERIAL" | "ALTER" | "ADD" | "RENAME" | "COLUMN"
        | "COPY" | "HEADER" | "DELIMITER" | "CAST" => logos::FilterResult::Skip,
        _ => logos::FilterResult::Emit(slice.to_string()),
    }
}

fn parse_string_lit(lex: &mut logos::Lexer<Token>) -> Option<String> {
    let slice = lex.slice();
    let inner = &slice[1..slice.len() - 1];
    let mut result = String::new();
    let mut chars = inner.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => result.push('\n'),
                Some('t') => result.push('\t'),
                Some('\\') => result.push('\\'),
                Some('\'') => result.push('\''),
                Some('"') => result.push('"'),
                Some(other) => {
                    result.push('\\');
                    result.push(other);
                }
                None => result.push('\\'),
            }
        } else {
            result.push(c);
        }
    }
    Some(result)
}

/// All tokens recognized by the gqlite lexer.
#[derive(Logos, Debug, Clone, PartialEq)]
#[logos(skip r"[ \t\r\n\f]+")]
#[logos(skip r"//[^\n]*")]
#[logos(skip r"/\*([^*]|\*[^/])*\*/")]
pub enum Token {
    // ── Keywords (case-insensitive) ───────────────────────
    #[regex("(?i)match", priority = 10)]
    Match,
    #[regex("(?i)optional", priority = 10)]
    Optional,
    #[regex("(?i)where", priority = 10)]
    Where,
    #[regex("(?i)return", priority = 10)]
    Return,
    #[regex("(?i)with", priority = 10)]
    With,
    #[regex("(?i)order", priority = 10)]
    Order,
    #[regex("(?i)by", priority = 10)]
    By,
    #[regex("(?i)asc", priority = 10)]
    Asc,
    #[regex("(?i)desc", priority = 10)]
    Desc,
    #[regex("(?i)limit", priority = 10)]
    Limit,
    #[regex("(?i)skip", priority = 10)]
    Skip,
    #[regex("(?i)create", priority = 10)]
    Create,
    #[regex("(?i)set", priority = 10)]
    Set,
    #[regex("(?i)delete", priority = 10)]
    Delete,
    #[regex("(?i)detach", priority = 10)]
    Detach,
    #[regex("(?i)merge", priority = 10)]
    Merge,
    #[regex("(?i)node", priority = 10)]
    Node,
    #[regex("(?i)rel", priority = 10)]
    Rel,
    #[regex("(?i)table", priority = 10)]
    Table,
    #[regex("(?i)drop", priority = 10)]
    Drop,
    #[regex("(?i)primary", priority = 10)]
    Primary,
    #[regex("(?i)key", priority = 10)]
    Key,
    #[regex("(?i)and", priority = 10)]
    And,
    #[regex("(?i)or", priority = 10)]
    Or,
    #[regex("(?i)not", priority = 10)]
    Not,
    #[regex("(?i)is", priority = 10)]
    Is,
    #[regex("(?i)null", priority = 10)]
    Null,
    #[regex("(?i)true", priority = 10)]
    True,
    #[regex("(?i)false", priority = 10)]
    False,
    #[regex("(?i)as", priority = 10)]
    As,
    #[regex("(?i)in", priority = 10)]
    In,
    #[regex("(?i)exists", priority = 10)]
    Exists,
    #[regex("(?i)distinct", priority = 10)]
    Distinct,
    #[regex("(?i)union", priority = 10)]
    Union,
    #[regex("(?i)all", priority = 10)]
    All,
    #[regex("(?i)unwind", priority = 10)]
    Unwind,
    #[regex("(?i)begin", priority = 10)]
    Begin,
    #[regex("(?i)commit", priority = 10)]
    Commit,
    #[regex("(?i)rollback", priority = 10)]
    Rollback,
    #[regex("(?i)case", priority = 10)]
    Case,
    #[regex("(?i)when", priority = 10)]
    When,
    #[regex("(?i)then", priority = 10)]
    Then,
    #[regex("(?i)else", priority = 10)]
    Else,
    #[regex("(?i)end", priority = 10)]
    End,
    #[regex("(?i)from", priority = 10)]
    From,
    #[regex("(?i)to", priority = 10)]
    To,
    #[regex("(?i)on", priority = 10)]
    On,
    #[regex("(?i)alter", priority = 10)]
    Alter,
    #[regex("(?i)add", priority = 10)]
    Add,
    #[regex("(?i)rename", priority = 10)]
    Rename,
    #[regex("(?i)column", priority = 10)]
    Column,
    #[regex("(?i)copy", priority = 10)]
    Copy,
    #[regex("(?i)header", priority = 10)]
    Header,
    #[regex("(?i)delimiter", priority = 10)]
    Delimiter,
    #[regex("(?i)cast", priority = 10)]
    Cast,

    // Type keywords
    #[regex("(?i)int64", priority = 10)]
    TypeInt64,
    #[regex("(?i)double", priority = 10)]
    TypeDouble,
    #[regex("(?i)string", priority = 10)]
    TypeString,
    #[regex("(?i)bool", priority = 10)]
    TypeBool,
    #[regex("(?i)serial", priority = 10)]
    TypeSerial,

    // ── Literals ─────────────────────────────────────────
    #[regex(r"[0-9]+\.[0-9]+([eE][+-]?[0-9]+)?", |lex| lex.slice().parse::<f64>().ok())]
    FloatLit(f64),

    #[regex(r"[0-9]+", priority = 5, callback = |lex| lex.slice().parse::<i64>().ok())]
    IntLit(i64),

    #[regex(r"'([^'\\]|\\.)*'", parse_string_lit)]
    StringLit(String),

    #[regex(r"\$[a-zA-Z_][a-zA-Z0-9_]*", |lex| lex.slice()[1..].to_string())]
    Param(String),

    #[regex(r"[a-zA-Z_][a-zA-Z0-9_]*", parse_ident_or_keyword)]
    Ident(String),

    // ── Symbols ──────────────────────────────────────────
    #[token("(")]
    LParen,
    #[token(")")]
    RParen,
    #[token("[")]
    LBracket,
    #[token("]")]
    RBracket,
    #[token("{")]
    LBrace,
    #[token("}")]
    RBrace,
    #[token(":")]
    Colon,
    #[token(",")]
    Comma,
    #[token("..")]
    DotDot,
    #[token(".")]
    Dot,
    #[token("->")]
    Arrow,
    #[token("<-")]
    LeftArrow,
    #[token("-")]
    Dash,
    #[token("<>")]
    Neq,
    #[token("!=")]
    BangEq,
    #[token("<=")]
    Le,
    #[token(">=")]
    Ge,
    #[token("=")]
    Eq,
    #[token("<")]
    Lt,
    #[token(">")]
    Gt,
    #[token("*")]
    Star,
    #[token("+")]
    Plus,
    #[token("/")]
    Slash,
    #[token("%")]
    Percent,
    #[token(";")]
    Semicolon,
    #[token("|")]
    Pipe,

    /// Sentinel for end-of-input (not emitted by logos).
    Eof,
}

/// Tokenize input into a Vec of Tokens (with Eof appended).
pub fn tokenize(input: &str) -> Result<Vec<Token>, String> {
    let lexer = Token::lexer(input);
    let mut tokens = Vec::new();
    for result in lexer {
        match result {
            Ok(token) => tokens.push(token),
            Err(()) => return Err("unexpected character in input".into()),
        }
    }
    tokens.push(Token::Eof);
    Ok(tokens)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_match_return() {
        let tokens = tokenize("MATCH (n:Person) RETURN n.name").unwrap();
        assert_eq!(tokens[0], Token::Match);
        assert_eq!(tokens[1], Token::LParen);
        assert_eq!(tokens[2], Token::Ident("n".into()));
        assert_eq!(tokens[3], Token::Colon);
        assert_eq!(tokens[4], Token::Ident("Person".into()));
        assert_eq!(tokens[5], Token::RParen);
        assert_eq!(tokens[6], Token::Return);
        assert_eq!(tokens[7], Token::Ident("n".into()));
        assert_eq!(tokens[8], Token::Dot);
        assert_eq!(tokens[9], Token::Ident("name".into()));
    }

    #[test]
    fn case_insensitive() {
        let t1 = tokenize("match").unwrap();
        let t2 = tokenize("MATCH").unwrap();
        let t3 = tokenize("Match").unwrap();
        assert_eq!(t1[0], Token::Match);
        assert_eq!(t2[0], Token::Match);
        assert_eq!(t3[0], Token::Match);
    }

    #[test]
    fn string_literal_escape() {
        let tokens = tokenize(r"'hello\nworld'").unwrap();
        assert_eq!(tokens[0], Token::StringLit("hello\nworld".into()));
    }

    #[test]
    fn int_and_float() {
        let tokens = tokenize("42 3.14").unwrap();
        assert_eq!(tokens[0], Token::IntLit(42));
        assert_eq!(tokens[1], Token::FloatLit(3.14));
    }

    #[test]
    fn parameter_token() {
        let tokens = tokenize("$name").unwrap();
        assert_eq!(tokens[0], Token::Param("name".into()));
    }

    #[test]
    fn comments_skipped() {
        let tokens = tokenize("MATCH // comment\n(n) /* block */ RETURN n").unwrap();
        assert_eq!(tokens[0], Token::Match);
        assert_eq!(tokens[1], Token::LParen);
        assert_eq!(tokens[2], Token::Ident("n".into()));
        assert_eq!(tokens[3], Token::RParen);
        assert_eq!(tokens[4], Token::Return);
    }

    #[test]
    fn ddl_and_type_keywords() {
        let tokens = tokenize("CREATE NODE TABLE Person ( id INT64, PRIMARY KEY (id) )").unwrap();
        assert_eq!(tokens[0], Token::Create);
        assert_eq!(tokens[1], Token::Node);
        assert_eq!(tokens[2], Token::Table);
        assert_eq!(tokens[3], Token::Ident("Person".into()));
        assert_eq!(tokens[4], Token::LParen);
        assert_eq!(tokens[5], Token::Ident("id".into()));
        assert_eq!(tokens[6], Token::TypeInt64);
    }
}
