use gqlite_parser::token::{tokenize, Token};

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
#[allow(clippy::approx_constant)]
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
