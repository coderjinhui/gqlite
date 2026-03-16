//! gqlite-parser — Cypher query language parser for gqlite.
//!
//! This crate provides a standalone lexer, AST, and recursive-descent parser
//! for a Cypher subset. It has no dependency on the gqlite storage engine and
//! can be used independently.

pub mod ast;
pub mod data_type;
#[allow(clippy::module_inception)]
pub mod parser;
pub mod token;

// Re-export key types at crate root for convenience.
pub use data_type::DataType;
pub use parser::Parser;

use thiserror::Error;

/// Parse error returned by the lexer or parser.
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("parse error: {0}")]
    Parse(String),

    #[error("lex error: {0}")]
    Lex(String),
}
