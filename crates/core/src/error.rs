use thiserror::Error;

/// Unified error type for gqlite.
#[derive(Debug, Error)]
pub enum GqliteError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("transaction error: {0}")]
    Transaction(String),

    #[error("{0}")]
    Other(String),
}
