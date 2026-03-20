use thiserror::Error;

/// Error code categories for stable programmatic error handling.
///
/// | Range | Category |
/// |-------|----------|
/// | 1xxx | User error (syntax, type mismatch, constraint violation) |
/// | 2xxx | Transaction error (lock conflict, rollback) |
/// | 3xxx | System error (I/O, internal) |
/// | 4xxx | Corruption error (checksum, format) |
/// | 5xxx | Limit error (capacity exceeded) |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorCode {
    // 1xxx — User errors
    SyntaxError = 1001,
    TableNotFound = 1002,
    ColumnNotFound = 1003,
    TypeMismatch = 1004,
    DuplicateTable = 1005,
    DuplicatePrimaryKey = 1006,
    DuplicateRelationship = 1007,
    ConstraintViolation = 1008,

    // 2xxx — Transaction errors
    TransactionConflict = 2001,
    ReadOnlyViolation = 2002,
    InvalidTransactionState = 2003,

    // 3xxx — System errors
    IoError = 3001,
    InternalError = 3002,
    SerializationError = 3003,

    // 4xxx — Corruption errors
    ChecksumMismatch = 4001,
    InvalidFileFormat = 4002,
    CorruptedData = 4003,

    // 5xxx — Limit errors
    CapacityExceeded = 5001,
}

/// Unified error type for gqlite.
#[derive(Debug, Error)]
pub enum GqliteError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("parse error: {0}")]
    Parse(String),

    #[error("catalog error: {0}")]
    Catalog(String),

    #[error("storage error: {0}")]
    Storage(String),

    #[error("execution error: {0}")]
    Execution(String),

    #[error("transaction error: {0}")]
    Transaction(String),

    #[error("{0}")]
    Other(String),
}

impl GqliteError {
    /// Return the numeric error code for programmatic handling.
    pub fn error_code(&self) -> ErrorCode {
        match self {
            GqliteError::Io(_) => ErrorCode::IoError,
            GqliteError::Parse(_) => ErrorCode::SyntaxError,
            GqliteError::Catalog(_) => ErrorCode::TableNotFound,
            GqliteError::Storage(msg) => {
                if msg.contains("checksum") {
                    ErrorCode::ChecksumMismatch
                } else if msg.contains("format") || msg.contains("magic") {
                    ErrorCode::InvalidFileFormat
                } else {
                    ErrorCode::InternalError
                }
            }
            GqliteError::Execution(msg) => {
                if msg.contains("duplicate primary key") {
                    ErrorCode::DuplicatePrimaryKey
                } else if msg.contains("duplicate relationship") {
                    ErrorCode::DuplicateRelationship
                } else if msg.contains("not found") {
                    ErrorCode::ColumnNotFound
                } else if msg.contains("read-only") {
                    ErrorCode::ReadOnlyViolation
                } else {
                    ErrorCode::InternalError
                }
            }
            GqliteError::Transaction(_) => ErrorCode::TransactionConflict,
            GqliteError::Other(_) => ErrorCode::InternalError,
        }
    }
}

impl From<gqlite_parser::ParseError> for GqliteError {
    fn from(e: gqlite_parser::ParseError) -> Self {
        GqliteError::Parse(e.to_string())
    }
}
