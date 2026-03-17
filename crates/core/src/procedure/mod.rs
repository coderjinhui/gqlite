//! Procedure framework for CALL procedure_name(args) YIELD col1, col2.
//!
//! Provides a trait for built-in procedures and a registry for managing them.

pub mod graph_algo;
pub mod registry;

use crate::error::GqliteError;
use crate::types::value::Value;

/// A single row returned by a procedure.
pub type ProcedureRow = Vec<Value>;

/// Trait for built-in procedures that return tabular results.
pub trait Procedure: Send + Sync {
    /// The fully-qualified name of this procedure (e.g., "dbms.tables").
    fn name(&self) -> &str;

    /// Column names in the output.
    fn output_columns(&self) -> Vec<String>;

    /// Execute the procedure with the given arguments.
    fn execute(
        &self,
        args: &[Value],
        db: &crate::DatabaseInner,
    ) -> Result<Vec<ProcedureRow>, GqliteError>;
}
