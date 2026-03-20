//! Procedure registry: stores and looks up built-in procedures by name.

use super::{Procedure, ProcedureRow};
use crate::error::GqliteError;
use crate::types::value::Value;
use std::collections::HashMap;

/// Registry holding all built-in procedures.
pub struct ProcedureRegistry {
    procedures: HashMap<String, Box<dyn Procedure>>,
}

impl ProcedureRegistry {
    /// Create a new registry with all built-in procedures pre-registered.
    pub fn new() -> Self {
        let mut reg = ProcedureRegistry { procedures: HashMap::new() };
        // Register built-in procedures
        reg.register(Box::new(DbmsTables));
        reg.register(Box::new(super::graph_algo::DegreeCentrality));
        reg.register(Box::new(super::graph_algo::Wcc));
        reg.register(Box::new(super::graph_algo::Dijkstra));
        reg.register(Box::new(super::graph_algo::PageRank));
        reg.register(Box::new(super::graph_algo::LabelPropagation));
        reg.register(Box::new(super::graph_algo::TriangleCount));
        reg.register(Box::new(super::graph_algo::Betweenness));
        reg
    }

    /// Register a procedure.
    pub fn register(&mut self, proc: Box<dyn Procedure>) {
        self.procedures.insert(proc.name().to_string(), proc);
    }

    /// Look up a procedure by name.
    pub fn get(&self, name: &str) -> Option<&dyn Procedure> {
        self.procedures.get(name).map(|p| p.as_ref())
    }
}

impl Default for ProcedureRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ── Built-in: dbms.tables ──────────────────────────────────────

/// Lists all tables (node + relationship) in the database.
///
/// Output columns: `name` (STRING), `type` (STRING: "NODE" or "REL").
struct DbmsTables;

impl Procedure for DbmsTables {
    fn name(&self) -> &str {
        "dbms.tables"
    }

    fn output_columns(&self) -> Vec<String> {
        vec!["name".to_string(), "type".to_string()]
    }

    fn execute(
        &self,
        _args: &[Value],
        db: &crate::DatabaseInner,
    ) -> Result<Vec<ProcedureRow>, GqliteError> {
        let catalog = db.catalog.read().unwrap();
        let mut rows = Vec::new();
        for table in catalog.node_tables() {
            rows.push(vec![Value::String(table.name.clone()), Value::String("NODE".to_string())]);
        }
        for table in catalog.rel_tables() {
            rows.push(vec![Value::String(table.name.clone()), Value::String("REL".to_string())]);
        }
        Ok(rows)
    }
}
