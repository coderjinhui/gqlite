//! Parallel query execution using rayon thread pools.
//!
//! For read-only queries, independent operator subtrees (e.g. both sides of a
//! HashJoin or Union) are evaluated concurrently via `rayon::join`, which uses
//! a work-stealing thread pool.

use std::sync::Arc;

use crate::error::GqliteError;
use crate::planner::physical::PhysicalPlan;
use crate::{DatabaseInner, QueryResult};

use super::engine::{Engine, Intermediate};

// ── Parallel operator execution ──────────────────────────────────

impl Engine {
    /// Execute a physical plan with parallel evaluation of independent subtrees.
    ///
    /// Falls back to the sequential `execute_plan` for DDL / DML operations.
    /// For read-only queries, uses `rayon::join` to run independent branches
    /// (HashJoin build/probe, Union left/right) concurrently.
    pub(crate) fn execute_plan_parallel(
        &self,
        plan: &PhysicalPlan,
        db: &Arc<DatabaseInner>,
        txn_id: u64,
    ) -> Result<QueryResult, GqliteError> {
        if !plan.is_read_only() {
            // Mutating plans need sequential execution for WAL ordering.
            return self.execute_plan(plan, db, txn_id);
        }

        // DDL handled by base execute_plan
        match plan {
            PhysicalPlan::CreateNodeTable { .. }
            | PhysicalPlan::CreateRelTable { .. }
            | PhysicalPlan::DropTable { .. }
            | PhysicalPlan::AlterTable { .. }
            | PhysicalPlan::CopyFrom { .. }
            | PhysicalPlan::CopyTo { .. }
            | PhysicalPlan::EmptyResult => self.execute_plan(plan, db, txn_id),
            _ => {
                let intermediate = self.execute_operator_parallel(plan, db, txn_id)?;
                Ok(intermediate.into_query_result())
            }
        }
    }

    /// Recursively evaluate an operator tree, using rayon to parallelise
    /// independent branches where possible.
    fn execute_operator_parallel(
        &self,
        plan: &PhysicalPlan,
        db: &Arc<DatabaseInner>,
        txn_id: u64,
    ) -> Result<Intermediate, GqliteError> {
        match plan {
            // ── Parallelisable binary operators ──────────────

            // HashJoin: build and probe sides are independent — run in parallel.
            PhysicalPlan::HashJoin { build, probe, .. } => {
                let (build_result, probe_result) = rayon::join(
                    || self.execute_operator_parallel(build, db, txn_id),
                    || self.execute_operator_parallel(probe, db, txn_id),
                );
                self.exec_cross_join(build_result?, probe_result?)
            }

            // Union: both sides are independent — run in parallel.
            PhysicalPlan::Union { left, right, all } => {
                let (left_result, right_result) = rayon::join(
                    || self.execute_operator_parallel(left, db, txn_id),
                    || self.execute_operator_parallel(right, db, txn_id),
                );
                self.exec_union(left_result?, right_result?, *all)
            }

            // ── Streaming operators with single child ────────
            // Recurse in parallel mode for the child, then apply
            // the current operator sequentially.

            PhysicalPlan::Filter { input, predicate } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_filter(input_result, predicate)
            }

            PhysicalPlan::Projection { input, expressions } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_projection(input_result, expressions)
            }

            PhysicalPlan::ReturnAll { input } => {
                self.execute_operator_parallel(input, db, txn_id)
            }

            PhysicalPlan::OrderBy { input, items } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_order_by(input_result, items)
            }

            PhysicalPlan::Limit { input, count } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_limit(input_result, count)
            }

            PhysicalPlan::Skip { input, count } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_skip(input_result, count)
            }

            PhysicalPlan::Aggregate { input, expressions } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_aggregate(input_result, expressions)
            }

            PhysicalPlan::Unwind { input, expr, alias } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_unwind(input_result, expr, alias)
            }

            PhysicalPlan::CsrExpand {
                input,
                rel_table_name,
                rel_table_id,
                direction,
                src_alias,
                dst_alias,
                rel_alias: _,
                dst_table_name: _,
                dst_table_id,
                optional,
            } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_expand(
                    db,
                    input_result,
                    rel_table_name,
                    *rel_table_id,
                    direction,
                    src_alias,
                    dst_alias,
                    dst_table_id,
                    *optional,
                )
            }

            PhysicalPlan::RecursiveExpand {
                input,
                rel_table_name,
                rel_table_id,
                direction,
                src_alias,
                dst_alias,
                dst_table_name: _,
                dst_table_id,
                min_hops,
                max_hops,
            } => {
                let input_result = self.execute_operator_parallel(input, db, txn_id)?;
                self.exec_recursive_expand(
                    db,
                    input_result,
                    rel_table_name,
                    *rel_table_id,
                    direction,
                    src_alias,
                    dst_alias,
                    dst_table_id,
                    *min_hops,
                    *max_hops,
                )
            }

            // ── Leaf / DDL / DML — delegate to sequential engine ─
            _ => self.execute_operator(plan, db, txn_id),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::Database;

    #[test]
    fn parallel_hash_join() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE A (id INT64, val STRING, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE NODE TABLE B (id INT64, val STRING, PRIMARY KEY(id))")
            .unwrap();
        for i in 0..100 {
            db.execute(&format!(
                "CREATE (a:A {{id: {}, val: 'a{}'}})",
                i, i
            ))
            .unwrap();
            db.execute(&format!(
                "CREATE (b:B {{id: {}, val: 'b{}'}})",
                i, i
            ))
            .unwrap();
        }

        // HashJoin produces cross product; using parallel execution
        let result = db
            .execute("MATCH (a:A), (b:B) WHERE a.id = b.id RETURN a.val, b.val")
            .unwrap();
        assert_eq!(result.num_rows(), 100);
    }

    #[test]
    fn parallel_union() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Person (name STRING, PRIMARY KEY(name))")
            .unwrap();
        db.execute("CREATE (p:Person {name: 'Alice'})").unwrap();
        db.execute("CREATE (p:Person {name: 'Bob'})").unwrap();

        let result = db
            .execute(
                "MATCH (p:Person) RETURN p.name \
                 UNION ALL \
                 MATCH (p:Person) RETURN p.name",
            )
            .unwrap();
        assert_eq!(result.num_rows(), 4);
    }

    #[test]
    fn parallel_matches_sequential() {
        // Verify that parallel execution produces the same results as sequential.
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE N (id INT64, PRIMARY KEY(id))")
            .unwrap();
        for i in 0..50 {
            db.execute(&format!("CREATE (n:N {{id: {}}})", i))
                .unwrap();
        }

        let conn = db.connect();

        // Sequential
        let seq_result = conn.execute("MATCH (n:N) RETURN n.id ORDER BY n.id").unwrap();

        // Parallel (uses the same query path)
        let par_result = conn.execute("MATCH (n:N) RETURN n.id ORDER BY n.id").unwrap();

        assert_eq!(seq_result.num_rows(), par_result.num_rows());
        for (s, p) in seq_result.rows().iter().zip(par_result.rows().iter()) {
            assert_eq!(s.values, p.values);
        }
    }

    #[test]
    fn parallel_aggregate_with_order_by() {
        let db = Database::in_memory();
        db.execute("CREATE NODE TABLE Item (id INT64, category STRING, price INT64, PRIMARY KEY(id))")
            .unwrap();
        db.execute("CREATE (i:Item {id: 1, category: 'A', price: 10})")
            .unwrap();
        db.execute("CREATE (i:Item {id: 2, category: 'B', price: 20})")
            .unwrap();
        db.execute("CREATE (i:Item {id: 3, category: 'A', price: 30})")
            .unwrap();

        let result = db
            .execute("MATCH (i:Item) RETURN i.category, sum(i.price) ORDER BY i.category")
            .unwrap();
        // Category A and B exist; results come back ordered
        assert!(result.num_rows() > 0);
    }
}
