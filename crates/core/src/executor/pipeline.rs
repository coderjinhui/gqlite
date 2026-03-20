//! Pipeline splitting: decompose a physical plan into a dependency graph of pipelines.
//!
//! A **pipeline** is a chain of streaming (non-blocking) operators that can
//! execute without materializing intermediate results. **Pipeline breakers**
//! (blocking operators like HashJoin build, OrderBy, Aggregate) force
//! materialisation and create dependency edges between pipelines.

use crate::planner::physical::PhysicalPlan;

/// A unique pipeline identifier.
pub type PipelineId = usize;

/// A single pipeline: a linear chain of operators that stream data.
#[derive(Debug)]
pub struct Pipeline {
    pub id: PipelineId,
    /// The physical plan subtree for this pipeline.
    pub plan: PhysicalPlan,
    /// Pipelines that must complete before this one can start.
    pub depends_on: Vec<PipelineId>,
}

/// A graph of pipelines with dependency edges.
#[derive(Debug)]
pub struct PipelineGraph {
    pub pipelines: Vec<Pipeline>,
}

impl PipelineGraph {
    /// Return pipeline IDs in a valid execution order (dependencies first).
    pub fn execution_order(&self) -> Vec<PipelineId> {
        // Topological sort via Kahn's algorithm
        let n = self.pipelines.len();
        let mut in_degree = vec![0usize; n];
        let mut dependents: Vec<Vec<PipelineId>> = vec![vec![]; n];

        for p in &self.pipelines {
            in_degree[p.id] = p.depends_on.len();
            for &dep in &p.depends_on {
                dependents[dep].push(p.id);
            }
        }

        let mut queue: Vec<PipelineId> = (0..n).filter(|&i| in_degree[i] == 0).collect();
        let mut order = Vec::with_capacity(n);

        while let Some(pid) = queue.pop() {
            order.push(pid);
            for &next in &dependents[pid] {
                in_degree[next] -= 1;
                if in_degree[next] == 0 {
                    queue.push(next);
                }
            }
        }

        order
    }
}

/// Split a physical plan into a pipeline graph.
///
/// Pipeline breakers:
/// - `HashJoin`: build side is a separate pipeline; probe side continues.
/// - `OrderBy`: input is a separate pipeline.
/// - `Aggregate`: input is a separate pipeline.
pub fn split_into_pipelines(plan: &PhysicalPlan) -> PipelineGraph {
    let mut ctx = SplitCtx { pipelines: Vec::new(), next_id: 0 };
    ctx.split(plan);
    PipelineGraph { pipelines: ctx.pipelines }
}

struct SplitCtx {
    pipelines: Vec<Pipeline>,
    next_id: PipelineId,
}

impl SplitCtx {
    fn alloc_id(&mut self) -> PipelineId {
        let id = self.next_id;
        self.next_id += 1;
        id
    }

    /// Recursively split a plan. Returns the PipelineId of the top-level
    /// pipeline that produces the result for this subtree.
    fn split(&mut self, plan: &PhysicalPlan) -> PipelineId {
        match plan {
            // ── Pipeline breakers ───────────────────────────────

            // HashJoin: build side is a separate pipeline.
            PhysicalPlan::HashJoin { build, probe, build_key, probe_key } => {
                // Build pipeline must complete first.
                let build_pid = self.split(build);

                // Probe pipeline depends on build pipeline.
                let probe_pid = self.split(probe);

                // The join pipeline itself depends on both build and probe.
                let join_id = self.alloc_id();
                self.pipelines.push(Pipeline {
                    id: join_id,
                    plan: PhysicalPlan::HashJoin {
                        build: Box::new(self.pipelines[build_pid].plan.clone()),
                        probe: Box::new(self.pipelines[probe_pid].plan.clone()),
                        build_key: build_key.clone(),
                        probe_key: probe_key.clone(),
                    },
                    depends_on: vec![build_pid, probe_pid],
                });
                join_id
            }

            // OrderBy: input must be fully materialised first.
            PhysicalPlan::OrderBy { input, items } => {
                let input_pid = self.split(input);
                let id = self.alloc_id();
                self.pipelines.push(Pipeline {
                    id,
                    plan: PhysicalPlan::OrderBy { input: input.clone(), items: items.clone() },
                    depends_on: vec![input_pid],
                });
                id
            }

            // Aggregate: input must be fully processed first.
            PhysicalPlan::Aggregate { input, expressions } => {
                let input_pid = self.split(input);
                let id = self.alloc_id();
                self.pipelines.push(Pipeline {
                    id,
                    plan: PhysicalPlan::Aggregate {
                        input: input.clone(),
                        expressions: expressions.clone(),
                    },
                    depends_on: vec![input_pid],
                });
                id
            }

            // Union: both sides are independent pipelines.
            PhysicalPlan::Union { left, right, all } => {
                let left_pid = self.split(left);
                let right_pid = self.split(right);
                let id = self.alloc_id();
                self.pipelines.push(Pipeline {
                    id,
                    plan: PhysicalPlan::Union {
                        left: left.clone(),
                        right: right.clone(),
                        all: *all,
                    },
                    depends_on: vec![left_pid, right_pid],
                });
                id
            }

            // ── Streaming operators (not breakers) ─────────────

            // For non-breaker operators, we keep them as a single pipeline
            // since they can stream data.
            _ => {
                let id = self.alloc_id();
                self.pipelines.push(Pipeline { id, plan: plan.clone(), depends_on: vec![] });
                id
            }
        }
    }
}
