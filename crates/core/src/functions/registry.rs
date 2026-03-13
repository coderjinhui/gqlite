//! Function registry — maps function names to implementations.

use std::collections::HashMap;

use crate::error::GqliteError;
use crate::types::value::Value;

use super::scalar;
use super::aggregate;

/// A scalar function: takes a list of values, returns one value.
pub type ScalarFn = fn(&[Value]) -> Result<Value, GqliteError>;

/// Registry of built-in functions.
pub struct FunctionRegistry {
    scalar_fns: HashMap<String, ScalarFn>,
}

impl FunctionRegistry {
    pub fn new() -> Self {
        let mut reg = Self {
            scalar_fns: HashMap::new(),
        };
        reg.register_builtins();
        reg
    }

    fn register_builtins(&mut self) {
        self.register_scalar("lower", scalar::fn_lower);
        self.register_scalar("upper", scalar::fn_upper);
        self.register_scalar("trim", scalar::fn_trim);
        self.register_scalar("length", scalar::fn_length);
        self.register_scalar("size", scalar::fn_length);
        self.register_scalar("concat", scalar::fn_concat);
        self.register_scalar("contains", scalar::fn_contains);
        self.register_scalar("starts_with", scalar::fn_starts_with);
        self.register_scalar("ends_with", scalar::fn_ends_with);
        self.register_scalar("to_string", scalar::fn_to_string);
        self.register_scalar("coalesce", scalar::fn_coalesce);
        self.register_scalar("abs", scalar::fn_abs);
    }

    fn register_scalar(&mut self, name: &str, f: ScalarFn) {
        self.scalar_fns.insert(name.to_lowercase(), f);
    }

    pub fn get_scalar(&self, name: &str) -> Option<&ScalarFn> {
        self.scalar_fns.get(&name.to_lowercase())
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Aggregate accumulator trait.
pub trait AggregateAccumulator: Send {
    fn accumulate(&mut self, value: &Value);
    fn finalize(&self) -> Value;
    fn reset(&mut self);
}

/// Create an aggregate accumulator by name.
pub fn create_accumulator(name: &str) -> Option<Box<dyn AggregateAccumulator>> {
    match name.to_lowercase().as_str() {
        "count" => Some(Box::new(aggregate::CountAccumulator::new())),
        "sum" => Some(Box::new(aggregate::SumAccumulator::new())),
        "avg" => Some(Box::new(aggregate::AvgAccumulator::new())),
        "min" => Some(Box::new(aggregate::MinAccumulator::new())),
        "max" => Some(Box::new(aggregate::MaxAccumulator::new())),
        "collect" => Some(Box::new(aggregate::CollectAccumulator::new())),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_case_insensitive() {
        let reg = FunctionRegistry::new();
        assert!(reg.get_scalar("lower").is_some());
        assert!(reg.get_scalar("LOWER").is_some());
        assert!(reg.get_scalar("Lower").is_some());
    }

    #[test]
    fn unknown_function() {
        let reg = FunctionRegistry::new();
        assert!(reg.get_scalar("nonexistent").is_none());
    }
}
