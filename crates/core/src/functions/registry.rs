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
        self.register_scalar("ltrim", scalar::fn_ltrim);
        self.register_scalar("rtrim", scalar::fn_rtrim);
        self.register_scalar("substring", scalar::fn_substring);
        self.register_scalar("replace", scalar::fn_replace);
        self.register_scalar("reverse", scalar::fn_reverse);
        self.register_scalar("left", scalar::fn_left);
        self.register_scalar("right", scalar::fn_right);
        self.register_scalar("lpad", scalar::fn_lpad);
        self.register_scalar("rpad", scalar::fn_rpad);
        self.register_scalar("repeat", scalar::fn_repeat);
        // List functions
        self.register_scalar("list_len", scalar::fn_list_len);
        self.register_scalar("list_extract", scalar::fn_list_extract);
        self.register_scalar("list_append", scalar::fn_list_append);
        self.register_scalar("list_prepend", scalar::fn_list_prepend);
        self.register_scalar("list_concat", scalar::fn_list_concat);
        self.register_scalar("list_contains", scalar::fn_list_contains);
        self.register_scalar("list_reverse", scalar::fn_list_reverse);
        self.register_scalar("list_sort", scalar::fn_list_sort);
        self.register_scalar("list_distinct", scalar::fn_list_distinct);
        self.register_scalar("range", scalar::fn_range);
        // Path functions
        self.register_scalar("nodes", scalar::fn_nodes);
        // Math functions
        self.register_scalar("ceil", scalar::fn_ceil);
        self.register_scalar("floor", scalar::fn_floor);
        self.register_scalar("round", scalar::fn_round);
        self.register_scalar("sqrt", scalar::fn_sqrt);
        self.register_scalar("log", scalar::fn_log);
        self.register_scalar("log10", scalar::fn_log10);
        self.register_scalar("sign", scalar::fn_sign);
        self.register_scalar("rand", scalar::fn_rand);
        self.register_scalar("pi", scalar::fn_pi);
        self.register_scalar("e", scalar::fn_e);
        // Conversion functions
        self.register_scalar("tointeger", scalar::fn_to_integer);
        self.register_scalar("tofloat", scalar::fn_to_float);
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

