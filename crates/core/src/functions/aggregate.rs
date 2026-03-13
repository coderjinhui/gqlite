//! Built-in aggregate function accumulators.

use crate::functions::registry::AggregateAccumulator;
use crate::types::value::Value;

// ── COUNT ───────────────────────────────────────────────────────

pub struct CountAccumulator {
    count: i64,
    count_star: bool,
}

impl CountAccumulator {
    pub fn new() -> Self {
        Self {
            count: 0,
            count_star: false,
        }
    }

    pub fn new_star() -> Self {
        Self {
            count: 0,
            count_star: true,
        }
    }
}

impl AggregateAccumulator for CountAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if self.count_star || !value.is_null() {
            self.count += 1;
        }
    }

    fn finalize(&self) -> Value {
        Value::Int(self.count)
    }

    fn reset(&mut self) {
        self.count = 0;
    }
}

// ── SUM ─────────────────────────────────────────────────────────

pub struct SumAccumulator {
    sum_int: i64,
    sum_float: f64,
    has_float: bool,
    has_value: bool,
}

impl SumAccumulator {
    pub fn new() -> Self {
        Self {
            sum_int: 0,
            sum_float: 0.0,
            has_float: false,
            has_value: false,
        }
    }
}

impl AggregateAccumulator for SumAccumulator {
    fn accumulate(&mut self, value: &Value) {
        match value {
            Value::Int(i) => {
                self.sum_int += i;
                self.sum_float += *i as f64;
                self.has_value = true;
            }
            Value::Float(f) => {
                self.sum_float += f;
                self.has_float = true;
                self.has_value = true;
            }
            _ => {}
        }
    }

    fn finalize(&self) -> Value {
        if !self.has_value {
            return Value::Null;
        }
        if self.has_float {
            Value::Float(self.sum_float)
        } else {
            Value::Int(self.sum_int)
        }
    }

    fn reset(&mut self) {
        self.sum_int = 0;
        self.sum_float = 0.0;
        self.has_float = false;
        self.has_value = false;
    }
}

// ── AVG ─────────────────────────────────────────────────────────

pub struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl AggregateAccumulator for AvgAccumulator {
    fn accumulate(&mut self, value: &Value) {
        match value {
            Value::Int(i) => {
                self.sum += *i as f64;
                self.count += 1;
            }
            Value::Float(f) => {
                self.sum += f;
                self.count += 1;
            }
            _ => {}
        }
    }

    fn finalize(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            Value::Float(self.sum / self.count as f64)
        }
    }

    fn reset(&mut self) {
        self.sum = 0.0;
        self.count = 0;
    }
}

// ── MIN ─────────────────────────────────────────────────────────

pub struct MinAccumulator {
    min: Option<Value>,
}

impl MinAccumulator {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl AggregateAccumulator for MinAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        self.min = Some(match &self.min {
            None => value.clone(),
            Some(current) => {
                if value_lt(value, current) {
                    value.clone()
                } else {
                    current.clone()
                }
            }
        });
    }

    fn finalize(&self) -> Value {
        self.min.clone().unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.min = None;
    }
}

// ── MAX ─────────────────────────────────────────────────────────

pub struct MaxAccumulator {
    max: Option<Value>,
}

impl MaxAccumulator {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl AggregateAccumulator for MaxAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if value.is_null() {
            return;
        }
        self.max = Some(match &self.max {
            None => value.clone(),
            Some(current) => {
                if value_lt(current, value) {
                    value.clone()
                } else {
                    current.clone()
                }
            }
        });
    }

    fn finalize(&self) -> Value {
        self.max.clone().unwrap_or(Value::Null)
    }

    fn reset(&mut self) {
        self.max = None;
    }
}

// ── COLLECT ─────────────────────────────────────────────────────

pub struct CollectAccumulator {
    values: Vec<Value>,
}

impl CollectAccumulator {
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }
}

impl AggregateAccumulator for CollectAccumulator {
    fn accumulate(&mut self, value: &Value) {
        if !value.is_null() {
            self.values.push(value.clone());
        }
    }

    fn finalize(&self) -> Value {
        Value::List(self.values.clone())
    }

    fn reset(&mut self) {
        self.values.clear();
    }
}

/// Compare two Values for less-than ordering.
fn value_lt(a: &Value, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => a < b,
        (Value::Float(a), Value::Float(b)) => a < b,
        (Value::Int(a), Value::Float(b)) => (*a as f64) < *b,
        (Value::Float(a), Value::Int(b)) => *a < (*b as f64),
        (Value::String(a), Value::String(b)) => a < b,
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn count_accumulator() {
        let mut acc = CountAccumulator::new();
        acc.accumulate(&Value::Int(1));
        acc.accumulate(&Value::Null);
        acc.accumulate(&Value::Int(3));
        assert_eq!(acc.finalize(), Value::Int(2));
    }

    #[test]
    fn count_star() {
        let mut acc = CountAccumulator::new_star();
        acc.accumulate(&Value::Int(1));
        acc.accumulate(&Value::Null);
        acc.accumulate(&Value::Int(3));
        assert_eq!(acc.finalize(), Value::Int(3));
    }

    #[test]
    fn sum_int() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&Value::Int(10));
        acc.accumulate(&Value::Int(20));
        assert_eq!(acc.finalize(), Value::Int(30));
    }

    #[test]
    fn sum_mixed() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&Value::Int(10));
        acc.accumulate(&Value::Float(2.5));
        assert_eq!(acc.finalize(), Value::Float(12.5));
    }

    #[test]
    fn avg() {
        let mut acc = AvgAccumulator::new();
        acc.accumulate(&Value::Int(10));
        acc.accumulate(&Value::Int(20));
        assert_eq!(acc.finalize(), Value::Float(15.0));
    }

    #[test]
    fn min_max() {
        let mut min = MinAccumulator::new();
        let mut max = MaxAccumulator::new();
        for v in &[Value::Int(3), Value::Int(1), Value::Int(2)] {
            min.accumulate(v);
            max.accumulate(v);
        }
        assert_eq!(min.finalize(), Value::Int(1));
        assert_eq!(max.finalize(), Value::Int(3));
    }

    #[test]
    fn collect() {
        let mut acc = CollectAccumulator::new();
        acc.accumulate(&Value::String("a".into()));
        acc.accumulate(&Value::Null);
        acc.accumulate(&Value::String("b".into()));
        assert_eq!(
            acc.finalize(),
            Value::List(vec![
                Value::String("a".into()),
                Value::String("b".into()),
            ])
        );
    }
}
