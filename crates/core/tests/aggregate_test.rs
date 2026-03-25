use gqlite_core::functions::aggregate::*;
use gqlite_core::functions::registry::AggregateAccumulator;
use gqlite_core::types::value::Value;

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
        Value::List(vec![Value::String("a".into()), Value::String("b".into()),])
    );
}
