use gqlite_core::functions::scalar::*;
use gqlite_core::types::value::Value;

#[test]
fn math_ceil_floor_round() {
    // ceil
    assert_eq!(fn_ceil(&[Value::Float(3.2)]).unwrap(), Value::Float(4.0));
    assert_eq!(fn_ceil(&[Value::Int(5)]).unwrap(), Value::Float(5.0));
    assert_eq!(fn_ceil(&[Value::Float(-2.3)]).unwrap(), Value::Float(-2.0));

    // floor
    assert_eq!(fn_floor(&[Value::Float(3.8)]).unwrap(), Value::Float(3.0));
    assert_eq!(fn_floor(&[Value::Int(5)]).unwrap(), Value::Float(5.0));
    assert_eq!(fn_floor(&[Value::Float(-2.3)]).unwrap(), Value::Float(-3.0));

    // round
    assert_eq!(fn_round(&[Value::Float(3.5)]).unwrap(), Value::Float(4.0));
    assert_eq!(fn_round(&[Value::Float(3.4)]).unwrap(), Value::Float(3.0));
    assert_eq!(fn_round(&[Value::Int(7)]).unwrap(), Value::Float(7.0));
}

#[test]
fn math_sqrt_log() {
    // sqrt
    assert_eq!(fn_sqrt(&[Value::Float(4.0)]).unwrap(), Value::Float(2.0));
    assert_eq!(fn_sqrt(&[Value::Int(9)]).unwrap(), Value::Float(3.0));

    // log (natural logarithm)
    assert_eq!(fn_log(&[Value::Float(1.0)]).unwrap(), Value::Float(0.0));
    assert_eq!(fn_log(&[Value::Int(1)]).unwrap(), Value::Float(0.0));

    // log10
    assert_eq!(fn_log10(&[Value::Float(100.0)]).unwrap(), Value::Float(2.0));
    assert_eq!(fn_log10(&[Value::Int(1000)]).unwrap(), Value::Float(3.0));
}

#[test]
fn math_sign() {
    assert_eq!(fn_sign(&[Value::Int(-5)]).unwrap(), Value::Int(-1));
    assert_eq!(fn_sign(&[Value::Int(0)]).unwrap(), Value::Int(0));
    assert_eq!(fn_sign(&[Value::Int(3)]).unwrap(), Value::Int(1));

    assert_eq!(fn_sign(&[Value::Float(-2.5)]).unwrap(), Value::Int(-1));
    assert_eq!(fn_sign(&[Value::Float(0.0)]).unwrap(), Value::Int(0));
    assert_eq!(fn_sign(&[Value::Float(7.3)]).unwrap(), Value::Int(1));
}

#[test]
fn math_pi_e() {
    let pi = fn_pi(&[]).unwrap();
    let e = fn_e(&[]).unwrap();

    if let Value::Float(v) = pi {
        assert!((v - std::f64::consts::PI).abs() < 1e-10);
    } else {
        panic!("pi() should return Float");
    }

    if let Value::Float(v) = e {
        assert!((v - std::f64::consts::E).abs() < 1e-10);
    } else {
        panic!("e() should return Float");
    }
}

#[test]
fn math_to_integer_to_float() {
    // toInteger: Float truncates toward zero
    assert_eq!(fn_to_integer(&[Value::Float(3.7)]).unwrap(), Value::Int(3));
    assert_eq!(fn_to_integer(&[Value::Float(-3.7)]).unwrap(), Value::Int(-3));
    // toInteger: Int passthrough
    assert_eq!(fn_to_integer(&[Value::Int(42)]).unwrap(), Value::Int(42));
    // toInteger: String parse
    assert_eq!(
        fn_to_integer(&[Value::String("123".into())]).unwrap(),
        Value::Int(123)
    );
    // toInteger: String parse failure
    assert!(fn_to_integer(&[Value::String("abc".into())]).is_err());

    // toFloat: Int -> Float
    assert_eq!(fn_to_float(&[Value::Int(42)]).unwrap(), Value::Float(42.0));
    // toFloat: Float passthrough
    assert_eq!(fn_to_float(&[Value::Float(3.14)]).unwrap(), Value::Float(3.14));
    // toFloat: String parse
    assert_eq!(
        fn_to_float(&[Value::String("2.5".into())]).unwrap(),
        Value::Float(2.5)
    );
    // toFloat: String parse failure
    assert!(fn_to_float(&[Value::String("xyz".into())]).is_err());
}

#[test]
fn math_rand() {
    let result = fn_rand(&[]).unwrap();
    if let Value::Float(v) = result {
        assert!(v >= 0.0 && v < 1.0, "rand() should return [0, 1), got {}", v);
    } else {
        panic!("rand() should return Float");
    }
}

#[test]
fn math_null_handling() {
    assert_eq!(fn_ceil(&[Value::Null]).unwrap(), Value::Null);
    assert_eq!(fn_floor(&[Value::Null]).unwrap(), Value::Null);
    assert_eq!(fn_round(&[Value::Null]).unwrap(), Value::Null);
    assert_eq!(fn_sqrt(&[Value::Null]).unwrap(), Value::Null);
    assert_eq!(fn_log(&[Value::Null]).unwrap(), Value::Null);
    assert_eq!(fn_log10(&[Value::Null]).unwrap(), Value::Null);
    assert_eq!(fn_sign(&[Value::Null]).unwrap(), Value::Null);
    assert_eq!(fn_to_integer(&[Value::Null]).unwrap(), Value::Null);
    assert_eq!(fn_to_float(&[Value::Null]).unwrap(), Value::Null);
}
