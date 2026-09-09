use super::*;

fn a_duck_error() -> duckdb::Error {
    let conn = duckdb::Connection::open_in_memory().unwrap();
    conn.execute_batch("SELECT no_such_function();")
        .unwrap_err()
}

#[test]
fn duck_preserves_the_typed_error_and_operation() {
    let source = a_duck_error();
    let expected_source = source.to_string();
    let got = Err::<(), duckdb::Error>(source)
        .duck("configure S3")
        .unwrap_err();

    assert_eq!(got.to_string(), "DuckDB: configure S3");
    assert_eq!(
        std::error::Error::source(&got).unwrap().to_string(),
        expected_source
    );
}

#[test]
fn duck_with_preserves_the_typed_error_and_formatted_operation() {
    let source = a_duck_error();
    let expected_source = source.to_string();
    let got = Err::<(), duckdb::Error>(source)
        .duck_with(|| format!("prune {}_raw", "orders"))
        .unwrap_err();

    assert_eq!(got.to_string(), "DuckDB: prune orders_raw");
    assert_eq!(
        std::error::Error::source(&got).unwrap().to_string(),
        expected_source
    );
}

/// The defaulted `duck` is `duck_with` over a constant, so the two spellings cannot describe the
/// same failure differently. An override that broke this would be caught here.
#[test]
fn defaulted_duck_agrees_with_the_required_duck_with() {
    let via_duck = Err::<(), duckdb::Error>(a_duck_error())
        .duck("attach catalog")
        .unwrap_err();
    let via_duck_with = Err::<(), duckdb::Error>(a_duck_error())
        .duck_with(|| String::from("attach catalog"))
        .unwrap_err();

    assert_eq!(via_duck.to_string(), via_duck_with.to_string());
    assert!(
        matches!(&via_duck, TransformerError::Duck { op, .. } if op.as_str() == "attach catalog")
    );
}

/// Adding context is expressible with the receiver as the *only* type variable: `R::Ok` names the
/// payload. Turning `Ok` back into a trait parameter would force a second, free `T` here.
fn add_context<R: DuckResultExt>(result: R, op: &str) -> Result<R::Ok, TransformerError> {
    result.duck(op)
}

#[test]
fn a_bound_carries_the_payload_without_a_free_type_parameter() {
    let ok: Result<u8, duckdb::Error> = Ok(9);
    assert_eq!(add_context(ok, "unused").unwrap(), 9);

    let got = add_context(Err::<u8, duckdb::Error>(a_duck_error()), "vacuum").unwrap_err();
    assert_eq!(got.to_string(), "DuckDB: vacuum");
}

#[test]
fn duck_with_does_not_format_on_success() {
    let mut called = false;
    let ok: Result<u8, duckdb::Error> = Ok(7);
    let value = ok
        .duck_with(|| {
            called = true;
            String::from("never rendered")
        })
        .unwrap();

    assert_eq!(value, 7);
    assert!(!called, "the operation closure must not run on the Ok path");
}
