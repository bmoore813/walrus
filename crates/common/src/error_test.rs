use super::*;
use crate::FailureClass;

/// Every variant, paired with its DoD-mandated terminal classification.
fn one_of_each() -> Vec<(Error, bool)> {
    vec![
        (Error::Config("bad bound".into()), true),
        (Error::ControlDb("connection refused".into()), false),
        (Error::ObjectStore("503 from MinIO".into()), false),
        (Error::SourceDb("connection refused".into()), false),
        (Error::Preflight("wal_level=replica".into()), true),
        (
            Error::KeylessTable {
                table: "public.orders".into(),
            },
            true,
        ),
        (Error::LeaseContended("held by transformer-0".into()), true),
        (
            Error::Quarantine("lossy cast on public.orders.n".into()),
            true,
        ),
        (Error::Internal("unreachable".into()), true),
    ]
}

#[test]
fn config_is_terminal_control_db_is_transient() {
    assert!(Error::Config("x".into()).is_terminal());
    assert!(!Error::ControlDb("x".into()).is_terminal());
    assert!(Error::ControlDb("x".into()).is_transient());

    // The full classification contract, exactly as the DoD states it.
    for (err, terminal) in one_of_each() {
        assert_eq!(
            err.is_terminal(),
            terminal,
            "{err:?} classified wrong (expected terminal={terminal})",
        );
        assert_eq!(err.is_transient(), !terminal, "transient is the complement");
    }
}

#[test]
fn each_terminal_variant_maps_to_a_distinct_exit_code() {
    let terminal_codes: Vec<i32> = one_of_each()
        .into_iter()
        .filter(|(_, terminal)| *terminal)
        .map(|(err, _)| err.exit_code() as i32)
        .collect();

    assert!(
        terminal_codes.iter().all(|&c| c != 0),
        "no terminal failure may exit 0 (only Success is 0)",
    );

    let mut distinct = terminal_codes.clone();
    distinct.sort_unstable();
    distinct.dedup();
    assert_eq!(
        distinct.len(),
        terminal_codes.len(),
        "terminal exit codes must be distinct: {terminal_codes:?}",
    );
}

#[test]
fn display_states_precondition_and_observed_value() {
    // Preflight names the precondition class AND the observed value.
    let e = Error::Preflight("wal_level=replica".into());
    let s = e.to_string();
    assert!(
        s.contains("source preflight failed"),
        "names precondition: {s}"
    );
    assert!(s.contains("wal_level=replica"), "names observed value: {s}");

    // Keyless table names the offending table so the log is actionable.
    let k = Error::KeylessTable {
        table: "public.orders".into(),
    };
    let ks = k.to_string();
    assert!(ks.contains("no usable key"), "names precondition: {ks}");
    assert!(ks.contains("public.orders"), "names observed value: {ks}");
}

#[test]
fn exit_code_zero_is_success_only() {
    assert_eq!(ExitCode::Success as i32, 0);

    // No Error variant — terminal or transient — maps to the success code.
    for (err, _) in one_of_each() {
        assert_ne!(err.exit_code() as i32, 0, "{err:?} must not map to Success");
    }

    // The seam bins use in `main` exists and compiles for a real code.
    let _process_code: std::process::ExitCode = ExitCode::Success.into();
}

#[test]
fn every_exit_code_variant_fits_a_u8() {
    for code in [
        ExitCode::Success,
        ExitCode::Config,
        ExitCode::ControlDb,
        ExitCode::ObjectStore,
        ExitCode::Preflight,
        ExitCode::KeylessTable,
        ExitCode::LeaseContended,
        ExitCode::SourceDb,
        ExitCode::Quarantine,
        ExitCode::Internal,
    ] {
        let raw = code as i32;
        assert!(u8::try_from(raw).is_ok(), "{code:?} no longer fits a u8");
    }
}

#[test]
fn every_exit_code_round_trips_through_its_i32() {
    for (err, _) in one_of_each() {
        let code = err.exit_code();
        assert_eq!(ExitCode::try_from(code as i32), Ok(code), "{err:?}");
    }
    assert_eq!(ExitCode::try_from(0), Ok(ExitCode::Success));
    assert_eq!(ExitCode::try_from(99), Err(UnknownExitCode(99)));
}
