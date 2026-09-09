use super::*;

fn store_error() -> crate::staging::StagingError {
    crate::staging::StagingError::Store(object_store::Error::Generic {
        store: "test",
        source: "gone".into(),
    })
}

fn postgres_error() -> tokio_postgres::Error {
    match "port=not-a-port".parse::<tokio_postgres::Config>() {
        Ok(_) => panic!("invalid port unexpectedly parsed"),
        Err(err) => err,
    }
}

#[test]
fn object_store_failure_exits_12_not_70() {
    let err = anyhow::Error::new(store_error());
    assert_eq!(code_for(&err), common::ExitCode::ObjectStore);
}

#[test]
fn manifest_failure_exits_11() {
    let err = anyhow::Error::new(crate::manifest::ManifestError::Control(
        control::ControlError::Decode(control::ParseEnumError::new(
            "file_manifest.kind",
            "bad manifest row",
        )),
    ));
    assert_eq!(code_for(&err), common::ExitCode::ControlDb);
}

#[test]
fn control_failure_exits_11() {
    let err = anyhow::Error::new(control::ControlError::Decode(control::ParseEnumError::new(
        "file_manifest.status",
        "bad control row",
    )));
    assert_eq!(code_for(&err), common::ExitCode::ControlDb);
}

#[test]
fn config_failure_exits_10() {
    let err = anyhow::Error::new(crate::config::ConfigError::Missing("source_db_url"));
    assert_eq!(code_for(&err), common::ExitCode::Config);
}

#[test]
fn keyless_preflight_failure_exits_14() {
    let err = anyhow::Error::new(crate::preflight::PreflightError::NoPrimaryKey {
        schema: "public".to_string(),
        table: "orders".to_string(),
    });
    assert_eq!(code_for(&err), common::ExitCode::KeylessTable);
}

/// The other side of the exhaustive `PreflightError` match: everything that is not a keyless table
/// shares exit 13, and each such variant is now listed rather than absorbed by a wildcard.
#[test]
fn a_non_keyless_preflight_failure_exits_13() {
    let err = anyhow::Error::new(crate::preflight::PreflightError::WalLevel {
        found: "replica".to_string(),
    });
    assert_eq!(code_for(&err), common::ExitCode::Preflight);
}

#[test]
fn guarded_publication_revalidation_keeps_its_preflight_exit_through_context() {
    let issue = crate::source_catalog::PublicationCoverageIssue::DisabledOperations {
        publication: "walrus_pub".to_string(),
        disabled: "DELETE".to_string(),
    };
    let err = anyhow::Error::new(crate::preflight::PreflightError::from(issue))
        .context("publication changed after startup preflight");
    assert_eq!(code_for(&err), common::ExitCode::Preflight);
}

#[test]
fn heartbeat_failure_exits_16() {
    let err = anyhow::Error::new(crate::heartbeat::HeartbeatError::Connect(postgres_error()));
    assert_eq!(code_for(&err), common::ExitCode::SourceDb);
}

#[test]
fn context_layers_do_not_hide_the_typed_error() {
    let err = anyhow::Error::new(store_error()).context("flush batch");
    assert_eq!(code_for(&err), common::ExitCode::ObjectStore);
}

#[test]
fn an_unclassified_error_still_exits_internal() {
    assert_eq!(
        code_for(&anyhow::anyhow!("boom")),
        common::ExitCode::Internal
    );
}
