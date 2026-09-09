use super::*;
use common::FailureClass;
use std::borrow::Cow;

/// Minimal database-error double: sqlx's Postgres error cannot be constructed without the driver.
#[derive(Debug)]
struct FakeDbError {
    code: &'static str,
    message: &'static str,
}

impl std::fmt::Display for FakeDbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for FakeDbError {}

impl sqlx::error::DatabaseError for FakeDbError {
    fn message(&self) -> &str {
        self.message
    }

    fn code(&self) -> Option<Cow<'_, str>> {
        Some(Cow::Borrowed(self.code))
    }

    fn kind(&self) -> sqlx::error::ErrorKind {
        if self.code == "23514" {
            sqlx::error::ErrorKind::CheckViolation
        } else {
            sqlx::error::ErrorKind::Other
        }
    }

    fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
        self
    }

    fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
        self
    }
}

#[test]
fn connect_errors_are_transient_migrations_are_terminal() {
    // A cold/unreachable control DB is transient — bootstrap retries it to the deadline.
    let connect = ControlError::Connect(sqlx::Error::PoolClosed);
    assert!(connect.is_transient());
    assert!(!connect.is_terminal());

    // A broken migration is a deploy bug — terminal, no retry.
    let migrate = ControlError::Migrate(sqlx::migrate::MigrateError::VersionMissing(1));
    assert!(migrate.is_terminal());
    assert!(!migrate.is_transient());

    // A violated invariant (CHECK constraint) is a programming bug — terminal. Built through the
    // real classifier: the variant now carries the driver error that produced the verdict.
    let check = ControlError::from(sqlx::Error::Database(Box::new(FakeDbError {
        code: "23514",
        message: "transformed_lsn > raw_appended_lsn",
    })));
    assert!(check.is_terminal());
    assert!(!check.is_transient());
}

#[test]
fn from_sqlx_error_classifies_23514_as_terminal_check_violation() {
    let error = ControlError::from(sqlx::Error::Database(Box::new(FakeDbError {
        code: "23514",
        message: "raw watermark passed transformed watermark",
    })));

    assert!(matches!(&error, ControlError::CheckViolation { .. }));
    assert!(error.is_terminal());
    assert!(!error.is_transient());
    assert!(
        error
            .to_string()
            .contains("raw watermark passed transformed watermark")
    );
    // Classifying the failure must not consume it: the driver error stays in the chain, so the
    // SQLSTATE and constraint detail behind the verdict are still reachable.
    let cause = std::error::Error::source(&error).expect("check violation keeps the sqlx error");
    let driver = cause
        .downcast_ref::<sqlx::Error>()
        .expect("the driver error stays downcastable");
    assert!(matches!(driver, sqlx::Error::Database(_)));
}

#[test]
fn from_sqlx_error_classifies_everything_else_as_transient_connect() {
    let error = ControlError::from(sqlx::Error::PoolClosed);

    assert!(matches!(&error, ControlError::Connect(_)));
    assert!(error.is_transient());
    assert!(!error.is_terminal());
}

#[test]
fn decode_preserves_column_and_input() {
    let error = ControlError::from(ParseEnumError::new("file_manifest.kind", "snapshottt"));

    assert!(error.is_terminal());
    let ControlError::Decode(source) = &error else {
        panic!("expected typed decode error");
    };
    assert_eq!(source.column, "file_manifest.kind");
    assert_eq!(source.input, "snapshottt");

    let source = std::error::Error::source(&error).expect("decode error must retain its source");
    let source = source
        .downcast_ref::<ParseEnumError>()
        .expect("decode source must remain ParseEnumError");
    assert_eq!(source.column, "file_manifest.kind");
    assert_eq!(source.input, "snapshottt");
}
