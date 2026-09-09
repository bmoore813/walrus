//! Typed failures raised while loading or validating Walrus configuration.

use std::path::PathBuf;
use std::time::Duration;

/// A terminal configuration failure.
///
/// The taxonomy is additive so callers can classify failures without parsing display strings.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// `WALRUS_CONFIG` was not present for a normal service launch.
    #[error("missing WALRUS_CONFIG: expected path to the shared Walrus YAML file")]
    MissingConfigPath,
    /// The configured path did not use the supported YAML extension.
    #[error("unsupported WALRUS_CONFIG format: expected .yaml or .yml, got {0:?}")]
    UnsupportedFormat(PathBuf),
    /// The document uses a schema version this binary does not understand.
    #[error("unsupported config version {configured}; this binary supports version {supported}")]
    UnsupportedVersion {
        /// Version read from the document.
        configured: u64,
        /// Version understood by this crate.
        supported: u64,
    },
    /// YAML or an environment override could not be deserialized.
    #[error("config load/parse failed: {0}")]
    Load(#[source] Box<figment::Error>),
    /// A required string field was absent or blank.
    #[error("missing required field: {0}")]
    Missing(&'static str),
    /// A field parsed but lies outside its supported range.
    #[error("field {field} out of bounds: {detail}")]
    OutOfBounds {
        /// Configuration field outside its admitted range.
        field: &'static str,
        /// Caller-facing description of the required bound.
        detail: String,
    },
    /// A replication slot name violates PostgreSQL's identifier rules.
    #[error("invalid slot_name {slot:?}: {detail}")]
    InvalidSlotName {
        /// Rejected replication slot name.
        slot: String,
        /// Slot-name rule the value violated.
        detail: String,
    },
    /// A transformer lease is too short for its renewal cadence.
    #[error(
        "lease_ttl {ttl:?} is too short — renewal runs at TTL/3 and must land inside the TTL; use >= {minimum:?}"
    )]
    LeaseTtlTooShort {
        /// Rejected configured lease lifetime.
        ttl: Duration,
        /// Smallest lease lifetime the renewal schedule supports.
        minimum: Duration,
    },
    /// A cadence driving a transformer loop was zero.
    #[error("{0} must be greater than zero")]
    ZeroInterval(&'static str),
    /// A shard ordinal lies outside the configured shard ring.
    #[error("shard_index {index} must be less than shard_count {count}")]
    ShardIndex {
        /// Rejected zero-based shard index.
        index: u32,
        /// Number of shards in the configured ring.
        count: u32,
    },
    /// A StatefulSet ordinal could not be derived from the instance identity.
    #[error("cannot derive shard_index from instance {instance:?}; set shard_index explicitly")]
    ShardIdentity {
        /// Instance identity that did not end in an ordinal.
        instance: String,
    },
    /// A DuckLake catalog or schema identifier violates the shared SQL identifier policy.
    #[error("{field}: {source}")]
    Identifier {
        /// Configuration field containing the invalid identifier.
        field: &'static str,
        /// Identifier-policy violation.
        #[source]
        source: common::sql::IdentError,
    },
    /// DuckLake data must live in object storage.
    #[error("ducklake.data_path must be an s3:// URI, got {0}")]
    DuckLakeDataPath(String),
    /// The configured Tokio worker count is outside the shared supported range.
    #[error("worker_threads: {0}")]
    WorkerThreads(#[from] common::runtime::WorkerThreadsError),
    /// The durable PostgreSQL retry counter cannot represent the configured budget.
    #[error("max_integrity_resnapshots {configured} exceeds PostgreSQL int")]
    IntegrityResnapshotBudget {
        /// Rejected retry budget.
        configured: u32,
    },
}

impl From<ConfigError> for common::Error {
    fn from(error: ConfigError) -> Self {
        common::Error::Config(error.to_string())
    }
}
