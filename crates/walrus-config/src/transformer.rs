//! Transformer-specific settings and validated value types.

use crate::loader::{self, Service};
use crate::{CommonConfig, ConfigError};
use common::Redacted;
use serde::Deserialize;
use std::net::SocketAddr;
use std::num::{NonZeroI64, NonZeroU32};
use std::path::Path;
use std::time::Duration;

/// Minimum renewable transformer lease lifetime.
pub const MIN_LEASE_TTL: Duration = Duration::from_secs(3);

const fn nonzero_i64(value: i64) -> NonZeroI64 {
    match NonZeroI64::new(value) {
        Some(value) => value,
        None => NonZeroI64::MIN,
    }
}

const fn nonzero_u32(value: u32) -> NonZeroU32 {
    match NonZeroU32::new(value) {
        Some(value) => value,
        None => NonZeroU32::MIN,
    }
}

/// A lease lifetime proven long enough for the TTL/3 renewal cadence.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LeaseTtl(Duration);

impl LeaseTtl {
    /// Construct a renewable lease lifetime.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::LeaseTtlTooShort`] below [`MIN_LEASE_TTL`].
    pub fn new(ttl: Duration) -> Result<Self, ConfigError> {
        if ttl < MIN_LEASE_TTL {
            return Err(ConfigError::LeaseTtlTooShort {
                ttl,
                minimum: MIN_LEASE_TTL,
            });
        }
        Ok(Self(ttl))
    }

    /// Return the admitted lifetime.
    #[must_use]
    pub const fn get(self) -> Duration {
        self.0
    }
}

impl TryFrom<Duration> for LeaseTtl {
    type Error = ConfigError;

    fn try_from(ttl: Duration) -> Result<Self, Self::Error> {
        Self::new(ttl)
    }
}

/// DuckLake catalog and object-data settings.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct DuckLakeConfig {
    /// PostgreSQL URI for the dedicated DuckLake metadata database.
    pub catalog_url: Redacted<String>,
    /// DuckDB attachment name exposed to generated SQL.
    pub attach_name: String,
    /// PostgreSQL schema owned by DuckLake.
    pub metadata_schema: String,
    /// Object-store root for DuckLake-managed Parquet.
    pub data_path: String,
    /// Optional pre-populated DuckDB extension directory.
    pub extension_directory: Option<String>,
    /// Whether missing extensions may be installed at startup.
    pub install_extensions: bool,
    /// Time-travel history retained before snapshot expiration.
    #[serde(with = "humantime_serde")]
    pub snapshot_retention: Duration,
    /// Additional age before orphaned files may be deleted.
    #[serde(with = "humantime_serde")]
    pub cleanup_grace: Duration,
    /// Catalog-level expiration and cleanup cadence.
    #[serde(with = "humantime_serde")]
    pub maintenance_interval: Duration,
}

impl Default for DuckLakeConfig {
    fn default() -> Self {
        Self {
            catalog_url: Redacted::default(),
            attach_name: "walrus".to_string(),
            metadata_schema: "walrus_ducklake".to_string(),
            data_path: String::new(),
            extension_directory: None,
            install_extensions: false,
            snapshot_retention: Duration::from_secs(7 * 24 * 60 * 60),
            cleanup_grace: Duration::from_secs(7 * 24 * 60 * 60),
            maintenance_interval: Duration::from_secs(24 * 60 * 60),
        }
    }
}

/// Transformer-owned portion of the versioned YAML document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TransformerSettings {
    /// Tokio worker threads; `None` uses available parallelism.
    pub worker_threads: Option<usize>,
    /// Stable process and lease-owner identity.
    pub instance: String,
    /// DuckLake metadata and object-data configuration.
    pub ducklake: DuckLakeConfig,
    /// Number of deterministic table shards.
    pub shard_count: NonZeroU32,
    /// Explicit zero-based shard ordinal, or `None` to derive it from `instance`.
    pub shard_index: Option<u32>,
    /// Ownership lease lifetime.
    #[serde(with = "humantime_serde")]
    pub lease_ttl: Duration,
    /// Incremental apply-loop poll cadence.
    #[serde(with = "humantime_serde")]
    pub poll_interval: Duration,
    /// Per-table full-rebuild and retention cadence.
    #[serde(with = "humantime_serde")]
    pub compaction_interval: Duration,
    /// Raw WAL bytes retained behind the transformed LSN.
    pub retention_lsn_lag: u64,
    /// Manifest files claimed per apply cycle.
    pub max_files_per_cycle: NonZeroI64,
    /// Automatic replacement snapshots allowed after integrity failure.
    pub max_integrity_resnapshots: u32,
    /// Bootstrap retry budget.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Health endpoint bind address.
    pub health_addr: SocketAddr,
}

impl Default for TransformerSettings {
    fn default() -> Self {
        Self {
            worker_threads: None,
            instance: String::new(),
            ducklake: DuckLakeConfig::default(),
            shard_count: nonzero_u32(1),
            shard_index: None,
            lease_ttl: Duration::from_secs(30),
            poll_interval: Duration::from_secs(5),
            compaction_interval: Duration::from_secs(3600),
            retention_lsn_lag: 16 << 20,
            max_files_per_cycle: nonzero_i64(32),
            max_integrity_resnapshots: 1,
            startup_deadline: Duration::from_secs(60),
            health_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
        }
    }
}

impl TransformerSettings {
    /// Return the configured shard index or derive it from the instance suffix.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::ShardIdentity`] when sharding needs an ordinal that cannot be derived.
    pub fn effective_shard_index(&self) -> Result<u32, ConfigError> {
        if let Some(index) = self.shard_index {
            return Ok(index);
        }
        if self.shard_count.get() == 1 {
            return Ok(0);
        }
        self.instance
            .rsplit_once('-')
            .and_then(|(_, ordinal)| ordinal.parse().ok())
            .ok_or_else(|| ConfigError::ShardIdentity {
                instance: self.instance.clone(),
            })
    }

    fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            ("transformer.instance", self.instance.as_str()),
            (
                "transformer.ducklake.catalog_url",
                self.ducklake.catalog_url.expose(),
            ),
            (
                "transformer.ducklake.attach_name",
                self.ducklake.attach_name.as_str(),
            ),
            (
                "transformer.ducklake.metadata_schema",
                self.ducklake.metadata_schema.as_str(),
            ),
            (
                "transformer.ducklake.data_path",
                self.ducklake.data_path.as_str(),
            ),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::Missing(field));
            }
        }
        LeaseTtl::new(self.lease_ttl)?;
        for (field, value) in [
            ("transformer.poll_interval", self.poll_interval),
            ("transformer.compaction_interval", self.compaction_interval),
            (
                "transformer.ducklake.snapshot_retention",
                self.ducklake.snapshot_retention,
            ),
            (
                "transformer.ducklake.cleanup_grace",
                self.ducklake.cleanup_grace,
            ),
            (
                "transformer.ducklake.maintenance_interval",
                self.ducklake.maintenance_interval,
            ),
        ] {
            if value.is_zero() {
                return Err(ConfigError::ZeroInterval(field));
            }
        }
        let index = self.effective_shard_index()?;
        if index >= self.shard_count.get() {
            return Err(ConfigError::ShardIndex {
                index,
                count: self.shard_count.get(),
            });
        }
        for (field, value) in [
            (
                "transformer.ducklake.attach_name",
                self.ducklake.attach_name.as_str(),
            ),
            (
                "transformer.ducklake.metadata_schema",
                self.ducklake.metadata_schema.as_str(),
            ),
        ] {
            common::sql::SqlIdent::new(value)
                .map_err(|source| ConfigError::Identifier { field, source })?;
        }
        if !self.ducklake.data_path.starts_with("s3://") {
            return Err(ConfigError::DuckLakeDataPath(
                self.ducklake.data_path.clone(),
            ));
        }
        common::runtime::validate_worker_threads(self.worker_threads)?;
        if i32::try_from(self.max_integrity_resnapshots).is_err() {
            return Err(ConfigError::IntegrityResnapshotBudget {
                configured: self.max_integrity_resnapshots,
            });
        }
        Ok(())
    }
}

/// Fully resolved configuration returned to the transformer.
#[derive(Debug, Clone, Default)]
pub struct TransformerConfig {
    /// Settings shared by both services.
    pub common: CommonConfig,
    /// Transformer-owned settings.
    pub transformer: TransformerSettings,
}

impl TransformerConfig {
    /// Load the mandatory path in `WALRUS_CONFIG`, overlay environment values, and validate.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for a missing path, malformed input, or invalid active settings.
    pub fn load() -> Result<Self, ConfigError> {
        loader::load_from_environment(Service::Transformer).and_then(Self::from_document)
    }

    /// Load, overlay, and validate a particular YAML file.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for an unsupported path, malformed input, or invalid settings.
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        loader::load(path.as_ref(), Service::Transformer).and_then(Self::from_document)
    }

    fn from_document(document: loader::ConfigDocument) -> Result<Self, ConfigError> {
        let config = Self {
            common: document.common,
            transformer: document.transformer,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate shared and transformer settings without network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a semantic invariant is violated.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.common.validate()?;
        self.transformer.validate()
    }
}
