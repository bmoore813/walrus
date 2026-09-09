//! Settings consumed by both services.

use crate::ConfigError;
use common::{Redacted, TelemetryConfig};
use serde::Deserialize;

/// Database, object-store, and telemetry settings shared by both Walrus services.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct CommonConfig {
    /// Control PostgreSQL connection string.
    pub control_db_url: Redacted<String>,
    /// S3 or MinIO staging location.
    pub object_store: ObjectStoreConfig,
    /// Logging format and filter.
    pub telemetry: TelemetryConfig,
}

/// Where staged Parquet objects live.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ObjectStoreConfig {
    /// Bucket written by the extractor and read by the transformer.
    pub bucket: String,
    /// Custom S3-compatible endpoint; `None` selects AWS.
    pub endpoint: Option<String>,
    /// Region sent with object-store requests.
    pub region: String,
}

impl Default for ObjectStoreConfig {
    fn default() -> Self {
        Self {
            bucket: String::new(),
            endpoint: None,
            region: "us-east-1".to_string(),
        }
    }
}

impl CommonConfig {
    /// Validate the shared settings without performing network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::Missing`] when a required shared value is blank.
    pub fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            ("common.control_db_url", self.control_db_url.expose()),
            ("common.object_store.bucket", &self.object_store.bucket),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::Missing(field));
            }
        }
        Ok(())
    }
}
