//! Extractor-specific settings and validated value types.

use crate::loader::{self, Service};
use crate::{CommonConfig, ConfigError};
use common::Redacted;
use serde::Deserialize;
use std::fmt;
use std::net::SocketAddr;
use std::num::NonZeroU64;
use std::path::Path;
use std::time::Duration;

const MAX_DURATION: Duration = Duration::from_secs(60 * 60);
const MAX_SLOT_NAME_LEN: usize = 63;

const fn nz(value: u64) -> NonZeroU64 {
    match NonZeroU64::new(value) {
        Some(value) => value,
        None => NonZeroU64::MIN,
    }
}

/// A ratio in the open unit interval `(0.0, 1.0)`.
#[derive(Debug, Clone, Copy, PartialEq, PartialOrd, Deserialize)]
#[serde(try_from = "f64")]
pub struct Ratio(f64);

/// Why a raw floating-point value was rejected as a [`Ratio`].
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum RatioError {
    /// NaN and infinities cannot participate in backpressure comparisons.
    #[error("ratio {0} is not finite — NaN and infinities disable the backstop")]
    NonFinite(f64),
    /// The finite value was not strictly between zero and one.
    #[error("ratio {0} is out of range — require 0.0 < r < 1.0")]
    OutOfRange(f64),
}

impl Ratio {
    /// Construct a validated ratio.
    ///
    /// # Errors
    ///
    /// Returns [`RatioError`] unless `raw` is finite and strictly between zero and one.
    pub const fn new(raw: f64) -> Result<Self, RatioError> {
        if !raw.is_finite() {
            return Err(RatioError::NonFinite(raw));
        }
        if 0.0 < raw && raw < 1.0 {
            Ok(Self(raw))
        } else {
            Err(RatioError::OutOfRange(raw))
        }
    }

    /// Return the validated floating-point ratio.
    #[must_use]
    pub const fn as_f64(self) -> f64 {
        self.0
    }
}

impl TryFrom<f64> for Ratio {
    type Error = RatioError;

    fn try_from(raw: f64) -> Result<Self, Self::Error> {
        Self::new(raw)
    }
}

/// A pause/resume band whose resume threshold is below its activation threshold.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct HysteresisBand {
    activate: Ratio,
    resume: Ratio,
}

/// Why two valid ratios did not form a valid [`HysteresisBand`].
#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
#[error("require 0 < resume ({resume}) < activate ({activate}) < 1.0")]
pub struct BandError {
    /// Rejected activation ratio.
    pub activate: f64,
    /// Rejected resume ratio.
    pub resume: f64,
}

impl HysteresisBand {
    /// The shipped pause/resume thresholds.
    pub const DEFAULT: Self = Self {
        activate: Ratio(0.85),
        resume: Ratio(0.75),
    };

    /// Construct a band with a strict gap between resume and activation.
    ///
    /// # Errors
    ///
    /// Returns [`BandError`] when `resume` is greater than or equal to `activate`.
    pub fn new(activate: Ratio, resume: Ratio) -> Result<Self, BandError> {
        if resume < activate {
            Ok(Self { activate, resume })
        } else {
            Err(BandError {
                activate: activate.as_f64(),
                resume: resume.as_f64(),
            })
        }
    }

    /// Activation threshold.
    #[must_use]
    pub const fn activate(self) -> Ratio {
        self.activate
    }

    /// Resume threshold.
    #[must_use]
    pub const fn resume(self) -> Ratio {
        self.resume
    }
}

/// A PostgreSQL replication slot name validated against the server's accepted alphabet and length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SlotName(String);

impl SlotName {
    /// Parse a raw replication slot name.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidSlotName`] for an empty, overlong, or invalid name.
    pub fn new(raw: &str) -> Result<Self, ConfigError> {
        let reject = |detail| ConfigError::InvalidSlotName {
            slot: raw.to_string(),
            detail,
        };
        if raw.is_empty() {
            return Err(reject("must not be empty".to_string()));
        }
        if raw.len() > MAX_SLOT_NAME_LEN {
            return Err(reject(format!(
                "is {} bytes; Postgres accepts at most {MAX_SLOT_NAME_LEN}",
                raw.len()
            )));
        }
        if let Some(bad) = raw
            .chars()
            .find(|character| !matches!(character, 'a'..='z' | '0'..='9' | '_'))
        {
            return Err(reject(format!(
                "contains {bad:?}; a slot name may only contain lower case letters, numbers, and the underscore character"
            )));
        }
        Ok(Self(raw.to_string()))
    }

    /// Return the bare slot name.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SlotName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Extractor-owned portion of the versioned YAML document.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ExtractorSettings {
    /// Source PostgreSQL logical-replication connection string.
    pub source_db_url: Redacted<String>,
    /// Tokio worker threads; `None` uses available parallelism.
    pub worker_threads: Option<usize>,
    /// Stable process instance identity.
    pub instance: String,
    /// Replication slot owned by this extractor.
    pub slot_name: String,
    /// Publication streamed through the slot.
    pub publication_name: String,
    /// Maximum time before an open batch is flushed.
    #[serde(with = "humantime_serde")]
    pub max_fill: Duration,
    /// Idle duration before publishing a heartbeat.
    #[serde(with = "humantime_serde")]
    pub heartbeat_idle_after: Duration,
    /// Maximum heartbeat round-trip duration before degraded health.
    #[serde(with = "humantime_serde")]
    pub heartbeat_roundtrip_deadline: Duration,
    /// Compatibility-only, ignored snapshot backfill statement timeout.
    #[serde(with = "humantime_serde")]
    pub backfill_statement_timeout: Duration,
    /// Row-count batch flush threshold.
    pub max_rows: NonZeroU64,
    /// Byte-count batch flush threshold.
    pub max_bytes: NonZeroU64,
    /// Process-wide buffered-byte backpressure ceiling.
    pub max_inflight_bytes: NonZeroU64,
    /// Ratio at which intake pauses.
    pub backpressure_activate_ratio: Ratio,
    /// Lower ratio at which intake resumes.
    pub backpressure_resume_ratio: Ratio,
    /// Bootstrap retry budget.
    #[serde(with = "humantime_serde")]
    pub startup_deadline: Duration,
    /// Health endpoint bind address.
    pub health_addr: SocketAddr,
    /// Maximum tables reloaded concurrently.
    pub max_concurrent_reloads: NonZeroU64,
    /// Source COPY workers assigned to each table reload.
    pub reload_workers_per_table: NonZeroU64,
    /// Reload ownership lease lifetime.
    #[serde(with = "humantime_serde")]
    pub reload_lease_ttl: Duration,
    /// Rows written to each reload object.
    pub reload_chunk_rows: NonZeroU64,
    /// Maximum wait for a reload watermark echo.
    #[serde(with = "humantime_serde")]
    pub reload_echo_timeout: Duration,
    /// Allowed restarts after schema changes during a reload.
    pub reload_max_restarts: i32,
    /// Whether the extractor creates and updates the configured publication.
    pub manage_publication: bool,
    /// Require every published table to have a primary key.
    pub strict_keys: bool,
}

impl Default for ExtractorSettings {
    fn default() -> Self {
        Self {
            source_db_url: Redacted::default(),
            worker_threads: None,
            instance: String::new(),
            slot_name: String::new(),
            publication_name: String::new(),
            max_fill: Duration::from_secs(5),
            heartbeat_idle_after: Duration::from_secs(10),
            heartbeat_roundtrip_deadline: Duration::from_secs(30),
            backfill_statement_timeout: Duration::ZERO,
            max_rows: nz(100_000),
            max_bytes: nz(128 * 1024 * 1024),
            max_inflight_bytes: nz(512 * 1024 * 1024),
            backpressure_activate_ratio: HysteresisBand::DEFAULT.activate(),
            backpressure_resume_ratio: HysteresisBand::DEFAULT.resume(),
            startup_deadline: Duration::from_secs(60),
            health_addr: SocketAddr::from(([0, 0, 0, 0], 8080)),
            max_concurrent_reloads: nz(2),
            reload_workers_per_table: nz(4),
            reload_lease_ttl: Duration::from_secs(60),
            reload_chunk_rows: nz(10_000),
            reload_echo_timeout: Duration::from_secs(30),
            reload_max_restarts: 3,
            manage_publication: false,
            strict_keys: true,
        }
    }
}

impl ExtractorSettings {
    /// Return the validated backpressure hysteresis band.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::OutOfBounds`] if resume is not lower than activation.
    pub fn hysteresis_band(&self) -> Result<HysteresisBand, ConfigError> {
        HysteresisBand::new(
            self.backpressure_activate_ratio,
            self.backpressure_resume_ratio,
        )
        .map_err(|error| ConfigError::OutOfBounds {
            field: "extractor.backpressure_activate_ratio",
            detail: error.to_string(),
        })
    }

    /// Return the parsed replication slot name.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError::InvalidSlotName`] if the configured name is invalid.
    pub fn slot(&self) -> Result<SlotName, ConfigError> {
        SlotName::new(&self.slot_name)
    }

    fn validate(&self) -> Result<(), ConfigError> {
        for (field, value) in [
            ("extractor.source_db_url", self.source_db_url.expose()),
            ("extractor.instance", &self.instance),
            ("extractor.slot_name", &self.slot_name),
            ("extractor.publication_name", &self.publication_name),
        ] {
            if value.trim().is_empty() {
                return Err(ConfigError::Missing(field));
            }
        }
        self.slot()?;
        if !self.strict_keys {
            return Err(ConfigError::OutOfBounds {
                field: "extractor.strict_keys",
                detail: "must be true: keyless tables cannot be durably excluded from a generation"
                    .to_string(),
            });
        }
        common::runtime::validate_worker_threads(self.worker_threads).map_err(|error| {
            ConfigError::OutOfBounds {
                field: "extractor.worker_threads",
                detail: error.to_string(),
            }
        })?;
        for (field, value) in [
            ("extractor.max_fill", self.max_fill),
            ("extractor.startup_deadline", self.startup_deadline),
            ("extractor.heartbeat_idle_after", self.heartbeat_idle_after),
            (
                "extractor.heartbeat_roundtrip_deadline",
                self.heartbeat_roundtrip_deadline,
            ),
            ("extractor.reload_echo_timeout", self.reload_echo_timeout),
            ("extractor.reload_lease_ttl", self.reload_lease_ttl),
        ] {
            duration_bound(field, value)?;
        }
        if self.heartbeat_idle_after >= self.heartbeat_roundtrip_deadline {
            return Err(ConfigError::OutOfBounds {
                field: "extractor.heartbeat_idle_after",
                detail: format!(
                    "must be < heartbeat_roundtrip_deadline ({:?})",
                    self.heartbeat_roundtrip_deadline
                ),
            });
        }
        if self.reload_max_restarts < 0 {
            return Err(ConfigError::OutOfBounds {
                field: "extractor.reload_max_restarts",
                detail: "must be greater than or equal to zero".to_string(),
            });
        }
        if self.reload_lease_ttl < Duration::from_secs(15) {
            return Err(ConfigError::OutOfBounds {
                field: "extractor.reload_lease_ttl",
                detail: "must be at least 15s".to_string(),
            });
        }
        if self.max_inflight_bytes < self.max_bytes {
            return Err(ConfigError::OutOfBounds {
                field: "extractor.max_inflight_bytes",
                detail: format!("must be at least max_bytes ({})", self.max_bytes),
            });
        }
        let tables = usize::try_from(self.max_concurrent_reloads.get()).map_err(|_| {
            ConfigError::OutOfBounds {
                field: "extractor.max_concurrent_reloads",
                detail: "does not fit this platform's connection-count type".to_string(),
            }
        })?;
        if tables > tokio::sync::Semaphore::MAX_PERMITS {
            return Err(ConfigError::OutOfBounds {
                field: "extractor.max_concurrent_reloads",
                detail: format!(
                    "{tables} exceeds Tokio's semaphore limit {}",
                    tokio::sync::Semaphore::MAX_PERMITS
                ),
            });
        }
        let workers = usize::try_from(self.reload_workers_per_table.get()).map_err(|_| {
            ConfigError::OutOfBounds {
                field: "extractor.reload_workers_per_table",
                detail: "does not fit this platform's connection-count type".to_string(),
            }
        })?;
        tables
            .checked_mul(workers)
            .ok_or_else(|| ConfigError::OutOfBounds {
                field: "extractor.reload_workers_per_table",
                detail: format!("{tables} tables × {workers} workers overflows the derived limit"),
            })?;
        self.hysteresis_band()?;
        Ok(())
    }
}

/// Fully resolved configuration returned to the extractor.
#[derive(Debug, Clone, Default)]
pub struct ExtractorConfig {
    /// Settings shared by both services.
    pub common: CommonConfig,
    /// Extractor-owned settings.
    pub extractor: ExtractorSettings,
}

impl ExtractorConfig {
    /// Load the mandatory path in `WALRUS_CONFIG`, overlay environment values, and validate.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for a missing path, malformed input, or invalid active settings.
    pub fn load() -> Result<Self, ConfigError> {
        loader::load_from_environment(Service::Extractor).and_then(Self::from_document)
    }

    /// Load, overlay, and validate a particular YAML file.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] for an unsupported path, malformed input, or invalid settings.
    pub fn load_from(path: impl AsRef<Path>) -> Result<Self, ConfigError> {
        loader::load(path.as_ref(), Service::Extractor).and_then(Self::from_document)
    }

    fn from_document(document: loader::ConfigDocument) -> Result<Self, ConfigError> {
        let config = Self {
            common: document.common,
            extractor: document.extractor,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate shared and extractor settings without network I/O.
    ///
    /// # Errors
    ///
    /// Returns [`ConfigError`] when a semantic invariant is violated.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.common.validate()?;
        self.extractor.validate()
    }
}

fn duration_bound(field: &'static str, duration: Duration) -> Result<(), ConfigError> {
    const {
        assert!(
            !MAX_DURATION.is_zero(),
            "MAX_DURATION must be nonzero or every positive cadence exceeds the ceiling"
        );
    }
    if duration.is_zero() {
        return Err(ConfigError::OutOfBounds {
            field,
            detail: "must be greater than zero".to_string(),
        });
    }
    if duration > MAX_DURATION {
        return Err(ConfigError::OutOfBounds {
            field,
            detail: format!("{duration:?} exceeds the {MAX_DURATION:?} ceiling"),
        });
    }
    Ok(())
}
