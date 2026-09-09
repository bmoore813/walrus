//! One versioned YAML configuration contract shared by every Walrus service.
//!
//! Normal startup requires `WALRUS_CONFIG` to name a `.yaml` or `.yml` file. Flat
//! `WALRUS_*` environment variables remain supported as the highest-precedence layer: shared keys
//! are routed under `common`, and all other keys are routed under the service being loaded.

mod error;
mod extractor;
mod loader;
mod shared;
mod transformer;

pub use error::ConfigError;
pub use extractor::{
    BandError, ExtractorConfig, ExtractorSettings, HysteresisBand, Ratio, RatioError, SlotName,
};
pub use shared::{CommonConfig, ObjectStoreConfig};
pub use transformer::{
    DuckLakeConfig, LeaseTtl, MIN_LEASE_TTL, TransformerConfig, TransformerSettings,
};

/// The only configuration document version this crate currently accepts.
pub const CONFIG_VERSION: u64 = 1;
