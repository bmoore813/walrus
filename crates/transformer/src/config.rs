//! Transformer-facing re-exports of the shared Walrus configuration contract.

pub use walrus_config::{
    CommonConfig, ConfigError, DuckLakeConfig, LeaseTtl, MIN_LEASE_TTL, ObjectStoreConfig,
    TransformerConfig, TransformerSettings,
};
