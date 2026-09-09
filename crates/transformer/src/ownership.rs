//! Multi-pod transformer table-sharding compatibility module.
//!
//! The original public module was an inert placeholder. Routing now lives in
//! [`crate::duck::table_shard`], and ordered control-lease plus PostgreSQL catalog-lock fencing lives
//! in [`crate::bootstrap`]. This empty module is retained to avoid needlessly breaking code that
//! imported the former namespace. See `docs/ducklake-migration.md` for the rollout contract.
