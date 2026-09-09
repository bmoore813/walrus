//! The walrus control plane: sqlx access to the coordination-contract tables
//! (`replication_state`, `file_manifest`, `transformer_checkpoint`, `schema_registry`, `ddl_manifest`).
//!
//! This crate owns the control-DB connection pool, versioned migrations, and row-level models for
//! manifest claims, checkpoints, schema history, ownership, and reload coordination.

// Every item these eight modules declare is published by the `pub use` block below, so `pub` on the
// modules themselves would only mint a second public path for each one. Crate visibility makes that
// flat block the whole API, which leaves the row models, their queries and the module boundaries
// between them free to move without breaking a consumer. `reload` is the documented exception: its
// transition functions stay module-qualified (see the note on its re-export), so it must stay
// reachable as a path.
pub(crate) mod checkpoint;
pub(crate) mod db;
pub(crate) mod ddl_manifest;
pub(crate) mod integrity;
pub(crate) mod manifest;
pub(crate) mod parse;
pub mod reload;
pub(crate) mod replication_state;
pub(crate) mod schema_registry;
pub(crate) mod table_ownership;

pub use checkpoint::{
    Checkpoint, advance_raw_appended, advance_transformed, ensure_checkpoint, read_checkpoint,
};
pub use db::{ControlError, connect, run_migrations};
pub use ddl_manifest::{
    DdlRow, insert_ddl, read_all_ddl, read_latest_ddl_snapshot, read_latest_ddl_version_through,
    read_pending_ddl,
};
pub use integrity::{
    IntegrityFailure, IntegrityFailureOutcome, IntegrityPublicationFence, IntegrityRecoveryRow,
    IntegrityRecoveryStatus, handle_integrity_failure, read_integrity_recovery,
};
pub use manifest::{
    ManifestGroupId, ManifestKind, ManifestRow, ManifestStatus, NewManifestFile,
    NewStreamCommitPublication, PublishManifestOutcome, PublishStreamOutcome, ReadyManifestUnit,
    StreamSchemaBarrier, claim_ready, claim_ready_units, complete_schema_barriers, delete_claimed,
    delete_publication_superseded, insert_ready, list_manifest_uris,
    lock_manifest_publication_table, lock_manifest_work_groups, manifest_work_exists,
    max_ready_lsn_end, publish_ready_manifest, publish_stream_commit, validate_claimed_groups,
};
pub use parse::ParseEnumError;
// The reload transition functions stay module-qualified (`reload::request`, `reload::fail`, …):
// several of their names (`renew_lease`, `complete`, `get`) would collide with or read vaguer
// than the flat exports above. Only the types go flat.
pub use reload::{
    ExportRangePlan, ExportSeal, ExportSnapshot, ExporterLease, ReloadFenceIdentity, ReloadFlavor,
    ReloadMarkerKind, ReloadMarkerRow, ReloadPublication, ReloadRow, ReloadScope, ReloadStatus,
    SourceReloadRequest,
};
pub use replication_state::{
    BootstrapProgress, CURRENT_CATALOG_FENCE_VERSION, ReplicationState, ReplicationStatus,
    bump_bootstrap_epoch, bump_epoch, complete_bootstrap, insert_epoch, mark_total_restart,
    read_bootstrap_progress, read_current_epoch,
};
pub use schema_registry::{
    RegistryRow, read_all_latest_registry, read_all_registry, read_latest_version, read_registry,
    read_registry_range, upsert_registry,
};
pub use table_ownership::{Lease, acquire_lease, release_lease, renew_lease};
