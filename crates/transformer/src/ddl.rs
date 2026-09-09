//! Additive / lossless DDL apply (transformer §5.7, architecture "per-change-type handling") — the transformer's
//! schema-evolution half. **Schema-DIFF, never `c_ddl_text` replay:** each change is derived from the
//! `new − old` column sets in `schema_registry`. A common ordinal is not durable attnum lineage, so
//! production reconciliation rejects name substitutions that could be either RENAME or DROP+ADD.
//!
//! The **homogeneous-file rule** (one `schema_version` per Parquet file) lets the transformer gate cleanly:
//! before it appends/transforms any file at version V it reconciles both DuckDB tables *up to* V, so no
//! file ever straddles a structural boundary. The mirror `<table>` is kept at the **exact** current
//! source shape; `<table>_raw` is an **additive superset** (columns only ever added / widened / renamed —
//! never dropped here), so old verbatim rows stay valid (a new column reads NULL for them).
//!
//! **Additive / lossless**: `ADD COLUMN`, `RENAME TABLE`, and a lossless/widening
//! `ALTER COLUMN TYPE`. `COMMENT` is a metadata revision: the source records a complete table and
//! column-comment snapshot, and the transformer applies that snapshot only to the mirror without parsing
//! PostgreSQL DDL text or creating a structural data barrier.
//! **Destructive**, where mirror and raw diverge most: `DROP COLUMN` (physical on
//! the mirror, retained-nullable on raw), a **lossy** `ALTER COLUMN TYPE` (attempt the in-place mirror
//! cast → on failure **quarantine + alert + stop**, an accepted terminal v1 outcome; raw is widened to
//! `VARCHAR`, never re-cast), and `DROP TABLE` (retire both tables + file). Raw is an additive superset:
//! it only ever adds / widens — it never destructively drops or re-casts history.
//! A common-position source-name substitution is classified as a possible `RENAME COLUMN`, but
//! production reconciliation quarantines it: the current registry omits stable attnum lineage, so a
//! genuine rename and one same-statement `DROP old, ADD new` are indistinguishable.

use crate::duck::{TableDb, duck_type, user_view_sql};
use crate::duck_ext::DuckResultExt;
use crate::error::TransformerError;
use crate::plan::TablePlan;
use common::oids::{FLOAT4, FLOAT8, INT2, INT4, INT8};
use common::sql::{SqlIdent, SqlStrExt};
use common::{DdlId, EpochNo, PgColumn, PgRelation, SchemaVersionNo};
use std::collections::{HashMap, HashSet};
use std::fmt::Write as _;

/// One `schema_version` of a table's shape — the `schema_registry` `columns` snapshot for that version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaVersion {
    /// The version number this shape belongs to.
    pub version: SchemaVersionNo,
    /// The table's columns at that version, in attnum order — the input both sides of a diff take.
    pub relation: PgRelation,
}

/// What an explicitly supplied local `COMMENT` helper targets. Registry reconciliation never
/// constructs this from the source audit text, which is not a structured replay contract.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommentTarget {
    /// `COMMENT ON TABLE`.
    Table,
    /// `COMMENT ON COLUMN`, carrying the column name.
    Column(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct CommentSnapshot {
    table: Option<String>,
    columns: Vec<(String, Option<String>)>,
}

/// One additive/lossless structural change. The explicit comment variant is a local helper only;
/// [`diff_additive`] cannot derive it from a registry shape.
///
/// A classification result is pure data — every payload is a `String`, a `usize`, or a [`PgColumn`],
/// all of which are already `Clone`/`Eq` — so a caller can compare a derived change against an
/// expected one directly instead of pattern-matching it field by field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AdditiveChange {
    /// A column appended at the end (a higher attnum). Nullable on BOTH tables so pre-change rows read
    /// NULL and old verbatim `<table>_raw` rows stay valid.
    AddColumn(PgColumn),
    /// A position-tracked rename applied to both tables (never a drop+add).
    RenameColumn {
        /// Zero-based position of the renamed source column.
        position: usize,
        /// Previous source column name.
        from: String,
        /// Current source column name.
        to: String,
    },
    /// A source `RENAME TABLE` — the mirror, its CDC log, and the user view all follow.
    RenameTable {
        /// Previous source table name.
        from: String,
        /// Current source table name.
        to: String,
    },
    /// A lossless/widening `ALTER COLUMN TYPE` (e.g. int4→int8) — an in-place DuckDB cast on both tables.
    WidenColumn {
        /// Zero-based position of the changed source column.
        position: usize,
        /// Stable source column name.
        name: String,
        /// New source-column descriptor.
        new: PgColumn,
    },
    /// An explicitly supplied `COMMENT ON` helper. Never derived from source `c_ddl_text`.
    Comment {
        /// Table or column receiving the comment.
        target: CommentTarget,
        /// New comment text, or `None` to remove the comment.
        text: Option<String>,
    },
}

/// One destructive change — where mirror and raw **diverge**: the mirror follows the exact
/// current shape (physical drop / in-place cast), the raw log preserves history (retain / widen-only).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DestructiveChange {
    /// `DROP COLUMN` — physically dropped from `<table>`; **retained nullable** in `<table>_raw`.
    DropColumn {
        /// Source column removed from the current mirror shape.
        name: String,
    },
    /// A lossy/incompatible `ALTER COLUMN TYPE` — attempt the in-place mirror cast (→ quarantine on
    /// failure); widen `<table>_raw` to `VARCHAR` (never re-cast historical values).
    LossyType {
        /// Stable source column name.
        name: String,
        /// New source-column descriptor requiring a lossy cast.
        new: PgColumn,
    },
    /// `DROP TABLE` — retire both DuckDB tables (the `.duckdb` file is retired by the caller).
    DropTable {
        /// Source table removed from the current registry shape.
        name: String,
    },
}

/// Whether the two columns map to the SAME DuckDB type (the only thing an `ALTER COLUMN TYPE` would
/// change) — a typmod-only tweak (e.g. `varchar(10)→varchar(20)`, both DuckDB `VARCHAR`) is a no-op.
fn is_same_duck_type(a: &PgColumn, b: &PgColumn) -> bool {
    duck_type(a.type_oid) == duck_type(b.type_oid)
}

/// A widening DuckDB *can* do in place without loss — the additive subset. Anything else whose DuckDB
/// type changes is **lossy/narrowing** and belongs to the quarantine path.
const fn is_lossless_widen(old: &PgColumn, new: &PgColumn) -> bool {
    // int2→int4→int8 and float4→float8 are the only in-place widenings, grouped by source type.
    matches!(
        (old.type_oid, new.type_oid),
        (INT2, INT4 | INT8) | (INT4, INT8) | (FLOAT4, FLOAT8)
    )
}

/// The full classification of one version step: additive/lossless changes plus destructive ones.
/// The extractor cuts one file per structural change, so a step usually yields a single change.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SchemaDiff {
    /// Changes that can be applied in place to both tables without losing anything.
    pub additive: Vec<AdditiveChange>,
    /// Changes where the mirror and the raw log must diverge. A non-empty vector here is what
    /// routes the step away from [`apply_additive`] and onto the quarantine-capable path.
    pub destructive: Vec<DestructiveChange>,
}

/// Diff `old → new` by common ordinal, classifying each change as additive or **destructive**.
/// A shorter shape is accepted only when every survivor is an unchanged ordered subsequence; an
/// empty new column set is a `DROP TABLE`. A type change is a lossless widen (additive) or a
/// lossy/narrowing one (destructive). Production preflight rejects ambiguous name substitutions.
///
/// # Errors
///
/// Returns [`TransformerError::ManifestInvariant`] when a shrinking relation does not leave an exactly
/// unchanged ordered subsequence. That means the source combined DROP with another structural
/// operation the registry snapshots cannot replay safely as one step.
pub fn diff(old: &SchemaVersion, new: &SchemaVersion) -> Result<SchemaDiff, TransformerError> {
    let mut d = SchemaDiff::default();

    // DROP TABLE: the registered version carries no columns (the extractor's `sql_drop` sentinel).
    if new.relation.columns.is_empty() && !old.relation.columns.is_empty() {
        d.destructive.push(DestructiveChange::DropTable {
            name: old.relation.name.clone(),
        });
        return Ok(d);
    }

    if old.relation.name != new.relation.name {
        d.additive.push(AdditiveChange::RenameTable {
            from: old.relation.name.clone(),
            to: new.relation.name.clone(),
        });
    }

    let (oc, nc) = (&old.relation.columns, &new.relation.columns);
    if nc.len() < oc.len() {
        // DROP COLUMN(s): every survivor must be an exactly unchanged, ordered subsequence. A
        // concurrent rename/type/identity change in the same step is deliberately rejected instead
        // of being mistaken for another dropped column after attnums shift.
        let mut old_floor = 0;
        for survivor in nc {
            let Some(relative) = oc[old_floor..]
                .iter()
                .position(|column| column.name == survivor.name)
            else {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "schema step {}->{} shrinks columns but survivor {:?} is not an ordered old column",
                        old.version, new.version, survivor.name
                    ),
                });
            };
            let old_position = old_floor + relative;
            if oc[old_position] != *survivor {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "schema step {}->{} changes surviving column {:?} while dropping columns",
                        old.version, new.version, survivor.name
                    ),
                });
            }
            old_floor = old_position + 1;
        }
        let kept: std::collections::HashSet<&str> = nc.iter().map(|c| c.name.as_str()).collect();
        let dropped = oc.iter().filter(|o| !kept.contains(o.name.as_str()));
        d.destructive
            .extend(dropped.map(|o| DestructiveChange::DropColumn {
                name: o.name.clone(),
            }));
        return Ok(d);
    }

    // len(new) >= len(old): position-matched rename / type-change over the common prefix, then any
    // trailing appended columns are ADDs.
    for (i, (o, n)) in oc.iter().zip(nc.iter()).enumerate() {
        if o.name != n.name {
            d.additive.push(AdditiveChange::RenameColumn {
                position: i,
                from: o.name.clone(),
                to: n.name.clone(),
            });
        }
        if !is_same_duck_type(o, n) {
            if is_lossless_widen(o, n) {
                d.additive.push(AdditiveChange::WidenColumn {
                    position: i,
                    name: n.name.clone(),
                    new: n.clone(),
                });
            } else {
                d.destructive.push(DestructiveChange::LossyType {
                    name: n.name.clone(),
                    new: n.clone(),
                });
            }
        }
    }
    let appended = nc[oc.len()..].iter().cloned();
    d.additive.extend(appended.map(AdditiveChange::AddColumn));
    Ok(d)
}

/// The additive-only view of [`diff`] — errors if the step is destructive (use [`diff`] +
/// [`apply_destructive`] for those). Kept so the additive path stays a total function.
///
/// # Errors
///
/// Returns [`TransformerError::Internal`] when the version step contains any destructive change.
pub fn diff_additive(
    old: &SchemaVersion,
    new: &SchemaVersion,
) -> Result<Vec<AdditiveChange>, TransformerError> {
    let d = diff(old, new)?;
    if !d.destructive.is_empty() {
        return Err(TransformerError::Internal(format!(
            "destructive change on {} — not an additive diff",
            new.relation.name
        )));
    }
    Ok(d.additive)
}

/// Apply explicitly supplied changes to the DuckDB tables per the taxonomy: mirror = exact shape,
/// `<table>_raw` = additive superset (nullable adds). A `COMMENT` changes only the mirror. Production
/// metadata reconciliation supplies these changes from structured source snapshots. A `SELECT *` view binds its
/// columns at creation, so the user view is recreated after any structural change.
///
/// # Errors
///
/// Returns [`TransformerError::Duck`] if DuckDB rejects the assembled additive DDL transaction batch.
pub fn apply_additive(
    conn: &duckdb::Connection,
    table: &str,
    changes: &[AdditiveChange],
) -> Result<(), TransformerError> {
    apply_additive_inner(conn, table, changes, None)
}

/// Registry-aware additive apply used by [`reconcile_to_version`]. Unlike the public scalar helper,
/// this preserves the physical boundary of each logical source column, so Tier-2 ADD/RENAME changes
/// update every emitted raw and mirror sibling instead of assuming a one-to-one column shape.
fn apply_additive_registry(
    conn: &duckdb::Connection,
    table: &str,
    changes: &[AdditiveChange],
    old: &RegistryVersion,
    new: &RegistryVersion,
) -> Result<(), TransformerError> {
    apply_additive_inner(conn, table, changes, Some((old, new)))
}

fn apply_additive_inner(
    conn: &duckdb::Connection,
    table: &str,
    changes: &[AdditiveChange],
    registry: Option<(&RegistryVersion, &RegistryVersion)>,
) -> Result<(), TransformerError> {
    let sql = render_additive_inner(table, changes, registry)?;
    if sql.is_empty() {
        return Ok(());
    }
    conn.execute_batch(&sql)
        .duck_with(|| format!("apply additive DDL to {table}"))
}

/// Assemble the exact additive-DDL batch executed by [`apply_additive_inner`]. Keeping rendering
/// pure makes the whole ordered plan reviewable without replacing DuckDB execution coverage.
fn render_additive_inner(
    table: &str,
    changes: &[AdditiveChange],
    registry: Option<(&RegistryVersion, &RegistryVersion)>,
) -> Result<String, TransformerError> {
    // Each change emits a mirror + a _raw statement, roughly 120 bytes together.
    let mut sql = String::with_capacity(changes.len() * 128);
    let mut cur = table.to_string();
    let mut structural = false;
    for ch in changes {
        match ch {
            AdditiveChange::AddColumn(c) => {
                if let Some((_, new)) = registry {
                    let position = new
                        .shape
                        .relation
                        .columns
                        .iter()
                        .position(|column| column == c)
                        .ok_or_else(|| TransformerError::ManifestInvariant {
                            message: format!(
                                "schema {} ADD COLUMN {:?} is absent from its registry relation",
                                new.shape.version, c.name
                            ),
                        })?;
                    let plan = registry_column_plan(new, position)?;
                    for column in &plan.mirror_cols {
                        let name = &column.name;
                        let ty = &column.duckdb_type;
                        let _write_result = write!(
                            &mut sql,
                            "ALTER TABLE \"{cur}\" ADD COLUMN IF NOT EXISTS {name} {ty};"
                        );
                    }
                    for column in &plan.raw_cols {
                        let name = &column.name;
                        let ty = &column.duckdb_type;
                        let _write_result = write!(
                            &mut sql,
                            "ALTER TABLE \"{cur}_raw\" ADD COLUMN IF NOT EXISTS {name} {ty};"
                        );
                    }
                } else {
                    let ty = duck_type(c.type_oid);
                    let name = &c.name;
                    // Nullable on both (no NOT NULL): pre-change rows read NULL; old raw rows stay valid.
                    // `String`'s `fmt::Write` implementation is infallible.
                    let _write_result = write!(
                        &mut sql,
                        "ALTER TABLE \"{cur}\" ADD COLUMN IF NOT EXISTS {name} {ty}; \
                         ALTER TABLE \"{cur}_raw\" ADD COLUMN IF NOT EXISTS {name} {ty};"
                    );
                }
                structural = true;
            }
            AdditiveChange::RenameColumn { position, from, to } => {
                if let Some((old, new)) = registry {
                    let old_plan = registry_column_plan(old, *position)?;
                    let new_plan = registry_column_plan(new, *position)?;
                    append_physical_renames(
                        &mut sql,
                        &cur,
                        from,
                        to,
                        &old_plan,
                        &new_plan,
                        old.shape.version,
                        new.shape.version,
                    )?;
                } else {
                    let _write_result = write!(
                        &mut sql,
                        "ALTER TABLE \"{cur}\" RENAME COLUMN {from} TO {to}; \
                         ALTER TABLE \"{cur}_raw\" RENAME COLUMN {from} TO {to};"
                    );
                }
                structural = true;
            }
            AdditiveChange::WidenColumn {
                position,
                name,
                new: widened,
            } => {
                if let Some((old, new)) = registry {
                    let old_plan = registry_column_plan(old, *position)?;
                    let new_plan = registry_column_plan(new, *position)?;
                    append_physical_widen(
                        &mut sql,
                        &cur,
                        name,
                        &old_plan,
                        &new_plan,
                        old.shape.version,
                        new.shape.version,
                    )?;
                } else {
                    let ty = duck_type(widened.type_oid);
                    let _write_result = write!(
                        &mut sql,
                        "ALTER TABLE \"{cur}\" ALTER COLUMN {name} TYPE {ty}; \
                         ALTER TABLE \"{cur}_raw\" ALTER COLUMN {name} TYPE {ty};"
                    );
                }
                structural = true;
            }
            AdditiveChange::RenameTable { from, to } => {
                let _write_result = write!(
                    &mut sql,
                    "ALTER TABLE \"{cur}\" RENAME TO \"{to}\"; \
                     ALTER TABLE \"{cur}_raw\" RENAME TO \"{to}_raw\"; \
                     DROP VIEW IF EXISTS \"{from}_current\";"
                );
                cur.clone_from(to);
                structural = true;
            }
            AdditiveChange::Comment { target, text } => {
                // Metadata only — mirror `<table>` never `<table>_raw`; does NOT set `structural`, so it
                // neither recreates the view nor implies a data gate.
                let lit = match text {
                    Some(t) => t.to_quoted_literal(),
                    None => "NULL".to_string(),
                };
                match target {
                    CommentTarget::Table => {
                        let _write_result =
                            write!(&mut sql, "COMMENT ON TABLE \"{cur}\" IS {lit};");
                    }
                    CommentTarget::Column(col) => {
                        let _write_result =
                            write!(&mut sql, "COMMENT ON COLUMN \"{cur}\".{col} IS {lit};");
                    }
                }
            }
        }
    }
    if structural {
        sql.push_str(&user_view_sql(&cur));
    }
    Ok(sql)
}

fn registry_column_plan(
    version: &RegistryVersion,
    position: usize,
) -> Result<TablePlan, TransformerError> {
    let column = version
        .shape
        .relation
        .columns
        .get(position)
        .ok_or_else(|| TransformerError::ManifestInvariant {
            message: format!(
                "schema {} has no logical column at position {position}",
                version.shape.version
            ),
        })?;
    let descriptor = version
        .descriptor_positions
        .get(&column.name)
        .map(|position| &version.descriptors[*position]);
    Ok(TablePlan::for_registry_column(
        &version.shape.relation,
        column,
        descriptor,
    ))
}

#[allow(
    clippy::too_many_arguments,
    reason = "both logical and version labels make failures actionable"
)]
fn append_physical_renames(
    sql: &mut String,
    table: &str,
    from: &str,
    to: &str,
    old: &TablePlan,
    new: &TablePlan,
    old_version: SchemaVersionNo,
    new_version: SchemaVersionNo,
) -> Result<(), TransformerError> {
    if old.raw_cols.len() != new.raw_cols.len() || old.mirror_cols.len() != new.mirror_cols.len() {
        return Err(TransformerError::Quarantine {
            table: table.to_string(),
            reason: format!(
                "schema step {old_version}->{new_version} rename {from:?}->{to:?} changes physical emit arity"
            ),
        });
    }
    for (old_column, new_column) in old.mirror_cols.iter().zip(&new.mirror_cols) {
        if old_column.name != new_column.name {
            let old_name = &old_column.name;
            let new_name = &new_column.name;
            let _write_result = write!(
                sql,
                "ALTER TABLE \"{table}\" RENAME COLUMN {old_name} TO {new_name};"
            );
        }
    }
    for (old_column, new_column) in old.raw_cols.iter().zip(&new.raw_cols) {
        if old_column.name != new_column.name {
            let old_name = &old_column.name;
            let new_name = &new_column.name;
            let _write_result = write!(
                sql,
                "ALTER TABLE \"{table}_raw\" RENAME COLUMN {old_name} TO {new_name};"
            );
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_arguments,
    reason = "both logical and version labels make failures actionable"
)]
fn append_physical_widen(
    sql: &mut String,
    table: &str,
    name: &str,
    old: &TablePlan,
    new: &TablePlan,
    old_version: SchemaVersionNo,
    new_version: SchemaVersionNo,
) -> Result<(), TransformerError> {
    let one_to_one = old.raw_cols.len() == 1
        && new.raw_cols.len() == 1
        && old.mirror_cols.len() == 1
        && new.mirror_cols.len() == 1
        && old.raw_cols[0].name == new.raw_cols[0].name
        && old.mirror_cols[0].name == new.mirror_cols[0].name;
    if !one_to_one {
        return Err(TransformerError::Quarantine {
            table: table.to_string(),
            reason: format!(
                "schema step {old_version}->{new_version} widening {name:?} changes a non-1:1 physical shape"
            ),
        });
    }
    let mirror = &new.mirror_cols[0];
    let raw = &new.raw_cols[0];
    let _write_result = write!(
        sql,
        "ALTER TABLE \"{table}\" ALTER COLUMN {} TYPE {}; \
         ALTER TABLE \"{table}_raw\" ALTER COLUMN {} TYPE {};",
        mirror.name, mirror.duckdb_type, raw.name, raw.duckdb_type
    );
    Ok(())
}

/// Apply destructive changes — the mirror-vs-raw asymmetry is the whole point: the mirror
/// takes the exact current shape (physical drop / in-place cast), the raw log preserves history
/// (retain / widen-to-VARCHAR, **never** a re-cast that could fail on stored values). A lossy cast that
/// fails on the mirror returns [`TransformerError::Quarantine`] — a terminal, alerting v1 outcome (single-
/// table reload out of quarantine is **out of scope in v1**). `DROP TABLE` retires both DuckDB tables
/// idempotently (`IF EXISTS`); the `.duckdb` file is retired separately by [`retire_file`].
///
/// # Errors
///
/// Returns [`TransformerError::Duck`] for a failed drop or raw-table widening, and
/// [`TransformerError::Quarantine`] when a lossy mirror cast cannot be applied without data loss.
pub fn apply_destructive(
    db: &TableDb,
    table: &str,
    changes: &[DestructiveChange],
) -> Result<(), TransformerError> {
    apply_destructive_inner(db, table, changes, None)
}

fn apply_destructive_registry(
    db: &TableDb,
    table: &str,
    changes: &[DestructiveChange],
    old: &RegistryVersion,
    new: &RegistryVersion,
) -> Result<(), TransformerError> {
    apply_destructive_inner(db, table, changes, Some((old, new)))
}

fn apply_destructive_inner(
    db: &TableDb,
    table: &str,
    changes: &[DestructiveChange],
    registry: Option<(&RegistryVersion, &RegistryVersion)>,
) -> Result<(), TransformerError> {
    let conn = db.conn();
    for ch in changes {
        match ch {
            DestructiveChange::DropColumn { name } => {
                // Mirror: physical drop. Raw: RETAIN the column (already nullable) — verbatim history; a
                // post-drop file simply NULL-fills it (name-explicit append). Recreate the view.
                let mut sql = String::new();
                if let Some((old, _)) = registry {
                    let position = old
                        .shape
                        .relation
                        .columns
                        .iter()
                        .position(|column| column.name == *name)
                        .ok_or_else(|| TransformerError::ManifestInvariant {
                            message: format!(
                                "schema {} DROP COLUMN {name:?} is absent from its registry relation",
                                old.shape.version
                            ),
                        })?;
                    let plan = registry_column_plan(old, position)?;
                    for column in &plan.mirror_cols {
                        let physical_name = &column.name;
                        let _write_result = write!(
                            &mut sql,
                            "ALTER TABLE \"{table}\" DROP COLUMN IF EXISTS {physical_name};"
                        );
                    }
                    sql.push_str(&user_view_sql(table));
                } else {
                    sql = format!(
                        "ALTER TABLE \"{table}\" DROP COLUMN IF EXISTS {name}; {}",
                        user_view_sql(table)
                    );
                }
                conn.execute_batch(&sql)
                    .duck_with(|| format!("drop column {name} on {table}"))?;
            }
            DestructiveChange::LossyType { name, new } => {
                if let Some((old_version, new_version)) = registry {
                    validate_lossy_registry_shape(old_version, new_version, name, table)?;
                }
                let ty = duck_type(new.type_oid);
                // Raw FIRST: widen to VARCHAR so rows of BOTH schema_versions coexist in one column;
                // never narrow historical values. Native DuckDB can do that in-place. DuckLake only
                // permits lossless type promotions, so replace the raw table transactionally from a
                // VARCHAR projection instead. The all-or-nothing rewrite preserves every historical
                // row and leaves no half-renamed table if any statement fails.
                widen_raw_to_varchar(db, table, name)?;
                // Mirror: attempt the in-place cast. DuckDB validates before applying, so a failure
                // leaves the mirror unchanged → QUARANTINE (loud, terminal). Never silent data loss.
                if let Err(e) = conn.execute_batch(&format!(
                    "ALTER TABLE \"{table}\" ALTER COLUMN {name} TYPE {ty};"
                )) {
                    return Err(TransformerError::Quarantine {
                        table: table.to_string(),
                        reason: format!("lossy ALTER COLUMN {name} TYPE {ty} failed: {e}"),
                    });
                }
            }
            DestructiveChange::DropTable { name } => {
                conn.execute_batch(&format!(
                    "DROP VIEW IF EXISTS \"{name}_current\"; \
                     DROP TABLE IF EXISTS \"{name}\"; DROP TABLE IF EXISTS \"{name}_raw\";"
                ))
                .duck_with(|| format!("drop table {name}"))?;
            }
        }
    }
    Ok(())
}

fn validate_lossy_registry_shape(
    old: &RegistryVersion,
    new: &RegistryVersion,
    name: &str,
    table: &str,
) -> Result<(), TransformerError> {
    let position = new
        .shape
        .relation
        .columns
        .iter()
        .position(|column| column.name == name)
        .ok_or_else(|| TransformerError::ManifestInvariant {
            message: format!(
                "schema {} lossy column {name:?} is absent from its registry relation",
                new.shape.version
            ),
        })?;
    let old_plan = registry_column_plan(old, position)?;
    let new_plan = registry_column_plan(new, position)?;
    let one_to_one = old_plan.raw_cols.len() == 1
        && new_plan.raw_cols.len() == 1
        && old_plan.mirror_cols.len() == 1
        && new_plan.mirror_cols.len() == 1;
    if !one_to_one {
        return Err(TransformerError::Quarantine {
            table: table.to_string(),
            reason: format!(
                "schema step {}->{} changes lossy column {name:?} across a non-1:1 physical shape",
                old.shape.version, new.shape.version
            ),
        });
    }
    Ok(())
}

/// Preserve one raw column's complete history while changing its common storage type to `VARCHAR`.
///
/// DuckLake deliberately rejects incompatible `ALTER COLUMN TYPE` operations (including
/// `INTEGER -> VARCHAR`), even though native DuckDB can perform that cast. Rebuilding from a
/// `SELECT * REPLACE` projection gives the replacement column a fresh type without asking DuckLake
/// to reinterpret an existing field id. The transaction makes the create/drop/rename atomic.
fn widen_raw_to_varchar(db: &TableDb, table: &str, name: &str) -> Result<(), TransformerError> {
    let conn = db.conn();
    if !db.is_ducklake() {
        return conn
            .execute_batch(&format!(
                "ALTER TABLE \"{table}_raw\" ALTER COLUMN {name} TYPE VARCHAR;"
            ))
            .duck_with(|| format!("widen raw {name} on {table}"));
    }

    let replacement = format!("{table}_raw__walrus_retype");
    db.in_txn("rewrite raw column as VARCHAR", |conn| {
        conn.execute_batch(&format!(
            "DROP TABLE IF EXISTS \"{replacement}\"; \
             CREATE TABLE \"{replacement}\" AS \
             SELECT * REPLACE (CAST({name} AS VARCHAR) AS {name}) \
             FROM \"{table}_raw\"; \
             DROP TABLE \"{table}_raw\"; \
             ALTER TABLE \"{replacement}\" RENAME TO \"{table}_raw\";"
        ))
        .duck_with(|| format!("rewrite raw {name} as VARCHAR on {table}"))
    })
}

/// Retire a dropped table's `.duckdb` file (call after its owning connection is closed). Idempotent —
/// a missing file (a crash mid-retire re-run) is success.
///
/// The path is only ever *read* here, so it is taken as a borrowed view — the same bound
/// [`TableDb::open`](crate::duck::TableDb::open) and the wrapped `tokio::fs::remove_file` already
/// carry. A caller holding the `<dir>/<table>.duckdb` name as a `String` (or a literal) reaches it
/// without a [`Path::new`](std::path::Path::new) at the call site; nothing is owned or allocated
/// by this signature.
///
/// # Errors
///
/// Returns [`TransformerError::File`] if removing an existing file fails for any reason other than
/// `NotFound`, keeping the OS error itself as the failure's source.
pub async fn retire_file(path: impl AsRef<std::path::Path>) -> Result<(), TransformerError> {
    // Bind the borrow ONCE — the removal and the failure message share this one view.
    let path = path.as_ref();
    match tokio::fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(source) => Err(TransformerError::File {
            op: "retire",
            path: path.display().to_string(),
            source,
        }),
    }
}

/// Bring both DuckDB tables up to `target` by applying each version step's additive diff, advancing the
/// `_walrus_meta` watermark after each — **before** any file at that version is appended/transformed.
/// Idempotent: the watermark is persisted in the `.duckdb`, so a re-run resumes from where it left off.
///
/// # Errors
///
/// Returns [`TransformerError::Control`] for registry reads, [`TransformerError::RegistryDecode`] for invalid
/// stored shapes, [`TransformerError::Duck`] for local DDL/watermark failures, or
/// [`TransformerError::Quarantine`] when a destructive cast is unsafe.
pub async fn reconcile_to_version(
    db: &TableDb,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    target: SchemaVersionNo,
) -> Result<(), TransformerError> {
    let mut cur = db.schema_version()?;
    while cur < target {
        let next = SchemaVersionNo(cur.0.checked_add(1).ok_or_else(|| {
            TransformerError::ManifestInvariant {
                message: format!("schema reconciliation overflows after version {cur}"),
            }
        })?);
        // Both rows are read unconditionally and neither consumes the other's output, so one step
        // costs the slower round trip instead of their sum — and a reconcile can walk many steps.
        // Same caveat as `phase_a::read_lag_inputs`: each read may hold one pool connection while the
        // other runs, which cannot deadlock here because no transaction is open across either await.
        let (old, new) = tokio::try_join!(
            load_version(pool, epoch, schema, table, cur),
            load_version(pool, epoch, schema, table, next),
        )?;
        let (old, new) = require_registry_pair(cur, next, old, new)?;
        let d = diff(&old.shape, &new.shape)?;
        validate_registry_step(db, table, &d, &old, &new)?;

        let drops_table = d
            .destructive
            .iter()
            .any(|change| matches!(change, DestructiveChange::DropTable { .. }));
        let has_lossy_type = d
            .destructive
            .iter()
            .any(|change| matches!(change, DestructiveChange::LossyType { .. }));

        // Every step except a lossy type rewrite (whose DuckLake path owns a nested replacement
        // transaction) commits its complete physical DDL and reconcile watermark atomically.
        // This closes both the rename replay gap and partial multi-sibling Tier-2 ADD/DROP.
        if !has_lossy_type {
            db.in_txn("apply schema version", |conn| {
                apply_additive_registry(conn, table, &d.additive, &old, &new)?;
                if drops_table {
                    db.unpublish_current_view()?;
                }
                apply_destructive_registry(db, table, &d.destructive, &old, &new)?;
                if !drops_table {
                    db.publish_current_view()?;
                }
                db.set_schema_version(next)
            })?;
            cur = next;
            continue;
        }

        apply_additive_registry(db.conn(), table, &d.additive, &old, &new)?;
        // Destructive changes apply after additive ones; a lossy cast failure short-circuits
        // with `Quarantine` and the watermark is NOT advanced (re-run re-quarantines idempotently).
        if drops_table {
            db.unpublish_current_view()?;
        }
        apply_destructive_registry(db, table, &d.destructive, &old, &new)?;
        if !drops_table {
            db.publish_current_view()?;
        }
        db.set_schema_version(next)?;
        cur = next;
    }
    Ok(())
}

/// Apply the newest complete source comment snapshot valid for the mirror's current structural
/// version. The DDL id is stored in `_walrus_meta` in the same DuckDB transaction as the comments,
/// making create, replacement, and `COMMENT ... IS NULL` removal idempotent across crashes.
///
/// When a generation has no DDL history yet, the initial catalog-fence snapshot embedded in the
/// current registry row seeds comments that existed before the replication slot was created.
///
/// # Errors
///
/// Returns [`TransformerError::Control`] for metadata/registry reads, [`TransformerError::ManifestInvariant`]
/// for malformed snapshots, or [`TransformerError::Duck`] when DuckDB rejects the comment transaction.
pub async fn reconcile_comments(
    db: &TableDb,
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    schema: &str,
    table: &str,
) -> Result<(), TransformerError> {
    let applied = db.applied_comment_ddl_id()?;
    let version = db.schema_version()?;
    let latest = control::read_latest_ddl_snapshot(
        pool,
        epoch,
        schema,
        table,
        applied.unwrap_or(DdlId(0)),
        version,
    )
    .await?;

    let (watermark, snapshot, registry) = if let Some(row) = latest {
        let registry = load_version(pool, epoch, schema, table, version)
            .await?
            .ok_or_else(|| TransformerError::ManifestInvariant {
                message: format!(
                    "cannot reconcile comments for {schema}.{table}: registry is missing current version {version}"
                ),
            })?;
        let snapshot = comment_snapshot_from_ddl(&row)?;
        (row.id, snapshot, registry)
    } else if applied.is_none() {
        let Some(registry) = load_version(pool, epoch, schema, table, version).await? else {
            // A minimal/test apply loop may own an already-created local table without registry
            // history. With no durable DDL snapshot there is no comment state to apply, so leave the
            // watermark absent and retry if registry history becomes available later.
            return Ok(());
        };
        let snapshot = registry.comments.clone();
        (DdlId(0), snapshot, registry)
    } else {
        return Ok(());
    };

    db.in_txn("apply comment metadata", |conn| {
        if let Some(snapshot) = &snapshot {
            apply_comment_snapshot(conn, table, snapshot, &registry)?;
        }
        conn.execute(
            "DELETE FROM \"_walrus_meta\" WHERE k = 'comment_ddl_id'",
            [],
        )
        .duck("clear old comment DDL watermark")?;
        conn.execute(
            "INSERT INTO \"_walrus_meta\" (k, v) VALUES ('comment_ddl_id', ?)",
            [watermark.0],
        )
        .duck("record comment DDL watermark")?;
        Ok(())
    })
}

fn comment_snapshot_from_ddl(
    row: &control::DdlRow,
) -> Result<Option<CommentSnapshot>, TransformerError> {
    let Some(columns) = row.c_columns.as_ref().and_then(serde_json::Value::as_array) else {
        return Ok(None);
    };
    // Empty is the DROP TABLE sentinel. A pre-protocol-4 row lacks `comment` keys and is not an
    // authoritative metadata snapshot; skip it without accidentally clearing existing comments.
    if columns.is_empty() || columns.iter().any(|column| column.get("comment").is_none()) {
        return Ok(None);
    }
    Ok(Some(CommentSnapshot {
        table: row.c_table_comment.clone(),
        columns: parse_comment_columns(columns, &row.source_schema, &row.source_table)?,
    }))
}

fn comment_snapshot_from_registry(
    value: &serde_json::Value,
    schema: &str,
    table: &str,
) -> Result<Option<CommentSnapshot>, TransformerError> {
    let Some(object) = value.as_object() else {
        return Ok(None);
    };
    let Some(table_comment) = object.get("table_comment") else {
        return Ok(None);
    };
    let columns = object
        .get("columns")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| TransformerError::ManifestInvariant {
            message: format!("comment registry snapshot for {schema}.{table} has no column array"),
        })?;
    if columns.iter().any(|column| column.get("comment").is_none()) {
        return Err(TransformerError::ManifestInvariant {
            message: format!(
                "comment registry snapshot for {schema}.{table} omits a column comment field"
            ),
        });
    }
    Ok(Some(CommentSnapshot {
        table: parse_optional_comment(table_comment, schema, table, "table")?,
        columns: parse_comment_columns(columns, schema, table)?,
    }))
}

fn parse_comment_columns(
    columns: &[serde_json::Value],
    schema: &str,
    table: &str,
) -> Result<Vec<(String, Option<String>)>, TransformerError> {
    columns
        .iter()
        .map(|column| {
            let name = column
                .get("name")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| TransformerError::ManifestInvariant {
                    message: format!(
                        "comment snapshot for {schema}.{table} contains a column without a name"
                    ),
                })?;
            let value =
                column
                    .get("comment")
                    .ok_or_else(|| TransformerError::ManifestInvariant {
                        message: format!(
                            "comment snapshot for {schema}.{table}.{name} has no comment field"
                        ),
                    })?;
            Ok((
                name.to_string(),
                parse_optional_comment(value, schema, table, name)?,
            ))
        })
        .collect()
}

fn parse_optional_comment(
    value: &serde_json::Value,
    schema: &str,
    table: &str,
    target: &str,
) -> Result<Option<String>, TransformerError> {
    match value {
        serde_json::Value::Null => Ok(None),
        serde_json::Value::String(text) => Ok(Some(text.clone())),
        _ => Err(TransformerError::ManifestInvariant {
            message: format!(
                "comment snapshot for {schema}.{table} target {target:?} is not text or null"
            ),
        }),
    }
}

fn apply_comment_snapshot(
    conn: &duckdb::Connection,
    table: &str,
    snapshot: &CommentSnapshot,
    registry: &RegistryVersion,
) -> Result<(), TransformerError> {
    let table_ident =
        SqlIdent::new(table).map_err(|source| TransformerError::ManifestInvariant {
            message: format!("invalid mirror identifier {table:?} in comment snapshot: {source}"),
        })?;
    let table_comment = snapshot
        .table
        .as_deref()
        .map_or_else(|| "NULL".to_string(), SqlStrExt::to_quoted_literal);
    let mut sql = format!("COMMENT ON TABLE {table_ident} IS {table_comment};");
    let comments = snapshot
        .columns
        .iter()
        .map(|(name, comment)| (name.as_str(), comment.as_deref()))
        .collect::<HashMap<_, _>>();
    for (position, column) in registry.shape.relation.columns.iter().enumerate() {
        let comment = comments.get(column.name.as_str()).ok_or_else(|| {
            TransformerError::ManifestInvariant {
                message: format!(
                    "comment snapshot for {}.{} omits current column {:?}",
                    registry.shape.relation.schema, table, column.name
                ),
            }
        })?;
        let literal = comment.map_or_else(|| "NULL".to_string(), SqlStrExt::to_quoted_literal);
        for physical in registry_column_plan(registry, position)?.mirror_cols.iter() {
            let column_ident = common::sql::SqlColumn::new(&physical.name).map_err(|source| {
                TransformerError::ManifestInvariant {
                    message: format!(
                        "invalid mirror column identifier {:?} in comment snapshot: {source}",
                        physical.name
                    ),
                }
            })?;
            let _write_result = write!(
                &mut sql,
                "COMMENT ON COLUMN {table_ident}.{column_ident} IS {literal};"
            );
        }
    }
    conn.execute_batch(&sql)
        .duck_with(|| format!("apply comments to {table}"))
}

fn require_registry_pair(
    current: SchemaVersionNo,
    next: SchemaVersionNo,
    old: Option<RegistryVersion>,
    new: Option<RegistryVersion>,
) -> Result<(RegistryVersion, RegistryVersion), TransformerError> {
    let old = old.ok_or_else(|| TransformerError::ManifestInvariant {
        message: format!(
            "cannot reconcile schema {current}->{next}: registry is missing source version {current}"
        ),
    })?;
    let new = new.ok_or_else(|| TransformerError::ManifestInvariant {
        message: format!(
            "cannot reconcile schema {current}->{next}: registry is missing destination version {next}"
        ),
    })?;
    Ok((old, new))
}

struct RegistryVersion {
    shape: SchemaVersion,
    descriptors: Vec<common::TypeDescriptor>,
    descriptor_positions: HashMap<String, usize>,
    comments: Option<CommentSnapshot>,
}

impl RegistryVersion {
    fn new(
        shape: SchemaVersion,
        descriptors: Vec<common::TypeDescriptor>,
    ) -> Result<Self, TransformerError> {
        // The shared constructor is the single validation boundary used by bootstrap, Phase A,
        // Phase B, and DDL reconciliation. Build once here to reject malformed descriptor identity
        // and physical-name collisions before this version can participate in a diff.
        TablePlan::from_registry(&shape.relation, &descriptors)?;
        let descriptor_positions = descriptors
            .iter()
            .enumerate()
            .map(|(position, descriptor)| (descriptor.column.clone(), position))
            .collect();
        Ok(Self {
            shape,
            descriptors,
            descriptor_positions,
            comments: None,
        })
    }
}

fn validate_registry_step(
    db: &TableDb,
    table: &str,
    diff: &SchemaDiff,
    old: &RegistryVersion,
    new: &RegistryVersion,
) -> Result<(), TransformerError> {
    if let Some((position, old_column, new_column)) = old
        .shape
        .relation
        .columns
        .iter()
        .zip(&new.shape.relation.columns)
        .enumerate()
        .find_map(|(position, (old, new))| (old.name != new.name).then_some((position, old, new)))
    {
        return Err(TransformerError::Quarantine {
            table: table.to_string(),
            reason: format!(
                "schema step {}->{} substitutes common position {position} {:?}->{:?}; a genuine RENAME and same-statement DROP+ADD are intentionally indistinguishable until stable column-lineage evidence is persisted",
                old.shape.version, new.shape.version, old_column.name, new_column.name
            ),
        });
    }
    if !diff.additive.is_empty() && !diff.destructive.is_empty() {
        return Err(TransformerError::Quarantine {
            table: table.to_string(),
            reason: format!(
                "schema step {}->{} mixes additive and destructive changes; refusing a partially replayable mutation",
                old.shape.version, new.shape.version
            ),
        });
    }
    validate_registry_physical_drift(table, diff, old, new)?;
    for change in &diff.destructive {
        if let DestructiveChange::LossyType { name, .. } = change {
            validate_lossy_registry_shape(old, new, name, table)?;
        }
    }
    validate_retained_raw_names(db, table, old, new)
}

fn validate_registry_physical_drift(
    table: &str,
    diff: &SchemaDiff,
    old: &RegistryVersion,
    new: &RegistryVersion,
) -> Result<(), TransformerError> {
    let mut changed_positions = HashSet::new();
    for change in &diff.additive {
        match change {
            AdditiveChange::RenameColumn { position, .. }
            | AdditiveChange::WidenColumn { position, .. } => {
                changed_positions.insert(*position);
            }
            AdditiveChange::AddColumn(_)
            | AdditiveChange::RenameTable { .. }
            | AdditiveChange::Comment { .. } => {}
        }
    }
    for change in &diff.destructive {
        if let DestructiveChange::LossyType { name, .. } = change
            && let Some(position) = new
                .shape
                .relation
                .columns
                .iter()
                .position(|column| column.name == *name)
        {
            changed_positions.insert(position);
        }
    }

    if new.shape.relation.columns.len() < old.shape.relation.columns.len() {
        let old_positions = old
            .shape
            .relation
            .columns
            .iter()
            .enumerate()
            .map(|(position, column)| (column.name.as_str(), position))
            .collect::<HashMap<_, _>>();
        for (new_position, new_column) in new.shape.relation.columns.iter().enumerate() {
            let old_position = old_positions
                .get(new_column.name.as_str())
                .copied()
                .ok_or_else(|| TransformerError::ManifestInvariant {
                    message: format!(
                        "schema {} drop survivor {:?} is absent from schema {}",
                        new.shape.version, new_column.name, old.shape.version
                    ),
                })?;
            if !registry_column_plans_match(
                &registry_column_plan(old, old_position)?,
                &registry_column_plan(new, new_position)?,
            ) {
                return Err(TransformerError::Quarantine {
                    table: table.to_string(),
                    reason: format!(
                        "schema step {}->{} changes physical emit plan for surviving column {:?} while dropping columns",
                        old.shape.version, new.shape.version, new_column.name
                    ),
                });
            }
        }
        return Ok(());
    }

    for (position, _) in old.shape.relation.columns.iter().enumerate() {
        if changed_positions.contains(&position) {
            continue;
        }
        let old_plan = registry_column_plan(old, position)?;
        let new_plan = registry_column_plan(new, position)?;
        if !registry_column_plans_match(&old_plan, &new_plan) {
            return Err(TransformerError::Quarantine {
                table: table.to_string(),
                reason: format!(
                    "schema step {}->{} changes physical emit plan at unchanged position {position}",
                    old.shape.version, new.shape.version
                ),
            });
        }
    }
    Ok(())
}

fn registry_column_plans_match(old: &TablePlan, new: &TablePlan) -> bool {
    old.raw_cols.len() == new.raw_cols.len()
        && old
            .raw_cols
            .iter()
            .zip(&new.raw_cols)
            .all(|(old, new)| old.name == new.name && old.duckdb_type == new.duckdb_type)
        && old.mirror_cols.len() == new.mirror_cols.len()
        && old
            .mirror_cols
            .iter()
            .zip(&new.mirror_cols)
            .all(|(old, new)| {
                old.name == new.name
                    && old.duckdb_type == new.duckdb_type
                    && old.is_key == new.is_key
                    && old.toast_source == new.toast_source
                    && match (&old.value, &new.value) {
                        (
                            crate::plan::MirrorValue::Passthrough,
                            crate::plan::MirrorValue::Passthrough,
                        ) => true,
                        (
                            crate::plan::MirrorValue::Recombine(old),
                            crate::plan::MirrorValue::Recombine(new),
                        ) => old == new,
                        (
                            crate::plan::MirrorValue::Passthrough,
                            crate::plan::MirrorValue::Recombine(_),
                        )
                        | (
                            crate::plan::MirrorValue::Recombine(_),
                            crate::plan::MirrorValue::Passthrough,
                        ) => false,
                    }
            })
}

/// Reject a new live physical name that aliases a column retained only for raw history. PostgreSQL
/// permits `DROP x` followed by `ADD x` (or renaming another column to `x`), but Walrus deliberately
/// does not drop the first `x` from `_raw`. Treating both source lineages as one DuckDB column would
/// silently merge unrelated values, so this remains a loud quarantine until raw columns carry
/// stable lineage identities independent of source names.
fn validate_retained_raw_names(
    db: &TableDb,
    table: &str,
    old: &RegistryVersion,
    new: &RegistryVersion,
) -> Result<(), TransformerError> {
    let old_plan = TablePlan::from_registry(&old.shape.relation, &old.descriptors)?;
    let new_plan = TablePlan::from_registry(&new.shape.relation, &new.descriptors)?;
    let old_current = old_plan
        .raw_cols
        .iter()
        .map(|column| column.name.as_str())
        .collect::<HashSet<_>>();
    const INTERNAL: [&str; 5] = [
        "walrus_extractor_meta",
        "_walrus_op",
        "_walrus_commit_lsn",
        "_walrus_lsn",
        "_walrus_extractor_processed_at",
    ];
    let raw_table = format!("{table}_raw");
    let mut stmt = db
        .conn()
        .prepare(&format!("DESCRIBE SELECT * FROM \"{raw_table}\""))
        .duck_with(|| format!("inspect retained raw columns on {raw_table}"))?;
    let actual = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .duck_with(|| format!("inspect retained raw columns on {raw_table}"))?
        .collect::<Result<Vec<_>, _>>()
        .duck_with(|| format!("inspect retained raw columns on {raw_table}"))?;
    let retained = actual
        .iter()
        .map(String::as_str)
        .filter(|name| !old_current.contains(name) && !INTERNAL.contains(name))
        .collect::<HashSet<_>>();
    let collisions = new_plan
        .raw_cols
        .iter()
        .map(|column| column.name.as_str())
        .filter(|name| retained.contains(name))
        .collect::<Vec<_>>();
    if collisions.is_empty() {
        return Ok(());
    }
    Err(TransformerError::Quarantine {
        table: table.to_string(),
        reason: format!(
            "schema step {}->{} reuses retained raw column name(s) {}; stable physical lineage is required",
            old.shape.version,
            new.shape.version,
            collisions.join(", ")
        ),
    })
}

/// Load one `schema_version`'s complete physical-planning input from `schema_registry` (`None` if
/// that version has no row).
async fn load_version(
    pool: &sqlx::PgPool,
    epoch: EpochNo,
    schema: &str,
    table: &str,
    version: SchemaVersionNo,
) -> Result<Option<RegistryVersion>, TransformerError> {
    let Some(row) = control::read_registry(pool, epoch, schema, table, version).await? else {
        return Ok(None);
    };
    // `reconcile_to_version` calls this twice per version step; the `schema.table` label is built
    // inside the closure so only an actual decode failure allocates it.
    let comments = comment_snapshot_from_registry(&row.columns, schema, table)?;
    let relation: PgRelation =
        serde_json::from_value(row.columns).map_err(|source| TransformerError::RegistryDecode {
            table: format!("{schema}.{table}"),
            version: version.0,
            source,
        })?;
    let mut registry = RegistryVersion::new(SchemaVersion { version, relation }, row.descriptors)?;
    registry.comments = comments;
    Ok(Some(registry))
}

#[cfg(test)]
#[path = "ddl_test.rs"]
mod tests;
