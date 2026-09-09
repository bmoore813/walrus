//! The per-table **schema plan** (transformer §2.6 / architecture "Types") — the bridge from the extractor's
//! `schema_registry` [`TypeDescriptor`]s to the DuckDB `<table>_raw` (verbatim emit columns) and
//! `<table>` (mirror, recombined to the target type) shapes, and the transform's per-mirror-column value
//! expressions.
//!
//! Three shapes per source column:
//! - **Tier-1** (scalar / native, incl. `uuid`, `numeric`, `timestamptz`, `jsonb`, `bytea`): one emit
//!   column == the source column; the mirror holds it as the descriptor's DuckDB type.
//! - **Tier-2 recombine** (`interval`, `timetz`): several emit columns collapse to ONE mirror column via
//!   a DuckDB expression (`to_months(...)+to_days(...)+to_microseconds(...)`).
//! - **Tier-2 flat** (`range`): several emit columns pass through as several mirror columns (DuckDB has
//!   no range type — it is the 5 flat `_lower/_upper/_lower_inc/_upper_inc/_empty` siblings).
//!
//! A plan built from a bare [`PgRelation`] ([`TablePlan::tier1`]) reproduces the pre-descriptor scalar
//! behaviour exactly (emit == source column via `crate::duck::duck_type`), so the hermetic/compose
//! tests that pass a [`PgRelation`] are unchanged; the registry path ([`TablePlan::from_registry`])
//! adds the Tier-2 shapes.

use crate::error::TransformerError;
use common::oids::{INTERVAL, TIMETZ};
use common::sql::SqlColumn;
use common::{PgRelation, TypeDescriptor};
use std::collections::{HashMap, HashSet};

const RESERVED_PHYSICAL_COLUMNS: [&str; 7] = [
    "walrus_extractor_meta",
    "_walrus_op",
    "_walrus_commit_lsn",
    "_walrus_lsn",
    "_walrus_extractor_processed_at",
    "_applied_commit_lsn",
    "_applied_lsn",
];

/// A `<table>_raw` column: the verbatim emit column the extractor wrote to Parquet.
#[derive(Debug, Clone)]
pub(crate) struct RawCol {
    pub(crate) name: String,
    pub(crate) duckdb_type: String,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<RawCol>() == 48,
    "RawCol is stored once per emit column, and a range column emits five"
);

/// How a mirror column's value is produced from the winning raw row `s` (and, for a TOAST-resolvable
/// scalar, the current mirror `t`).
#[derive(Debug, Clone)]
pub(crate) enum MirrorValue {
    /// `s."<name>"` — a direct copy of the like-named raw column. Unchanged-TOAST eligibility lives
    /// on [`MirrorCol::toast_source`] because flat Tier-2 siblings also pass through this way.
    Passthrough,
    /// A recombine SQL expression over the raw emit columns (already `s.`-qualified), e.g. an INTERVAL.
    Recombine(String),
}

/// A `<table>` mirror column: name, DuckDB type, key flag, and how its value is computed.
#[derive(Debug, Clone)]
pub(crate) struct MirrorCol {
    pub(crate) name: String,
    pub(crate) duckdb_type: String,
    pub(crate) is_key: bool,
    /// Original PostgreSQL column named in `ExtractorMeta::unchanged_toast`. Tier-2 emit columns use
    /// sibling names, so this cannot be reconstructed from `name` in the transformer.
    pub(crate) toast_source: Option<Box<str>>,
    pub(crate) value: MirrorValue,
}

/// Size budget for one [`TablePlan::mirror_cols`] entry. The [`TablePlan`] assertion below pins the
/// three fat pointers — that the lists stay frozen — but not the payload behind them, which is the
/// half that multiplies by column count.
///
/// A ceiling rather than an equality because whether [`MirrorValue`] borrows `String`'s niche for
/// its discriminant is a compiler detail; either way a new owned field breaches this. If it trips,
/// shrink the entry or raise the budget deliberately in review.
const MIRROR_COL_MAX_BYTES: usize = 104;
const _: () = assert!(std::mem::size_of::<MirrorCol>() <= MIRROR_COL_MAX_BYTES);

/// The full plan for one table: the raw emit columns and the mirror columns.
#[derive(Debug, Clone)]
pub(crate) struct TablePlan {
    pub(crate) table: Box<str>,
    pub(crate) raw_cols: Box<[RawCol]>,
    pub(crate) mirror_cols: Box<[MirrorCol]>,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<TablePlan>() == 48,
    "TablePlan is rebuilt every Phase-B poll; keep its lists frozen"
);

impl TablePlan {
    /// Clone this shape for another physical DuckDB table name.
    ///
    /// Reload reconciliation builds into a hidden generation while the source table's canonical
    /// name remains live. The columns and recombination expressions are identical; only the mirror
    /// (and, by derivation, raw-table) name changes.
    #[must_use]
    pub(crate) fn for_table(&self, table: impl Into<Box<str>>) -> Self {
        Self {
            table: table.into(),
            raw_cols: self.raw_cols.clone(),
            mirror_cols: self.mirror_cols.clone(),
        }
    }

    /// The Tier-1 (scalar-only) plan from a bare relation — one emit column == source column via
    /// `crate::duck::duck_type`, mirror = same.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::ManifestInvariant`] when a source column is not safe to emit as a
    /// bare lowercase snake_case SQL name, is reserved, or duplicates another physical column.
    pub(crate) fn tier1(rel: &PgRelation) -> Result<Self, TransformerError> {
        // Tier-1 exact one raw + one mirror per source column.
        let mut raw_cols = Vec::with_capacity(rel.columns.len());
        let mut mirror_cols = Vec::with_capacity(rel.columns.len());
        for c in &rel.columns {
            let ty = crate::duck::duck_type(c.type_oid).to_string();
            raw_cols.push(RawCol {
                name: c.name.clone(),
                duckdb_type: ty.clone(),
            });
            mirror_cols.push(MirrorCol {
                name: c.name.clone(),
                duckdb_type: ty,
                is_key: c.is_key,
                toast_source: (!c.is_key).then(|| c.name.as_str().into()),
                value: MirrorValue::Passthrough,
            });
        }
        let plan = TablePlan {
            table: rel.name.as_str().into(),
            raw_cols: raw_cols.into_boxed_slice(),
            mirror_cols: mirror_cols.into_boxed_slice(),
        };
        validate_physical_names(rel, &plan)?;
        Ok(plan)
    }

    /// The full plan from the registry: descriptors (Tier-1/2/3) aligned with the relation's columns
    /// (for key flags). Falls back to the Tier-1 shape for a column without a descriptor, but rejects
    /// duplicate/unknown/type-mismatched descriptors and physical names that would alias another
    /// source field or Walrus metadata.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::ManifestInvariant`] when registry identity or its physical emit plan is
    /// ambiguous/corrupt.
    pub(crate) fn from_registry(
        rel: &PgRelation,
        descriptors: &[TypeDescriptor],
    ) -> Result<Self, TransformerError> {
        let mut columns_by_name = HashMap::with_capacity(rel.columns.len());
        for column in &rel.columns {
            if columns_by_name
                .insert(column.name.as_str(), column.type_oid)
                .is_some()
            {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "schema relation {}.{} contains duplicate column {:?}",
                        rel.schema, rel.name, column.name
                    ),
                });
            }
        }
        let mut by_name = HashMap::with_capacity(descriptors.len());
        for descriptor in descriptors {
            if by_name
                .insert(descriptor.column.as_str(), descriptor)
                .is_some()
            {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "schema relation {}.{} has duplicate descriptors for column {:?}",
                        rel.schema, rel.name, descriptor.column
                    ),
                });
            }
            let relation_oid = columns_by_name
                .get(descriptor.column.as_str())
                .copied()
                .ok_or_else(|| TransformerError::ManifestInvariant {
                    message: format!(
                        "schema relation {}.{} has a descriptor for unknown column {:?}",
                        rel.schema, rel.name, descriptor.column
                    ),
                })?;
            if descriptor.pg_type_oid != relation_oid {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "schema relation {}.{} descriptor for {:?} has type OID {}, relation has {}",
                        rel.schema,
                        rel.name,
                        descriptor.column,
                        descriptor.pg_type_oid,
                        relation_oid
                    ),
                });
            }
            if let Some(entry) = descriptor.emit.iter().find(|entry| {
                entry
                    .rsplit_once(':')
                    .is_none_or(|(name, arrow_type)| name.is_empty() || arrow_type.is_empty())
            }) {
                return Err(TransformerError::ManifestInvariant {
                    message: format!(
                        "schema relation {}.{} descriptor for {:?} has malformed emit entry {:?}; expected name:ARROW_TYPE",
                        rel.schema, rel.name, descriptor.column, entry
                    ),
                });
            }
        }
        // Lower bound because a Tier-2 range fans out to five raw columns.
        let mut raw_cols = Vec::with_capacity(rel.columns.len());
        let mut mirror_cols = Vec::with_capacity(rel.columns.len());
        for c in &rel.columns {
            append_registry_column(
                c,
                by_name.get(c.name.as_str()).copied(),
                &mut raw_cols,
                &mut mirror_cols,
            );
        }
        let plan = TablePlan {
            table: rel.name.as_str().into(),
            raw_cols: raw_cols.into_boxed_slice(),
            mirror_cols: mirror_cols.into_boxed_slice(),
        };
        validate_physical_names(rel, &plan)?;
        Ok(plan)
    }

    /// Plan one logical source column while preserving its physical raw/mirror emit boundary.
    ///
    /// Full [`TablePlan`]s intentionally flatten Tier-2 siblings. Schema-lineage callers need this
    /// focused view to rename the exact siblings belonging to one PostgreSQL attnum without
    /// position-zipping unrelated columns after an ADD or DROP.
    #[must_use]
    pub(crate) fn for_registry_column(
        rel: &PgRelation,
        column: &common::PgColumn,
        descriptor: Option<&TypeDescriptor>,
    ) -> Self {
        let mut raw_cols = Vec::new();
        let mut mirror_cols = Vec::new();
        append_registry_column(column, descriptor, &mut raw_cols, &mut mirror_cols);
        Self {
            table: rel.name.as_str().into(),
            raw_cols: raw_cols.into_boxed_slice(),
            mirror_cols: mirror_cols.into_boxed_slice(),
        }
    }
}

fn validate_physical_names(rel: &PgRelation, plan: &TablePlan) -> Result<(), TransformerError> {
    for (shape, names) in [
        (
            "raw",
            plan.raw_cols
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
        ),
        (
            "mirror",
            plan.mirror_cols
                .iter()
                .map(|column| column.name.as_str())
                .collect::<Vec<_>>(),
        ),
    ] {
        let mut seen = HashSet::with_capacity(names.len());
        if let Some(name) = names
            .iter()
            .copied()
            .find(|name| RESERVED_PHYSICAL_COLUMNS.contains(name) || !seen.insert(*name))
        {
            return Err(TransformerError::ManifestInvariant {
                message: format!(
                    "schema relation {}.{} has duplicate or reserved {shape} physical column {name:?}",
                    rel.schema, rel.name
                ),
            });
        }
        for name in names {
            SqlColumn::new(name).map_err(|source| TransformerError::ManifestInvariant {
                message: format!(
                    "schema relation {}.{} has invalid {shape} physical column {name:?}: {source}",
                    rel.schema, rel.name
                ),
            })?;
        }
    }
    Ok(())
}

fn append_registry_column(
    column: &common::PgColumn,
    descriptor: Option<&TypeDescriptor>,
    raw_cols: &mut Vec<RawCol>,
    mirror_cols: &mut Vec<MirrorCol>,
) {
    let Some(descriptor) = descriptor else {
        // No descriptor — treat as a Tier-1 scalar.
        let ty = crate::duck::duck_type(column.type_oid).to_string();
        raw_cols.push(RawCol {
            name: column.name.clone(),
            duckdb_type: ty.clone(),
        });
        mirror_cols.push(MirrorCol {
            name: column.name.clone(),
            duckdb_type: ty,
            is_key: column.is_key,
            toast_source: (!column.is_key).then(|| column.name.as_str().into()),
            value: MirrorValue::Passthrough,
        });
        return;
    };
    plan_column(
        column.name.as_str(),
        column.is_key,
        descriptor,
        raw_cols,
        mirror_cols,
    );
}

/// Plan one column from its descriptor, appending to `raw_cols`/`mirror_cols`.
fn plan_column(
    name: &str,
    is_key: bool,
    d: &TypeDescriptor,
    raw_cols: &mut Vec<RawCol>,
    mirror_cols: &mut Vec<MirrorCol>,
) {
    let emit = parse_emit(&d.emit);
    // These branches dispatch on the column's EMIT SHAPE (recombine / single-column / flat), which is
    // deliberately NOT the descriptor's declared `tier`: a Tier-2 `geometric`/
    // `multirange` stays NESTED — one struct/list emit column — so it flows through the single-column
    // arm below alongside Tier-1/Tier-3, while a Tier-2 `range` expands to 5 flat columns. A
    // `debug_assert!(inferred == d.tier)` would therefore be wrong. Emit shape is the sole source of
    // truth: it comes from the same `emit_fields` dispatch the extractor wrote the Parquet with, so it
    // can't drift from the staged file.
    // Tier-2 that recombines to a single DuckDB scalar (interval / timetz).
    if let Some(expr) = recombine_expr(d.pg_type_oid, &emit) {
        raw_cols.extend(emit.iter().map(|&(n, t)| RawCol {
            name: n.to_string(),
            duckdb_type: t.to_string(),
        }));
        mirror_cols.push(MirrorCol {
            name: name.to_string(),
            duckdb_type: d.duckdb.clone(),
            is_key,
            // FULL and INDEX identities may include a Tier-2 value. The extractor normalizes any `u`
            // marker in an identity field from the old tuple, so key components are never resolved
            // by the transformer.
            toast_source: (!is_key).then(|| name.into()),
            value: MirrorValue::Recombine(expr),
        });
        return;
    }
    // Tier-1 / Tier-3: a single emit column == the source column, mirror = descriptor's DuckDB type.
    if emit.len() <= 1 {
        // The descriptor `duckdb` is the LOGICAL target (e.g. `UUID`, `TIMESTAMP WITH TIME ZONE`, `VARCHAR`
        // for a Tier-3 jsonb) — read_parquet yields it. The one exception is `numeric`, whose descriptor
        // `duckdb` is the bare `DECIMAL`; the precise `DECIMAL(p,s)` lives in the emit type, so prefer it.
        let ty = match emit.first() {
            Some(&(_, t)) if t.starts_with("DECIMAL(") => t.to_string(),
            _ => d.duckdb.clone(),
        };
        raw_cols.push(RawCol {
            name: name.to_string(),
            duckdb_type: ty.clone(),
        });
        mirror_cols.push(MirrorCol {
            name: name.to_string(),
            duckdb_type: ty,
            is_key,
            toast_source: (!is_key).then(|| name.into()),
            value: MirrorValue::Passthrough,
        });
        return;
    }
    // Tier-2 flat (range / geometric): the emit columns pass through as several mirror columns — DuckDB
    // has no range/geo type, so a range IS its 5 flat siblings.
    for &(n, t) in &emit {
        raw_cols.push(RawCol {
            name: n.to_string(),
            duckdb_type: t.to_string(),
        });
        mirror_cols.push(MirrorCol {
            name: n.to_string(),
            duckdb_type: t.to_string(),
            // One logical identity column fans out into identity siblings in the mirror key.
            is_key,
            toast_source: (!is_key).then(|| name.into()),
            value: MirrorValue::Passthrough,
        });
    }
}

/// Parse the descriptor `emit` list (`"name:ARROW_TYPE"`) into `(name, duckdb_type)` pairs.
///
/// Both halves BORROW the descriptor: the name is a slice of the emit entry, and the DuckDB type is a
/// fixed name (or, for `DECIMAL(p,s)`, another slice of that same entry). The pairs are scratch for
/// [`plan_column`]'s emit-shape dispatch, so the owned `String`s are built once — where they are stored
/// on the plan — instead of here and again on the way in.
fn parse_emit(emit: &[String]) -> Vec<(&str, &str)> {
    emit.iter()
        .filter_map(|e| {
            let (n, arrow) = e.rsplit_once(':')?;
            Some((n, emit_arrow_to_duck(arrow)))
        })
        .collect()
}

/// The transformer recombine expression for a type that collapses to one DuckDB scalar — over the winning raw
/// row `s`. `None` for anything that stays flat (Tier-1, range, geometric).
fn recombine_expr(pg_type_oid: u32, emit: &[(&str, &str)]) -> Option<String> {
    // Slice patterns, not `emit[0]` behind a length guard: the arity each expression needs is then
    // checked by the compiler, and the emit columns are named where they are read.
    match (pg_type_oid, emit) {
        (INTERVAL, [(months, _), (days, _), (micros, _)]) => Some(format!(
            "to_months(s.{months}) + to_days(s.{days}) + to_microseconds(s.{micros})"
        )),
        (TIMETZ, [(micros, _), (offset, _)]) => {
            Some(format!("make_timetz(s.{micros}, s.{offset})"))
        }
        _ => None,
    }
}

/// Map an emit Arrow type name ([`pg_to_arrow`'s `arrow_emit_name`]) to a DuckDB storage type for the
/// raw column. `DECIMAL(p,s)` passes through; `FIXEDBINARY`/`STRUCT`/`LIST` (only ever Tier-1 uuid, which
/// uses the descriptor `duckdb` string instead) fall back to `BLOB`/`VARCHAR`.
///
/// Every fixed name is a `&'static str` and the one parameterised form (`DECIMAL(p,s)`) is returned as a
/// slice of `arrow` itself, so the mapping never allocates — the mirror image of the sibling that produced
/// these names, `pg_to_arrow::descriptor::arrow_emit_name`.
fn emit_arrow_to_duck(arrow: &str) -> &str {
    match arrow {
        "BOOLEAN" => "BOOLEAN",
        "INT16" => "SMALLINT",
        "INT32" => "INTEGER",
        "INT64" => "BIGINT",
        "FLOAT" => "REAL",
        "DOUBLE" => "DOUBLE",
        "VARCHAR" => "VARCHAR",
        "BLOB" => "BLOB",
        "DATE" => "DATE",
        "TIME" => "TIME",
        "TIMESTAMPTZ" => "TIMESTAMP WITH TIME ZONE",
        "TIMESTAMP" => "TIMESTAMP",
        other if other.starts_with("DECIMAL(") => other,
        other if other.starts_with("FIXEDBINARY(") => "BLOB",
        _ => "VARCHAR",
    }
}

#[cfg(test)]
#[path = "plan_test.rs"]
mod tests;
