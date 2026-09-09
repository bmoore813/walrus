//! The **relation cache** — the bridge from decoded pgoutput messages to typed Arrow.
//!
//! Every pgoutput `Relation` describes a table's shape at a point in time; every later
//! `Insert`/`Update`/`Delete` references it by OID. This cache turns a `Relation` into a Tier-1 Arrow
//! schema (+ per-column [`TypeDescriptor`]s) via `pg-to-arrow` and stores it keyed by
//! **`(relation_oid, schema_version)`** — the version in the key is what makes a schema change
//! a *new* entry rather than a mutation, so in-flight batches at the old version still
//! resolve. At bootstrap the cache is **hydrated** from `schema_registry` so a restart is a resume.

use arrow::datatypes::SchemaRef;
use common::{PgRelation, SchemaVersionNo, TypeDescriptor};
use std::collections::{BTreeMap, btree_map};
use std::sync::Arc;

/// Everything the batching path needs for one relation at one `schema_version`, shared by
/// `Arc` so it is read without cloning per row.
#[derive(Debug, Clone)]
pub struct CachedRelation {
    /// The source shape this entry caches, exactly as the Relation message announced it.
    pub relation: PgRelation,
    /// Built by `pg-to-arrow`: one field per source column + the trailing `walrus_extractor_meta` Utf8.
    pub arrow_schema: SchemaRef,
    /// Per source column, for the transformer to rebuild the exact types (§2.6).
    pub descriptors: Vec<TypeDescriptor>,
    /// The version this shape *is*. Half of the cache key, and stamped onto every row encoded
    /// against it, which is how the transformer later finds the matching registry entry.
    pub schema_version: SchemaVersionNo,
    /// Exact immutable registry JSON for this version. It may contain additive metadata such as
    /// the bootstrap comment snapshot that is intentionally ignored by [`PgRelation`] decoding.
    pub registry_columns: serde_json::Value,
}

/// Walrus-internal source tables: control-plane, never registered or schematised as user data.
/// Reload signals/events and DDL audit rows are consumed in-band, never materialised.
#[must_use]
pub fn is_internal_table(schema: &str, table: &str) -> bool {
    schema == "public"
        && matches!(
            table,
            "walrus_ddl_audit"
                | "walrus_heartbeat"
                | "walrus_reload_signal"
                | "walrus_reload_event"
        )
}

/// Every relation shape the decode loop has seen, keyed by OID **and** schema version.
///
/// Versions are kept rather than overwritten: a file already in flight was encoded against an
/// older shape, so evicting it on a DDL bump would strand rows that still need to be sealed.
#[derive(Debug, Default)]
pub struct RelationCache {
    /// Ordered by `(relation_oid, schema_version)`: tuple ordering makes all versions of one oid a
    /// contiguous range, which [`Self::latest_for`] uses instead of scanning the full cache.
    ///
    /// `IndexMap` was considered and declined because insertion order cannot serve that range and
    /// does not justify a new dependency.
    by_key: BTreeMap<(u32, SchemaVersionNo), Arc<CachedRelation>>,
}

impl RelationCache {
    /// The shape cached for exactly this `(oid, schema_version)`, or `None` if that pair was never
    /// hydrated. Use [`latest_for`](Self::latest_for) when any version of the relation will do.
    #[must_use]
    pub fn get(&self, oid: u32, schema_version: SchemaVersionNo) -> Option<Arc<CachedRelation>> {
        self.by_key.get(&(oid, schema_version)).cloned()
    }

    /// The cached shape for `oid` at its **highest** `schema_version`. DDL identity checks use this
    /// current view; change routing must instead use an explicit relation binding because replayed
    /// WAL may describe an older hydrated version.
    #[must_use]
    pub fn latest_for(&self, oid: u32) -> Option<Arc<CachedRelation>> {
        self.by_key
            .range((oid, SchemaVersionNo(i64::MIN))..=(oid, SchemaVersionNo(i64::MAX)))
            .next_back()
            .map(|(_, cached)| Arc::clone(cached))
    }

    /// The OID of a cached `schema.table` (any version) — the DDL-capture cut needs it to find
    /// the affected table's batcher.
    #[must_use]
    pub fn oid_for(&self, schema: &str, table: &str) -> Option<u32> {
        self.iter()
            .find(|cached| cached.relation.schema == schema && cached.relation.name == table)
            .map(|cached| cached.relation.oid)
    }

    /// Highest-version cached relation for a qualified table name.
    #[must_use]
    pub fn latest_for_name(&self, schema: &str, table: &str) -> Option<Arc<CachedRelation>> {
        self.iter()
            .filter(|cached| cached.relation.schema == schema && cached.relation.name == table)
            .max_by_key(|cached| cached.schema_version)
            .map(Arc::clone)
    }

    /// Remove one provisional version after its streamed source transaction or savepoint aborts.
    /// Committed older versions remain available to in-flight files and replay.
    pub fn remove_version(&mut self, schema: &str, table: &str, version: SchemaVersionNo) {
        self.by_key.retain(|_, cached| {
            cached.schema_version != version
                || cached.relation.schema != schema
                || cached.relation.name != table
        });
    }

    /// The cached relations in ascending `(relation_oid, schema_version)` key order. The map key is
    /// a projection of each value, so iteration yields values directly.
    #[must_use = "iterators are lazy and do nothing unless consumed"]
    pub fn iter(&self) -> Iter<'_> {
        <&Self as IntoIterator>::into_iter(self)
    }

    /// Cached entries — `(oid, schema_version)` pairs, so several versions of one relation each
    /// count once.
    #[must_use]
    pub fn len(&self) -> usize {
        self.by_key.len()
    }

    /// Whether nothing has been cached yet, which before hydration means no relation can be decoded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.by_key.is_empty()
    }

    /// Build the Arrow schema + descriptors from a decoded `Relation`, cache under
    /// `(oid, schema_version)`, and return the entry.
    ///
    /// # Errors
    ///
    /// Returns [`RelationError::Schema`] when the relation contains an unsupported or invalid Arrow
    /// mapping.
    pub fn upsert_from_relation(
        &mut self,
        relation: PgRelation,
        schema_version: SchemaVersionNo,
    ) -> Result<Arc<CachedRelation>, RelationError> {
        let key = (relation.oid, schema_version);
        match self.by_key.entry(key) {
            btree_map::Entry::Occupied(mut occupied) => {
                if occupied.get().relation == relation {
                    // A pgoutput Relation message carries structural shape only. Keep any additive
                    // metadata hydrated or attached at the catalog fence so an identical message
                    // cannot manufacture a different value for the immutable schema_registry key.
                    return Ok(Arc::clone(occupied.get()));
                }
                let entry = Arc::new(build_cached(relation, schema_version)?);
                occupied.insert(Arc::clone(&entry));
                Ok(entry)
            }
            btree_map::Entry::Vacant(vacant) => {
                let entry = Arc::new(build_cached(relation, schema_version)?);
                vacant.insert(Arc::clone(&entry));
                Ok(entry)
            }
        }
    }

    /// Replace the exact registry JSON for a cached shape after the catalog fence attaches additive
    /// bootstrap metadata.
    pub fn set_registry_columns(
        &mut self,
        oid: u32,
        schema_version: SchemaVersionNo,
        columns: serde_json::Value,
    ) {
        if let Some(cached) = self.by_key.get_mut(&(oid, schema_version)) {
            Arc::make_mut(cached).registry_columns = columns;
        }
    }

    /// Rebuild cache entries at bootstrap from persisted `schema_registry` rows (step 7). Each row's
    /// `columns` snapshot is the serialized [`PgRelation`]; the Arrow schema is recomputed from it,
    /// and the stored descriptors are used verbatim.
    ///
    /// # Errors
    ///
    /// Returns [`RelationError::Hydrate`] when a persisted snapshot is not a [`PgRelation`], or
    /// [`RelationError::Schema`] when its shape cannot be mapped to Arrow.
    pub fn hydrate(&mut self, rows: Vec<control::RegistryRow>) -> Result<(), RelationError> {
        let decoded = rows
            .into_iter()
            .map(|row| {
                let registry_columns = row.columns;
                let relation: PgRelation = serde_json::from_value(registry_columns.clone())
                    .map_err(|source| RelationError::Hydrate {
                        schema: row.source_schema,
                        table: row.source_table,
                        source,
                    })?;
                let arrow_schema = build_arrow(&relation)?;
                Ok(CachedRelation {
                    arrow_schema,
                    descriptors: row.descriptors,
                    schema_version: row.schema_version,
                    relation,
                    registry_columns,
                })
            })
            .collect::<Result<Vec<_>, RelationError>>()?;
        self.extend(decoded);
        Ok(())
    }
}

impl FromIterator<CachedRelation> for RelationCache {
    fn from_iter<I: IntoIterator<Item = CachedRelation>>(iter: I) -> Self {
        let mut cache = RelationCache::default();
        cache.extend(iter);
        cache
    }
}

impl Extend<CachedRelation> for RelationCache {
    fn extend<I: IntoIterator<Item = CachedRelation>>(&mut self, iter: I) {
        for cached in iter {
            let key = (cached.relation.oid, cached.schema_version);
            self.by_key.insert(key, Arc::new(cached));
        }
    }
}

impl IntoIterator for RelationCache {
    type Item = Arc<CachedRelation>;
    type IntoIter = IntoIter;

    fn into_iter(self) -> Self::IntoIter {
        IntoIter {
            inner: self.by_key.into_values(),
        }
    }
}

impl<'a> IntoIterator for &'a RelationCache {
    type Item = &'a Arc<CachedRelation>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Self::IntoIter {
        Iter {
            inner: self.by_key.values(),
        }
    }
}

impl<'a> IntoIterator for &'a mut RelationCache {
    type Item = &'a mut Arc<CachedRelation>;
    type IntoIter = IterMut<'a>;

    fn into_iter(self) -> Self::IntoIter {
        IterMut {
            inner: self.by_key.values_mut(),
        }
    }
}

// The three types below are named after the method that builds them — `iter` → `Iter`, `iter_mut` →
// `IterMut`, `into_iter` → `IntoIter` — rather than handing back the map's own `Values`/`ValuesMut`/
// `IntoValues`. Those spellings name the return of a `values()` this cache does not have, and they
// put the private `(relation_oid, schema_version)` key and the choice of `BTreeMap` in every public
// signature, so the `IndexMap` question recorded on `by_key` stays an implementation detail only
// while it is wrapped. Each forwards `next` plus `size_hint`, so `collect` still preallocates;
// `ExactSizeIterator`, `DoubleEndedIterator`, and `Clone` are left off until a caller needs them.

/// The shared iterator over a [`RelationCache`], returned by [`RelationCache::iter`] and by
/// `IntoIterator for &RelationCache`.
#[derive(Debug)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct Iter<'a> {
    inner: btree_map::Values<'a, (u32, SchemaVersionNo), Arc<CachedRelation>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = &'a Arc<CachedRelation>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// The exclusive iterator over a [`RelationCache`], returned by
/// `IntoIterator for &mut RelationCache`.
///
/// The cache has no inherent `iter_mut`: nothing rewrites an entry in place, so the `&mut` form
/// exists only to complete the `IntoIterator` trio.
#[derive(Debug)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IterMut<'a> {
    inner: btree_map::ValuesMut<'a, (u32, SchemaVersionNo), Arc<CachedRelation>>,
}

impl<'a> Iterator for IterMut<'a> {
    type Item = &'a mut Arc<CachedRelation>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// The owning iterator over a [`RelationCache`], returned by `IntoIterator for RelationCache`.
#[derive(Debug)]
#[must_use = "iterators are lazy and do nothing unless consumed"]
pub struct IntoIter {
    inner: btree_map::IntoValues<(u32, SchemaVersionNo), Arc<CachedRelation>>,
}

impl Iterator for IntoIter {
    type Item = Arc<CachedRelation>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next()
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

fn build_arrow(relation: &PgRelation) -> Result<SchemaRef, RelationError> {
    for column in &relation.columns {
        common::sql::SqlColumn::new(&column.name).map_err(|source| RelationError::ColumnName {
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            column: column.name.clone(),
            source,
        })?;
    }
    pg_to_arrow::build_schema(relation)
        .map(Arc::new)
        .map_err(|source| RelationError::Schema {
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            source,
        })
}

fn build_cached(
    relation: PgRelation,
    schema_version: SchemaVersionNo,
) -> Result<CachedRelation, RelationError> {
    let arrow_schema = build_arrow(&relation)?;
    let descriptors =
        pg_to_arrow::describe_relation(&relation).map_err(|source| RelationError::Schema {
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            source,
        })?;
    let registry_columns =
        serde_json::to_value(&relation).map_err(|source| RelationError::Snapshot {
            schema: relation.schema.clone(),
            table: relation.name.clone(),
            source,
        })?;
    Ok(CachedRelation {
        arrow_schema,
        descriptors,
        schema_version,
        relation,
        registry_columns,
    })
}

/// This taxonomy is still growing; new variants must remain additive for downstream crates.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RelationError {
    /// A source column would require identifier quotes or collide with SQL grammar downstream.
    #[error("source column {schema}.{table}.{column} cannot be used as a bare SQL name: {source}")]
    ColumnName {
        /// Source schema containing the invalid column.
        schema: String,
        /// Source table containing the invalid column.
        table: String,
        /// Rejected source column name.
        column: String,
        /// Column-name policy violation.
        #[source]
        source: common::sql::ColumnNameError,
    },
    /// The relation's shape could not be mapped to an Arrow schema — a column type walrus does not
    /// yet handle at any tier. `Display` inlines the cause so a log line names the column.
    #[error("build Arrow schema for {schema}.{table}: {source}")]
    Schema {
        /// Source schema of the unsupported relation.
        schema: String,
        /// Source table whose Arrow schema could not be built.
        table: String,
        /// Typed source-to-Arrow mapping failure.
        #[source]
        source: pg_to_arrow::Error,
    },
    /// A persisted `columns` snapshot did not decode into a [`PgRelation`]. The rendered sentence is
    /// unchanged; what it gains is serde's own failure staying in the chain, so a reporter can reach
    /// its line/column and category instead of re-parsing them back out of the text.
    #[error(
        "hydrate from walrus_schema_registry: {schema}.{table}: \
         columns snapshot is not a PgRelation: {source}"
    )]
    Hydrate {
        /// Source schema named by the registry row.
        schema: String,
        /// Source table named by the registry row.
        table: String,
        /// Typed registry JSON decoding failure.
        #[source]
        source: serde_json::Error,
    },
    /// A decoded source relation could not be serialized into its registry snapshot.
    #[error("serialize schema_registry snapshot for {schema}.{table}: {source}")]
    Snapshot {
        /// Source schema of the relation being serialized.
        schema: String,
        /// Source table of the relation being serialized.
        table: String,
        /// Typed registry JSON serialization failure.
        #[source]
        source: serde_json::Error,
    },
}

#[cfg(test)]
#[path = "relcache_test.rs"]
mod tests;
