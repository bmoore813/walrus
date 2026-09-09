//! [`ExtractorMeta`] — the provenance document embedded in every Parquet row.
//!
//! Each row walrus writes carries one added column, `walrus_extractor_meta`, a JSON document
//! bunching *all* batch/row provenance. The extractor **serializes** [`ExtractorMeta`] into that column; the
//! transformer persists it verbatim into `<table>_raw` and **promotes** `op`, `commit_lsn`, `lsn`, and
//! `extractor_processed_at` to typed columns, then drops the meta from the derived `<table>` mirror
//! (it's provenance, not current state).
//!
//! **The JSON keys and value shapes here are a cross-service wire contract** (architecture.md
//! §1.4): the extractor and transformer must agree byte-for-byte, so a renamed field or a stray offset on a
//! timestamp silently breaks the transformer. Field names match the documented keys 1:1; [`Op`]/[`Kind`]
//! serialize to the documented scalars; and every datetime is UTC RFC-3339 with a `Z` suffix.

use crate::{EpochNo, Error, Lsn, Result, SchemaVersionNo};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::mem::{align_of, size_of};

/// Postgres' epoch, 2000-01-01T00:00:00Z, as seconds after the Unix epoch: 946_684_800.
///
/// pgoutput commit timestamps (proto §4) and the CopyBoth standby-status clock (§1.9) are measured
/// from this instant, not the Unix epoch. Unix time has no leap seconds, so the offset is exact and
/// fixed. This is the workspace-wide definition; never re-type the digits at a use site.
pub const PG_EPOCH_UNIX_SECS: i64 = 946_684_800;

/// The same instant in microseconds, derived so it cannot drift from [`PG_EPOCH_UNIX_SECS`].
pub const PG_EPOCH_UNIX_MICROS: i64 = PG_EPOCH_UNIX_SECS * 1_000_000;

/// The change operation. Serializes to a single lowercase char: `i` | `u` | `d` | `t`.
/// Wire form locked by `crates/common/tests/enum_wire_form.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// A new row.
    #[serde(rename = "i")]
    Insert,
    /// An existing row's new image.
    #[serde(rename = "u")]
    Update,
    /// A removed row; the values are its old image.
    #[serde(rename = "d")]
    Delete,
    /// The whole table was emptied. Carries no row values — it is a boundary, not a change.
    #[serde(rename = "t")]
    Truncate,
}

/// Where the row originated: a legacy bootstrap snapshot, the live WAL stream, or a fenced
/// table-reload export.
/// Wire form locked by `crates/common/tests/enum_wire_form.rs`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Kind {
    /// A row from the retired exported-snapshot bootstrap path. Retained so existing Parquet and
    /// manifest data remain readable during upgrades.
    Snapshot,
    /// A row decoded from the live WAL stream — the steady state.
    Stream,
    /// A row from a single-table-reload chunk; carries snapshot-op semantics so an overlapping
    /// stream event wins the transformer's dedup.
    Reload,
}

/// A UTC instant rendered as RFC-3339 with a `Z` suffix — walrus's only legal datetime form.
///
/// Wrapping [`jiff::Timestamp`] (which is *always* a UTC instant) makes it impossible for a caller
/// to emit a local or source-offset timestamp: the inner value has no offset, and serialization
/// always renders the `Z` form.
///
/// The transparent representation guarantees that this validation wrapper costs exactly nothing
/// over the inner [`jiff::Timestamp`].
///
/// # Examples
///
/// ```
/// use common::UtcTimestamp;
///
/// let timestamp: UtcTimestamp = "2026-09-08T12:34:56.123456Z".parse()?;
/// assert_eq!(timestamp.to_string(), "2026-09-08T12:34:56.123456Z");
/// assert!("2026-09-08T08:34:56-04:00".parse::<UtcTimestamp>().is_err());
/// # Ok::<(), common::extractor_meta::TimestampParseError>(())
/// ```
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcTimestamp(jiff::Timestamp);

const _: () = assert!(
    size_of::<UtcTimestamp>() == size_of::<jiff::Timestamp>()
        && align_of::<UtcTimestamp>() == align_of::<jiff::Timestamp>()
);

/// Why an RFC-3339 string is not a legal walrus timestamp.
///
/// This taxonomy is still growing — "legal walrus timestamp" is a normalization policy that can
/// gain a rejection reason (sub-microsecond precision, say) rather than fold it into
/// [`TimestampParseError::Malformed`] — so new variants must remain additive for downstream crates.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
#[non_exhaustive]
pub enum TimestampParseError {
    /// The text was not already normalized to walrus's required UTC `Z` form.
    #[error("timestamp {input:?} must be UTC with a 'Z' suffix, not a numeric offset")]
    NotUtcZ {
        /// Rejected timestamp using a non-canonical zone suffix.
        input: String,
    },
    /// The text had a UTC suffix but was not a valid RFC-3339 timestamp.
    #[error("invalid RFC-3339 timestamp {input:?}: {reason}")]
    Malformed {
        /// Rejected timestamp text.
        input: String,
        /// Parser explanation for the malformed value.
        reason: String,
    },
}

impl UtcTimestamp {
    /// The current instant, in UTC.
    #[must_use]
    pub fn now() -> Self {
        UtcTimestamp(jiff::Timestamp::now())
    }

    /// Borrow the underlying UTC instant — free, no allocation.
    ///
    /// `as_` marks that borrow against the owning [`into_inner`](Self::into_inner) below, the way
    /// every other walrus newtype spells its free projection ([`Lsn::as_u64`](crate::Lsn::as_u64),
    /// [`SqlIdent::as_raw`](crate::sql::SqlIdent::as_raw)). The inner type is `Copy`, so a bare
    /// `inner()` would not say which of the two a call site gets.
    #[must_use]
    pub const fn as_inner(&self) -> &jiff::Timestamp {
        &self.0
    }

    /// Consume this wrapper and return the underlying UTC instant.
    #[must_use]
    pub const fn into_inner(self) -> jiff::Timestamp {
        self.0
    }

    /// Build from a pgoutput wire timestamp: **microseconds since 2000-01-01T00:00:00Z** (proto §4) —
    /// the Postgres epoch, not the Unix epoch. Offsets by [`PG_EPOCH_UNIX_SECS`] and defers to
    /// jiff's range check, so a corrupt or overflowing frame is a decode error — **never a panic**.
    ///
    /// This deliberately remains a named constructor rather than `TryFrom<i64>`: a bare `i64` is
    /// ambiguous between microseconds from the Postgres epoch and microseconds from the Unix epoch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Internal`] when adding the Postgres epoch offset overflows or the resulting
    /// instant is outside jiff's supported range. Either condition indicates a corrupt wire value.
    ///
    /// # Examples
    ///
    /// ```
    /// use common::UtcTimestamp;
    ///
    /// // Zero is the *Postgres* epoch — 2000-01-01, not 1970-01-01.
    /// assert_eq!(UtcTimestamp::from_pg_micros(0)?.to_string(), "2000-01-01T00:00:00Z");
    /// assert_eq!(
    ///     UtcTimestamp::from_pg_micros(-1_000_000)?.to_string(),
    ///     "1999-12-31T23:59:59Z"
    /// );
    /// # Ok::<(), common::Error>(())
    /// ```
    pub fn from_pg_micros(pg_micros: i64) -> Result<Self> {
        let unix_micros = pg_micros.checked_add(PG_EPOCH_UNIX_MICROS).ok_or_else(|| {
            Error::Internal(format!("pgoutput commit_ts overflow: {pg_micros} µs"))
        })?;
        let ts = jiff::Timestamp::from_microsecond(unix_micros).map_err(|e| {
            Error::Internal(format!(
                "pgoutput commit_ts {pg_micros} µs out of range: {e}"
            ))
        })?;
        Ok(UtcTimestamp(ts))
    }
}

impl std::str::FromStr for UtcTimestamp {
    type Err = TimestampParseError;

    /// Parse RFC-3339, rejecting anything not already normalized to UTC `Z`.
    ///
    /// # Errors
    ///
    /// Returns [`TimestampParseError::NotUtcZ`] if `s` has no `Z` suffix — a numeric offset is a
    /// different wire form, not a value to convert — and [`TimestampParseError::Malformed`] if the
    /// remaining text is not a valid RFC-3339 timestamp. Both preserve `s` verbatim.
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        if !(s.ends_with('Z') || s.ends_with('z')) {
            return Err(TimestampParseError::NotUtcZ {
                input: s.to_string(),
            });
        }

        let timestamp =
            s.parse::<jiff::Timestamp>()
                .map_err(|error| TimestampParseError::Malformed {
                    input: s.to_string(),
                    reason: error.to_string(),
                })?;
        Ok(Self(timestamp))
    }
}

impl std::fmt::Display for UtcTimestamp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl From<jiff::Timestamp> for UtcTimestamp {
    fn from(timestamp: jiff::Timestamp) -> Self {
        Self(timestamp)
    }
}

impl From<UtcTimestamp> for jiff::Timestamp {
    fn from(timestamp: UtcTimestamp) -> Self {
        timestamp.0
    }
}

impl Serialize for UtcTimestamp {
    fn serialize<S: Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        // UtcTimestamp's Display is the single RFC-3339 `Z` rendering hook.
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for UtcTimestamp {
    /// Parse through [`FromStr`](std::str::FromStr), so a local or offset timestamp is a
    /// deserialization error rather than a [`UtcTimestamp`] that is not UTC.
    ///
    /// Hand-written rather than `#[serde(try_from = "String")]` for the same reason as
    /// [`Lsn`]: that attribute would need a `TryFrom<String>` impl aliasing the
    /// [`FromStr`](std::str::FromStr) above to reach this identical path, and its `into = "String"`
    /// counterpart would allocate per row where [`Serialize`] renders through `collect_str`.
    fn deserialize<D: Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s: String = Deserialize::deserialize(d)?;
        s.parse::<UtcTimestamp>().map_err(serde::de::Error::custom)
    }
}

/// The provenance document embedded (as JSON `Utf8`) in every Parquet row's `walrus_extractor_meta`.
///
/// **Field order and keys are a cross-service wire contract** (architecture.md §1.4) — the transformer
/// reads this back verbatim. Deserialization is deliberately lenient in both rollout directions:
/// unknown keys let a newer extractor feed an older transformer, while a missing `unchanged_toast` defaults
/// to empty so an older extractor can feed a newer transformer. Every identity and ordering key stays required.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExtractorMeta {
    /// The change operation (`i`/`u`/`d`/`t`).
    pub op: Op,
    /// Per-row WAL LSN — the per-PK last-writer tiebreaker only (zero-padded 16-hex).
    pub lsn: Lsn,
    /// Transaction commit LSN — **the** order/watermark key (zero-padded 16-hex).
    pub commit_lsn: Lsn,
    /// Transaction commit time (UTC `Z`).
    pub commit_ts: UtcTimestamp,
    /// Source transaction id.
    pub xid: u32,
    /// Generation counter that namespaces all control-plane state (Postgres `bigint`).
    pub epoch: EpochNo,
    /// UUID of the Parquet batch this row belongs to.
    pub batch_id: String,
    /// Structural schema version of the source relation (Postgres `bigint`).
    pub schema_version: SchemaVersionNo,
    /// Source schema name.
    pub source_schema: String,
    /// Source table name.
    pub source_table: String,
    /// Whether the row came from legacy snapshot bootstrap, the WAL stream, or reload export.
    pub kind: Kind,
    /// Columns delivered as unchanged-TOAST placeholders (values absent from the wire).
    ///
    /// This is the one defaulting field: empty means no placeholders, so an older extractor that omits
    /// the key remains compatible with a newer transformer. Identity and ordering fields deliberately
    /// remain required. Empty values are also omitted when serializing; the transformer treats a missing
    /// key as `[]`, and the paired default makes the omitted document round-trip safely. Fixed at
    /// row construction and never grown afterwards.
    #[serde(default, skip_serializing_if = "<[String]>::is_empty")]
    pub unchanged_toast: Box<[String]>,
    /// Stable identity of the extractor pod that produced this row.
    pub extractor_instance: String,
    /// When the extractor processed this row (UTC `Z`) — promoted to a typed `<table>_raw` column.
    pub extractor_processed_at: UtcTimestamp,
}

/// Move-cost budget for the per-row provenance hot path (`own-move-large`).
///
/// The compile-time budget reflects a measured 192-byte value. If it trips, shrink or box the
/// offending field, or raise the budget deliberately with a new measurement.
const EXTRACTOR_META_MAX_BYTES: usize = 192;
const _: () = assert!(size_of::<ExtractorMeta>() <= EXTRACTOR_META_MAX_BYTES);

// --- amortized serialization -------------------------------------------------------------
//
// The meta column dominates `append_row` (`serde_json::to_string(ExtractorMeta)` ≈ 576 ns/row,
// ~91 % of the narrow-row cost). Within one sealed Parquet file the *batch-constant* fields never
// change, so the batcher serializes them ONCE and, per row, serializes only the varying fields —
// splicing the two into `{const,row}`. Byte-equivalence with `to_string(ExtractorMeta)` is guaranteed by
// construction: these borrow structs carry the identical field names and types and mirror every
// serialization-affecting serde attribute on `ExtractorMeta` (`default` remains deserialization-only).
// Only the key ORDER shifts, and the transformer parses by key (`$.op`, …), never by position. Proven by
// `amortized_meta_matches_full`.

/// The batch-constant subset of [`ExtractorMeta`] — the same for every row of one sealed file.
#[derive(Serialize)]
struct MetaConst<'a> {
    epoch: EpochNo,
    batch_id: &'a str,
    schema_version: SchemaVersionNo,
    source_schema: &'a str,
    source_table: &'a str,
    kind: &'a Kind,
    extractor_instance: &'a str,
}

/// The per-row subset of [`ExtractorMeta`].
#[derive(Serialize)]
struct MetaRow<'a> {
    op: &'a Op,
    lsn: &'a Lsn,
    commit_lsn: &'a Lsn,
    commit_ts: &'a UtcTimestamp,
    xid: u32,
    #[serde(skip_serializing_if = "<[String]>::is_empty")]
    unchanged_toast: &'a [String],
    extractor_processed_at: &'a UtcTimestamp,
}

/// The inner of a serialized JSON object (`{…}`) with the braces removed. Both fragments are
/// non-empty structs, so the output is always `{…}` and this never underflows.
fn object_inner(s: &str) -> &str {
    s.get(1..s.len().saturating_sub(1)).unwrap_or("")
}

impl ExtractorMeta {
    /// The batch-constant fields as a brace-less JSON fragment (e.g. `"epoch":7,"batch_id":"…",…`),
    /// serialized once per sealed batch and cached by the batcher.
    ///
    /// Serializes and allocates on every call — hence `to_`, unlike the appending
    /// [`Self::write_row_json_inner`]. Amortizing this cost is the entire point of the split, so a
    /// caller that invokes it per row has undone it.
    ///
    /// # Errors
    ///
    /// Returns [`serde_json::Error`] if a batch-constant provenance field cannot be serialized.
    pub fn to_const_json_inner(&self) -> std::result::Result<String, serde_json::Error> {
        let s = serde_json::to_string(&MetaConst {
            epoch: self.epoch,
            batch_id: &self.batch_id,
            schema_version: self.schema_version,
            source_schema: &self.source_schema,
            source_table: &self.source_table,
            kind: &self.kind,
            extractor_instance: &self.extractor_instance,
        })?;
        Ok(object_inner(&s).to_string())
    }

    /// Append the per-row fields (brace-less) to `buf`; the batcher wraps `{const,row}` around them.
    ///
    /// # Errors
    ///
    /// Returns [`serde_json::Error`] if a row-varying provenance field cannot be serialized. `buf`
    /// is left unchanged until serialization succeeds.
    pub fn write_row_json_inner(
        &self,
        buf: &mut String,
    ) -> std::result::Result<(), serde_json::Error> {
        let s = serde_json::to_string(&MetaRow {
            op: &self.op,
            lsn: &self.lsn,
            commit_lsn: &self.commit_lsn,
            commit_ts: &self.commit_ts,
            xid: self.xid,
            unchanged_toast: &self.unchanged_toast,
            extractor_processed_at: &self.extractor_processed_at,
        })?;
        buf.push_str(object_inner(&s));
        Ok(())
    }
}

#[cfg(test)]
#[path = "extractor_meta_test.rs"]
mod tests;
