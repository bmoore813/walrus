//! Typed domain IDs — newtypes over the bare `i64` primary keys the control plane hands around.
//!
//! [`ManifestId`] extends the [`Lsn`](crate::Lsn) newtype pattern to a `file_manifest` row's id, so
//! it can't be silently swapped for another bare `i64` (a manifest id vs an epoch vs a schema
//! version). [`ManifestId`], [`EpochNo`], [`SchemaVersionNo`], [`ReloadId`], and [`DdlId`] share the
//! same transparent `int8` boundary while remaining distinct types inside Rust.
//!
//! Pgoutput wire scalars (relation/type OIDs and transaction ids) deliberately stay bare because
//! they are protocol fields rather than control-plane identities.
//!
//! All five share one formatting contract: [`Display`](std::fmt::Display),
//! [`LowerHex`](std::fmt::LowerHex), and [`UpperHex`](std::fmt::UpperHex) hand the whole formatter
//! to the inner `i64`, so width, fill, `+`, and `#` behave exactly as they did on the bare integer
//! each type replaced — wrapping an id must not cost a format specifier. (Hex of a negative id is
//! the inner integer's two's-complement bit pattern, as it would be bare; real control-plane ids are
//! positive.) [`Octal`](std::fmt::Octal) and [`Binary`](std::fmt::Binary) are deliberately absent: a
//! `bigserial` key, a generation counter, and a schema version have no base-8 or base-2 reading.
//! [`Lsn`](crate::Lsn) implements all four because a WAL byte address does.
//!
//! Reading text back is a separate question from printing it, and the answer is not shared:
//! [`ReloadId`] alone implements [`FromStr`](std::str::FromStr), because it is the only one of the
//! five that ever arrives as a *string* — the extractor decodes `public.walrus_reload_signal.reload_id` out of a
//! pgoutput tuple, whose columns are text. The other four reach Rust as `int8` through SQLx, so a
//! parser for them would be an unused second way to build an id out of untrusted text.

use std::fmt;
use std::mem::{align_of, size_of};

/// A `file_manifest` row's primary key (`id`): returned by `insert_ready`, claimed as
/// `ManifestRow::id`, and retired through the transformer's Phase-A lifecycle (`delete_claimed` /
/// `mark_failed`). Those APIs live in downstream crates, so `common` cannot link to them.
///
/// The transparent representation makes the SQLx `i64` Encode/Decode delegation a
/// compiler-guaranteed layout contract rather than a convention.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ManifestId(pub i64);

const _: () = assert!(
    size_of::<ManifestId>() == size_of::<i64>() && align_of::<ManifestId>() == align_of::<i64>()
);

impl fmt::Display for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::LowerHex for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::UpperHex for ManifestId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::UpperHex::fmt(&self.0, f)
    }
}

impl From<i64> for ManifestId {
    fn from(v: i64) -> Self {
        ManifestId(v)
    }
}

impl From<ManifestId> for i64 {
    fn from(id: ManifestId) -> Self {
        id.0
    }
}

/// The control plane's **generation counter** (`replication_state.epoch`): bumped when the lifelong
/// replication slot is lost and a total restart opens a new generation (§1.8). It namespaces every
/// control-plane row and every S3 key prefix — it is *not* a row id, and it must never be confused
/// with a [`ManifestId`], a schema version, or a table OID.
///
/// The transparent representation guarantees the layout assumed by its SQLx `i64` delegation.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct EpochNo(pub i64);

const _: () =
    assert!(size_of::<EpochNo>() == size_of::<i64>() && align_of::<EpochNo>() == align_of::<i64>());

impl fmt::Display for EpochNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::LowerHex for EpochNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::UpperHex for EpochNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::UpperHex::fmt(&self.0, f)
    }
}

impl From<i64> for EpochNo {
    fn from(v: i64) -> Self {
        EpochNo(v)
    }
}

impl From<EpochNo> for i64 {
    fn from(value: EpochNo) -> Self {
        value.0
    }
}

/// A relation's structural schema-version number.
///
/// The transparent serde representation preserves the bare JSON number stored in Parquet metadata.
/// The transparent representation separately guarantees the layout assumed by SQLx's `i64`
/// delegation.
#[repr(transparent)]
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[serde(transparent)]
pub struct SchemaVersionNo(pub i64);

const _: () = assert!(
    size_of::<SchemaVersionNo>() == size_of::<i64>()
        && align_of::<SchemaVersionNo>() == align_of::<i64>()
);

impl fmt::Display for SchemaVersionNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::LowerHex for SchemaVersionNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::UpperHex for SchemaVersionNo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::UpperHex::fmt(&self.0, f)
    }
}

impl From<i64> for SchemaVersionNo {
    fn from(value: i64) -> Self {
        SchemaVersionNo(value)
    }
}

impl From<SchemaVersionNo> for i64 {
    fn from(value: SchemaVersionNo) -> Self {
        value.0
    }
}

/// A `table_reload` attempt's monotonic `bigserial` primary key.
///
/// The transparent representation guarantees the layout assumed by its SQLx `i64` delegation.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ReloadId(pub i64);

const _: () = assert!(
    size_of::<ReloadId>() == size_of::<i64>() && align_of::<ReloadId>() == align_of::<i64>()
);

impl fmt::Display for ReloadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::LowerHex for ReloadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::UpperHex for ReloadId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::UpperHex::fmt(&self.0, f)
    }
}

impl From<i64> for ReloadId {
    fn from(value: i64) -> Self {
        ReloadId(value)
    }
}

impl From<ReloadId> for i64 {
    fn from(value: ReloadId) -> Self {
        value.0
    }
}

/// The inverse of the [`Display`](fmt::Display) above — exactly `i64`'s decimal grammar, so
/// `id.to_string().parse()` round-trips.
///
/// This is the standard hook, not a named `parse_reload_id`, because the one caller is *generic*:
/// `extractor::reload_signal` pulls every signal column out of a text pgoutput tuple through one
/// helper bound by `T: FromStr`, so without this impl the id — alone among the three columns — has
/// to be parsed as a bare `i64` and re-wrapped at the call site.
///
/// The `Err` type is `i64`'s own [`ParseIntError`](std::num::ParseIntError) rather than a bespoke
/// error: this parse *is* the inner integer's, so forwarding its rejection keeps the two text
/// directions as symmetric as the formatting delegation above. Contrast [`Lsn`](crate::Lsn), whose
/// dual-dialect hex grammar is walrus's own and therefore owns a walrus error.
impl std::str::FromStr for ReloadId {
    type Err = std::num::ParseIntError;

    /// # Errors
    ///
    /// Returns [`ParseIntError`](std::num::ParseIntError) when `s` is not a decimal `i64` — empty,
    /// non-numeric, surrounded by whitespace, or out of range.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(ReloadId)
    }
}

/// A `ddl_manifest` row's monotonic `bigserial` primary key. The DDL history is ordered by
/// `(c_lsn, id)` — `id` is the tiebreaker between two schema changes committed at the same LSN — so
/// it is a real ordering key, not just a row handle, and must not be confused with the
/// [`SchemaVersionNo`] the same row carries.
///
/// The transparent representation guarantees the layout assumed by its SQLx `i64` delegation.
#[repr(transparent)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct DdlId(pub i64);

const _: () =
    assert!(size_of::<DdlId>() == size_of::<i64>() && align_of::<DdlId>() == align_of::<i64>());

impl fmt::Display for DdlId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

impl fmt::LowerHex for DdlId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

impl fmt::UpperHex for DdlId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::UpperHex::fmt(&self.0, f)
    }
}

impl From<i64> for DdlId {
    fn from(value: i64) -> Self {
        DdlId(value)
    }
}

impl From<DdlId> for i64 {
    fn from(value: DdlId) -> Self {
        value.0
    }
}

/// Postgres `int8` support (feature `sqlx`): [`ManifestId`] binds and decodes exactly as its inner
/// `i64` — the transparent-newtype trick, now backed by each type's representation attribute above
/// — so a `bigint` column round-trips with no SQL cast. Mirrors
/// [`Lsn`](crate::Lsn)'s `sqlx_support`; hand-written rather than derived so `common`'s `sqlx` dep
/// needn't pull the `macros` feature. Array binds (`&[ManifestId]`) convert to `&[i64]` at the call
/// site — a manual `Type` impl carries no `PgHasArrayType`.
#[cfg(feature = "sqlx")]
mod sqlx_support {
    use super::{DdlId, EpochNo, ManifestId, ReloadId, SchemaVersionNo};
    use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
    use sqlx::{Decode, Encode, Postgres, Type};

    impl Type<Postgres> for ManifestId {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for ManifestId {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for ManifestId {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(ManifestId(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }

    impl Type<Postgres> for EpochNo {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for EpochNo {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for EpochNo {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(EpochNo(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }

    impl Type<Postgres> for SchemaVersionNo {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for SchemaVersionNo {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for SchemaVersionNo {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(SchemaVersionNo(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }

    impl Type<Postgres> for ReloadId {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for ReloadId {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for ReloadId {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(ReloadId(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }

    impl Type<Postgres> for DdlId {
        fn type_info() -> PgTypeInfo {
            <i64 as Type<Postgres>>::type_info()
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            <i64 as Type<Postgres>>::compatible(ty)
        }
    }

    impl Encode<'_, Postgres> for DdlId {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            <i64 as Encode<Postgres>>::encode_by_ref(&self.0, buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for DdlId {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            Ok(DdlId(<i64 as Decode<Postgres>>::decode(value)?))
        }
    }
}

#[cfg(test)]
#[path = "ids_test.rs"]
mod tests;
