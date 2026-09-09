//! Canonical `pg_catalog` base-type OIDs (stable across Postgres installs).
//!
//! Each constant's *name* is the `pg_catalog` type name and its *value* is that type's OID. The
//! doc on each one names the SQL spelling a user would write — which is not always the catalog
//! name (`bpchar` is `character(n)`, `varbit` is `bit varying(n)`) — and, for the Tier-1 block,
//! the Arrow type `pg_to_arrow::schema::tier1_data_type` maps it to. The tier a type belongs to is
//! walrus's decision, not Postgres's, so it is stated here rather than inferred from the OID.

/// `boolean` — Tier 1, Arrow `Boolean`.
pub const BOOL: u32 = 16;
/// `bytea` — Tier 1, Arrow `Binary`. The only binary-valued Tier-1 type.
pub const BYTEA: u32 = 17;
/// `"char"` — Postgres's internal single-byte type, **not** `char(n)`. Tier 1, Arrow `Utf8`.
pub const CHAR: u32 = 18;
/// `bigint` — Tier 1, Arrow `Int64`.
pub const INT8: u32 = 20;
/// `smallint` — Tier 1, Arrow `Int16`.
pub const INT2: u32 = 21;
/// `integer` — Tier 1, Arrow `Int32`.
pub const INT4: u32 = 23;
/// `text` — Tier 1, Arrow `Utf8`.
pub const TEXT: u32 = 25;
/// `json` — Tier 1, Arrow `Utf8`; DuckDB infers JSON from the string.
pub const JSON: u32 = 114;
/// `real` — Tier 1, Arrow `Float32`.
pub const FLOAT4: u32 = 700;
/// `double precision` — Tier 1, Arrow `Float64`.
pub const FLOAT8: u32 = 701;
/// `character(n)` — blank-padded. Tier 1, Arrow `Utf8`; the padding is preserved in the value.
pub const BPCHAR: u32 = 1042;
/// `character varying(n)` — Tier 1, Arrow `Utf8`.
pub const VARCHAR: u32 = 1043;
/// `date` — Tier 1, Arrow `Date32` (days since the Unix epoch).
pub const DATE: u32 = 1082;
/// `time without time zone` — Tier 1, Arrow `Time64` in microseconds.
pub const TIME: u32 = 1083;
/// `timestamp without time zone` — Tier 1, Arrow `Timestamp` in microseconds, no zone.
pub const TIMESTAMP: u32 = 1114;
/// `timestamp with time zone` — Tier 1, Arrow `Timestamp` in microseconds tagged `UTC`, which is
/// the marker DuckDB reads as `isAdjustedToUTC=true`.
pub const TIMESTAMPTZ: u32 = 1184;
// Tier-2 decompositions: each fans out to several sibling columns (§2.4).
/// `interval` — decomposed into the `months` / `days` / `micros` sibling columns Postgres itself
/// stores, because no single Arrow type carries all three without loss.
pub const INTERVAL: u32 = 1186;
/// `time with time zone` — decomposed into a microsecond time-of-day plus its UTC offset.
pub const TIMETZ: u32 = 1266;
// Range families → 5 flat sibling columns. OIDs are stable pg_catalog built-ins.
/// `int4range` — Tier 2, over `integer`. `pg-to-arrow`'s `RangeFamily` groups the six families.
pub const INT4RANGE: u32 = 3904;
/// `numrange` — Tier 2; its element type is `numeric`, so it carries the same typmod.
pub const NUMRANGE: u32 = 3906;
/// `tsrange` — Tier 2, over `timestamp without time zone`.
pub const TSRANGE: u32 = 3908;
/// `tstzrange` — Tier 2, over `timestamp with time zone`.
pub const TSTZRANGE: u32 = 3910;
/// `daterange` — Tier 2, over `date`.
pub const DATERANGE: u32 = 3912;
/// `int8range` — Tier 2, over `bigint`.
pub const INT8RANGE: u32 = 3926;
// Multirange families (PG14+) → LIST<STRUCT>.
/// `int4multirange` (PG14+) — a list of `int4range` members.
pub const INT4MULTIRANGE: u32 = 4451;
/// `nummultirange` (PG14+) — a list of `numrange` members.
pub const NUMMULTIRANGE: u32 = 4532;
/// `tsmultirange` (PG14+) — a list of `tsrange` members.
pub const TSMULTIRANGE: u32 = 4533;
/// `tstzmultirange` (PG14+) — a list of `tstzrange` members.
pub const TSTZMULTIRANGE: u32 = 4534;
/// `datemultirange` (PG14+) — a list of `daterange` members.
pub const DATEMULTIRANGE: u32 = 4535;
/// `int8multirange` (PG14+) — a list of `int8range` members.
pub const INT8MULTIRANGE: u32 = 4536;
// Native geometric types → STRUCT/LIST of doubles.
/// `point` — one `(x, y)` pair.
pub const POINT: u32 = 600;
/// `lseg` — a line segment: its two endpoints.
pub const LSEG: u32 = 601;
/// `path` — an open or closed sequence of points; the open/closed flag is carried alongside.
pub const PATH: u32 = 602;
/// `box` — a rectangle, stored as its two opposite corners.
pub const BOX: u32 = 603;
/// `polygon` — a closed vertex list.
pub const POLYGON: u32 = 604;
/// `line` — an infinite line as the coefficients `(A, B, C)` of `Ax + By + C = 0`.
pub const LINE: u32 = 628;
/// `circle` — a center point plus a radius.
pub const CIRCLE: u32 = 718;
// Tier-3 canonical-text carriers → VARCHAR: no lossless structural target.
/// `xml` — Tier 3; carried as its canonical text.
pub const XML: u32 = 142;
/// `xid` — a 32-bit transaction id. Tier 3: it is not an `integer`, and wraps around.
pub const XID: u32 = 28;
/// `cidr` — a network address with a mask length. Tier 3.
pub const CIDR: u32 = 650;
/// `macaddr8` — an EUI-64 MAC address. Tier 3.
pub const MACADDR8: u32 = 774;
/// `macaddr` — an EUI-48 MAC address. Tier 3.
pub const MACADDR: u32 = 829;
/// `inet` — a host address with an optional mask length. Tier 3.
pub const INET: u32 = 869;
/// `bit(n)` — a fixed-length bit string. Tier 3, carried as its `0`/`1` text.
pub const BIT: u32 = 1560;
/// `bit varying(n)` — a variable-length bit string. Tier 3, carried as its `0`/`1` text.
pub const VARBIT: u32 = 1562;
/// `txid_snapshot` — a transaction-visibility snapshot. Tier 3.
pub const TXID_SNAPSHOT: u32 = 2970;
/// `pg_lsn` — a WAL byte address. Tier 3 as a *column* type; walrus's own LSNs are
/// [`Lsn`](crate::Lsn), which is a different thing from a user table holding this type.
pub const PG_LSN: u32 = 3220;
/// `tsvector` — a parsed full-text document. Tier 3.
pub const TSVECTOR: u32 = 3614;
/// `tsquery` — a full-text search expression. Tier 3.
pub const TSQUERY: u32 = 3615;
/// `xid8` — a 64-bit, non-wrapping transaction id (PG13+). Tier 3.
pub const XID8: u32 = 5069;
// uuid → native DuckDB UUID via the arrow.uuid extension.
/// `uuid` — the one type that reaches DuckDB as a native `UUID`, via arrow-rs's `arrow.uuid`
/// canonical extension on a `FixedSizeBinary(16)`.
pub const UUID: u32 = 2950;
/// Postgres `FirstNormalObjectId`: user-defined types (including enums) get OIDs at or above this.
/// It is a conservative fallback boundary when catalog-derived type metadata is unavailable.
///
/// This is a *boundary*, not a type OID — the only constant here that names no type.
pub const FIRST_NORMAL_OID: u32 = 16384;
/// `numeric(p, s)` — Tier 1 as an Arrow `Decimal128` only when `p ≤ 38`; unconstrained `numeric`
/// (typmod `-1`) or `p > 38` has no lossless fixed-width target and falls to the Tier-3 text
/// carrier instead (§2.3).
pub const NUMERIC: u32 = 1700;
/// `jsonb` — Tier 1, Arrow `Utf8`. Binary on the source, but its text rendering is canonical.
pub const JSONB: u32 = 3802;
