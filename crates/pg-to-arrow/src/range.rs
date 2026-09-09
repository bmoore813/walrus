//! `range` / `multirange` decomposition (walrus-extractor.md §2.4).
//!
//! DuckDB has no range type, so a Postgres range can never be a single 1:1 mirror column. A range is
//! **losslessly reconstructable** from exactly five values — `lower`, `upper`, `lower_inc`,
//! `upper_inc`, `isempty` — so the extractor emits those as five flat sibling columns; a multirange (an
//! ordered set of non-empty, non-null members) becomes a `LIST<STRUCT>`.
//!
//! This module owns two things: the **family → element-type** dispatch (`int4range → INT32`,
//! `tstzrange → TIMESTAMPTZ`, …) and the **wire-literal parsers** (`[1,10)`, `empty`, `(,5]`,
//! `{[1,4),[7,9)}`). The three states NULL / `empty` / `unbounded` are kept strictly distinct.

use crate::error::Error;
use crate::oids;
use arrow::datatypes::{DataType, TimeUnit};
use std::borrow::Cow;

/// The six built-in range/multirange families. The element type — and, in Postgres, the
/// canonicalization — differ per family, but the wire form the extractor parses is uniform.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RangeFamily {
    /// `int4range` / `int4multirange` — elements are `integer`.
    Int4,
    /// `int8range` / `int8multirange` — elements are `bigint`.
    Int8,
    /// `numrange` / `nummultirange` — elements are `numeric`; the only family whose element type
    /// depends on a typmod, and in practice always the text carrier (see `elem_data_type`).
    Num,
    /// `tsrange` / `tsmultirange` — elements are `timestamp without time zone`.
    Ts,
    /// `tstzrange` / `tstzmultirange` — elements are `timestamp with time zone`.
    TsTz,
    /// `daterange` / `datemultirange` — elements are `date`.
    Date,
}

impl RangeFamily {
    /// The family behind a *range* OID (`int4range`, `tstzrange`, …), or `None` if the OID is not
    /// one of the six built-in range types.
    #[must_use]
    pub const fn from_range_oid(oid: u32) -> Option<Self> {
        Some(match oid {
            oids::INT4RANGE => Self::Int4,
            oids::INT8RANGE => Self::Int8,
            oids::NUMRANGE => Self::Num,
            oids::TSRANGE => Self::Ts,
            oids::TSTZRANGE => Self::TsTz,
            oids::DATERANGE => Self::Date,
            _ => return None,
        })
    }

    /// The same, for a *multirange* OID (PG14+). Kept separate from
    /// [`from_range_oid`](Self::from_range_oid) rather than merged into one lookup, because a
    /// column's OID says which of the two shapes it is and the caller must not lose that.
    #[must_use]
    pub const fn from_multirange_oid(oid: u32) -> Option<Self> {
        Some(match oid {
            oids::INT4MULTIRANGE => Self::Int4,
            oids::INT8MULTIRANGE => Self::Int8,
            oids::NUMMULTIRANGE => Self::Num,
            oids::TSMULTIRANGE => Self::Ts,
            oids::TSTZMULTIRANGE => Self::TsTz,
            oids::DATEMULTIRANGE => Self::Date,
            _ => return None,
        })
    }

    /// Arrow element type for `_lower`/`_upper`. Unconstrained `numrange` falls back to `Utf8` — the
    /// Tier-3 VARCHAR carrier verified by the conformance tests (a range column carries no element
    /// typmod, so in practice [`Num`](RangeFamily::Num) is always `Utf8` today).
    #[must_use]
    pub fn elem_data_type(self, atttypmod: i32) -> DataType {
        match self {
            Self::Int4 => DataType::Int32,
            Self::Int8 => DataType::Int64,
            Self::Num => match crate::schema::numeric_precision_scale(atttypmod) {
                Some((p @ 1..=38, s)) => DataType::Decimal128(p, s),
                _ => DataType::Utf8,
            },
            Self::Ts => DataType::Timestamp(TimeUnit::Microsecond, None),
            Self::TsTz => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            Self::Date => DataType::Date32,
        }
    }
}

/// One parsed range. A `None` bound means *unbounded on that side* (unless `empty`). The three
/// states are distinct: `empty` (`isempty` true, both bounds `None`), an unbounded bound (`None` with
/// `empty=false`), and — at the column level — a whole SQL `NULL` (handled by the caller, not here).
/// `lower_inf`/`upper_inf` are **derivable** (`bound.is_none() && !empty`) and deliberately not stored.
///
/// Each bound **borrows the wire literal** it was cut from: this is per-value work on the batch-build
/// path, and a bound only ever needs its own buffer when Postgres quoted *and* escaped it — the
/// [`Cow`] condition. Read a bound through `as_deref()`; the borrow is invisible to callers.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParsedRange<'a> {
    /// Postgres's `isempty`. When set, both bounds are `None` and the inclusivity flags are
    /// meaningless — an empty range is not "unbounded on both sides".
    pub empty: bool,
    /// Lower bound literal, or `None` for unbounded below. Borrowed from the wire text unless
    /// unescaping forced a copy.
    pub lower: Option<Cow<'a, str>>,
    /// Upper bound literal, or `None` for unbounded above; same borrowing rule as `lower`.
    pub upper: Option<Cow<'a, str>>,
    /// Whether the lower bound is inclusive — the `[` of `[a,b)`.
    pub lower_inc: bool,
    /// Whether the upper bound is inclusive — the `]` of `(a,b]`.
    pub upper_inc: bool,
}

fn range_err(text: &str) -> Error {
    Error::value_parse("range", text, "range")
}

/// Parse `[1,10)` / `empty` / `(,5]` / `[2024-01-01,)` into a [`ParsedRange`]. An inclusivity marker
/// on an unbounded side is forced to `false` (an infinite bound is never inclusive — matches
/// Postgres' `lower_inc`/`upper_inc`).
///
/// # Errors
///
/// Returns [`Error::ValueParse`] when delimiters are invalid or the two bounds are not separated by
/// one top-level comma.
///
/// # Examples
///
/// ```
/// use pg_to_arrow::range::parse_range;
///
/// let range = parse_range("[1,10)")?;
/// assert_eq!(range.lower.as_deref(), Some("1"));
/// assert_eq!(range.upper.as_deref(), Some("10"));
/// assert!(range.lower_inc);
/// assert!(!range.upper_inc);
///
/// let unbounded = parse_range("(,5]")?;
/// assert_eq!(unbounded.lower, None);
/// assert_eq!(unbounded.upper.as_deref(), Some("5"));
/// # Ok::<(), pg_to_arrow::Error>(())
/// ```
pub fn parse_range(text: &str) -> Result<ParsedRange<'_>, Error> {
    let t = text.trim();
    if t.eq_ignore_ascii_case("empty") {
        return Ok(ParsedRange {
            empty: true,
            lower: None,
            upper: None,
            lower_inc: false,
            upper_inc: false,
        });
    }
    let bytes = t.as_bytes();
    if bytes.len() < 3 {
        return Err(range_err(text));
    }
    let lower_inc = match bytes.first() {
        Some(b'[') => true,
        Some(b'(') => false,
        _ => return Err(range_err(text)),
    };
    let upper_inc = match bytes.last() {
        Some(b']') => true,
        Some(b')') => false,
        _ => return Err(range_err(text)),
    };
    let inner = &t[1..t.len() - 1];
    let (lo, hi) = split_top_level_comma(inner).ok_or_else(|| range_err(text))?;
    let lower = parse_bound(lo);
    let upper = parse_bound(hi);
    Ok(ParsedRange {
        empty: false,
        lower_inc: lower_inc && lower.is_some(),
        upper_inc: upper_inc && upper.is_some(),
        lower,
        upper,
    })
}

/// Parse `{[1,4),[7,9)}` (and `{}`) into member ranges. Members are non-empty and non-null (Postgres
/// guarantees this); an empty multirange yields an empty `Vec` — distinct from a NULL column.
///
/// # Errors
///
/// Returns [`Error::ValueParse`] if the outer braces are missing or any member is not a valid range.
pub fn parse_multirange(text: &str) -> Result<Vec<ParsedRange<'_>>, Error> {
    let t = text.trim();
    if !t.starts_with('{') || !t.ends_with('}') {
        return Err(range_err(text));
    }
    let inner = t[1..t.len() - 1].trim();
    if inner.is_empty() {
        return Ok(Vec::new());
    }
    split_members(inner).into_iter().map(parse_range).collect()
}

/// One bound literal → its value: empty (unquoted) = unbounded (`None`); otherwise the raw text with
/// surrounding quotes stripped and `""`/`\x` un-escaped.
///
/// An unquoted bound (`[1,10)`) is returned as [`Cow::Borrowed`] — the overwhelmingly common shape,
/// and the one the discrete families always take.
fn parse_bound(s: &str) -> Option<Cow<'_, str>> {
    if s.is_empty() {
        return None; // unbounded side
    }
    if s.starts_with('"') {
        return Some(unquote(s));
    }
    Some(Cow::Borrowed(s))
}

/// Strip the surrounding `"` and un-escape `""` → `"` and `\x` → `x` (Postgres' range/element quoting).
///
/// Un-escaping is the *only* reason a bound needs its own buffer, so a quoted body with nothing to
/// un-escape stays [`Cow::Borrowed`] — that covers every `tsrange`/`tstzrange` bound, which Postgres
/// quotes unconditionally.
fn unquote(s: &str) -> Cow<'_, str> {
    // A lone `"` (`[a,")` splits the upper bound down to one delimiter) has no body between the
    // quotes, and `s[1..0]` is an inverted range that panics. Wire text is untrusted input, so ask
    // for the span and treat a missing one as the empty bound it describes.
    let inner = s.get(1..s.len().saturating_sub(1)).unwrap_or_default();
    if !inner.contains(['"', '\\']) {
        return Cow::Borrowed(inner);
    }
    let mut out = String::with_capacity(inner.len());
    let mut chars = inner.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '"' if chars.peek() == Some(&'"') => {
                out.push('"');
                chars.next();
            }
            '\\' => {
                if let Some(n) = chars.next() {
                    out.push(n);
                }
            }
            _ => out.push(c),
        }
    }
    Cow::Owned(out)
}

/// Split `inner` at the single top-level comma (quote-aware), returning `(lower, upper)`.
fn split_top_level_comma(inner: &str) -> Option<(&str, &str)> {
    let b = inner.as_bytes();
    let mut i = 0;
    let mut in_quotes = false;
    while i < b.len() {
        match b[i] {
            b'"' => {
                if in_quotes && b.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'\\' if in_quotes => {
                i += 2;
                continue;
            }
            b',' if !in_quotes => return Some((&inner[..i], &inner[i + 1..])),
            _ => {}
        }
        i += 1;
    }
    None
}

/// Split a multirange body into member-range literals at top-level commas (quote- and bracket-aware).
fn split_members(inner: &str) -> Vec<&str> {
    let b = inner.as_bytes();
    let mut out = Vec::new();
    let mut start = 0;
    let mut depth: i32 = 0;
    let mut in_quotes = false;
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'"' => {
                if in_quotes && b.get(i + 1) == Some(&b'"') {
                    i += 2;
                    continue;
                }
                in_quotes = !in_quotes;
            }
            b'\\' if in_quotes => {
                i += 2;
                continue;
            }
            b'[' | b'(' if !in_quotes => depth += 1,
            b']' | b')' if !in_quotes => depth -= 1,
            b',' if !in_quotes && depth == 0 => {
                out.push(inner[start..i].trim());
                start = i + 1;
            }
            _ => {}
        }
        i += 1;
    }
    out.push(inner[start..].trim());
    out
}

#[cfg(test)]
#[path = "range_test.rs"]
mod tests;
