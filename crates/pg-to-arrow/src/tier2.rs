//! Tier-2 **column-expansion** helpers: the source types that carry more than any single
//! Arrow/Parquet/DuckDB scalar can hold, so the extractor emits *several* sibling columns the transformer
//! recombines (walrus-extractor.md §2.4).
//!
//! `interval` expands to three signed integers; `timetz` expands to micros plus an offset. The
//! canonical-text parsers turn Postgres' output form (`IntervalStyle postgres`) into those
//! integer fields; the *reverse* (transformer-side `to_months + to_days + …` / TIMETZ rebuild) is the
//! transformer's job, not the extractor's.

// These parsers run once per Tier-2 cell on `batch.rs`'s append path, which carries the same deny:
// every read here is an iterator step or a `get`/`split_at_checked` proof, never an indexed cursor.
#![deny(clippy::indexing_slicing)]

use crate::error::Error;
use crate::range::RangeFamily;
use arrow::datatypes::{DataType, Field, Fields};
use std::sync::Arc;

/// `<c>_months INT32`, `<c>_days INT32`, `<c>_micros INT64` — Postgres' *un-normalized* three-field
/// interval (`'1 mon'` ≠ `'30 days'` ≠ `'720 hours'`), which is byte-identical to DuckDB's own
/// three-field `INTERVAL` struct (§2.4).
///
/// **Never a join key / `PARTITION BY`:** DuckDB normalizes intervals for equality and ordering
/// (days→24h, months→30d), so two byte-different intervals can compare equal. The transformer rebuilds
/// with `to_months + to_days + to_microseconds`; it must never key on these columns (§2.4 caveat).
/// The three fields share one logical NULL — all three NULL ⇔ the source value was NULL — so a real
/// zero interval `(0,0,0)` stays distinct from absence.
#[must_use]
pub fn interval_fields(name: &str) -> Vec<Field> {
    vec![
        Field::new(format!("{name}_months"), DataType::Int32, true),
        Field::new(format!("{name}_days"), DataType::Int32, true),
        Field::new(format!("{name}_micros"), DataType::Int64, true),
    ]
}

/// `<c>_micros BIGINT` (µs since midnight) + `<c>_offset_seconds INTEGER` (signed UTC offset).
/// Arrow has no tz-aware time type, so we carry the zone as a sibling column rather than dropping it
/// the way AWS DMS does (§2.4). Sign convention (pinned by the conformance test): `offset_seconds`
/// is the offset *as printed* — east of UTC positive, so `+05:30` → `+19800`, `-08` → `-28800`.
#[must_use]
pub fn timetz_fields(name: &str) -> Vec<Field> {
    vec![
        Field::new(format!("{name}_micros"), DataType::Int64, true),
        Field::new(format!("{name}_offset_seconds"), DataType::Int32, true),
    ]
}

/// The five flat sibling columns a `range` decomposes into (§2.4). Element type per family; all five
/// share the whole-column NULL, so `_empty=false` + a NULL bound is a genuine *unbounded* side (which
/// `lower_inf`/`upper_inf` derive from) — kept distinct from both `empty` and a NULL column.
#[must_use]
pub fn range_fields(name: &str, family: RangeFamily, atttypmod: i32) -> Vec<Field> {
    let elem = family.elem_data_type(atttypmod);
    vec![
        Field::new(format!("{name}_lower"), elem.clone(), true),
        Field::new(format!("{name}_upper"), elem, true),
        Field::new(format!("{name}_lower_inc"), DataType::Boolean, true),
        Field::new(format!("{name}_upper_inc"), DataType::Boolean, true),
        Field::new(format!("{name}_empty"), DataType::Boolean, true),
    ]
}

/// The 4-field struct a multirange member carries: `lower`/`upper` (nullable — a member may be
/// unbounded) and the always-present `lower_inc`/`upper_inc`. Shared by the schema field and the
/// builder so [`RecordBatch::try_new`](arrow::array::RecordBatch::try_new) sees identical types.
#[must_use]
pub fn multirange_struct_fields(family: RangeFamily, atttypmod: i32) -> Fields {
    let elem = family.elem_data_type(atttypmod);
    vec![
        Field::new("lower", elem.clone(), true),
        Field::new("upper", elem, true),
        Field::new("lower_inc", DataType::Boolean, false),
        Field::new("upper_inc", DataType::Boolean, false),
    ]
    .into()
}

/// A `multirange` → one `LIST(STRUCT(lower, upper, lower_inc, upper_inc))` field (§2.4). Empty
/// multirange = empty list; SQL NULL = NULL list — the two stay distinct via the outer list validity.
#[must_use]
pub fn multirange_field(name: &str, family: RangeFamily, atttypmod: i32) -> Field {
    let item = Field::new_list_field(
        DataType::Struct(multirange_struct_fields(family, atttypmod)),
        true,
    );
    Field::new(name, DataType::List(Arc::new(item)), true)
}

fn parse_err(kind: &str, text: &str) -> Error {
    Error::value_parse(kind, text, kind)
}

/// A fractional-seconds run (`"5"`, `"789"`, `"678901"`) as microseconds — scaling the BORROWED digits
/// rather than collecting a zero-padded `String` for every cell that carries a clock.
///
/// Only the first six digits are representable at microsecond resolution, so the rest is truncated; a
/// shorter run scales by `10^(6 - len)`. Digits are ASCII, so a six-byte prefix that is not a char
/// boundary already holds a non-digit and fails to parse either way — as does a run longer than six
/// bytes reaching the `unwrap_or` fallback, which has no scale.
fn frac_to_micros(frac: &str) -> Option<i64> {
    // `10^(6 - n)` for an `n`-digit run: the µs scale, indexed rather than computed.
    const SCALE: [i64; 7] = [1_000_000, 100_000, 10_000, 1_000, 100, 10, 1];

    let digits = frac.get(..6).unwrap_or(frac);
    if digits.is_empty() {
        return Some(0);
    }
    let scale = SCALE.get(digits.len())?;
    digits.parse::<i64>().ok()?.checked_mul(*scale)
}

/// Parse a `HH:MM:SS[.ffffff]` clock (no sign) into microseconds. Fractional seconds are padded /
/// truncated to microsecond resolution. Returns `None` for malformed input or integer overflow.
fn hms_to_micros(body: &str) -> Option<i64> {
    let mut it = body.split(':');
    let h: i64 = it.next()?.parse().ok()?;
    let m: i64 = it.next()?.parse().ok()?;
    let s = it.next().unwrap_or("0");
    if it.next().is_some() {
        return None;
    }
    let (sec, frac) = s.split_once('.').unwrap_or((s, ""));
    let sec: i64 = sec.parse().ok()?;
    let frac_micros = frac_to_micros(frac)?;
    h.checked_mul(3600)?
        .checked_add(m.checked_mul(60)?)?
        .checked_add(sec)?
        .checked_mul(1_000_000)?
        .checked_add(frac_micros)
}

/// A signed `[-]HH:MM:SS[.ffffff]` time token (the negative sign applies to the whole clock).
fn signed_time_to_micros(tok: &str) -> Option<i64> {
    match tok.strip_prefix('-') {
        Some(body) => hms_to_micros(body).and_then(i64::checked_neg),
        None => hms_to_micros(tok),
    }
}

/// Parse canonical interval text (`"1 year 2 mons 3 days 04:05:06.5"`) into `(months, days, micros)`.
///
/// Handles the server-default `postgres` IntervalStyle: word units (`year`/`mon`/`day` and, for
/// robustness, `hour`/`min`/`sec`) plus a trailing signed `HH:MM:SS[.f]` clock. The three fields stay
/// independent — `'1 mon'`→`(1,0,0)`, `'30 days'`→`(0,30,0)`, `'720:00:00'`→`(0,0,2_592_000_000_000)`.
///
/// # Errors
///
/// Returns [`Error::ValueParse`] for an unknown unit, malformed number or clock, a missing unit,
/// integer overflow, or a month/day total outside the emitted `i32` range.
///
/// # Examples
///
/// ```
/// use pg_to_arrow::parse_interval;
///
/// // 1 year folds into the month field; the clock becomes microseconds.
/// assert_eq!(parse_interval("1 year 2 mons 3 days 04:05:06.5")?, (14, 3, 14_706_500_000));
/// # Ok::<(), pg_to_arrow::Error>(())
/// ```
///
/// The three fields stay independent — none of these collapses into another:
///
/// ```
/// use pg_to_arrow::parse_interval;
///
/// assert_eq!(parse_interval("1 mon")?, (1, 0, 0));
/// assert_eq!(parse_interval("30 days")?, (0, 30, 0));
/// assert_eq!(parse_interval("720:00:00")?, (0, 0, 2_592_000_000_000));
/// # Ok::<(), pg_to_arrow::Error>(())
/// ```
pub fn parse_interval(text: &str) -> Result<(i32, i32, i64), Error> {
    let err = || parse_err("interval", text);
    let mut months: i64 = 0;
    let mut days: i64 = 0;
    let mut micros: i64 = 0;
    let mut ago = false;

    // Walk `split_whitespace` itself rather than collecting it: `<number> <unit>` is the only
    // lookahead, and taking the unit with a second `next()` consumes it exactly the way an indexed
    // `i += 2` cursor would — without the per-cell `Vec` or its bounds-checked reads. Every other
    // token advances by one, which is the iterator's own step.
    let mut toks = text.split_whitespace();
    while let Some(tok) = toks.next() {
        // A clock token (`04:05:06.5`) contributes microseconds directly.
        if tok.contains(':') {
            micros = micros
                .checked_add(signed_time_to_micros(tok).ok_or_else(err)?)
                .ok_or_else(err)?;
            continue;
        }
        // `postgres_verbose` decorations: `@ 1 day ago`.
        if tok == "@" {
            continue;
        }
        if tok == "ago" {
            ago = true;
            continue;
        }
        // Otherwise a `<number> <unit>` pair.
        let n: i64 = tok.parse().map_err(|_| err())?;
        match toks.next().ok_or_else(err)? {
            "year" | "years" | "yr" | "yrs" => {
                months = months
                    .checked_add(n.checked_mul(12).ok_or_else(err)?)
                    .ok_or_else(err)?;
            }
            "mon" | "mons" | "month" | "months" => {
                months = months.checked_add(n).ok_or_else(err)?;
            }
            "day" | "days" => {
                days = days.checked_add(n).ok_or_else(err)?;
            }
            "hour" | "hours" | "hr" | "hrs" => {
                micros = micros
                    .checked_add(n.checked_mul(3_600_000_000).ok_or_else(err)?)
                    .ok_or_else(err)?;
            }
            "min" | "mins" | "minute" | "minutes" => {
                micros = micros
                    .checked_add(n.checked_mul(60_000_000).ok_or_else(err)?)
                    .ok_or_else(err)?;
            }
            "sec" | "secs" | "second" | "seconds" => {
                micros = micros
                    .checked_add(n.checked_mul(1_000_000).ok_or_else(err)?)
                    .ok_or_else(err)?;
            }
            _ => return Err(err()),
        }
    }
    if ago {
        months = months.checked_neg().ok_or_else(err)?;
        days = days.checked_neg().ok_or_else(err)?;
        micros = micros.checked_neg().ok_or_else(err)?;
    }
    let months = i32::try_from(months).map_err(|_| err())?;
    let days = i32::try_from(days).map_err(|_| err())?;
    Ok((months, days, micros))
}

/// Parse canonical `timetz` text (`"12:34:56.789+05:30"`) into `(micros_since_midnight, offset_seconds)`.
///
/// The time part has no sign, so the first `+`/`-` in the string marks the offset. `offset_seconds`
/// keeps the printed sign (`+05:30` → `+19800`) — the transformer's TIMETZ rebuild depends on it.
///
/// # Errors
///
/// Returns [`Error::ValueParse`] if the clock, UTC-offset separator, or offset components are
/// missing, malformed, or outside the representable integer range.
///
/// # Examples
///
/// ```
/// use pg_to_arrow::parse_timetz;
///
/// // `offset_seconds` keeps the printed sign: east of UTC is positive, west negative.
/// assert_eq!(parse_timetz("12:34:56.789+05:30")?, (45_296_789_000, 19_800));
/// assert_eq!(parse_timetz("12:34:56-08")?, (45_296_000_000, -28_800));
/// # Ok::<(), pg_to_arrow::Error>(())
/// ```
///
/// A `timetz` without an offset is not canonical text and is rejected:
///
/// ```
/// use pg_to_arrow::parse_timetz;
///
/// assert!(parse_timetz("12:34:56").is_err());
/// ```
pub fn parse_timetz(text: &str) -> Result<(i64, i32), Error> {
    let err = || parse_err("timetz", text);
    // The clock carries no sign, so the first `+`/`-` opens the offset. Splitting there once yields
    // both halves, and stripping the sign off the tail reads it without indexing back into the
    // string. `find` returns a char boundary, so the `None` arm is unreachable — kept modelled
    // rather than converted into a panicking `split_at`.
    let idx = text.find(['+', '-']).ok_or_else(err)?;
    let (clock, signed_offset) = text.split_at_checked(idx).ok_or_else(err)?;
    let micros = hms_to_micros(clock).ok_or_else(err)?;
    let (sign, off): (i32, &str) = match signed_offset.strip_prefix('-') {
        Some(rest) => (-1, rest),
        // `find` matched one of the two signs, so this strip cannot fail either.
        None => (1, signed_offset.strip_prefix('+').ok_or_else(err)?),
    };
    let mut it = off.split(':');
    let oh: i32 = it.next().ok_or_else(err)?.parse().map_err(|_| err())?;
    let om: i32 = it.next().unwrap_or("0").parse().map_err(|_| err())?;
    let os: i32 = it.next().unwrap_or("0").parse().map_err(|_| err())?;
    if it.next().is_some() {
        return Err(err());
    }
    let offset = oh
        .checked_mul(3600)
        .and_then(|hours| hours.checked_add(om.checked_mul(60)?))
        .and_then(|seconds| seconds.checked_add(os))
        .and_then(|seconds| seconds.checked_mul(sign))
        .ok_or_else(err)?;
    Ok((micros, offset))
}

#[cfg(test)]
#[path = "tier2_test.rs"]
mod tests;
