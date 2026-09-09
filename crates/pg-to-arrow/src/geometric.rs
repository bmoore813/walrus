//! Native Postgres geometric types → `STRUCT` / `LIST` of doubles (walrus-extractor.md §2.4).
//!
//! Every native geometric type is just doubles, so we carry them as queryable nested Arrow — the
//! deepest nesting in the crate. The one correctness trap is `path.is_closed`: Postgres renders an
//! open path as `[(…)]` and a closed one as `((…))`, so without the flag the two are indistinguishable
//! on read-back (the design calls this out as lossy). PostGIS `geometry`/`geography` (WKB+SRID) is
//! **deferred entirely** — DuckDB's core `GEOMETRY` only arrived in v1.5 and the transformer pins 1.4.x LTS
//! (§2.4 PostGIS) — so those OIDs fall through to `NotTier1` here rather than getting a wrong mapping.

use crate::error::Error;
use crate::oids;
use arrow::datatypes::{DataType, Field, Fields};
use std::sync::Arc;

/// Which geometric shape a source column carries — selects the parser (and hence the nested builder).
/// [`Lseg`](GeoKind::Lseg)/[`Box`](GeoKind::Box) share one shape (`STRUCT(p1, p2)`) and one parser
/// (two points).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GeoKind {
    /// `point` — a single `(x, y)`.
    Point,
    /// `line` — the infinite line's three coefficients.
    Line,
    /// `lseg` — a segment's two endpoints.
    Lseg,
    /// `box` — a rectangle's two opposite corners. Same Arrow shape and parser as `Lseg`.
    Box,
    /// `circle` — a center point plus a radius.
    Circle,
    /// `path` — a point list plus its open/closed flag.
    Path,
    /// `polygon` — a point list, always closed, so no flag is carried.
    Polygon,
}

/// The geometric [`GeoKind`] for an OID, or `None` for a non-geometric (incl. PostGIS) type.
#[must_use]
pub const fn geo_kind(type_oid: u32) -> Option<GeoKind> {
    Some(match type_oid {
        oids::POINT => GeoKind::Point,
        oids::LINE => GeoKind::Line,
        oids::LSEG => GeoKind::Lseg,
        oids::BOX => GeoKind::Box,
        oids::CIRCLE => GeoKind::Circle,
        oids::PATH => GeoKind::Path,
        oids::POLYGON => GeoKind::Polygon,
        _ => return None,
    })
}

fn f64_field(name: &str) -> Field {
    Field::new(name, DataType::Float64, true)
}

/// `STRUCT(x DOUBLE, y DOUBLE)` — the single point type reused everywhere (box corners, path points,
/// polygon vertices) so the nested Arrow types are *literally* the same `Fields` (DuckDB compares the
/// full nested type on read-back).
fn point_fields() -> Fields {
    vec![f64_field("x"), f64_field("y")].into()
}

fn point_struct() -> DataType {
    DataType::Struct(point_fields())
}

/// `LIST(STRUCT(x, y))` — path points and polygon vertices. The list item is nullable to match
/// `ListBuilder`'s default item field.
fn point_list() -> DataType {
    DataType::List(Arc::new(Field::new_list_field(point_struct(), true)))
}

/// The single emitted Arrow field for a geometric column (a `STRUCT` or a `LIST<STRUCT>` of doubles),
/// or `None` for a non-geometric OID. Leaf fields are nullable so a whole-column NULL can null every
/// leaf (`StructBuilder` requires each child appended for every row).
#[must_use]
pub fn geometric_field(name: &str, type_oid: u32) -> Option<Field> {
    let dt = match type_oid {
        oids::POINT => point_struct(),
        oids::LINE => DataType::Struct(vec![f64_field("a"), f64_field("b"), f64_field("c")].into()),
        oids::LSEG | oids::BOX => DataType::Struct(
            vec![
                Field::new("p1", point_struct(), true),
                Field::new("p2", point_struct(), true),
            ]
            .into(),
        ),
        oids::CIRCLE => {
            DataType::Struct(vec![f64_field("x"), f64_field("y"), f64_field("r")].into())
        }
        oids::PATH => DataType::Struct(
            vec![
                Field::new("is_closed", DataType::Boolean, true),
                Field::new("points", point_list(), true),
            ]
            .into(),
        ),
        oids::POLYGON => point_list(),
        // Non-geometric — incl. PostGIS geometry/geography, deferred by design (§2.4 PostGIS).
        _ => return None,
    };
    Some(Field::new(name, dt, true))
}

/// A 2-D point (`x`, `y`) — the atom every geometric shape is built from.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Pt {
    /// Horizontal coordinate.
    pub x: f64,
    /// Vertical coordinate.
    pub y: f64,
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<Pt>() == 16,
    "Pt is the element of the unbounded path/polygon vertex Vec built per value"
);

fn geo_err(text: &str) -> Error {
    Error::value_parse("geometric", text, "geometric")
}

fn parse_f64(s: &str, text: &str) -> Result<f64, Error> {
    s.trim().parse().map_err(|_| geo_err(text))
}

/// Parse the inside of a `(x,y)` group.
fn parse_pt_inner(inner: &str, text: &str) -> Result<Pt, Error> {
    let (a, b) = inner.split_once(',').ok_or_else(|| geo_err(text))?;
    Ok(Pt {
        x: parse_f64(a, text)?,
        y: parse_f64(b, text)?,
    })
}

/// Extract every flat `(x,y)` coordinate group from a geometric literal, in order. Outer wrapping
/// parens (`((…),(…))`) are skipped because their inner span still contains a `(`.
fn extract_points(text: &str) -> Result<Vec<Pt>, Error> {
    let bytes = text.as_bytes();
    // Every group starts at its own `(`, so the `(` count is an exact upper bound on the points, and
    // a tight one — only the outer wrapper (`((…),(…))`) is ever slack. One extra byte scan buys a
    // single allocation for a `path`/`polygon`, which carries an unbounded vertex list and is parsed
    // per value on the batch-build path; the 1–2 point shapes size down to exactly what they need.
    let mut pts = Vec::with_capacity(bytes.iter().filter(|&&b| b == b'(').count());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'('
            && let Some(off) = text[i + 1..].find(')')
        {
            let close = i + 1 + off;
            let inner = &text[i + 1..close];
            if !inner.contains('(') {
                pts.push(parse_pt_inner(inner, text)?);
                i = close + 1;
                continue;
            }
        }
        i += 1;
    }
    Ok(pts)
}

/// `"(x,y)"` → [`Pt`].
///
/// # Errors
///
/// Returns [`Error::ValueParse`] unless the input contains exactly one point with finite-format
/// numeric coordinates.
pub fn parse_point(text: &str) -> Result<Pt, Error> {
    match extract_points(text)?.as_slice() {
        [p] => Ok(*p),
        _ => Err(geo_err(text)),
    }
}

/// `"(x1,y1),(x2,y2)"` (box) or `"[(x1,y1),(x2,y2)]"` (lseg) → two points.
///
/// # Errors
///
/// Returns [`Error::ValueParse`] unless the input contains exactly two parseable coordinate pairs.
pub fn parse_box(text: &str) -> Result<(Pt, Pt), Error> {
    match extract_points(text)?.as_slice() {
        [a, b] => Ok((*a, *b)),
        _ => Err(geo_err(text)),
    }
}

/// `"<(x,y),r>"` → center point + radius.
///
/// # Errors
///
/// Returns [`Error::ValueParse`] if the center is not one point or the radius is not an `f64`.
pub fn parse_circle(text: &str) -> Result<(Pt, f64), Error> {
    let t = text.trim();
    let t = t.strip_prefix('<').unwrap_or(t);
    let t = t.strip_suffix('>').unwrap_or(t);
    let center = match extract_points(t)?.as_slice() {
        [p] => *p,
        _ => return Err(geo_err(text)),
    };
    let close = t.rfind(')').ok_or_else(|| geo_err(text))?;
    let rest = t[close + 1..].trim().trim_start_matches(',').trim();
    Ok((center, parse_f64(rest, text)?))
}

/// `"{A,B,C}"` (line as `Ax + By + C = 0`) → `(a, b, c)`.
///
/// # Errors
///
/// Returns [`Error::ValueParse`] unless the input contains exactly three parseable coefficients.
pub fn parse_line(text: &str) -> Result<(f64, f64, f64), Error> {
    let t = text.trim();
    let t = t.strip_prefix('{').unwrap_or(t);
    let t = t.strip_suffix('}').unwrap_or(t);
    let parts: Vec<&str> = t.split(',').collect();
    match parts.as_slice() {
        [a, b, c] => Ok((
            parse_f64(a, text)?,
            parse_f64(b, text)?,
            parse_f64(c, text)?,
        )),
        _ => Err(geo_err(text)),
    }
}

/// Returns `(is_closed, points)`: `[(…)]` = open, `((…))` = closed — the flag is **mandatory**
/// (dropping it makes the two indistinguishable on read-back).
///
/// # Errors
///
/// Returns [`Error::ValueParse`] if any coordinate is invalid or the path contains no points.
pub fn parse_path(text: &str) -> Result<(bool, Vec<Pt>), Error> {
    let t = text.trim();
    let is_closed = t.starts_with('(');
    let pts = extract_points(t)?;
    if pts.is_empty() {
        return Err(geo_err(text));
    }
    Ok((is_closed, pts))
}

/// `"((x,y),…)"` → the polygon's vertices.
///
/// # Errors
///
/// Returns [`Error::ValueParse`] if any coordinate is invalid or the polygon has no vertices.
pub fn parse_polygon(text: &str) -> Result<Vec<Pt>, Error> {
    let pts = extract_points(text)?;
    if pts.is_empty() {
        return Err(geo_err(text));
    }
    Ok(pts)
}

#[cfg(test)]
#[path = "geometric_test.rs"]
mod tests;
