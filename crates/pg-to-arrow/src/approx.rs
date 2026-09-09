//! Tolerance-based float assertions for sibling unit tests.
//!
//! `clippy::float_cmp` is denied workspace-wide. Keep exact float equality out of tests instead of
//! suppressing the lint at a macro expansion site.

use crate::geometric::Pt;

/// Absolute tolerance. These helpers compare only small integer-valued geometric coordinates parsed
/// from Postgres text; those values are exactly representable in binary64, so a fixed epsilon is
/// adequate. General large-magnitude data needs relative error.
const EPSILON: f64 = 1e-9;

#[track_caller]
pub(crate) fn assert_approx_eq(got: f64, want: f64) {
    assert!(
        (got - want).abs() < EPSILON,
        "{got} != {want} (absolute tolerance {EPSILON})"
    );
}

#[track_caller]
pub(crate) fn assert_pt_approx(got: Pt, x: f64, y: f64) {
    assert_approx_eq(got.x, x);
    assert_approx_eq(got.y, y);
}
