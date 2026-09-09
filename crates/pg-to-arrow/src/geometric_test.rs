use super::*;
use crate::approx::{assert_approx_eq, assert_pt_approx};

#[test]
fn point_struct_x_y() {
    assert_pt_approx(parse_point("(1,2)").unwrap(), 1.0, 2.0);
    let f = geometric_field("p", oids::POINT).unwrap();
    assert_eq!(f.data_type(), &point_struct());
}

#[test]
fn box_is_two_nested_points() {
    let (upper_right, lower_left) = parse_box("(2,3),(0,1)").unwrap();
    assert_pt_approx(upper_right, 2.0, 3.0);
    assert_pt_approx(lower_left, 0.0, 1.0);
    // lseg uses the same two-point shape, with brackets.
    let (start, end) = parse_box("[(0,0),(1,1)]").unwrap();
    assert_pt_approx(start, 0.0, 0.0);
    assert_pt_approx(end, 1.0, 1.0);
}

#[test]
fn path_open_vs_closed_sets_is_closed() {
    // Same points, different delimiters → the ONLY difference is is_closed.
    let (open, open_pts) = parse_path("[(0,0),(1,1)]").unwrap();
    let (closed, closed_pts) = parse_path("((0,0),(1,1))").unwrap();
    assert!(!open, "brackets → open path");
    assert!(closed, "double parens → closed path");
    assert_eq!(open_pts.len(), closed_pts.len());
    for (open_point, closed_point) in open_pts.into_iter().zip(closed_pts) {
        assert_pt_approx(open_point, closed_point.x, closed_point.y);
    }
}

#[test]
fn polygon_is_list_of_points() {
    let pts = parse_polygon("((0,0),(1,0),(1,1))").unwrap();
    assert_eq!(pts.len(), 3);
    assert_pt_approx(pts[2], 1.0, 1.0);
}

#[test]
fn circle_carries_radius() {
    let (center, radius) = parse_circle("<(1,2),3>").unwrap();
    assert_pt_approx(center, 1.0, 2.0);
    assert_approx_eq(radius, 3.0);
}

#[test]
fn line_is_three_coefficients() {
    let (a, b, c) = parse_line("{1,2,3}").unwrap();
    assert_approx_eq(a, 1.0);
    assert_approx_eq(b, 2.0);
    assert_approx_eq(c, 3.0);
}

#[test]
fn postgis_and_unknown_oids_are_not_geometric() {
    // A PostGIS geometry OID is install-specific and never matches — deferred by design.
    assert_eq!(geometric_field("g", 99999), None);
    assert_eq!(geo_kind(99999), None);
}
