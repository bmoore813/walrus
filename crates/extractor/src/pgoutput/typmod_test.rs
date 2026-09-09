use super::*;

#[test]
fn atttypmod_sentinel_is_minus_one() {
    assert_eq!(atttypmod(0xFFFF_FFFF), -1);
    assert_eq!(atttypmod(0x000A_0006), 655366);
}

#[test]
fn numeric_unpacks_precision_and_scale() {
    assert_eq!(numeric_precision_scale(655366), Some((10, 2)));
    assert_eq!(numeric_precision_scale(-1), None);
    assert_eq!(numeric_precision_scale(0), None);
}
