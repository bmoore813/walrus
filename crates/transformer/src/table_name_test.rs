use super::*;

#[test]
fn the_tag_is_free() {
    assert_eq!(
        std::mem::size_of::<DuckTable<Raw>>(),
        std::mem::size_of::<String>(),
        "PhantomData must not add a byte"
    );
    assert_eq!(
        std::mem::size_of::<DuckTable<Mirror>>(),
        std::mem::size_of::<DuckTable<Raw>>()
    );
}

#[test]
fn to_raw_derives_the_suffix_exactly_once() {
    let mirror = DuckTable::<Mirror>::new("t");
    assert_eq!(mirror.as_str(), "t");
    assert_eq!(mirror.to_raw().as_str(), "t_raw");
}
