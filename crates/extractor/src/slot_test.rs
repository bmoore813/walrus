use super::*;

/// The failure keeps its typed cause instead of flattening it into the message: `{:#}` renders both
/// layers and `downcast_ref` still reaches the `LsnParseError` that an `anyhow!("…: {e:?}")` message
/// would have thrown away.
#[test]
fn a_malformed_lsn_keeps_its_typed_cause_under_the_field_context() {
    let err = parse_lsn("nonsense", "restart_lsn").expect_err("nonsense is not an LSN");
    assert_eq!(err.to_string(), "parse restart_lsn as a Postgres LSN");
    let chain = format!("{err:#}");
    assert!(
        chain.starts_with("parse restart_lsn as a Postgres LSN: invalid LSN \"nonsense\""),
        "the field context must lead the preserved cause: {chain}"
    );
    let cause = err
        .downcast_ref::<common::lsn::LsnParseError>()
        .expect("the typed parse failure must survive the context layer");
    assert_eq!(cause.input, "nonsense");
}

/// The label is what tells an operator *which* catalog column held the bad text — both columns are
/// read from the same row, so "invalid LSN" alone would not say.
#[test]
fn the_field_label_names_the_offending_column() {
    let err = parse_lsn("", "confirmed_flush_lsn").expect_err("the empty string is not an LSN");
    assert_eq!(
        err.to_string(),
        "parse confirmed_flush_lsn as a Postgres LSN"
    );
}

#[test]
fn a_canonical_lsn_still_parses() {
    let lsn = parse_lsn("0/199BAC8", "consistent_point").expect("a canonical X/Y LSN parses");
    assert_eq!(lsn, Lsn::new(0x0199_BAC8));
}

#[test]
fn invalidated_slot_replacement_can_only_drop_a_catalog_lost_slot() {
    assert!(DROP_INVALIDATED_SLOT_SQL.contains("wal_status = 'lost'"));
    assert!(DROP_INVALIDATED_SLOT_SQL.contains("slot_name = $1"));
}
