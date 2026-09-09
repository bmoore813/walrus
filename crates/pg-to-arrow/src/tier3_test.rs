use super::*;
use crate::schema::numeric_precision_scale;

/// numeric(p,s) typmod: `((p << 16) | s) + VARHDRSZ` (VARHDRSZ = 4).
fn numeric_typmod(p: i32, s: i32) -> i32 {
    ((p << 16) | s) + 4
}

#[test]
fn numeric_p_le_38_is_not_tier3() {
    // numeric(10,2) and the p==38 boundary both stay Tier-1 Decimal128.
    assert!(!is_tier3_text(oids::NUMERIC, numeric_typmod(10, 2)));
    assert!(!is_tier3_text(oids::NUMERIC, numeric_typmod(38, 0)));
    assert_eq!(
        numeric_precision_scale(numeric_typmod(38, 0)),
        Some((38, 0))
    );
}

#[test]
fn numeric_unconstrained_is_tier3_varchar() {
    assert!(is_tier3_text(oids::NUMERIC, -1));
    assert_eq!(tier3_field("amount").data_type(), &DataType::Utf8);
}

#[test]
fn numeric_p_gt_38_is_tier3_varchar() {
    // p == 39 crosses the boundary; numeric(40,10) is well past it.
    assert!(is_tier3_text(oids::NUMERIC, numeric_typmod(39, 0)));
    assert!(is_tier3_text(oids::NUMERIC, numeric_typmod(40, 10)));
}

#[test]
fn bit_and_inet_and_pglsn_are_varchar() {
    for oid in [
        oids::BIT,
        oids::VARBIT,
        oids::INET,
        oids::CIDR,
        oids::MACADDR,
        oids::MACADDR8,
        oids::TSVECTOR,
        oids::TSQUERY,
        oids::PG_LSN,
        oids::XID,
        oids::XID8,
        oids::TXID_SNAPSHOT,
        oids::XML,
    ] {
        assert!(is_tier3_text(oid, -1), "oid {oid} should be Tier-3 VARCHAR");
    }
    assert_eq!(tier3_field("x").data_type(), &DataType::Utf8);
    // uuid (2950) has its own native mapping and is not a Tier-3 text carrier.
    assert!(!is_tier3_text(2950, -1));
}
