use super::*;

#[test]
fn every_domain_id_is_layout_identical_to_i64() {
    assert_eq!(
        std::mem::size_of::<ManifestId>(),
        std::mem::size_of::<i64>()
    );
    assert_eq!(
        std::mem::align_of::<ManifestId>(),
        std::mem::align_of::<i64>()
    );
    assert_eq!(std::mem::size_of::<EpochNo>(), std::mem::size_of::<i64>());
    assert_eq!(std::mem::align_of::<EpochNo>(), std::mem::align_of::<i64>());
    assert_eq!(
        std::mem::size_of::<SchemaVersionNo>(),
        std::mem::size_of::<i64>()
    );
    assert_eq!(
        std::mem::align_of::<SchemaVersionNo>(),
        std::mem::align_of::<i64>()
    );
    assert_eq!(std::mem::size_of::<ReloadId>(), std::mem::size_of::<i64>());
    assert_eq!(
        std::mem::align_of::<ReloadId>(),
        std::mem::align_of::<i64>()
    );
    assert_eq!(std::mem::size_of::<DdlId>(), std::mem::size_of::<i64>());
    assert_eq!(std::mem::align_of::<DdlId>(), std::mem::align_of::<i64>());
}

#[test]
fn display_is_the_inner_integer() {
    assert_eq!(ManifestId(42).to_string(), "42");
    assert_eq!(format!("{}", ManifestId(-1)), "-1");
}

/// `Display` hands the formatter to the inner `i64` rather than re-`write!`ing it, so width, fill,
/// and sign flags survive the newtype instead of being silently dropped.
#[test]
fn display_forwards_formatter_flags_to_the_inner_i64() {
    assert_eq!(format!("{:>6}", ManifestId(42)), "    42");
    assert_eq!(format!("{:06}", EpochNo(42)), "000042");
    assert_eq!(format!("{:+}", SchemaVersionNo(42)), "+42");
    assert_eq!(format!("{:<6}|", ReloadId(42)), "42    |");
    assert_eq!(format!("{:^6}|", DdlId(42)), "  42  |");
}

/// Wrapping a bare `i64` must not cost the `{:x}` / `{:X}` specifiers it already supported.
#[test]
fn hex_specifiers_forward_to_the_inner_i64() {
    assert_eq!(format!("{:x}", ManifestId(0xDEAD)), "dead");
    assert_eq!(format!("{:X}", ManifestId(0xDEAD)), "DEAD");
    assert_eq!(format!("{:#06x}", EpochNo(0x2B)), "0x002b");
    assert_eq!(format!("{:X}", SchemaVersionNo(255)), "FF");
    assert_eq!(format!("{:x}", ReloadId(255)), "ff");
    assert_eq!(format!("{:X}", DdlId(1_048_575)), "FFFFF");
}

/// Hex of a negative id is the inner integer's two's-complement pattern — the bare `i64` behaviour,
/// documented here so it is a recorded consequence of forwarding rather than a surprise.
#[test]
fn hex_of_a_negative_id_is_the_inner_two_s_complement() {
    assert_eq!(format!("{:x}", ManifestId(-1)), "ffffffffffffffff");
    assert_eq!(format!("{:X}", DdlId(-1)), "FFFFFFFFFFFFFFFF");
}

#[test]
fn from_i64_and_back_round_trips() {
    let id = ManifestId::from(7);
    assert_eq!(id, ManifestId(7));
    assert_eq!(i64::from(id), 7);
}

#[test]
fn ordering_matches_the_inner_integer() {
    assert!(ManifestId(1) < ManifestId(2));
    let mut v = [ManifestId(3), ManifestId(1), ManifestId(2)];
    v.sort();
    assert_eq!(v, [ManifestId(1), ManifestId(2), ManifestId(3)]);
}

#[test]
fn epoch_from_i64_and_back_round_trips() {
    let epoch = EpochNo::from(7);
    assert_eq!(epoch, EpochNo(7));
    assert_eq!(i64::from(epoch), 7);
}

#[test]
fn epoch_ordering_matches_the_inner_integer() {
    assert!(EpochNo(1) < EpochNo(2));
    let mut epochs = [EpochNo(3), EpochNo(1), EpochNo(2)];
    epochs.sort();
    assert_eq!(epochs, [EpochNo(1), EpochNo(2), EpochNo(3)]);
}

#[test]
fn epoch_serializes_as_a_bare_number() {
    assert_eq!(serde_json::to_string(&EpochNo(42)).unwrap(), "42");
}

#[test]
fn schema_version_from_i64_and_back_round_trips() {
    let version = SchemaVersionNo::from(7);
    assert_eq!(version, SchemaVersionNo(7));
    assert_eq!(version.to_string(), "7");
    assert_eq!(i64::from(version), 7);
}

#[test]
fn schema_version_ordering_matches_the_inner_integer() {
    assert!(SchemaVersionNo(1) < SchemaVersionNo(2));
    let mut versions = [SchemaVersionNo(3), SchemaVersionNo(1), SchemaVersionNo(2)];
    versions.sort();
    assert_eq!(
        versions,
        [SchemaVersionNo(1), SchemaVersionNo(2), SchemaVersionNo(3)]
    );
}

#[test]
fn schema_version_serializes_as_a_bare_number() {
    assert_eq!(serde_json::to_string(&SchemaVersionNo(42)).unwrap(), "42");
    assert_eq!(
        serde_json::from_str::<SchemaVersionNo>("42").unwrap(),
        SchemaVersionNo(42)
    );
}

#[test]
fn reload_id_from_i64_and_back_round_trips() {
    let reload_id = ReloadId::from(11);
    assert_eq!(reload_id, ReloadId(11));
    assert_eq!(reload_id.to_string(), "11");
    assert_eq!(i64::from(reload_id), 11);
}

/// `ReloadId` is the one id decoded from *text* (a `reload_signal` pgoutput tuple), so its parse
/// must be exactly the inverse of the `Display` above — no bespoke dialect, no silent trimming.
#[test]
fn reload_id_parses_back_from_its_display_form() {
    assert_eq!("11".parse::<ReloadId>().unwrap(), ReloadId(11));
    assert_eq!("-1".parse::<ReloadId>().unwrap(), ReloadId(-1));
    for id in [ReloadId(0), ReloadId(990_042), ReloadId(i64::MIN)] {
        assert_eq!(id.to_string().parse::<ReloadId>().unwrap(), id);
    }
}

/// A malformed signal column must reject rather than land some default id on a waiter key, so the
/// rejected shapes are pinned as `i64`'s exactly — including the range edge a `bigserial` can reach.
#[test]
fn reload_id_rejects_everything_the_inner_integer_rejects() {
    assert!("".parse::<ReloadId>().is_err());
    assert!("nine".parse::<ReloadId>().is_err());
    assert!(" 7".parse::<ReloadId>().is_err());
    assert!("7 ".parse::<ReloadId>().is_err());
    assert!("7.0".parse::<ReloadId>().is_err());
    assert!("9223372036854775808".parse::<ReloadId>().is_err()); // i64::MAX + 1
}

#[test]
fn reload_id_ordering_matches_the_inner_integer() {
    assert!(ReloadId(1) < ReloadId(2));
    let mut ids = [ReloadId(3), ReloadId(1), ReloadId(2)];
    ids.sort();
    assert_eq!(ids, [ReloadId(1), ReloadId(2), ReloadId(3)]);
}

#[test]
fn ddl_id_from_i64_and_back_round_trips() {
    let ddl_id = DdlId::from(13);
    assert_eq!(ddl_id, DdlId(13));
    assert_eq!(ddl_id.to_string(), "13");
    assert_eq!(i64::from(ddl_id), 13);
}

/// `(c_lsn, id)` is the DDL history's order, so `DdlId`'s own ordering must be the integer's.
#[test]
fn ddl_id_ordering_matches_the_inner_integer() {
    assert!(DdlId(1) < DdlId(2));
    let mut ids = [DdlId(3), DdlId(1), DdlId(2)];
    ids.sort();
    assert_eq!(ids, [DdlId(1), DdlId(2), DdlId(3)]);
}
