use super::*;

#[test]
fn uuid_field_carries_arrow_uuid_extension() {
    let f = uuid_field("id");
    assert_eq!(f.data_type(), &DataType::FixedSizeBinary(16));
    assert_eq!(
        f.metadata().get("ARROW:extension:name").map(String::as_str),
        Some(ARROW_UUID_EXTENSION)
    );
    assert!(f.is_nullable());
}

#[test]
fn parse_uuid_bytes_roundtrips() {
    let text = "550e8400-e29b-41d4-a716-446655440000";
    let bytes = parse_uuid_bytes(text).unwrap();
    // Standard big-endian byte order: first byte is 0x55, last is 0x00.
    assert_eq!(bytes[0], 0x55);
    assert_eq!(bytes[15], 0x00);
    // Round-trips back to the same canonical text.
    assert_eq!(uuid::Uuid::from_bytes(bytes).to_string(), text);
}

#[test]
fn parse_uuid_bytes_rejects_malformed() {
    assert!(parse_uuid_bytes("not-a-uuid").is_err());
    assert!(parse_uuid_bytes("550e8400").is_err());
}

#[test]
fn enum_is_plain_utf8() {
    assert_eq!(enum_field("status").data_type(), &DataType::Utf8);
    // A user-defined OID looks like an enum; a builtin one does not.
    assert!(is_enum_oid(16400));
    assert!(!is_enum_oid(oids::UUID));
    assert!(!is_enum_oid(oids::TEXT));
}
