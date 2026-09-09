use super::{DecodeError, Message, Reader, StreamCtx, parse_message, parse_tuple};
use common::TupleValue;

const PG_MAX_COLUMNS: u16 = 1_600;
const PG_MAX_INDEX_KEYS: u16 = 32;

fn all_null_tuple(ncols: u16) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(usize::from(ncols) + 2);
    bytes.extend_from_slice(&ncols.to_be_bytes());
    bytes.resize(usize::from(ncols) + 2, b'n');
    bytes
}

#[test]
fn decodes_a_postgres_max_width_tuple_without_a_fixed_capacity_type() {
    let bytes = all_null_tuple(PG_MAX_COLUMNS);
    let mut reader = Reader::new(&bytes);

    let columns = parse_tuple(&mut reader).unwrap();

    assert_eq!(columns.len(), usize::from(PG_MAX_COLUMNS));
    assert!(columns.iter().all(|value| *value == TupleValue::Null));
    assert_eq!(reader.remaining(), 0);
    assert!(columns.capacity() >= usize::from(PG_MAX_COLUMNS));
}

#[test]
fn decodes_wider_than_the_postgres_ceiling_too() {
    let ncols = 2_000;
    let bytes = all_null_tuple(ncols);
    let mut reader = Reader::new(&bytes);

    let columns = parse_tuple(&mut reader).unwrap();

    assert_eq!(columns.len(), usize::from(ncols));
    assert_eq!(reader.remaining(), 0);
}

#[test]
fn relation_decodes_every_postgres_key_flag() {
    let mut bytes = vec![b'R'];
    bytes.extend_from_slice(&42_u32.to_be_bytes());
    bytes.extend_from_slice(b"public\0wide_keys\0");
    bytes.push(b'd');
    bytes.extend_from_slice(&PG_MAX_INDEX_KEYS.to_be_bytes());
    for index in 1..=PG_MAX_INDEX_KEYS {
        bytes.push(1); // pgoutput column flag bit 0: part of the replica identity.
        bytes.extend_from_slice(format!("key_{index:02}\0").as_bytes());
        bytes.extend_from_slice(&23_u32.to_be_bytes());
        bytes.extend_from_slice(&u32::MAX.to_be_bytes());
    }

    let mut reader = Reader::new(&bytes);
    let message = parse_message(&mut reader, &mut StreamCtx::default()).unwrap();
    let Message::Relation { relation, .. } = message else {
        panic!("expected Relation");
    };

    assert_eq!(relation.columns.len(), usize::from(PG_MAX_INDEX_KEYS));
    assert!(relation.columns.iter().all(|column| column.is_key));
    assert_eq!(relation.to_key_columns().first(), Some(&"key_01"));
    assert_eq!(relation.to_key_columns().last(), Some(&"key_32"));
}

#[test]
fn stream_start_rejects_a_non_boolean_first_segment_flag() {
    let bytes = [b'S', 0, 0, 0, 42, 2];

    let error = parse_message(&mut Reader::new(&bytes), &mut StreamCtx::default()).unwrap_err();

    assert_eq!(error, DecodeError::BadStreamStartFlag { byte: 2 });
}
