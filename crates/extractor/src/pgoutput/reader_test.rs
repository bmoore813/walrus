use super::*;

#[test]
fn slice_borrows_without_copying_and_advances() {
    let input = b"abcdef";
    let mut reader = Reader::new(input);

    let borrowed = reader.slice(3).unwrap();

    assert_eq!(borrowed, b"abc");
    assert_eq!(borrowed.as_ptr(), input.as_ptr());
    assert_eq!(reader.remaining(), 3);
}

#[test]
fn str_rejects_invalid_utf8_as_a_decode_error() {
    let mut reader = Reader::new(&[0xff]);

    assert!(matches!(reader.str(1), Err(DecodeError::Utf8(_))));
}

#[test]
fn slice_past_the_end_is_unexpected_eof() {
    let mut reader = Reader::new(b"ab");
    assert_eq!(reader.byte1().unwrap(), b'a');

    assert!(matches!(
        reader.slice(3),
        Err(DecodeError::UnexpectedEof {
            needed: 3,
            offset: 1,
            remaining: 1,
        })
    ));
}

#[test]
fn fixed_width_borrows_and_advances() {
    let input = [0x12, 0x34, 0x56];
    let mut reader = Reader::new(&input);
    let fixed = reader.fixed::<2>().unwrap();

    assert_eq!(fixed, &[0x12, 0x34]);
    assert_eq!(fixed.as_ptr(), input.as_ptr());
    assert_eq!(reader.remaining(), 1);
}

#[test]
fn byte1_at_the_end_is_a_one_byte_unexpected_eof() {
    let mut reader = Reader::new(b"a");
    assert_eq!(reader.byte1().unwrap(), b'a');

    // `byte1` reads its width through `fixed::<1>`, so the payload is the same shape the wider
    // readers report — `needed` is the const width, not a runtime length.
    assert!(matches!(
        reader.byte1(),
        Err(DecodeError::UnexpectedEof {
            needed: 1,
            offset: 1,
            remaining: 0,
        })
    ));
    assert_eq!(reader.remaining(), 0);
}

#[test]
fn fixed_width_short_read_preserves_cursor_and_payload() {
    let mut reader = Reader::new(&[0xaa, 0xbb]);

    assert!(matches!(
        reader.fixed::<4>(),
        Err(DecodeError::UnexpectedEof {
            needed: 4,
            offset: 0,
            remaining: 2,
        })
    ));
    assert_eq!(reader.remaining(), 2);
}
