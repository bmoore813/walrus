use super::*;

fn next_startup_cstring<'a>(message: &'a [u8], cursor: &mut usize) -> &'a str {
    let relative_end = message[*cursor..]
        .iter()
        .position(|byte| *byte == 0)
        .expect("StartupMessage field must be NUL-terminated");
    let end = *cursor + relative_end;
    let value =
        std::str::from_utf8(&message[*cursor..end]).expect("StartupMessage fields must be UTF-8");
    *cursor = end + 1;
    value
}

fn decode_startup_fields(message: &[u8]) -> Vec<(&str, &str)> {
    assert!(message.len() >= 9, "StartupMessage is too short");
    let declared = usize::try_from(u32::from_be_bytes([
        message[0], message[1], message[2], message[3],
    ]))
    .expect("u32 StartupMessage length fits usize");
    assert_eq!(declared, message.len());
    assert_eq!(
        u32::from_be_bytes([message[4], message[5], message[6], message[7]]),
        196_608,
        "protocol 3.0"
    );

    let mut cursor = 8;
    let mut fields = Vec::new();
    while message[cursor] != 0 {
        let key = next_startup_cstring(message, &mut cursor);
        let value = next_startup_cstring(message, &mut cursor);
        fields.push((key, value));
    }
    cursor += 1;
    assert_eq!(cursor, message.len(), "one final NUL terminates the fields");
    fields
}

#[test]
fn startup_message_pins_canonical_postgres_text_output() {
    // Arrange
    const EXPECTED_OPTIONS: &str = "-c DateStyle=ISO,YMD -c IntervalStyle=postgres -c bytea_output=hex \
         -c extra_float_digits=3 -c TimeZone=UTC";

    // Act
    let message = build_startup("walrus_replicator", "source").unwrap();
    let fields = decode_startup_fields(&message);

    // Assert
    assert_eq!(CANONICAL_TEXT_OUTPUT_STARTUP_OPTIONS, EXPECTED_OPTIONS);
    assert_eq!(
        fields,
        vec![
            ("user", "walrus_replicator"),
            ("database", "source"),
            ("replication", "database"),
            ("client_encoding", "UTF8"),
            ("options", EXPECTED_OPTIONS),
        ]
    );
}

#[test]
fn lsn_formats_as_x_slash_y() {
    assert_eq!(lsn_xy(Lsn::ZERO), "0/0");
    assert_eq!(lsn_xy(Lsn::new(0x1_9A2B_3C4D)), "1/9A2B3C4D");
    assert_eq!(lsn_xy(Lsn::new(0x0199_BAC8)), "0/199BAC8");
}

#[test]
fn standby_status_frame_layout() {
    let s = StandbyStatus {
        write: Lsn::new(0x100),
        flush: Lsn::new(0x80),
        apply: Lsn::new(0x40),
        reply_requested: true,
    };
    let msg = build_standby_status(s);
    assert_eq!(msg[0], b'd'); // CopyData
    let len = u32::from_be_bytes([msg[1], msg[2], msg[3], msg[4]]) as usize;
    assert_eq!(len, msg.len() - 1); // length is self-inclusive, excludes the tag
    assert_eq!(msg[5], b'r'); // standby status update
    assert_eq!(read_lsn(&msg[6..14]).unwrap().as_u64(), 0x100); // write
    assert_eq!(read_lsn(&msg[14..22]).unwrap().as_u64(), 0x80); // flush
    assert_eq!(read_lsn(&msg[22..30]).unwrap().as_u64(), 0x40); // apply
    assert_eq!(*msg.last().unwrap(), 1); // reply_requested
}

#[test]
fn take_message_needs_a_full_frame() {
    let mut buf = BytesMut::new();
    buf.extend_from_slice(b"Z"); // tag only
    buf.extend_from_slice(&5u32.to_be_bytes()); // length = 5 (4 + 1 body byte)
    assert!(
        take_message(&mut buf).is_none(),
        "body byte not yet present"
    );
    buf.extend_from_slice(b"I"); // the 1 body byte (idle)
    let (tag, body) = take_message(&mut buf).unwrap();
    assert_eq!(tag, b'Z');
    assert_eq!(&body[..], b"I");
    assert!(buf.is_empty());
}

#[test]
fn error_message_extracts_the_message_field() {
    // Fields: S<severity>\0 C<code>\0 M<message>\0 \0
    let body = b"SERROR\0C42704\0Mno such slot\0\0";
    assert_eq!(error_message(body), "no such slot");
    // The bare NUL closes the field list, so trailing bytes are padding and never a field.
    assert_eq!(error_message(b"SERROR\0\0Mhidden\0"), "(no message)");
    // A frame truncated mid-field still yields the bytes that did arrive.
    assert_eq!(error_message(b"C42704\0Mno such"), "no such");
    // A present but empty message, and a body carrying no fields at all.
    assert_eq!(error_message(b"M\0\0"), "");
    assert_eq!(error_message(b""), "(no message)");
}

#[test]
fn fixed_width_window_preserves_bytes_and_context() {
    assert_eq!(fixed::<4>(&[0, 0, 0, 7], "word").unwrap(), [0, 0, 0, 7]);
    assert_eq!(
        fixed::<4>(&[0, 0, 0], "word").unwrap_err().to_string(),
        "word: expected 4 bytes, got 3"
    );
}

#[test]
fn auth_sub_type_reports_a_truncated_body_instead_of_panicking() {
    assert_eq!(auth_sub_type(&[0, 0, 0, 0]).unwrap(), 0); // AuthenticationOk
    assert_eq!(auth_sub_type(&[0, 0, 0, 10, b'S']).unwrap(), 10); // SASL, mechanisms trail the Int32
    assert_eq!(
        auth_sub_type(&[0, 0, 0]).unwrap_err().to_string(),
        "short Authentication message (3 bytes)"
    );
    assert!(auth_sub_type(&[]).is_err());
}

#[test]
fn advisory_guard_data_row_requires_one_text_boolean() {
    let mut yes = Vec::new();
    yes.extend_from_slice(&1_u16.to_be_bytes());
    yes.extend_from_slice(&1_i32.to_be_bytes());
    yes.push(b't');
    assert!(data_row_bool(&yes).unwrap());

    let mut no = yes.clone();
    *no.last_mut().unwrap() = b'f';
    assert!(!data_row_bool(&no).unwrap());
    assert!(data_row_bool(&[]).is_err());
    assert!(data_row_bool(&[0, 2, 0, 0, 0, 1, b't']).is_err());
}

#[test]
fn parse_dsn_rejects_a_dsn_without_a_tcp_host() {
    let err = parse_dsn("user=walrus dbname=walrus").unwrap_err();
    assert!(err.to_string().contains("needs a TCP host"));
}
