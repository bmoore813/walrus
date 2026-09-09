use super::*;

#[test]
fn utc_timestamp_is_layout_identical_to_jiff_timestamp() {
    assert_eq!(
        std::mem::size_of::<UtcTimestamp>(),
        std::mem::size_of::<jiff::Timestamp>()
    );
    assert_eq!(
        std::mem::align_of::<UtcTimestamp>(),
        std::mem::align_of::<jiff::Timestamp>()
    );
}

#[test]
fn utc_timestamp_conversions_round_trip() {
    let timestamp: jiff::Timestamp = "2026-07-04T12:00:00.123Z".parse().unwrap();
    let wrapped = UtcTimestamp::from(timestamp);

    // The wrapper is `Copy`, so the one arranged value feeds all three projections.
    assert_eq!(wrapped.as_inner(), &timestamp);
    assert_eq!(wrapped.into_inner(), timestamp);
    assert_eq!(jiff::Timestamp::from(wrapped), timestamp);
}

#[test]
fn display_and_from_str_round_trip() {
    let timestamp = "2026-07-04T12:00:00.123Z".parse::<UtcTimestamp>().unwrap();

    assert_eq!(timestamp.to_string().parse::<UtcTimestamp>(), Ok(timestamp));
    assert_eq!(
        serde_json::to_string(&timestamp).unwrap(),
        format!("\"{timestamp}\"")
    );
}

/// The architecture.md §1.4 example block, comment-free (a real JSON document).
const DOCS_EXAMPLE: &str = r#"{
        "op": "u",
        "lsn": "00000000019A2B3C",
        "commit_lsn": "0000000001B4C000",
        "commit_ts": "2026-07-04T12:00:00Z",
        "xid": 918273,
        "epoch": 7,
        "batch_id": "3f2a0000-0000-0000-0000-000000000001",
        "schema_version": 12,
        "source_schema": "public",
        "source_table": "orders",
        "kind": "stream",
        "unchanged_toast": ["blob_col"],
        "extractor_instance": "walrus-extractor-0",
        "extractor_processed_at": "2026-07-04T12:00:00.123Z"
    }"#;

/// The architecture.md §1.4 example without `unchanged_toast`, as emitted by an older extractor.
const DOCS_EXAMPLE_NO_TOAST: &str = r#"{
        "op": "u",
        "lsn": "00000000019A2B3C",
        "commit_lsn": "0000000001B4C000",
        "commit_ts": "2026-07-04T12:00:00Z",
        "xid": 918273,
        "epoch": 7,
        "batch_id": "3f2a0000-0000-0000-0000-000000000001",
        "schema_version": 12,
        "source_schema": "public",
        "source_table": "orders",
        "kind": "stream",
        "extractor_instance": "walrus-extractor-0",
        "extractor_processed_at": "2026-07-04T12:00:00.123Z"
    }"#;

#[test]
fn op_serializes_as_single_char() {
    assert_eq!(serde_json::to_string(&Op::Insert).unwrap(), "\"i\"");
    assert_eq!(serde_json::to_string(&Op::Update).unwrap(), "\"u\"");
    assert_eq!(serde_json::to_string(&Op::Delete).unwrap(), "\"d\"");
    assert_eq!(serde_json::to_string(&Op::Truncate).unwrap(), "\"t\"");
    assert_eq!(serde_json::from_str::<Op>("\"d\"").unwrap(), Op::Delete);
}

#[test]
fn kind_serializes_lowercase() {
    assert_eq!(
        serde_json::to_string(&Kind::Snapshot).unwrap(),
        "\"snapshot\""
    );
    assert_eq!(serde_json::to_string(&Kind::Stream).unwrap(), "\"stream\"");
}

#[test]
fn meta_round_trips_exact_keys() {
    let meta: ExtractorMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    assert_eq!(meta.op, Op::Update);
    assert_eq!(meta.kind, Kind::Stream);
    assert_eq!(meta.epoch, EpochNo(7));
    assert_eq!(meta.xid, 918273);
    assert_eq!(meta.unchanged_toast.as_ref(), ["blob_col"]);

    // Re-serialize and confirm every key/value matches the docs block (order-independent).
    let reserialized: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
    let expected: serde_json::Value = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    assert_eq!(reserialized, expected);

    // And the round-trip is the identity on the struct itself.
    let again: ExtractorMeta = serde_json::from_value(reserialized).unwrap();
    assert_eq!(again, meta);
}

#[test]
fn meta_without_unchanged_toast_defaults_to_empty() {
    let meta: ExtractorMeta = serde_json::from_str(DOCS_EXAMPLE_NO_TOAST).unwrap();

    assert!(meta.unchanged_toast.is_empty());
    assert_eq!(meta.commit_lsn, Lsn::new(0x1B4C000));
    assert_eq!(meta.op, Op::Update);
}

#[test]
fn empty_unchanged_toast_is_omitted_from_the_wire() {
    let base: ExtractorMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    let empty = ExtractorMeta {
        unchanged_toast: Box::default(),
        ..base.clone()
    };

    let json = serde_json::to_string(&empty).unwrap();
    let document: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert!(
        document.get("unchanged_toast").is_none(),
        "empty unchanged_toast must be omitted, got {json}"
    );

    let round_trip: ExtractorMeta = serde_json::from_str(&json).unwrap();
    assert_eq!(round_trip, empty);

    let non_empty = serde_json::to_string(&base).unwrap();
    assert!(non_empty.contains("\"unchanged_toast\":[\"blob_col\"]"));
}

#[test]
fn a_missing_identity_field_is_still_a_hard_error() {
    let without_commit_lsn = DOCS_EXAMPLE.replace("\"commit_lsn\"", "\"_commit_lsn\"");
    let error = serde_json::from_str::<ExtractorMeta>(&without_commit_lsn).unwrap_err();

    assert!(
        error.to_string().contains("missing field `commit_lsn`"),
        "missing commit_lsn must remain a hard error: {error}"
    );
}

#[test]
fn op_and_lsn_keys_serialize_as_documented() {
    let meta: ExtractorMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
    assert_eq!(v["op"], "u");
    // Lsn fields render through the shared zero-padded 16-hex newtype.
    assert_eq!(v["lsn"], "00000000019A2B3C");
    assert_eq!(v["commit_lsn"], "0000000001B4C000");
}

#[test]
fn timestamps_always_render_with_z_suffix() {
    let meta: ExtractorMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    let v: serde_json::Value =
        serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
    assert_eq!(v["commit_ts"], "2026-07-04T12:00:00Z");
    assert_eq!(v["extractor_processed_at"], "2026-07-04T12:00:00.123Z");
    assert!(v["commit_ts"].as_str().unwrap().ends_with('Z'));
    assert!(v["extractor_processed_at"].as_str().unwrap().ends_with('Z'));

    // `now()` also renders with a Z suffix.
    assert!(
        serde_json::to_string(&UtcTimestamp::now())
            .unwrap()
            .ends_with("Z\"")
    );
}

#[test]
fn non_utc_timestamp_is_rejected() {
    // A numeric offset is refused rather than silently converted to UTC.
    assert_eq!(
        "2026-07-04T12:00:00+02:00".parse::<UtcTimestamp>(),
        Err(TimestampParseError::NotUtcZ {
            input: "2026-07-04T12:00:00+02:00".to_string(),
        })
    );
    assert!("2026-07-04T12:00:00-05:00".parse::<UtcTimestamp>().is_err());
    assert_eq!(
        "not a timestamp".parse::<UtcTimestamp>(),
        Err(TimestampParseError::NotUtcZ {
            input: "not a timestamp".to_string(),
        })
    );
    assert!(matches!(
        "not a timestampZ".parse::<UtcTimestamp>(),
        Err(TimestampParseError::Malformed { input, .. }) if input == "not a timestampZ"
    ));
    // The UTC `Z` form is accepted.
    assert!("2026-07-04T12:00:00Z".parse::<UtcTimestamp>().is_ok());
}

#[test]
fn timestamp_parse_error_keeps_common_error_wording() {
    let parse_error = "2026-07-04T12:00:00+02:00"
        .parse::<UtcTimestamp>()
        .unwrap_err();
    let parse_message = parse_error.to_string();
    let error = Error::from(parse_error);

    assert_eq!(
        error.to_string(),
        format!("internal error: {parse_message}")
    );
}

#[test]
fn deserializes_the_docs_example_block() {
    // The whole §1.4 block parses into a fully-populated ExtractorMeta.
    let meta: ExtractorMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    assert_eq!(meta.lsn, Lsn::new(0x19A2B3C));
    assert_eq!(meta.commit_lsn, Lsn::new(0x1B4C000));
    assert_eq!(meta.source_schema, "public");
    assert_eq!(meta.source_table, "orders");
    assert_eq!(meta.batch_id, "3f2a0000-0000-0000-0000-000000000001");
    assert_eq!(meta.schema_version, SchemaVersionNo(12));
    assert_eq!(meta.extractor_instance, "walrus-extractor-0");
}

#[test]
fn amortized_meta_matches_full() {
    // The amortized `{const,row}` splice must be byte-equivalent (key order aside) to
    // `serde_json::to_string(ExtractorMeta)` — with AND without unchanged-TOAST columns.
    let base: ExtractorMeta = serde_json::from_str(DOCS_EXAMPLE).unwrap();
    for toast in [vec!["blob_col".to_string()], Vec::new()] {
        let meta = ExtractorMeta {
            unchanged_toast: toast.into_boxed_slice(),
            ..base.clone()
        };
        let mut buf = String::from("{");
        buf.push_str(&meta.to_const_json_inner().unwrap());
        buf.push(',');
        meta.write_row_json_inner(&mut buf).unwrap();
        buf.push('}');

        let amortized: serde_json::Value = serde_json::from_str(&buf).unwrap();
        let full: serde_json::Value =
            serde_json::from_str(&serde_json::to_string(&meta).unwrap()).unwrap();
        assert_eq!(
            amortized, full,
            "amortized meta ≠ full for unchanged_toast={:?}",
            meta.unchanged_toast
        );
    }
}

#[test]
fn pg_epoch_zero_is_y2k() {
    // pgoutput µs=0 is the Postgres epoch, 2000-01-01T00:00:00Z — NOT the Unix epoch.
    let ts = UtcTimestamp::from_pg_micros(0).unwrap();
    assert_eq!(
        serde_json::to_string(&ts).unwrap(),
        "\"2000-01-01T00:00:00Z\""
    );
}

#[test]
fn negative_micros_pre_y2k() {
    // One second before the Postgres epoch.
    let ts = UtcTimestamp::from_pg_micros(-1_000_000).unwrap();
    assert_eq!(
        serde_json::to_string(&ts).unwrap(),
        "\"1999-12-31T23:59:59Z\""
    );
}

#[test]
fn round_trips_a_known_commit_ts() {
    // The µs the extractor would receive for a real commit time, reconstructed back to the same instant.
    let want = "2026-07-04T12:00:00.123Z".parse::<UtcTimestamp>().unwrap();
    let pg_micros = want.0.as_microsecond() - PG_EPOCH_UNIX_MICROS;
    assert_eq!(UtcTimestamp::from_pg_micros(pg_micros).unwrap(), want);
}

#[test]
fn overflow_is_an_error_not_a_panic() {
    assert!(UtcTimestamp::from_pg_micros(i64::MAX).is_err());
}
