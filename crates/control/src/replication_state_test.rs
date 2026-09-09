use super::*;

// `replication_state.status` has no SQL CHECK, so this table is the whole contract: the extractor writes
// `as_str()` and every reader parses it back through `FromStr`. Pin every variant in both directions
// plus the reject case, exactly as the CHECK-backed manifest enums are pinned.

#[test]
fn replication_status_round_trips_every_variant() {
    for (status, text) in [
        (ReplicationStatus::Bootstrapping, "bootstrapping"),
        (ReplicationStatus::Streaming, "streaming"),
        (ReplicationStatus::TotalRestart, "total_restart"),
    ] {
        assert_eq!(status.as_str(), text);
        assert_eq!(text.parse::<ReplicationStatus>(), Ok(status));
    }
}

#[test]
fn a_rejected_status_keeps_the_offending_input_as_data() {
    let err = "restarting".parse::<ReplicationStatus>().unwrap_err();

    assert_eq!(err.column, "replication_state.status");
    assert_eq!(err.input, "restarting");
}

#[test]
fn the_variant_spelling_is_not_the_persisted_spelling() {
    // `total_restart` is snake_case in the column and PascalCase in Rust; only the column form
    // parses, so a hand-written literal cannot drift into the DB unnoticed.
    let err = "TotalRestart".parse::<ReplicationStatus>().unwrap_err();

    assert_eq!(err.column, "replication_state.status");
    assert_eq!(err.input, "TotalRestart");
}
