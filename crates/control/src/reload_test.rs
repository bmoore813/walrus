use super::*;
use std::str::FromStr;

#[test]
fn reload_enums_round_trip_their_sql_strings() {
    // The strings are the contract with the migration's CHECK constraints AND with the
    // sqlx::Type derive (`rename_all`) — a drift in any of the three is a bug this catches.
    for status in [
        ReloadStatus::Requested,
        ReloadStatus::Exporting,
        ReloadStatus::ExportComplete,
        ReloadStatus::Publishing,
        ReloadStatus::Complete,
        ReloadStatus::Failed,
    ] {
        assert_eq!(ReloadStatus::from_str(status.as_str()), Ok(status));
    }
    assert_eq!(ReloadStatus::ExportComplete.as_str(), "export_complete");

    for flavor in [ReloadFlavor::Reload, ReloadFlavor::Resync] {
        assert_eq!(ReloadFlavor::from_str(flavor.as_str()), Ok(flavor));
    }

    assert!(
        ReloadStatus::from_str("superseded").is_err(),
        "unknown statuses remain rejected"
    );
    assert!(ReloadFlavor::from_str("rebuild").is_err());

    for (scope, encoded) in [
        (ReloadScope::Table, "table"),
        (ReloadScope::AllPublished, "all_published"),
    ] {
        assert_eq!(scope.as_str(), encoded);
        assert_eq!(ReloadScope::from_str(encoded), Ok(scope));
    }

    for (kind, encoded) in [
        (ReloadMarkerKind::Baseline, "baseline"),
        (ReloadMarkerKind::End, "end"),
    ] {
        assert_eq!(kind.as_str(), encoded);
        assert_eq!(ReloadMarkerKind::from_str(encoded), Ok(kind));
    }
}

#[test]
fn rejected_reload_values_keep_the_offending_input_as_data() {
    let status_err = ReloadStatus::from_str("superseded").unwrap_err();
    assert_eq!(status_err.column, "table_reload.status");
    assert_eq!(status_err.input, "superseded");

    let flavor_err = ReloadFlavor::from_str("rebuild").unwrap_err();
    assert_eq!(flavor_err.column, "table_reload.flavor");
    assert_eq!(flavor_err.input, "rebuild");
}

#[test]
fn restart_cap_counts_the_successor_not_the_predecessor() {
    // The next attempt carries restart_count+1, so the cap is measured against THAT.
    assert!(
        restart_would_exceed_cap(0, 0),
        "cap 0 fails the very first mid-export DDL"
    );
    assert!(!restart_would_exceed_cap(0, 3), "first restart is the 1st");
    assert!(
        !restart_would_exceed_cap(2, 3),
        "the 3rd restart still fits"
    );
    assert!(
        restart_would_exceed_cap(3, 3),
        "the 4th would exceed a cap of 3"
    );
}

#[test]
fn restart_cap_holds_at_the_integer_ceiling() {
    // A bare `restart_count + 1` wraps to i32::MIN in release at the ceiling, so a corrupt control
    // row would report even a spent cap as unspent — an unbounded restart loop with no diagnostic.
    assert!(
        restart_would_exceed_cap(i32::MAX, i32::MAX),
        "no i32 cap can hold i32::MAX + 1"
    );
    assert!(restart_would_exceed_cap(i32::MAX, 0));
    // One below the ceiling still answers exactly: the successor is i32::MAX, which fits that cap.
    assert!(!restart_would_exceed_cap(i32::MAX - 1, i32::MAX));
}

#[test]
fn durable_export_plan_requires_one_exact_physical_partition() {
    let full = [ExportRangePlan {
        range_no: 0,
        full_scan: true,
        start_block: None,
        end_block: None,
    }];
    assert!(valid_export_ranges(&full));

    let heap = [
        ExportRangePlan {
            range_no: 0,
            full_scan: false,
            start_block: Some(0),
            end_block: Some(10),
        },
        ExportRangePlan {
            range_no: 1,
            full_scan: false,
            start_block: Some(10),
            end_block: None,
        },
    ];
    assert!(valid_export_ranges(&heap));

    let mut wrong_ordinal = heap;
    wrong_ordinal[1].range_no = 2;
    assert!(!valid_export_ranges(&wrong_ordinal));

    let mut gap = heap;
    gap[1].start_block = Some(11);
    assert!(!valid_export_ranges(&gap));

    let two_full = [
        full[0],
        ExportRangePlan {
            range_no: 1,
            ..full[0]
        },
    ];
    assert!(!valid_export_ranges(&two_full));
}

#[test]
fn reload_publication_claim_and_drain_share_the_strict_h_barrier() {
    let claim = include_str!("../sql/postgres/queries/claim_publication_ready.sql");
    let drain = include_str!("../sql/postgres/queries/publication_pending_through.sql");

    assert!(
        claim.contains("m.lsn_end <= p.final_lsn"),
        "claiming must exclude a transaction whose commit group lands after H"
    );
    assert!(
        drain.contains("m.lsn_end <= r.final_lsn"),
        "publication drain must ignore (and therefore leave queued) post-H commit groups"
    );
    assert!(
        claim.contains("m.stream_group_id IS NULL OR g.status = 'ready'"),
        "a protocol-v2 group cannot be selected until its atomic group receipt is ready"
    );
    assert!(
        claim.contains("GROUP BY stream_group_id"),
        "protocol-v2 children must be selected as one indivisible commit unit"
    );
}
