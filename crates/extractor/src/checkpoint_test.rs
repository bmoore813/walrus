use super::*;

#[test]
fn advances_confirmed_flush_only_forward() {
    let mut cp = DurabilityCheckpoint::new("0/100".parse().unwrap());
    cp.observe_commit("0/150".parse().unwrap(), "0/160".parse().unwrap())
        .unwrap();
    cp.observe_commit("0/200".parse().unwrap(), "0/210".parse().unwrap())
        .unwrap();
    cp.on_commit_durable("0/200".parse().unwrap()).unwrap();
    assert_eq!(cp.confirmed_flush(), "0/210".parse().unwrap());
    // A delayed lower/older durable notification never regresses the confirmed LSN.
    cp.on_commit_durable("0/150".parse().unwrap()).unwrap();
    assert_eq!(cp.confirmed_flush(), "0/210".parse().unwrap());
    assert_eq!(
        cp.observe_commit("0/150".parse().unwrap(), "0/160".parse().unwrap()),
        Err(CheckpointError::CommitBehindDurableBoundary {
            commit_lsn: "0/150".parse().unwrap(),
            durable_commit_lsn: "0/200".parse().unwrap(),
        }),
        "decoding itself may not move behind the pruned durable boundary"
    );
}

#[test]
fn stream_start_itself_is_never_acknowledged() {
    let mut cp = DurabilityCheckpoint::new("0/100".parse().unwrap());
    cp.observe_commit("0/500".parse().unwrap(), "0/510".parse().unwrap())
        .unwrap();
    cp.on_commit_durable("0/500".parse().unwrap()).unwrap();
    let before_start = cp.capture_pre_stream_start_ceiling();

    cp.on_stream_start(7, before_start).unwrap();
    // Model durable work through and beyond a StreamStart at 0/600. Feedback stays at the position
    // captured before the start, rather than acknowledging the StreamStart record itself.
    cp.observe_commit("0/900".parse().unwrap(), "0/910".parse().unwrap())
        .unwrap();
    cp.on_commit_durable("0/900".parse().unwrap()).unwrap();
    assert_eq!(cp.confirmed_flush(), "0/510".parse().unwrap());

    // Commit/whole abort removes the fence and exposes the remembered durable high-water mark.
    assert!(cp.on_stream_end(7).unwrap());
    assert_eq!(cp.confirmed_flush(), "0/910".parse().unwrap());
}

#[test]
fn interleaved_streams_keep_every_pre_start_ceiling() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    cp.observe_commit("0/80".parse().unwrap(), "0/88".parse().unwrap())
        .unwrap();
    cp.on_commit_durable("0/80".parse().unwrap()).unwrap();
    let first_ceiling = cp.capture_pre_stream_start_ceiling();
    cp.on_stream_start(100, first_ceiling).unwrap();
    cp.observe_commit("0/500".parse().unwrap(), "0/510".parse().unwrap())
        .unwrap();
    cp.on_commit_durable("0/500".parse().unwrap()).unwrap();

    // A second top-level transaction starts while the first one holds feedback. Its safe captured
    // ceiling is independently retained, so ending xid 100 cannot accidentally release xid 200.
    let second_ceiling = cp.capture_pre_stream_start_ceiling();
    cp.on_stream_start(200, second_ceiling).unwrap();
    assert!(!cp.on_stream_end(100).unwrap());
    assert_eq!(cp.confirmed_flush(), "0/88".parse().unwrap());
    assert!(cp.on_stream_end(200).unwrap());
    assert_eq!(cp.confirmed_flush(), "0/510".parse().unwrap());
}

#[test]
fn checkpoint_rejects_stream_state_drift_without_mutating_the_fence() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    let ceiling = cp.capture_pre_stream_start_ceiling();
    cp.on_stream_start(100, ceiling).unwrap();
    assert_eq!(
        cp.on_stream_start(100, "0/20".parse().unwrap()),
        Err(CheckpointError::AlreadyOpen { top_xid: 100 })
    );
    cp.observe_commit("0/500".parse().unwrap(), "0/510".parse().unwrap())
        .unwrap();
    cp.on_commit_durable("0/500".parse().unwrap()).unwrap();
    assert_eq!(cp.confirmed_flush(), "0/10".parse().unwrap());
    assert_eq!(
        cp.on_stream_end(200),
        Err(CheckpointError::NotOpen { top_xid: 200 })
    );
    assert_eq!(cp.confirmed_flush(), "0/10".parse().unwrap());
}

#[test]
fn standby_status_carries_two_distinct_lsns() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    cp.observe_commit("0/40".parse().unwrap(), "0/48".parse().unwrap())
        .unwrap();
    cp.on_commit_durable("0/40".parse().unwrap()).unwrap();
    // write (received/keepalive) is ahead of flush (confirmed_flush) during a stall.
    let s = cp.standby_status("0/900".parse().unwrap(), false);
    assert_eq!(
        s.write,
        "0/900".parse().unwrap(),
        "received advances unconditionally"
    );
    assert_eq!(
        s.flush,
        "0/48".parse().unwrap(),
        "flush uses the durable commit's end LSN"
    );
    assert_eq!(s.apply, s.flush);
}

#[test]
fn durable_commit_without_observed_end_lsn_fails_closed() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    cp.observe_commit("0/20".parse().unwrap(), "0/28".parse().unwrap())
        .unwrap();
    assert_eq!(
        cp.confirmed_flush(),
        "0/10".parse().unwrap(),
        "merely decoding a commit boundary must not acknowledge it"
    );
    assert_eq!(
        cp.on_commit_durable("0/40".parse().unwrap()),
        Err(CheckpointError::MissingCommitBoundary {
            commit_lsn: "0/40".parse().unwrap(),
        })
    );
    assert_eq!(cp.confirmed_flush(), "0/10".parse().unwrap());
}

#[test]
fn commit_boundary_requires_a_strictly_later_stable_end_lsn() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    assert_eq!(
        cp.observe_commit("0/40".parse().unwrap(), "0/40".parse().unwrap()),
        Err(CheckpointError::InvalidCommitBoundary {
            commit_lsn: "0/40".parse().unwrap(),
            end_lsn: "0/40".parse().unwrap(),
        })
    );
    cp.observe_commit("0/40".parse().unwrap(), "0/48".parse().unwrap())
        .unwrap();
    cp.observe_commit("0/40".parse().unwrap(), "0/48".parse().unwrap())
        .unwrap();
    assert_eq!(
        cp.observe_commit("0/40".parse().unwrap(), "0/50".parse().unwrap()),
        Err(CheckpointError::ConflictingCommitBoundary {
            commit_lsn: "0/40".parse().unwrap(),
            observed_end_lsn: "0/48".parse().unwrap(),
            incoming_end_lsn: "0/50".parse().unwrap(),
        })
    );
    cp.on_commit_durable("0/40".parse().unwrap()).unwrap();
    assert_eq!(cp.confirmed_flush(), "0/48".parse().unwrap());
    cp.observe_commit("0/40".parse().unwrap(), "0/48".parse().unwrap())
        .unwrap();
    assert_eq!(
        cp.observe_commit("0/40".parse().unwrap(), "0/50".parse().unwrap()),
        Err(CheckpointError::ConflictingCommitBoundary {
            commit_lsn: "0/40".parse().unwrap(),
            observed_end_lsn: "0/48".parse().unwrap(),
            incoming_end_lsn: "0/50".parse().unwrap(),
        }),
        "the durable boundary remains verifiable after its pending map entry was pruned"
    );
}

#[test]
fn exact_commit_lsn_resume_is_repaired_to_its_end_lsn() {
    let commit_lsn = "0/40".parse().unwrap();
    let end_lsn = "0/48".parse().unwrap();
    let mut cp = DurabilityCheckpoint::new(commit_lsn);

    // This is the upgrade/lost-ACK case left by the old checkpoint: replaying the exact boundary
    // observes the missing end position and can move the slot beyond the complete record.
    cp.observe_commit(commit_lsn, end_lsn).unwrap();
    cp.on_commit_durable(commit_lsn).unwrap();
    assert_eq!(cp.confirmed_flush(), end_lsn);
}

#[test]
fn later_commit_mapping_survives_an_earlier_delayed_flush() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    cp.observe_commit("0/40".parse().unwrap(), "0/48".parse().unwrap())
        .unwrap();
    cp.observe_commit("0/60".parse().unwrap(), "0/68".parse().unwrap())
        .unwrap();

    cp.on_commit_durable("0/40".parse().unwrap()).unwrap();
    assert_eq!(cp.confirmed_flush(), "0/48".parse().unwrap());
    cp.on_commit_durable("0/60".parse().unwrap()).unwrap();
    assert_eq!(cp.confirmed_flush(), "0/68".parse().unwrap());
}

#[test]
fn durable_frontier_prunes_consumed_mappings_and_rejects_older_replay() {
    let mut cp = DurabilityCheckpoint::new("0/10".parse().unwrap());
    for (commit, end) in [(0x40, 0x48), (0x60, 0x68), (0x80, 0x88)] {
        cp.observe_commit(Lsn::new(commit), Lsn::new(end)).unwrap();
    }
    assert_eq!(cp.commit_end_lsn.len(), 3);

    cp.on_commit_durable("0/60".parse().unwrap()).unwrap();
    assert_eq!(
        cp.commit_end_lsn,
        [("0/80".parse().unwrap(), "0/88".parse().unwrap())]
            .into_iter()
            .collect(),
        "only mappings above the durable commit-order frontier stay resident"
    );

    assert_eq!(
        cp.observe_commit("0/40".parse().unwrap(), "0/48".parse().unwrap()),
        Err(CheckpointError::CommitBehindDurableBoundary {
            commit_lsn: "0/40".parse().unwrap(),
            durable_commit_lsn: "0/60".parse().unwrap(),
        }),
        "decoding must fail closed if it regresses below a boundary already made durable"
    );
    cp.observe_commit("0/60".parse().unwrap(), "0/68".parse().unwrap())
        .unwrap();
    assert_eq!(
        cp.commit_end_lsn.len(),
        1,
        "an exact replay of the durable boundary is verified without being retained"
    );
}

#[test]
fn overlapping_commit_boundaries_fail_closed_in_either_observation_order() {
    let mut forward = DurabilityCheckpoint::new("0/10".parse().unwrap());
    forward
        .observe_commit("0/40".parse().unwrap(), "0/70".parse().unwrap())
        .unwrap();
    assert_eq!(
        forward.observe_commit("0/60".parse().unwrap(), "0/68".parse().unwrap()),
        Err(CheckpointError::OverlappingCommitBoundaries {
            earlier_commit_lsn: "0/40".parse().unwrap(),
            earlier_end_lsn: "0/70".parse().unwrap(),
            later_commit_lsn: "0/60".parse().unwrap(),
        })
    );

    let mut reverse = DurabilityCheckpoint::new("0/10".parse().unwrap());
    reverse
        .observe_commit("0/60".parse().unwrap(), "0/68".parse().unwrap())
        .unwrap();
    assert_eq!(
        reverse.observe_commit("0/40".parse().unwrap(), "0/70".parse().unwrap()),
        Err(CheckpointError::OverlappingCommitBoundaries {
            earlier_commit_lsn: "0/40".parse().unwrap(),
            earlier_end_lsn: "0/70".parse().unwrap(),
            later_commit_lsn: "0/60".parse().unwrap(),
        })
    );
}
