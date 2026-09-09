use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};
use pg_to_arrow::oids;

fn cfg() -> HeartbeatConfig {
    HeartbeatConfig {
        idle_after: Duration::from_secs(10),
        roundtrip_deadline: Duration::from_secs(30),
    }
}

fn heartbeat_relation() -> PgRelation {
    PgRelation {
        oid: 90001,
        schema: "public".to_string(),
        name: "walrus_heartbeat".to_string(),
        replica_identity: ReplicaIdentity::Default,
        columns: vec![
            PgColumn {
                name: "id".into(),
                type_oid: oids::INT4,
                type_modifier: -1,
                is_key: true,
            },
            PgColumn {
                name: "beat_seq".into(),
                type_oid: oids::INT8,
                type_modifier: -1,
                is_key: false,
            },
        ],
    }
}

#[test]
fn beat_suppressed_when_active_within_idle_after() {
    let s = BeatState::new(cfg());
    let now = Instant::now();
    // Activity 1s ago, idle_after is 10s → not idle → suppressed.
    assert!(!s.should_beat(now, now - Duration::from_secs(1)));
}

#[test]
fn beat_fires_exactly_once_after_idle_threshold() {
    let mut s = BeatState::new(cfg());
    let now = Instant::now();
    let idle_since = now - Duration::from_secs(11); // idle > idle_after
    assert!(
        s.should_beat(now, idle_since),
        "idle past the threshold → beat"
    );
    // Simulate firing: on_beat_sent stamps last_beat = now.
    s.on_beat_sent(1, now);
    // Immediately after, the beat clock suppresses a second beat.
    assert!(
        !s.should_beat(now, idle_since),
        "just beat → suppressed until idle_after elapses again"
    );
    // After another idle_after with still no activity, it may beat again.
    let later = now + Duration::from_secs(11);
    assert!(s.should_beat(later, idle_since));
}

#[test]
fn roundtrip_recorded_when_returned_seq_matches_pending() {
    let mut s = BeatState::new(cfg());
    let now = Instant::now();
    s.on_beat_sent(5, now);
    assert!(
        s.is_degraded(now + Duration::from_secs(31)),
        "un-returned past deadline → degraded"
    );
    // A returned seq ≥ pending closes it (coalesced beat: 6 ≥ 5).
    s.observe_return(6, now + Duration::from_secs(1));
    assert_eq!(s.pending_seq, None);
    assert!(
        !s.is_degraded(now + Duration::from_secs(31)),
        "round-trip clears degraded"
    );
}

#[test]
fn degraded_true_only_after_deadline_without_roundtrip() {
    let mut s = BeatState::new(cfg());
    let now = Instant::now();
    assert!(!s.is_degraded(now), "no beat outstanding → never degraded");
    s.on_beat_sent(1, now);
    assert!(
        !s.is_degraded(now + Duration::from_secs(29)),
        "within deadline → not yet"
    );
    assert!(
        s.is_degraded(now + Duration::from_secs(31)),
        "past deadline, un-returned → degraded"
    );
}

#[test]
fn internal_heartbeat_oid_is_recognised() {
    let mut it = InternalTables::default();
    assert!(!it.is_internal(90001), "unknown until we see its Relation");
    it.note_relation(&heartbeat_relation());
    assert!(it.is_internal(90001));
    assert!(!it.is_internal(42), "a user table is not internal");
    // beat_seq is the second column → read it back from a new-tuple.
    let new = vec![TupleValue::Text("1".into()), TupleValue::Text("7".into())];
    assert_eq!(it.beat_seq_of(&new), Some(7));
}
