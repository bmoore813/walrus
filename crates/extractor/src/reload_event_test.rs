use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity};

fn relation() -> PgRelation {
    let names = [
        "event_id",
        "request_id",
        "reload_id",
        "event_kind",
        "scope",
        "source_schema",
        "source_table",
        "targets",
        "schema_version",
        "wal_insert_lsn",
    ];
    PgRelation {
        oid: 44,
        schema: "public".into(),
        name: "walrus_reload_event".into(),
        replica_identity: ReplicaIdentity::Default,
        columns: names
            .into_iter()
            .enumerate()
            .map(|(index, name)| PgColumn {
                name: name.into(),
                type_oid: 25,
                type_modifier: -1,
                is_key: index == 0,
            })
            .collect(),
    }
}

fn text(value: impl Into<String>) -> TupleValue {
    TupleValue::Text(value.into())
}

#[test]
fn parses_table_request_by_column_name() {
    let event_id = Uuid::new_v4();
    let values = vec![
        text(event_id.to_string()),
        text(event_id.to_string()),
        TupleValue::Null,
        text("request"),
        text("table"),
        text("public"),
        text("orders"),
        text("[]"),
        TupleValue::Null,
        text("0/20"),
    ];
    let event = PendingReloadEvent::from_tuple(&relation(), &values, None, None).unwrap();
    assert_eq!(event.event_id, event_id);
    assert_eq!(event.kind, ReloadEventKind::Request);
    assert_eq!(event.target(), Some(("public", "orders")));
}

#[test]
fn parses_frozen_all_published_inventory() {
    let event_id = Uuid::new_v4();
    let values = vec![
        text(event_id.to_string()),
        text(event_id.to_string()),
        TupleValue::Null,
        text("request"),
        text("all_published"),
        TupleValue::Null,
        TupleValue::Null,
        text(r#"[{"schema":"public","table":"orders"}]"#),
        TupleValue::Null,
        text("0/20"),
    ];
    let event = PendingReloadEvent::from_tuple(&relation(), &values, None, None).unwrap();
    assert_eq!(
        event.targets,
        [ReloadTarget {
            schema: "public".into(),
            table: "orders".into(),
        }]
    );
}

#[test]
fn requires_attempt_identity_for_a_fence() {
    let event_id = Uuid::new_v4();
    let values = vec![
        text(event_id.to_string()),
        text(event_id.to_string()),
        TupleValue::Null,
        text("end_fence"),
        text("table"),
        text("public"),
        text("orders"),
        text("[]"),
        text("1"),
        text("0/20"),
    ];
    assert_eq!(
        PendingReloadEvent::from_tuple(&relation(), &values, None, None),
        Err(EventTupleError("reload_id"))
    );
}

#[test]
fn abort_drops_only_the_matching_streamed_event() {
    let request_id = Uuid::new_v4();
    let mut pending = PendingReloadEvents::default();
    for xid in [Some(10), Some(11)] {
        pending.push(PendingReloadEvent {
            event_id: Uuid::new_v4(),
            request_id,
            reload_id: None,
            kind: ReloadEventKind::Request,
            scope: ReloadScope::AllPublished,
            source_schema: None,
            source_table: None,
            targets: vec![ReloadTarget {
                schema: "public".into(),
                table: "orders".into(),
            }],
            schema_version: None,
            embedded_lsn: "0/10".parse().unwrap(),
            xid,
            top_xid: Some(10),
        });
    }
    pending.on_stream_abort(10, 11);
    assert_eq!(
        pending.stream_effect_count(10),
        1,
        "the mixed-DDL guard counts only surviving subtransactions"
    );
    let committed = pending.on_stream_commit(10, "0/30".parse().unwrap());
    assert_eq!(committed.len(), 1);
    assert_eq!(committed[0].event.xid, Some(10));
}

#[test]
fn stream_commit_promotes_only_its_top_level_transaction() {
    let request_id = Uuid::new_v4();
    let mut pending = PendingReloadEvents::default();
    for top_xid in [10, 20] {
        pending.push(PendingReloadEvent {
            event_id: Uuid::new_v4(),
            request_id,
            reload_id: None,
            kind: ReloadEventKind::Request,
            scope: ReloadScope::AllPublished,
            source_schema: None,
            source_table: None,
            targets: vec![ReloadTarget {
                schema: "public".into(),
                table: "orders".into(),
            }],
            schema_version: None,
            embedded_lsn: "0/10".parse().unwrap(),
            xid: Some(11),
            top_xid: Some(top_xid),
        });
    }

    assert_eq!(pending.stream_effect_count(10), 1);
    assert_eq!(pending.stream_effect_count(20), 1);
    assert_eq!(pending.ordinary_effect_count(), 0);

    let first = pending.on_stream_commit(10, "0/30".parse().unwrap());
    assert_eq!(first.len(), 1);
    assert_eq!(first[0].event.top_xid, Some(10));

    let second = pending.on_stream_commit(20, "0/40".parse().unwrap());
    assert_eq!(second.len(), 1);
    assert_eq!(second[0].event.top_xid, Some(20));
}

#[tokio::test]
async fn fence_waiter_resolves_by_phase() {
    let waiters = FenceWaiters::default();
    let reload_id = ReloadId(7);
    let waiter = waiters.subscribe(reload_id, FencePhase::End);
    let echo = FenceEcho {
        commit_lsn: "0/30".parse().unwrap(),
        embedded_lsn: "0/20".parse().unwrap(),
    };
    waiters.resolve(reload_id, FencePhase::End, echo);
    assert_eq!(waiter.await.unwrap(), echo);
}

#[tokio::test]
async fn fence_waiter_resolves_every_subscriber_for_the_same_phase() {
    let waiters = FenceWaiters::default();
    let reload_id = ReloadId(9);
    let first = waiters.subscribe(reload_id, FencePhase::End);
    let second = waiters.subscribe(reload_id, FencePhase::End);
    assert_eq!(waiters.waiter_count(), 2);

    let echo = FenceEcho {
        commit_lsn: "0/40".parse().unwrap(),
        embedded_lsn: "0/30".parse().unwrap(),
    };
    waiters.resolve(reload_id, FencePhase::End, echo);

    assert_eq!(first.await.unwrap(), echo);
    assert_eq!(second.await.unwrap(), echo);
    assert_eq!(waiters.waiter_count(), 0);
}

#[tokio::test]
async fn dropping_one_fence_subscriber_preserves_the_others() {
    let waiters = FenceWaiters::default();
    let reload_id = ReloadId(10);
    let dropped = waiters.subscribe(reload_id, FencePhase::Start);
    let live = waiters.subscribe(reload_id, FencePhase::Start);
    drop(dropped);
    assert_eq!(waiters.waiter_count(), 1);

    let echo = FenceEcho {
        commit_lsn: "0/40".parse().unwrap(),
        embedded_lsn: "0/30".parse().unwrap(),
    };
    waiters.resolve(reload_id, FencePhase::Start, echo);

    assert_eq!(live.await.unwrap(), echo);
    assert_eq!(waiters.waiter_count(), 0);
}

#[tokio::test]
async fn fence_waiter_counts_lsn_crosscheck_violations() {
    let waiters = FenceWaiters::default();
    let reload_id = ReloadId(8);
    let waiter = waiters.subscribe(reload_id, FencePhase::Start);
    let echo = FenceEcho {
        commit_lsn: "0/30".parse().unwrap(),
        embedded_lsn: "0/30".parse().unwrap(),
    };
    waiters.resolve(reload_id, FencePhase::Start, echo);
    assert_eq!(waiter.await.unwrap(), echo);
    assert_eq!(waiters.crosscheck_violations(), 1);
}

#[test]
fn fence_identity_is_namespaced_by_source_request() {
    let first = Uuid::new_v4();
    let second = Uuid::new_v4();
    assert_ne!(
        deterministic_fence_id(first, ReloadId(1), FencePhase::Start),
        deterministic_fence_id(second, ReloadId(1), FencePhase::Start),
    );
    assert_ne!(
        deterministic_fence_id(first, ReloadId(1), FencePhase::Start),
        deterministic_fence_id(first, ReloadId(1), FencePhase::End),
    );
}
