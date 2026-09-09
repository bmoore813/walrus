#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
//! Live-wire decode against the compose source (`#[ignore]` — needs source PG on `trust` auth). The
//! Rust analogue of the proof harness's `run-tests.sh`: an `INSERT` decodes to the canonical
//! `Begin → Relation → Insert → Commit` sequence.
//!
//!   cargo test -p extractor --test live_decode -- --ignored

use common::Lsn;
use extractor::consume::on_frame;
use extractor::pgoutput::{Message, StreamCtx};
use extractor::replication::{ReplicationMessage, ReplicationStream};
use extractor::slot::verify_or_create_slot;
use std::time::Duration;
use tokio_postgres::NoTls;

const SOURCE_MIGRATION: &str = include_str!("../../../migrations/source/0001_publication.sql");

static SOURCE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

fn source_url() -> String {
    std::env::var("WALRUS_SOURCE_DB_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/walrus".to_string())
}

async fn admin() -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(&source_url(), NoTls)
        .await
        .expect("connect to source");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

async fn drop_slot(client: &tokio_postgres::Client, slot: &str) {
    let _ = client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}

#[derive(Debug)]
struct DecodedFrame {
    wal_start: Lsn,
    message: Message,
}

/// Drive the stream until a `Commit`, retaining the XLogData position that the extractor stamps onto
/// each decoded row as its ordering LSN.
async fn collect_frames_until_commit(stream: &mut ReplicationStream) -> Vec<DecodedFrame> {
    let mut ctx = StreamCtx::default();
    let mut frames = Vec::new();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let frame = stream.next().await.expect("frame").expect("stream open");
            let wal_start = match &frame {
                ReplicationMessage::XLogData { wal_start, .. } => Some(*wal_start),
                ReplicationMessage::Keepalive { .. } => None,
            };
            if let Some(msg) = on_frame(&mut ctx, frame).expect("decode") {
                let done = matches!(msg, Message::Commit { .. });
                frames.push(DecodedFrame {
                    wal_start: wal_start.expect("decoded messages come from XLogData frames"),
                    message: msg,
                });
                if done {
                    break;
                }
            }
        }
    })
    .await
    .expect("a Commit within 10s");
    frames
}

/// Compatibility helper for tests concerned only with decoded message families.
async fn collect_until_commit(stream: &mut ReplicationStream) -> Vec<Message> {
    collect_frames_until_commit(stream)
        .await
        .into_iter()
        .map(|frame| frame.message)
        .collect()
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG, trust auth)"]
async fn insert_into_orders_decodes_begin_relation_insert_commit() {
    let _guard = SOURCE_LOCK.lock().await;
    let slot = "walrus_decode_insert";
    let admin = admin().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    // Cleanup BEFORE creating the slot, so this delete's WAL is not streamed.
    admin
        .execute("DELETE FROM public.orders WHERE id = 990001", &[])
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();

    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    // The change to decode (arrives after START_REPLICATION → streamed).
    admin
        .execute(
            "INSERT INTO public.orders (id, status, amount) VALUES (990001, 'new', 19.99)",
            &[],
        )
        .await
        .unwrap();

    let msgs = collect_until_commit(&mut stream).await;

    // The canonical sequence: a small txn arrives whole at commit (no stream frames).
    assert!(
        matches!(msgs.first(), Some(Message::Begin { .. })),
        "first is Begin: {msgs:?}"
    );
    assert!(
        msgs.iter()
            .any(|m| matches!(m, Message::Relation { relation, .. } if relation.name == "orders")),
        "a Relation for orders precedes the tuple: {msgs:?}"
    );
    let insert_pos = msgs
        .iter()
        .position(|m| matches!(m, Message::Insert { .. }))
        .expect("an Insert");
    let relation_pos = msgs
        .iter()
        .position(|m| matches!(m, Message::Relation { .. }))
        .expect("a Relation");
    assert!(
        relation_pos < insert_pos,
        "Relation precedes Insert: {msgs:?}"
    );
    assert!(
        matches!(msgs.last(), Some(Message::Commit { .. })),
        "last is Commit: {msgs:?}"
    );

    drop(stream);
    drop_slot(&admin, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG)"]
async fn insert_carries_the_expected_column_count() {
    let _guard = SOURCE_LOCK.lock().await;
    let slot = "walrus_decode_cols";
    let admin = admin().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute("DELETE FROM public.orders WHERE id = 990002", &[])
        .await
        .unwrap();
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    admin
        .execute(
            "INSERT INTO public.orders (id, status, amount, note) VALUES (990002, 'new', 5.00, 'hi')",
            &[],
        )
        .await
        .unwrap();

    let msgs = collect_until_commit(&mut stream).await;
    // A non-streamed (small) txn → the Insert's per-message xid is None (xid appears only in a stream).
    let insert = msgs
        .iter()
        .find_map(|m| match m {
            Message::Insert { xid, new, .. } => Some((*xid, new.len())),
            _ => None,
        })
        .expect("an Insert");
    assert_eq!(insert.0, None, "non-streamed Insert has no per-message xid");
    assert_eq!(
        insert.1, 5,
        "orders has 5 columns (id, status, amount, feeling, note)"
    );

    drop(stream);
    drop_slot(&admin, slot).await;
}

#[tokio::test]
#[ignore = "requires docker compose up --wait (source PG, trust auth)"]
async fn delete_insert_delete_in_one_transaction_has_strict_change_lsns() {
    let _guard = SOURCE_LOCK.lock().await;
    let slot = "walrus_decode_did";
    let id = 990_003_i32;
    let admin = admin().await;
    admin.batch_execute(SOURCE_MIGRATION).await.unwrap();
    admin
        .execute("DELETE FROM public.orders WHERE id = $1", &[&id])
        .await
        .unwrap();
    admin
        .execute(
            "INSERT INTO public.orders (id, status, amount) VALUES ($1, 'before', 1.00)",
            &[&id],
        )
        .await
        .unwrap();
    // The seed must predate the slot: the captured transaction should contain exactly d/i/d, not
    // the row that establishes the first DELETE's source precondition.
    drop_slot(&admin, slot).await;
    let resume = verify_or_create_slot(&admin, slot).await.unwrap();
    let mut stream =
        ReplicationStream::start(&source_url(), slot, resume.start_lsn(), "walrus_pub")
            .await
            .unwrap();

    admin
        .batch_execute(&format!(
            "BEGIN; \
             DELETE FROM public.orders WHERE id = {id}; \
             INSERT INTO public.orders (id, status, amount) VALUES ({id}, 'replacement', 2.00); \
             DELETE FROM public.orders WHERE id = {id}; \
             COMMIT;"
        ))
        .await
        .unwrap();

    let frames = collect_frames_until_commit(&mut stream).await;
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.message, Message::Begin { .. }))
            .count(),
        1,
        "one explicit source transaction: {frames:?}"
    );
    assert_eq!(
        frames
            .iter()
            .filter(|frame| matches!(frame.message, Message::Commit { .. }))
            .count(),
        1,
        "one explicit source transaction: {frames:?}"
    );

    let changes = frames
        .iter()
        .filter_map(|frame| match &frame.message {
            Message::Delete { relation_oid, .. } => Some(('d', *relation_oid, frame.wal_start)),
            Message::Insert { relation_oid, .. } => Some(('i', *relation_oid, frame.wal_start)),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        changes.iter().map(|(op, _, _)| *op).collect::<Vec<_>>(),
        ['d', 'i', 'd'],
        "the source emits every same-key lifecycle change in statement order: {frames:?}"
    );
    assert!(
        changes
            .iter()
            .all(|(_, relation_oid, lsn)| *relation_oid == changes[0].1 && *lsn != Lsn::ZERO),
        "all three changes target one relation and carry real row LSNs: {changes:?}"
    );
    assert!(
        changes.windows(2).all(|pair| pair[0].2 < pair[1].2),
        "same-key change LSNs must be strictly increasing: {changes:?}"
    );

    drop(stream);
    drop_slot(&admin, slot).await;
}
