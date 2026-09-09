//! Minimal logical-decoding harness for reload exporter integration tests.
//!
//! Production persists request/fence consequences inside the main decoder before acknowledging
//! WAL. These tests do not run the whole extractor, so this helper supplies only that half of the
//! protocol: commit-gate `public.walrus_reload_event`, persist `F`/`H`, then resolve the exporter waiter.

use common::Lsn;
use extractor::consume::on_frame;
use extractor::heartbeat::InternalTables;
use extractor::pgoutput::{Message, StreamCtx};
use extractor::reload_event::{
    FenceEcho, FencePhase, FenceWaiters, PendingReloadEvent, PendingReloadEvents,
};
use extractor::replication::ReplicationStream;
use extractor::slot::verify_or_create_slot;
use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use tokio_postgres::NoTls;
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
pub struct Resolver {
    pub handle: tokio::task::JoinHandle<()>,
    pub ready: tokio::sync::oneshot::Receiver<()>,
    #[allow(
        dead_code,
        reason = "only the overlap test asks the shared resolver for target commit observations"
    )]
    pub watched_commit: tokio::sync::watch::Receiver<Option<Lsn>>,
    #[allow(
        dead_code,
        reason = "only the long-transaction export test asks whether a watched commit used proto v2"
    )]
    pub watched_stream_commit: tokio::sync::watch::Receiver<Option<Lsn>>,
}

pub fn spawn(
    source_db_url: String,
    slot: &'static str,
    pool: sqlx::PgPool,
    waiters: Arc<FenceWaiters>,
    watch_table: Option<&'static str>,
    token: CancellationToken,
) -> Resolver {
    let (ready_tx, ready) = tokio::sync::oneshot::channel();
    let (watched_tx, watched_commit) = tokio::sync::watch::channel(None);
    let (watched_stream_tx, watched_stream_commit) = tokio::sync::watch::channel(None);
    let handle = tokio::spawn(async move {
        let (admin, connection) = tokio_postgres::connect(&source_db_url, NoTls)
            .await
            .unwrap();
        tokio::spawn(async move {
            let _ = connection.await;
        });
        drop_slot(&admin, slot).await;
        let resume = verify_or_create_slot(&admin, slot).await.unwrap();
        let mut stream =
            ReplicationStream::start(&source_db_url, slot, resume.start_lsn(), "walrus_pub")
                .await
                .unwrap();
        // Once START_REPLICATION has succeeded, every later fence is retained by this slot even if
        // the task has not polled its first frame yet. This closes the test setup race directly.
        let _ = ready_tx.send(());

        let mut ctx = StreamCtx::default();
        let mut internal = InternalTables::default();
        let mut pending = PendingReloadEvents::default();
        let mut current_top_xid = None;
        let mut watch_oid = None;
        let mut ordinary_touched_watch = false;
        let mut streamed_watch_subxids: HashMap<u32, HashSet<u32>> = HashMap::new();
        loop {
            let frame = tokio::select! {
                _ = token.cancelled() => break,
                frame = stream.next() => frame.unwrap().unwrap(),
            };
            let Some(message) = on_frame(&mut ctx, frame).unwrap() else {
                continue;
            };
            match &message {
                Message::StreamStart { xid, .. } => current_top_xid = Some(*xid),
                Message::StreamStop => current_top_xid = None,
                Message::Relation { relation, .. } => {
                    internal.note_relation(relation);
                    if watch_table == Some(relation.name.as_str()) {
                        watch_oid = Some(relation.oid);
                    }
                }
                Message::Insert {
                    relation_oid,
                    new,
                    xid,
                } if internal.is_reload_event(*relation_oid) => {
                    let relation = internal.reload_event_rel().unwrap();
                    let event = PendingReloadEvent::from_tuple(
                        relation,
                        new,
                        *xid,
                        xid.map(|_| current_top_xid.expect("streamed event outside StreamStart")),
                    )
                    .unwrap();
                    pending.push(event);
                }
                Message::Insert { relation_oid, .. }
                | Message::Update { relation_oid, .. }
                | Message::Delete { relation_oid, .. }
                    if watch_oid == Some(*relation_oid) =>
                {
                    match &message {
                        Message::Insert { xid: Some(sub), .. }
                        | Message::Update { xid: Some(sub), .. }
                        | Message::Delete { xid: Some(sub), .. } => {
                            streamed_watch_subxids
                                .entry(
                                    current_top_xid
                                        .expect("streamed watched change outside StreamStart"),
                                )
                                .or_default()
                                .insert(*sub);
                        }
                        _ => ordinary_touched_watch = true,
                    }
                }
                Message::Commit { commit_lsn, .. } => {
                    resolve_committed(pending.on_commit(*commit_lsn), &pool, &waiters).await;
                    if std::mem::take(&mut ordinary_touched_watch) {
                        watched_tx.send_replace(Some(*commit_lsn));
                    }
                }
                Message::StreamCommit {
                    xid, commit_lsn, ..
                } => {
                    resolve_committed(pending.on_stream_commit(*xid, *commit_lsn), &pool, &waiters)
                        .await;
                    if streamed_watch_subxids.remove(xid).is_some() {
                        watched_tx.send_replace(Some(*commit_lsn));
                        watched_stream_tx.send_replace(Some(*commit_lsn));
                    }
                }
                Message::StreamAbort { top_xid, sub_xid } => {
                    pending.on_stream_abort(*top_xid, *sub_xid);
                    if top_xid == sub_xid {
                        streamed_watch_subxids.remove(top_xid);
                    } else if let Some(subxids) = streamed_watch_subxids.get_mut(top_xid) {
                        subxids.remove(sub_xid);
                        if subxids.is_empty() {
                            streamed_watch_subxids.remove(top_xid);
                        }
                    }
                }
                _ => {}
            }
        }
        drop(stream);
        drop_slot(&admin, slot).await;
    });
    Resolver {
        handle,
        ready,
        watched_commit,
        watched_stream_commit,
    }
}

async fn resolve_committed(
    committed: Vec<extractor::reload_event::CommittedReloadEvent>,
    pool: &sqlx::PgPool,
    waiters: &FenceWaiters,
) {
    for committed in committed {
        let Some(phase) = committed.event.fence_phase() else {
            continue;
        };
        let reload_id = committed.event.reload_id.unwrap();
        match phase {
            FencePhase::Start => {
                control::reload::record_start_fence(
                    pool,
                    reload_id,
                    committed.commit_lsn,
                    control::ReloadFenceIdentity {
                        request_id: Some(committed.event.request_id),
                        source_schema: committed.event.source_schema.as_deref().unwrap(),
                        source_table: committed.event.source_table.as_deref().unwrap(),
                        schema_version: committed.event.schema_version.unwrap(),
                    },
                )
                .await
                .unwrap();
            }
            FencePhase::End => {
                // The mini resolver has no user-data batcher. Reaching this commit frame is its
                // complete target drain, so persist H before waking the exporter just as production
                // does after its forced target flush.
                control::reload::record_end_marker(
                    pool,
                    reload_id,
                    committed.commit_lsn,
                    control::ReloadFenceIdentity {
                        request_id: Some(committed.event.request_id),
                        source_schema: committed.event.source_schema.as_deref().unwrap(),
                        source_table: committed.event.source_table.as_deref().unwrap(),
                        schema_version: committed.event.schema_version.unwrap(),
                    },
                )
                .await
                .unwrap();
            }
        }
        waiters.resolve(
            reload_id,
            phase,
            FenceEcho {
                commit_lsn: committed.commit_lsn,
                embedded_lsn: committed.event.embedded_lsn,
            },
        );
    }
}

async fn drop_slot(admin: &tokio_postgres::Client, slot: &str) {
    let _ = admin
        .execute(
            "SELECT pg_drop_replication_slot(slot_name)
             FROM pg_replication_slots WHERE slot_name = $1 AND NOT active",
            &[&slot],
        )
        .await;
}
