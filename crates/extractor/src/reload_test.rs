#![allow(
    clippy::unreachable,
    reason = "unit-test fakes: unreachable arms assert scripted lease outcomes"
)]

use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn connect_time_schema_change_enters_the_restart_path() {
    let expected = common::SchemaVersionNo(7);
    let error = anyhow::Error::new(crate::reload_export::ConnectSchemaChanged {
        new_version: expected,
    })
    .context("connect exporter");
    assert_eq!(connect_schema_change(&error), Some(expected));
    assert_eq!(connect_schema_change(&anyhow::anyhow!("network")), None);
}

#[tokio::test]
async fn a_panicking_exporter_is_observed_not_swallowed() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(async { panic!("exporter blew up mid-PUT") });
    let joined = set.join_next().await.expect("one task was spawned");
    assert_eq!(observe_exporter_end(joined), ExporterExit::Panicked);

    set.spawn(async {});
    let joined = set.join_next().await.expect("one task was spawned");
    assert_eq!(observe_exporter_end(joined), ExporterExit::Completed);
}

#[tokio::test(start_paused = true)]
async fn drain_is_bounded_and_aborts_a_wedged_exporter() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(std::future::pending::<()>());
    let budget = Duration::from_secs(5);
    let started = tokio::time::Instant::now();

    drain_exporters(&mut set, budget).await;

    assert!(set.is_empty(), "the aborted task must be joined");
    assert_eq!(started.elapsed(), budget);
}

#[test]
fn preflight_rejections_read_as_operator_reasons() {
    // These strings land verbatim in table_reload.error — they ARE the operator UX.
    assert_eq!(
        PreflightRejection::NotPublished("public".into(), "ghost".into()).to_string(),
        "table public.ghost is not in the publication"
    );
    assert_eq!(
        PreflightRejection::NoPrimaryKey("public".into(), "keyless".into()).to_string(),
        "table public.keyless has no primary key"
    );
}

#[test]
fn an_unpublished_keyless_table_reports_the_publication_gap_first() {
    // The two catalog reads run concurrently, so BOTH answers are always available here: the
    // precedence is `classify_target`'s ordering alone. A table that is neither published nor keyed
    // must still name the publication — the gap the operator fixes first.
    let both_wrong = classify_target(false, false, "public", "ghost").unwrap_err();
    assert!(
        matches!(both_wrong, PreflightRejection::NotPublished(..)),
        "not-published outranks no-primary-key; got {both_wrong}"
    );

    let keyless = classify_target(true, false, "public", "keyless").unwrap_err();
    assert!(
        matches!(keyless, PreflightRejection::NoPrimaryKey(..)),
        "a published keyless table is rejected for its key; got {keyless}"
    );

    assert!(classify_target(true, true, "public", "orders").is_ok());
}

#[test]
fn reload_preflight_preserves_full_coverage_reasons() {
    let disabled = crate::source_catalog::PublicationCoverageIssue::DisabledOperations {
        publication: "walrus_pub".into(),
        disabled: "DELETE".into(),
    };
    let rejection = classify_publication_issue(disabled, "public", "orders");
    assert!(matches!(
        rejection,
        PreflightRejection::PublicationCoverage(_)
    ));
    assert!(rejection.to_string().contains("DELETE"));

    let missing = crate::source_catalog::PublicationCoverageIssue::MissingTarget {
        publication: "walrus_pub".into(),
        schema: "public".into(),
        table: "ghost".into(),
    };
    assert!(matches!(
        classify_publication_issue(missing, "public", "ghost"),
        PreflightRejection::NotPublished(..)
    ));
}

#[test]
fn an_ad_hoc_preflight_failure_is_infra_never_a_rejection() {
    // `preflight` propagates its catalog queries with `?`; the From impl is what keeps a dead
    // connection out of `table_reload.error` as a false "not in the publication" reason.
    let outcome = PreflightOutcome::from(anyhow::anyhow!("connection closed mid-query"));

    match outcome {
        PreflightOutcome::Infra(e) => assert_eq!(e.to_string(), "connection closed mid-query"),
        PreflightOutcome::Rejected(r) => panic!("an infra failure must never reject: {r}"),
    }
}

#[test]
fn the_configured_reload_cap_narrows_without_losing_its_non_zero_proof() {
    // `app::pipeline` narrows `ExtractorConfig`'s `NonZeroU64` into this struct's `NonZeroUsize`: only
    // the 64-bit magnitude can fail to fit a `usize`, never the "at least one exporter" proof the
    // config already made. A zero-permit semaphore would not *pause* the controller but kill it —
    // `tick` and `adopt_and_resume` both return early on no free permits, so `requested` rows would
    // queue forever with nothing in the logs to say why.
    let configured = crate::config::ExtractorConfig::default()
        .extractor
        .max_concurrent_reloads;
    let cap = NonZeroUsize::try_from(configured).expect("the shipped default fits a usize");
    assert_eq!(u64::try_from(cap.get()), Ok(configured.get()));
    assert_eq!(Semaphore::new(cap.get()).available_permits(), cap.get());
}

fn adopted_row(
    start_lsn: Option<common::Lsn>,
    chunk_no: i64,
    cursor_pk: Option<serde_json::Value>,
) -> control::ReloadRow {
    control::ReloadRow {
        reload_id: common::ReloadId(7),
        epoch: common::EpochNo(3),
        source_schema: "public".to_string(),
        source_table: "orders".to_string(),
        flavor: control::ReloadFlavor::Reload,
        source_request_id: Some(uuid::Uuid::from_u128(7)),
        parent_request_id: None,
        scope: control::ReloadScope::Table,
        status: control::ReloadStatus::Exporting,
        chunk_no,
        cursor_pk,
        start_lsn,
        first_lsn: None,
        final_lsn: None,
        schema_version: Some(common::SchemaVersionNo(1)),
        restart_count: 0,
        lease_holder: Some("extractor-a".to_string()),
        exporter_generation: 1,
        has_export_plan: false,
        error: None,
    }
}

#[test]
fn adoption_progress_decides_whether_fresh_identity_spends_restart_budget() {
    let pre_f = adopted_row(None, 0, None);
    assert_eq!(
        adopted_snapshot_ownership(&pre_f),
        SnapshotOwnership::AdoptedPristine,
        "a claimed row with no F gets a fresh identity without spending the restart budget"
    );

    let fenced_pre_chunk = adopted_row(Some(common::Lsn::new(0x100)), 0, None);
    assert_eq!(
        adopted_snapshot_ownership(&fenced_pre_chunk),
        SnapshotOwnership::AdoptedPristine,
        "F alone is not durable baseline material, but its marker identity still cannot be reused"
    );

    let mut planned_empty_snapshot = fenced_pre_chunk;
    planned_empty_snapshot.has_export_plan = true;
    assert_eq!(
        adopted_snapshot_ownership(&planned_empty_snapshot),
        SnapshotOwnership::AdoptedWithProgress,
        "even a zero-file snapshot plan is connection-local work and can never be resumed"
    );

    let durable_chunk = adopted_row(
        Some(common::Lsn::new(0x100)),
        1,
        Some(serde_json::json!([42])),
    );
    assert_eq!(
        adopted_snapshot_ownership(&durable_chunk),
        SnapshotOwnership::AdoptedWithProgress,
        "a committed chunk belongs to the lost connection-local snapshot and spends budget"
    );
}

#[tokio::test(start_paused = true)]
async fn lost_lease_cancels_the_exporter() {
    let token = CancellationToken::new();
    // First renewal succeeds, second reports the lease gone. The `AsyncFnMut` bound lets the
    // attempt mutate a plain local counter — no `Arc<AtomicUsize>` to smuggle it out of the
    // closure, which the old `R: FnMut() -> Fut` bound would have forced.
    let mut calls = 0_usize;
    let end = lease_guarded_export(
        token,
        Duration::from_secs(20),
        async || {
            calls += 1;
            Ok(calls == 1)
        },
        async {
            std::future::pending::<()>().await;
            unreachable!()
        },
    )
    .await;
    assert!(matches!(end, ExporterEnd::LostLease), "got {end:?}");
    assert_eq!(calls, 2);
}

#[tokio::test(start_paused = true)]
async fn transient_renewal_errors_do_not_cancel() {
    let token = CancellationToken::new();
    let cancel = token.clone();
    let mut calls = 0_usize;
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(70)).await;
        cancel.cancel();
    });
    let end = lease_guarded_export(
        token,
        Duration::from_secs(20),
        async || {
            calls += 1;
            Err(anyhow::anyhow!("control-pg blinked"))
        },
        async {
            std::future::pending::<()>().await;
            unreachable!()
        },
    )
    .await;
    // Errors are retried (the lease expiry is the real deadline); only cancellation ends it.
    assert!(matches!(end, ExporterEnd::Cancelled), "got {end:?}");
    assert!(calls >= 3);
}

#[tokio::test(start_paused = true)]
async fn cap_of_two_schedules_third_request_only_after_a_permit_frees() {
    // The scheduling shape `tick` relies on: permits live inside the exporter tasks, so a
    // third export starts only when one of the first two releases its permit.
    let semaphore = Arc::new(Semaphore::new(2));
    let started: Vec<Arc<AtomicUsize>> = (0..3).map(|_| Arc::new(AtomicUsize::new(0))).collect();
    let mut releases = Vec::new();
    // A `JoinSet`, like the controller's own exporter set — the fake exporters are held and drained
    // exactly the way `spawn_exporter` / `drain_exporters` hold and drain the real ones.
    let mut exporters = tokio::task::JoinSet::new();
    for flag in &started {
        let sem = Arc::clone(&semaphore);
        let flag = Arc::clone(flag);
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        releases.push(Some(tx));
        exporters.spawn(async move {
            let _permit = sem.acquire_owned().await.unwrap();
            // Relaxed: an independent per-task latch. Nothing rides behind the flag, and the
            // permits — not this store — are what the test's ordering claim rests on.
            flag.store(1, Ordering::Relaxed);
            let _ = rx.await; // park while holding the permit
        });
    }

    // Two acquire; the third is parked on the semaphore.
    tokio::time::advance(Duration::from_millis(50)).await;
    let started_count = || {
        started
            .iter()
            .filter(|f| f.load(Ordering::Relaxed) == 1)
            .count()
    };
    assert_eq!(started_count(), 2, "cap of two holds; the third waits");

    // Free exactly ONE permit (release a task that actually started): the third now runs.
    let running_idx = started
        .iter()
        .position(|f| f.load(Ordering::Relaxed) == 1)
        .unwrap();
    releases[running_idx].take().unwrap().send(()).unwrap();
    tokio::time::advance(Duration::from_millis(50)).await;
    assert_eq!(started_count(), 3, "the third started once a permit freed");

    for tx in releases.into_iter().flatten() {
        let _ = tx.send(());
    }
    while let Some(joined) = exporters.join_next().await {
        joined.unwrap(); // a panicking fake exporter must fail the test, not be swallowed
    }
    assert_eq!(semaphore.available_permits(), 2, "permits returned on exit");
}
