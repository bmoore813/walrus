use super::*;

#[test]
fn orphan_sweep_is_reachable_only_through_the_active_slot_guard() {
    let source = include_str!("app.rs");
    let cleanup_call = ["crate::orphan::", "cleanup_epoch_orphans("].concat();
    assert_eq!(
        source.matches(&cleanup_call).count(),
        1,
        "startup must never call the destructive sweep outside the helper that requires an active ReplicationStream"
    );
    let guarded_signature = [
        "async fn cleanup_epoch_orphans_while_holding_slot(\n",
        "    stream: &mut ReplicationStream,",
    ]
    .concat();
    assert!(source.contains(&guarded_signature));
}

#[test]
fn slot_loss_recovery_distinguishes_absence_from_invalidation() {
    let absent = slot_loss_recovery(crate::epoch::SlotStatus::Absent);
    assert_eq!(absent, Some(SlotLossRecovery::CreateIfAbsent));
    let invalidated = slot_loss_recovery(crate::epoch::SlotStatus::Invalidated);
    assert_eq!(invalidated, Some(SlotLossRecovery::ReplaceInvalidated));
    assert_ne!(
        invalidated,
        Some(SlotLossRecovery::CreateIfAbsent),
        "same-name invalidation replaces its occupied slot net-zero and must not require spare capacity"
    );
    assert_eq!(
        slot_loss_recovery(crate::epoch::SlotStatus::Unreachable),
        None,
        "a connection failure must never drop or create a slot"
    );
}

#[test]
fn a_generation_can_resume_only_its_recorded_slot_without_a_restart_intent() {
    assert!(generation_can_resume(
        "walrus_a",
        "walrus_a",
        control::ReplicationStatus::Streaming,
        true,
    ));
    assert!(!generation_can_resume(
        "walrus_b",
        "walrus_a",
        control::ReplicationStatus::Streaming,
        true,
    ));
    assert!(!generation_can_resume(
        "walrus_a",
        "walrus_a",
        control::ReplicationStatus::TotalRestart,
        true,
    ));
    assert!(!generation_can_resume(
        "walrus_a",
        "walrus_a",
        control::ReplicationStatus::Streaming,
        publication_guard_continuity_proven_on_startup(),
    ));

    let drift = assert_generation_slot_name("walrus_b", "walrus_a").unwrap_err();
    assert!(matches!(
        drift,
        crate::preflight::PreflightError::SlotNameDrift {
            configured,
            recorded,
        } if configured == "walrus_b" && recorded == "walrus_a"
    ));
}

#[test]
fn resume_never_starts_before_the_generation_catalog_fence() {
    let confirmed = common::Lsn::new(0x100);
    let created = common::Lsn::new(0x200);
    assert_eq!(resumed_generation_start(confirmed, created), created);
    assert_eq!(
        resumed_generation_start(common::Lsn::new(0x300), created),
        common::Lsn::new(0x300)
    );
}

#[test]
fn future_catalog_fence_versions_block_both_absent_and_invalidated_slot_recovery() {
    let future = control::ReplicationState {
        epoch: EpochNo(7),
        slot_name: "walrus_slot".to_owned(),
        created_lsn: common::Lsn::new(0x100),
        catalog_fence_version: control::CURRENT_CATALOG_FENCE_VERSION + 1,
        status: control::ReplicationStatus::Streaming,
    };
    for status in [
        crate::epoch::SlotStatus::Absent,
        crate::epoch::SlotStatus::Invalidated,
    ] {
        assert!(
            slot_loss_recovery(status).is_some(),
            "fixture must exercise a destructive FreshSlot branch"
        );
        let error = ensure_catalog_fence_version_supported(&future).unwrap_err();
        assert!(error.to_string().contains("unsafe binary downgrade"));
    }

    let mut supported = future;
    supported.catalog_fence_version = control::CURRENT_CATALOG_FENCE_VERSION;
    ensure_catalog_fence_version_supported(&supported).unwrap();
    supported.catalog_fence_version = 0;
    ensure_catalog_fence_version_supported(&supported)
        .expect("legacy provenance opens a successor instead of rejecting the binary");
}

#[test]
fn epoch_resume_requires_exact_publication_and_registry_inventory() {
    let registered = BTreeSet::from([
        ("public".to_owned(), "customers".to_owned()),
        ("public".to_owned(), "orders".to_owned()),
    ]);

    assert_eq!(
        inventory_resume_decision(&registered, &registered),
        InventoryResumeDecision::Resume
    );

    let published = BTreeSet::from([
        ("public".to_owned(), "new_table".to_owned()),
        ("public".to_owned(), "orders".to_owned()),
    ]);
    assert_eq!(
        inventory_resume_decision(&published, &registered),
        InventoryResumeDecision::OpenSuccessor(InventoryDifference {
            published_without_registry: vec![("public".to_owned(), "new_table".to_owned())],
            registry_without_publication: vec![("public".to_owned(), "customers".to_owned())],
        })
    );
}
