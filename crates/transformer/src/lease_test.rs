use super::*;

/// Every case below feeds `renew_interval`, whose parameter *is* the proof that the TTL clears
/// `MIN_LEASE_TTL` — so the fixtures parse a duration rather than pass a bare one.
fn renewable(secs: u64) -> LeaseTtl {
    LeaseTtl::new(Duration::from_secs(secs)).expect("fixture TTLs are at or above MIN_LEASE_TTL")
}

#[test]
fn renew_interval_is_one_third_of_a_normal_ttl() {
    assert_eq!(renew_interval(renewable(30)), Duration::from_secs(10));
}

#[test]
fn renew_interval_never_reaches_the_ttl() {
    for secs in [3, 5, 30, 300, 3600] {
        let lease = renewable(secs);
        assert!(
            renew_interval(lease) < lease.get(),
            "renew must land strictly inside the {:?} TTL",
            lease.get()
        );
    }
}

#[test]
fn renew_interval_floors_at_one_second() {
    assert_eq!(renew_interval(renewable(3)), MIN_RENEW_INTERVAL);
}

/// `app::pipeline` cancels the token and **joins** the renewer before `release_all`, instead of
/// aborting it. Owner+token fencing makes a stale renewal incapable of reviving a successor's
/// acquisition, but joining still proves this process has finished using its lease capability
/// before it performs its graceful release. That join is only finite because cancellation ends the
/// task, so assert exactly that under a timeout.
#[tokio::test]
async fn a_cancelled_renewer_ends_so_the_drain_can_join_it() {
    // Lazy: the DSN is parsed, never dialled. With no owned keys the renewer touches no connection,
    // which leaves the cancellation path as the only thing under test.
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://walrus@127.0.0.1:1/unused")
        .expect("a lazy pool parses its DSN without connecting");
    let token = CancellationToken::new();
    let renewer = spawn_renewer(
        pool,
        EpochNo(1),
        Vec::new(),
        "transformer-test".to_string(),
        renewable(30),
        token.clone(),
    );

    token.cancel();
    tokio::time::timeout(Duration::from_secs(5), renewer)
        .await
        .expect("a cancelled renewer must end, or `app::pipeline`'s join would stall the drain")
        .expect("the renewer returns rather than panicking");
}

/// `renew_interval`'s `clamp` would invert its own bounds on a sub-floor TTL. The type is what keeps
/// one out of reach: nothing under `MIN_LEASE_TTL` parses, in release builds as well as debug.
#[test]
fn a_ttl_the_renewer_could_not_fit_inside_never_parses() {
    for too_short in [
        Duration::ZERO,
        Duration::from_millis(500),
        MIN_RENEW_INTERVAL,
    ] {
        assert!(
            matches!(
                LeaseTtl::new(too_short),
                Err(crate::config::ConfigError::LeaseTtlTooShort { ttl, minimum })
                    if ttl == too_short && minimum == MIN_LEASE_TTL
            ),
            "{too_short:?} must not parse as a renewable TTL"
        );
    }
    // The floor itself is admitted, and the parsed value is exactly the duration handed in.
    let floor = LeaseTtl::new(MIN_LEASE_TTL).expect("MIN_LEASE_TTL is an inclusive floor");
    assert_eq!(floor.get(), MIN_LEASE_TTL);
}
