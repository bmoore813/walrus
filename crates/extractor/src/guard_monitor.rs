//! Read-only runtime guards for the source replication slot.
//!
//! This monitor deliberately owns an ordinary SQL connection. It never touches the CopyBoth socket,
//! the durability checkpoint, or standby-status feedback, so an observation can neither acknowledge
//! WAL nor change where PostgreSQL believes the consumer is durable.

use anyhow::Context as _;
use common::Lsn;
use std::future::Future;
use std::time::Duration;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const CAPABILITIES_SQL: &str = "
SELECT
    EXISTS (
        SELECT 1
          FROM pg_catalog.pg_attribute
         WHERE attrelid = 'pg_catalog.pg_replication_slots'::regclass
           AND attname = 'wal_status'
           AND NOT attisdropped
    ),
    EXISTS (
        SELECT 1
          FROM pg_catalog.pg_attribute
         WHERE attrelid = 'pg_catalog.pg_replication_slots'::regclass
           AND attname = 'safe_wal_size'
           AND NOT attisdropped
    )";

const SAMPLE_BOTH_SQL: &str = "
SELECT pg_current_wal_lsn()::text,
       restart_lsn::text,
       confirmed_flush_lsn::text,
       wal_status::text,
       safe_wal_size
  FROM pg_catalog.pg_replication_slots
 WHERE slot_name = $1";

const SAMPLE_WAL_STATUS_SQL: &str = "
SELECT pg_current_wal_lsn()::text,
       restart_lsn::text,
       confirmed_flush_lsn::text,
       wal_status::text,
       NULL::bigint
  FROM pg_catalog.pg_replication_slots
 WHERE slot_name = $1";

const SAMPLE_SAFE_WAL_SIZE_SQL: &str = "
SELECT pg_current_wal_lsn()::text,
       restart_lsn::text,
       confirmed_flush_lsn::text,
       NULL::text,
       safe_wal_size
  FROM pg_catalog.pg_replication_slots
 WHERE slot_name = $1";

const SAMPLE_BASE_SQL: &str = "
SELECT pg_current_wal_lsn()::text,
       restart_lsn::text,
       confirmed_flush_lsn::text,
       NULL::text,
       NULL::bigint
  FROM pg_catalog.pg_replication_slots
 WHERE slot_name = $1";

const QUERY_TIMEOUT: Duration = Duration::from_secs(30);

/// Which optional columns this PostgreSQL exposes on `pg_replication_slots`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SlotCatalogCapabilities {
    wal_status: bool,
    safe_wal_size: bool,
}

impl SlotCatalogCapabilities {
    const fn sample_sql(self) -> &'static str {
        match (self.wal_status, self.safe_wal_size) {
            (true, true) => SAMPLE_BOTH_SQL,
            (true, false) => SAMPLE_WAL_STATUS_SQL,
            (false, true) => SAMPLE_SAFE_WAL_SIZE_SQL,
            (false, false) => SAMPLE_BASE_SQL,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum SlotWalStatus {
    Reserved,
    Extended,
    Unreserved,
    Lost,
    Unknown(String),
}

impl SlotWalStatus {
    fn from_catalog(value: String) -> Self {
        match value.as_str() {
            "reserved" => Self::Reserved,
            "extended" => Self::Extended,
            "unreserved" => Self::Unreserved,
            "lost" => Self::Lost,
            _ => Self::Unknown(value),
        }
    }

    const fn gauge_code(&self) -> u8 {
        match self {
            Self::Reserved | Self::Extended => 0,
            Self::Unreserved | Self::Unknown(_) => 1,
            Self::Lost => 2,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RawSlotSample {
    current_lsn: String,
    restart_lsn: Option<String>,
    confirmed_flush_lsn: Option<String>,
    wal_status: Option<String>,
    safe_wal_size: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SlotSample {
    current_lsn: Lsn,
    restart_lsn: Option<Lsn>,
    confirmed_flush_lsn: Option<Lsn>,
    wal_status: Option<SlotWalStatus>,
    safe_wal_size: Option<u64>,
}

impl SlotSample {
    fn try_from_raw(raw: RawSlotSample) -> anyhow::Result<Self> {
        let parse_optional = |value: Option<String>, field: &'static str| {
            value
                .map(|text| {
                    text.parse::<Lsn>()
                        .with_context(|| format!("parse {field} from replication-slot guard poll"))
                })
                .transpose()
        };
        let safe_wal_size = raw
            .safe_wal_size
            .map(|bytes| {
                u64::try_from(bytes).with_context(|| {
                    format!("safe_wal_size was negative in replication-slot guard poll: {bytes}")
                })
            })
            .transpose()?;

        Ok(Self {
            current_lsn: raw
                .current_lsn
                .parse()
                .context("parse current_lsn from replication-slot guard poll")?,
            restart_lsn: parse_optional(raw.restart_lsn, "restart_lsn")?,
            confirmed_flush_lsn: parse_optional(raw.confirmed_flush_lsn, "confirmed_flush_lsn")?,
            wal_status: raw.wal_status.map(SlotWalStatus::from_catalog),
            safe_wal_size,
        })
    }

    const fn replication_lag_bytes(&self) -> u64 {
        match self.confirmed_flush_lsn {
            Some(confirmed) => self.current_lsn.as_u64().saturating_sub(confirmed.as_u64()),
            None => 0,
        }
    }

    const fn retained_wal_bytes(&self) -> u64 {
        match self.restart_lsn {
            Some(restart) => self.current_lsn.as_u64().saturating_sub(restart.as_u64()),
            None => 0,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ObservedState {
    Absent,
    Present(Option<SlotWalStatus>),
}

/// Spawn a read-only monitor for the configured replication slot.
///
/// Connection/catalog failures are logged and retried. A missing slot or catalog `lost` state is
/// terminal: continuing would imply that an unknown WAL interval can be reconciled, so the monitor
/// cancels the pipeline and returns an error. This task has no handle to feedback/ACK state.
#[must_use = "the slot guard must be joined so terminal slot loss reaches the pipeline"]
pub fn spawn(
    source_db_url: String,
    slot: String,
    period: Duration,
    token: CancellationToken,
) -> JoinHandle<anyhow::Result<()>> {
    tokio::spawn(run(
        source_db_url,
        slot,
        period.max(Duration::from_secs(1)),
        token,
    ))
}

async fn run(
    source_db_url: String,
    slot: String,
    period: Duration,
    token: CancellationToken,
) -> anyhow::Result<()> {
    common::metrics::set_slot_guard_unknown();
    let mut prior_state = None;
    loop {
        let Some(connected) = cancellable_query(&token, async {
            tokio_postgres::connect(&source_db_url, tokio_postgres::NoTls)
                .await
                .context("connect replication-slot guard SQL session")
        })
        .await
        else {
            return Ok(());
        };
        let (client, connection) = match connected {
            Ok(value) => value,
            Err(error) => {
                common::metrics::set_slot_guard_unknown();
                tracing::warn!(%slot, error = ?error, "replication-slot guard connection failed; retrying");
                if cancelled_during_delay(&token, period).await {
                    return Ok(());
                }
                continue;
            }
        };
        let connection_task = tokio::spawn(async move {
            if let Err(error) = connection.await {
                tracing::warn!(error = ?error, "replication-slot guard SQL connection closed");
            }
        });

        let Some(capabilities) = cancellable_query(&token, read_capabilities(&client)).await else {
            drop(client);
            connection_task.abort();
            return Ok(());
        };
        let capabilities = match capabilities {
            Ok(value) => value,
            Err(error) => {
                common::metrics::set_slot_guard_unknown();
                tracing::warn!(%slot, error = ?error, "replication-slot guard capability probe failed; retrying");
                drop(client);
                connection_task.abort();
                if cancelled_during_delay(&token, period).await {
                    return Ok(());
                }
                continue;
            }
        };
        if !capabilities.wal_status || !capabilities.safe_wal_size {
            tracing::warn!(
                %slot,
                wal_status_supported = capabilities.wal_status,
                safe_wal_size_supported = capabilities.safe_wal_size,
                "source PostgreSQL lacks optional replication-slot guard columns; using an explicit compatibility query"
            );
        }

        let mut ticker = tokio::time::interval(period);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    drop(client);
                    connection_task.abort();
                    return Ok(());
                }
                _ = ticker.tick() => {
                    let Some(sample) = cancellable_query(
                        &token,
                        read_sample(&client, &slot, capabilities),
                    ).await else {
                        drop(client);
                        connection_task.abort();
                        return Ok(());
                    };
                    match sample {
                        Ok(sample) => {
                            if let Err(error) = observe_sample(
                                &slot,
                                capabilities,
                                sample,
                                &mut prior_state,
                            ) {
                                token.cancel();
                                drop(client);
                                connection_task.abort();
                                return Err(error);
                            }
                        }
                        Err(error) => {
                            common::metrics::set_slot_guard_unknown();
                            tracing::warn!(%slot, error = ?error, "replication-slot guard poll failed; reconnecting");
                            break;
                        }
                    }
                }
            }
        }
        drop(client);
        connection_task.abort();
        if cancelled_during_delay(&token, period).await {
            return Ok(());
        }
    }
}

async fn cancellable_query<T>(
    token: &CancellationToken,
    query: impl Future<Output = anyhow::Result<T>>,
) -> Option<anyhow::Result<T>> {
    tokio::select! {
        _ = token.cancelled() => None,
        result = tokio::time::timeout(QUERY_TIMEOUT, query) => Some(
            result
                .context("replication-slot guard query timed out")
                .and_then(std::convert::identity),
        ),
    }
}

async fn cancelled_during_delay(token: &CancellationToken, delay: Duration) -> bool {
    tokio::select! {
        _ = token.cancelled() => true,
        _ = tokio::time::sleep(delay) => false,
    }
}

async fn read_capabilities(
    client: &tokio_postgres::Client,
) -> anyhow::Result<SlotCatalogCapabilities> {
    let row = client
        .query_one(CAPABILITIES_SQL, &[])
        .await
        .context("query pg_replication_slots column capabilities")?;
    Ok(SlotCatalogCapabilities {
        wal_status: row.try_get(0).context("decode wal_status capability")?,
        safe_wal_size: row.try_get(1).context("decode safe_wal_size capability")?,
    })
}

async fn read_sample(
    client: &tokio_postgres::Client,
    slot: &str,
    capabilities: SlotCatalogCapabilities,
) -> anyhow::Result<Option<SlotSample>> {
    let Some(row) = client
        .query_opt(capabilities.sample_sql(), &[&slot])
        .await
        .with_context(|| format!("poll replication slot {slot:?}"))?
    else {
        return Ok(None);
    };
    SlotSample::try_from_raw(RawSlotSample {
        current_lsn: row.try_get(0).context("decode current_lsn")?,
        restart_lsn: row.try_get(1).context("decode restart_lsn")?,
        confirmed_flush_lsn: row.try_get(2).context("decode confirmed_flush_lsn")?,
        wal_status: row.try_get(3).context("decode wal_status")?,
        safe_wal_size: row.try_get(4).context("decode safe_wal_size")?,
    })
    .map(Some)
}

fn observe_sample(
    slot: &str,
    capabilities: SlotCatalogCapabilities,
    sample: Option<SlotSample>,
    prior_state: &mut Option<ObservedState>,
) -> anyhow::Result<()> {
    let state = match sample {
        None => {
            common::metrics::set_slot_guard(
                0,
                0,
                None,
                false,
                capabilities.wal_status,
                capabilities.safe_wal_size,
                Some(2),
            );
            ObservedState::Absent
        }
        Some(sample) => {
            let lag = sample.replication_lag_bytes();
            let retained = sample.retained_wal_bytes();
            common::metrics::set_slot_guard(
                lag,
                retained,
                sample.safe_wal_size,
                true,
                capabilities.wal_status,
                capabilities.safe_wal_size,
                sample.wal_status.as_ref().map(SlotWalStatus::gauge_code),
            );
            tracing::debug!(
                %slot,
                current_lsn = %sample.current_lsn,
                confirmed_flush_lsn = ?sample.confirmed_flush_lsn,
                restart_lsn = ?sample.restart_lsn,
                replication_lag_bytes = lag,
                retained_wal_bytes = retained,
                safe_wal_size = ?sample.safe_wal_size,
                wal_status = ?sample.wal_status,
                "replication-slot guard sample"
            );
            ObservedState::Present(sample.wal_status)
        }
    };

    if prior_state.as_ref() != Some(&state) {
        match &state {
            ObservedState::Absent => {
                tracing::error!(%slot, "configured replication slot is absent");
            }
            ObservedState::Present(Some(SlotWalStatus::Unreserved)) => {
                tracing::warn!(%slot, "replication slot WAL is unreserved and may be removed");
            }
            ObservedState::Present(Some(SlotWalStatus::Lost)) => {
                tracing::error!(%slot, "replication slot WAL has been lost");
            }
            ObservedState::Present(Some(SlotWalStatus::Unknown(value))) => {
                tracing::warn!(%slot, wal_status = %value, "source reported an unknown replication-slot wal_status");
            }
            ObservedState::Present(status) => {
                tracing::info!(%slot, ?status, "replication-slot guard state changed");
            }
        }
        *prior_state = Some(state);
    }
    match prior_state.as_ref() {
        Some(ObservedState::Absent) => {
            anyhow::bail!("configured replication slot {slot:?} is absent")
        }
        Some(ObservedState::Present(Some(SlotWalStatus::Lost))) => {
            anyhow::bail!("replication slot {slot:?} has lost required WAL")
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
#[path = "guard_monitor_test.rs"]
mod tests;
