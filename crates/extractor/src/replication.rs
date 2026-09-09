//! The hand-rolled logical-replication consumer (§1.2 — "we own the connection … don't adopt a
//! framework") and its standby-status keepalive feedback (§1.9).
//!
//! **Implementation choice:** `tokio-postgres` 0.7 has **no** replication surface — no way
//! to open a `replication=database` connection, no CopyBoth duplex. Rather than adopt
//! `pgwire-replication`, we hand-roll the wire protocol over a raw `TcpStream`, exactly as §1.2
//! prescribes: a Startup handshake, `START_REPLICATION`, then the CopyBoth byte stream (`'w'`
//! XLogData / `'k'` primary keepalive), replying with `'r'` standby-status updates. The dev harness
//! uses `trust` auth so the handshake carries no SCRAM (SCRAM would be added here if a
//! password-authed source were required). The [`ReplicationStream`] / [`ReplicationMessage`] /
//! [`StandbyStatus`] seam is unchanged for callers, so the decoder plugs in regardless.
//!
//! **Two LSNs, kept apart (§1.9):** the *received* LSN (sent as `write` to stay connected) advances
//! here on every frame; `flush`/`apply` (= `confirmed_flush_lsn`, which releases source WAL) only
//! advance through [`crate::checkpoint`] after durability, so we hold them at the durable baseline.
//! Keepalive feedback is
//! **unconditional**: it goes out well under `wal_sender_timeout`, never gated on S3 durability, or
//! the walsender severs us with a reconnect storm.
//!
//! **The connection's two states are two types.** A `replication=database` connection that has only
//! completed the startup handshake is *not* in CopyBoth: reading a frame or writing a `'r'`
//! standby-status there is a wire-protocol violation, and the walsender's reply would be decoded as
//! garbage rather than rejected. So [`ReplicationStream`] carries which state it is in
//! ([`Idle`] vs [`Streaming`]) as a type parameter, and the only way to reach the streaming API is
//! [`ReplicationStream::into_streaming`], which consumes the idle connection and is the Rust half of
//! `START_REPLICATION`'s `CopyBothResponse`. "Forgot to `START_REPLICATION`" is a compile error, not
//! a torn stream.

use anyhow::{Context, anyhow, bail};
use bytes::{Bytes, BytesMut};
use common::{Lsn, PG_EPOCH_UNIX_MICROS};
use std::marker::PhantomData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::Instant;

/// Default feedback cadence: well under any sane `wal_sender_timeout` (the dev harness uses 5s).
const DEFAULT_FEEDBACK_INTERVAL: Duration = Duration::from_secs(1);
/// Pin session-sensitive PostgreSQL text output to the grammar shared by WAL decoding and reload
/// COPY. Startup `options` applies these before pgoutput can emit a tuple.
const CANONICAL_TEXT_OUTPUT_STARTUP_OPTIONS: &str = "-c DateStyle=ISO,YMD -c IntervalStyle=postgres -c bytea_output=hex \
     -c extra_float_digits=3 -c TimeZone=UTC";

/// One CopyBoth frame off the wire. The XLogData payload stays opaque `Bytes` — the pgoutput
/// decoder consumes it directly (zero-copy).
#[derive(Debug)]
pub enum ReplicationMessage {
    /// `'w'` — XLogData.
    XLogData {
        /// First WAL position represented by the payload.
        wal_start: Lsn,
        /// Server WAL end at frame emission time.
        wal_end: Lsn,
        /// PostgreSQL protocol timestamp in microseconds since 2000-01-01.
        server_clock: i64,
        /// Opaque pgoutput payload bytes.
        data: Bytes,
    },
    /// `'k'` — primary keepalive.
    Keepalive {
        /// Server WAL end at keepalive emission time.
        wal_end: Lsn,
        /// PostgreSQL protocol timestamp in microseconds since 2000-01-01.
        server_clock: i64,
        /// Whether the server requests an immediate status reply.
        reply_requested: bool,
    },
}

/// A `'r'` standby status update. **`write ≥ flush ≥ apply`.** The keepalive path moves only `write`
/// (the received LSN); durability is the only thing that advances `flush`/`apply`.
#[derive(Clone, Copy, Debug)]
pub struct StandbyStatus {
    /// Highest LSN received. Moved by the keepalive path, and it does **not** free WAL.
    pub write: Lsn,
    /// Highest LSN made durable. This is what lets the source discard WAL, so only a completed
    /// durability step may advance it.
    pub flush: Lsn,
    /// Highest LSN applied. walrus keeps it equal to `flush`; it has no separate apply stage.
    pub apply: Lsn,
    /// Ask the server to reply immediately rather than at its own cadence — set when the server
    /// demanded a reply, or when a prompt answer keeps the connection from timing out.
    pub reply_requested: bool,
}

/// State marker: the startup handshake is done but no `START_REPLICATION` has been issued, so the
/// connection is in the simple-query state, not CopyBoth.
#[derive(Debug, Clone, Copy)]
pub struct Idle;

/// State marker: `START_REPLICATION` returned `CopyBothResponse`, so CopyBoth frames may flow.
#[derive(Debug, Clone, Copy)]
pub struct Streaming;

/// A hand-rolled replication connection, typed by which protocol state it is in. [`Streaming`] is the
/// default because every consumer of this module ([`crate::consume`], [`crate::shutdown`],
/// [`crate::checkpoint`]) only ever holds a live CopyBoth stream; the [`Idle`] form ensures startup
/// completes before the connection can enter CopyBoth mode.
///
/// Frames cannot be read before `START_REPLICATION`:
///
/// ```compile_fail
/// # use extractor::replication::{Idle, ReplicationStream};
/// # async fn demo(dsn: &str) -> anyhow::Result<()> {
/// let mut conn = ReplicationStream::<Idle>::connect(dsn).await?;
/// let _frame = conn.next().await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
pub struct ReplicationStream<S = Streaming> {
    stream: TcpStream,
    rbuf: BytesMut,
    /// The highest LSN we've received (sent as `write` in feedback).
    last_received: Lsn,
    /// The durable baseline (`flush`/`apply`); updated only after the checkpoint advances it.
    durable: Lsn,
    /// Unconditional-feedback cadence (< `wal_sender_timeout`).
    feedback_interval: Duration,
    /// When the next unconditional feedback is due.
    feedback_deadline: Instant,
    /// Which protocol state the connection is in. Stores nothing — the compiler reads it, the wire
    /// never does.
    _state: PhantomData<S>,
}

impl ReplicationStream<Idle> {
    /// Open a `replication=database` connection and complete the startup handshake **without** yet
    /// issuing `START_REPLICATION`. The caller then consumes it with
    /// [`into_streaming`](Self::into_streaming).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if the DSN is invalid, the TCP connection fails, or the replication
    /// startup handshake is rejected or malformed.
    pub async fn connect(dsn: &str) -> anyhow::Result<Self> {
        let (host, port, user, database) = parse_dsn(dsn)?;
        let stream = TcpStream::connect((host.as_str(), port))
            .await
            .with_context(|| format!("connect to source {host}:{port} for replication"))?;
        let mut this = ReplicationStream {
            stream,
            rbuf: BytesMut::with_capacity(16 * 1024),
            last_received: Lsn::ZERO,
            durable: Lsn::ZERO,
            feedback_interval: DEFAULT_FEEDBACK_INTERVAL,
            feedback_deadline: Instant::now() + DEFAULT_FEEDBACK_INTERVAL,
            _state: PhantomData,
        };
        this.startup(&user, &database).await?;
        Ok(this)
    }

    /// Issue `START_REPLICATION` from `start_lsn`, seeding the received/durable baselines, and hand
    /// back the streaming half of the connection.
    ///
    /// `into_`, not `start_`: the idle connection is **spent** here. Only one of the two states may
    /// exist at a time, so the idle handle cannot linger and issue a second simple query into what is
    /// now a CopyBoth stream. A failed transition drops the connection rather than leaving a
    /// half-started one behind.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if PostgreSQL rejects the replication command or the CopyBoth
    /// response cannot be written, read, or decoded.
    pub async fn into_streaming(
        self,
        slot: &str,
        start_lsn: Lsn,
        publication: &str,
    ) -> anyhow::Result<ReplicationStream<Streaming>> {
        self.into_streaming_with_feedback_floor(slot, start_lsn, start_lsn, publication)
            .await
    }

    /// Issue `START_REPLICATION` at `start_lsn` while keeping feedback's durable floor at an older
    /// retained position. New-generation bootstrap uses this to claim the source slot before its
    /// control-plane compare-and-set: keepalives may prove liveness, but cannot release WAL through
    /// the new boundary until that generation is durable.
    ///
    /// # Errors
    ///
    /// Returns an error if `feedback_floor` exceeds `start_lsn`, or for the same connection and
    /// replication-protocol failures as [`Self::into_streaming`].
    pub async fn into_streaming_with_feedback_floor(
        mut self,
        slot: &str,
        start_lsn: Lsn,
        feedback_floor: Lsn,
        publication: &str,
    ) -> anyhow::Result<ReplicationStream<Streaming>> {
        anyhow::ensure!(
            feedback_floor <= start_lsn,
            "replication feedback floor {feedback_floor} exceeds start LSN {start_lsn}"
        );
        self.last_received = start_lsn;
        self.durable = feedback_floor;
        self.feedback_deadline = Instant::now() + self.feedback_interval;
        self.try_acquire_publication_ddl_guard().await?;
        self.begin_replication(slot, start_lsn, publication).await?;
        Ok(ReplicationStream {
            stream: self.stream,
            rbuf: self.rbuf,
            last_received: self.last_received,
            durable: self.durable,
            feedback_interval: self.feedback_interval,
            feedback_deadline: self.feedback_deadline,
            _state: PhantomData,
        })
    }
}

impl ReplicationStream<Streaming> {
    /// Connect, hand-shake, and issue `START_REPLICATION SLOT … LOGICAL <lsn> (proto_version '2',
    /// streaming 'on', publication_names '<publication>')`. `dsn` is parsed for host/port/user/db
    /// (its auth is `trust` in the dev harness).
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if the DSN is invalid, TCP/startup negotiation fails, or PostgreSQL
    /// rejects `START_REPLICATION`.
    pub async fn start(
        dsn: &str,
        slot: &str,
        start_lsn: Lsn,
        publication: &str,
    ) -> anyhow::Result<Self> {
        ReplicationStream::<Idle>::connect(dsn)
            .await?
            .into_streaming(slot, start_lsn, publication)
            .await
    }

    /// Claim a slot at `start_lsn` without acknowledging beyond `feedback_floor` until the caller
    /// explicitly advances durability with [`Self::set_durable`].
    ///
    /// # Errors
    ///
    /// Returns the same errors as [`ReplicationStream::<Idle>::into_streaming_with_feedback_floor`].
    pub async fn start_with_feedback_floor(
        dsn: &str,
        slot: &str,
        start_lsn: Lsn,
        feedback_floor: Lsn,
        publication: &str,
    ) -> anyhow::Result<Self> {
        ReplicationStream::<Idle>::connect(dsn)
            .await?
            .into_streaming_with_feedback_floor(slot, start_lsn, feedback_floor, publication)
            .await
    }

    /// Override the unconditional-feedback cadence (must stay under the source's `wal_sender_timeout`).
    /// Tests use a long interval to let the server *demand* a reply (`reply_requested`).
    pub fn set_feedback_interval(&mut self, interval: Duration) {
        self.feedback_interval = interval;
        self.feedback_deadline = Instant::now() + interval;
    }

    /// Time remaining until the next unconditional feedback is due. The flush path races this
    /// against a slow S3 PUT so keepalive keeps flowing while the read loop is busy — a stalled flush
    /// must never starve the walsender past `wal_sender_timeout` (§1.9). Saturates to zero when overdue.
    ///
    /// This read and the two watermark reads below compute a value and touch no wire state, so each
    /// carries `#[must_use]` explicitly. `clippy::must_use_candidate` reaches none of them: the
    /// [`TcpStream`] inside `&self` reads to that lint as a mutable — therefore side-effecting —
    /// argument, which is why this module has no lint-driven annotation to inherit.
    #[must_use]
    pub fn feedback_budget(&self) -> Duration {
        self.feedback_deadline
            .saturating_duration_since(Instant::now())
    }

    /// Read one frame. Sends unconditional feedback whenever the interval elapses (so an idle stream
    /// stays alive), and answers a `reply_requested` keepalive immediately. `None` on stream end.
    ///
    /// ## Cancel safety
    ///
    /// **Not cancel-safe.** Buffered reads retain partial bytes in `self.rbuf`, but this operation
    /// also calls [`Self::send_received_feedback`], whose direct socket write can be left as a
    /// partial standby-status frame if the future is dropped. Callers must pin one `next()` future
    /// and poll it by mutable reference across sibling `select!` branches.
    ///
    /// The remaining drop point is decode-loop cancellation. If it tears a frame, the connection
    /// may remain unrecoverable until disconnect/reconnect: after flushing committed data, the drain
    /// attempts a standby-status frame and `CopyDone`, but those writes are best-effort and cannot
    /// repair framing. Fully eliminating the residual requires resumable outbound staging in a
    /// `wbuf` field mirroring `rbuf`, which is deferred.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if reading or parsing a backend frame fails, PostgreSQL reports an
    /// error, an unexpected CopyBoth message arrives, or periodic feedback cannot be sent.
    pub async fn next(&mut self) -> anyhow::Result<Option<ReplicationMessage>> {
        loop {
            let budget = self
                .feedback_deadline
                .saturating_duration_since(Instant::now());
            let Ok(frame) = tokio::time::timeout(budget, self.read_message()).await else {
                // Feedback due — send it (received LSN as write) and keep waiting.
                self.send_received_feedback(false).await?;
                continue;
            };
            let (tag, body) = frame?;
            match tag {
                b'd' => {
                    if let Some(msg) = self.handle_copy_data(body).await? {
                        return Ok(Some(msg));
                    }
                }
                // CopyDone / ReadyForQuery — the stream ended.
                b'c' | b'Z' => return Ok(None),
                // CommandComplete / NoticeResponse / ParameterStatus — keep going.
                b'C' | b'N' | b'S' => {}
                b'E' => bail!("replication stream error: {}", error_message(&body)),
                other => bail!(
                    "unexpected message '{}' on the CopyBoth stream",
                    char::from(other)
                ),
            }
        }
    }

    /// Send an `'r'` standby status update. Callers use this to advance `flush`/`apply` on
    /// durability; the keepalive path uses [`Self::send_received_feedback`].
    ///
    /// ## Cancel safety
    ///
    /// **Not cancel-safe.** Dropping during `write_all` or `flush` can leave a partial frame on the
    /// CopyBoth socket. Callers await it to completion inside a selected branch body, or through a
    /// pinned [`Self::next`] future, so a sibling branch cannot tear the write.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if writing or flushing the status frame to PostgreSQL fails.
    pub async fn send_standby_status(&mut self, s: StandbyStatus) -> anyhow::Result<()> {
        self.stream
            .write_all(&build_standby_status(s))
            .await
            .context("write standby status")?;
        self.stream.flush().await.context("flush standby status")?;
        self.feedback_deadline = Instant::now() + self.feedback_interval;
        Ok(())
    }

    /// Send `CopyDone` and flush — end our side of the CopyBoth stream on a graceful drain.
    /// The replication **slot is untouched** (never `DROP_REPLICATION_SLOT`); a replacement pod
    /// resumes from `confirmed_flush_lsn`. `CopyDone` is a bare frame: tag `'c'`, Int32 length `4`.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if writing or flushing the `CopyDone` frame fails.
    pub async fn copy_done(&mut self) -> anyhow::Result<()> {
        self.stream
            .write_all(&[b'c', 0, 0, 0, 4])
            .await
            .context("write CopyDone")?;
        self.stream.flush().await.context("flush CopyDone")?;
        Ok(())
    }

    /// The highest received LSN (what the keepalive path reports as `write`).
    #[must_use]
    pub const fn last_received(&self) -> Lsn {
        self.last_received
    }

    /// Advance the durable (`flush`/`apply`) baseline the periodic keepalive reports — set by the
    /// durability checkpoint only after S3 + manifest are durable. Never regresses.
    pub fn set_durable(&mut self, lsn: Lsn) {
        self.durable = self.durable.max(lsn);
    }

    /// The current durable (`confirmed_flush`) baseline.
    #[must_use]
    pub const fn durable(&self) -> Lsn {
        self.durable
    }

    // ---- CopyBoth internals -------------------------------------------------------------------

    async fn handle_copy_data(
        &mut self,
        body: Bytes,
    ) -> anyhow::Result<Option<ReplicationMessage>> {
        match body.first().copied() {
            Some(b'w') => {
                // 'w'(1) walStart(8) walEnd(8) clock(8) data(rest)
                if body.len() < 25 {
                    bail!("short XLogData frame ({} bytes)", body.len());
                }
                let wal_start = read_lsn(&body[1..9])?;
                let wal_end = read_lsn(&body[9..17])?;
                let server_clock = read_i64(&body[17..25])?;
                let data = body.slice(25..);
                self.last_received = self.last_received.max(wal_end.max(wal_start));
                Ok(Some(ReplicationMessage::XLogData {
                    wal_start,
                    wal_end,
                    server_clock,
                    data,
                }))
            }
            Some(b'k') => {
                // 'k'(1) walEnd(8) clock(8) replyRequested(1)
                if body.len() < 18 {
                    bail!("short keepalive frame ({} bytes)", body.len());
                }
                let wal_end = read_lsn(&body[1..9])?;
                let server_clock = read_i64(&body[9..17])?;
                let reply_requested = body[17] != 0;
                self.last_received = self.last_received.max(wal_end);
                // A demanded reply is answered *immediately*, not on the next interval.
                if reply_requested {
                    self.send_received_feedback(false).await?;
                }
                Ok(Some(ReplicationMessage::Keepalive {
                    wal_end,
                    server_clock,
                    reply_requested,
                }))
            }
            other => bail!("unknown CopyData sub-type {other:?}"),
        }
    }

    /// Feedback carrying the received LSN as `write`; `flush`/`apply` stay at the durable baseline.
    /// Public so the flush path can pump keepalive while a slow S3 PUT blocks the read loop — the PUT
    /// touches the object store, not this socket, so feedback rides concurrently (§1.9: keepalive is
    /// unconditional, never gated on durability). Resets the feedback deadline on each send.
    ///
    /// ## Cancel safety
    ///
    /// **Not cancel-safe.** It delegates to [`Self::send_standby_status`], whose socket write must
    /// finish once started. The decode loop preserves the enclosing [`Self::next`] future across
    /// heartbeat ticks, and the flush keepalive branch awaits this method to completion.
    ///
    /// # Errors
    ///
    /// Returns [`anyhow::Error`] if [`Self::send_standby_status`] cannot write or flush the feedback.
    pub async fn send_received_feedback(&mut self, reply_requested: bool) -> anyhow::Result<()> {
        self.send_standby_status(StandbyStatus {
            write: self.last_received,
            flush: self.durable,
            apply: self.durable,
            reply_requested,
        })
        .await
    }
}

/// Framing and simple-query plumbing, shared by both states: reading a framed backend message and
/// writing a `'Q'` are the same bytes whether or not CopyBoth has started, so they are the one impl
/// block that does not name a state.
///
/// `S: Send` is not a state-machine requirement — it is what keeps these `async fn`s' futures `Send`
/// for the multi-thread scheduler. Auto traits leak through `PhantomData<S>`, so an unbounded `S`
/// would make `&mut Self` (and every future holding it) conditionally `Send`. Both markers are ZSTs,
/// so the bound costs callers nothing.
impl<S: Send> ReplicationStream<S> {
    /// Buffered, cancellation-safe read of one backend message (`tag`, `body`). Retained bytes in
    /// `rbuf` survive a cancelled `read_buf`, so the feedback timer can cancel this mid-wait.
    async fn read_message(&mut self) -> anyhow::Result<(u8, Bytes)> {
        loop {
            if let Some(msg) = take_message(&mut self.rbuf) {
                return Ok(msg);
            }
            let n = self
                .stream
                .read_buf(&mut self.rbuf)
                .await
                .context("read from source replication connection")?;
            if n == 0 {
                bail!("source closed the replication connection");
            }
        }
    }

    /// Protocol-3.0 startup with `replication=database`; the dev harness answers `trust` (no SCRAM).
    async fn startup(&mut self, user: &str, database: &str) -> anyhow::Result<()> {
        let startup_message = build_startup(user, database)?;
        self.stream
            .write_all(&startup_message)
            .await
            .context("send StartupMessage")?;
        self.stream.flush().await.context("flush StartupMessage")?;
        loop {
            let (tag, body) = self.read_message().await?;
            match tag {
                b'R' => {
                    let sub = auth_sub_type(&body)?;
                    if sub != 0 {
                        bail!(
                            "source demands auth type {sub}; the dev harness must use trust auth \
                             (this replication client does not implement SCRAM)"
                        );
                    }
                }
                // ParameterStatus / BackendKeyData / NoticeResponse — ignore.
                b'S' | b'K' | b'N' => {}
                b'Z' => return Ok(()), // ReadyForQuery
                b'E' => bail!("startup failed: {}", error_message(&body)),
                other => bail!("unexpected startup message '{}'", char::from(other)),
            }
        }
    }

    async fn begin_replication(
        &mut self,
        slot: &str,
        start_lsn: Lsn,
        publication: &str,
    ) -> anyhow::Result<()> {
        let sql = format!(
            "START_REPLICATION SLOT {slot} LOGICAL {} \
             (proto_version '2', streaming 'on', publication_names '{publication}')",
            lsn_xy(start_lsn)
        );
        self.send_query(&sql).await?;
        loop {
            let (tag, body) = self.read_message().await?;
            match tag {
                b'W' => return Ok(()), // CopyBothResponse — streaming has begun
                b'N' | b'S' => {}
                b'E' => bail!("START_REPLICATION failed: {}", error_message(&body)),
                other => bail!(
                    "unexpected reply '{}' to START_REPLICATION",
                    char::from(other)
                ),
            }
        }
    }

    /// Tie the guard to the CopyBoth backend as well as the orchestration SQL session. This MUST be
    /// a try-lock: if an exclusive publication-DDL request queued behind the first shared holder,
    /// blocking for a second shared lock would deadlock startup against that writer.
    async fn try_acquire_publication_ddl_guard(&mut self) -> anyhow::Result<()> {
        let key = crate::source_catalog::PUBLICATION_DDL_GUARD_KEY;
        self.send_query(&format!(
            "SELECT pg_catalog.pg_try_advisory_lock_shared({key})"
        ))
        .await?;
        let mut acquired = None;
        loop {
            let (tag, body) = self.read_message().await?;
            match tag {
                b'D' => acquired = Some(data_row_bool(&body)?),
                b'T' | b'C' | b'I' | b'N' | b'S' => {}
                b'Z' => {
                    return match acquired {
                        Some(true) => Ok(()),
                        Some(false) => bail!(
                            "publication DDL queued while replication was starting; retry startup"
                        ),
                        None => bail!("publication-DDL guard query returned no row"),
                    };
                }
                b'E' => {
                    bail!(
                        "acquire publication-DDL guard failed: {}",
                        error_message(&body)
                    );
                }
                other => bail!(
                    "unexpected reply '{}' while acquiring publication-DDL guard",
                    char::from(other)
                ),
            }
        }
    }

    async fn send_query(&mut self, sql: &str) -> anyhow::Result<()> {
        let capacity = sql
            .len()
            .checked_add(6)
            .context("query too large to buffer for the wire protocol")?;
        let len = sql
            .len()
            .checked_add(5)
            .and_then(|len| u32::try_from(len).ok())
            .with_context(|| {
                format!("query too long for the wire protocol: {} bytes", sql.len())
            })?;
        let mut msg = Vec::with_capacity(capacity);
        msg.push(b'Q');
        msg.extend_from_slice(&len.to_be_bytes());
        msg.extend_from_slice(sql.as_bytes());
        msg.push(0);
        self.stream.write_all(&msg).await.context("send Query")?;
        self.stream.flush().await.context("flush Query")?;
        Ok(())
    }
}

/// Micros since the Postgres epoch (2000-01-01), for the standby-status timestamp.
fn pg_epoch_micros() -> i64 {
    // INTENTIONAL discard: `duration_since` fails only on a clock set before 1970, this runs once
    // per feedback frame (so a log would flood rather than inform), and there is no `Result` to
    // return through — a keepalive must not fail on a clock quirk. Zero back-dates the stamp, which
    // Postgres reads for `pg_stat_replication` lag only; slot advancement rides the LSNs beside it.
    let unix = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    // The offset is `common`'s derived constant, not a product recomputed per feedback frame.
    i64::try_from(unix.as_micros())
        .unwrap_or(i64::MAX)
        .saturating_sub(PG_EPOCH_UNIX_MICROS)
}

fn build_startup(user: &str, database: &str) -> anyhow::Result<Vec<u8>> {
    let mut params = Vec::new();
    for (k, v) in [
        ("user", user),
        ("database", database),
        ("replication", "database"),
        ("client_encoding", "UTF8"),
        // pgoutput sends text values through the source type-output functions. Pin their
        // session-sensitive forms to the exact grammar pg-to-arrow parses, matching reload COPY.
        ("options", CANONICAL_TEXT_OUTPUT_STARTUP_OPTIONS),
    ] {
        params.extend_from_slice(k.as_bytes());
        params.push(0);
        params.extend_from_slice(v.as_bytes());
        params.push(0);
    }
    params.push(0); // parameter-list terminator
    let len = params
        .len()
        .checked_add(8)
        .context("startup message length overflow")?;
    let wire_len = u32::try_from(len).context("startup message exceeds the Int32 wire limit")?;
    let mut msg = Vec::with_capacity(len);
    msg.extend_from_slice(&wire_len.to_be_bytes());
    msg.extend_from_slice(&196_608u32.to_be_bytes()); // protocol 3.0
    msg.extend_from_slice(&params);
    Ok(msg)
}

/// A whole `'r'` frame: the `'d'` tag, its 4-byte length, and the fixed 34-byte CopyData payload.
/// The protocol fixes every field, so this is a compile-time width, not a capacity hint.
const STANDBY_STATUS_FRAME_BYTES: usize = 39;

/// Build the fixed-width `'r'` frame in a stack array — no field is variable-width, so the feedback
/// path allocates nothing (`copy_done` writes its bare frame the same way). The offsets below are
/// the wire layout; `replication_test::standby_status_frame_layout` reads them back.
fn build_standby_status(s: StandbyStatus) -> [u8; STANDBY_STATUS_FRAME_BYTES] {
    let mut msg = [0u8; STANDBY_STATUS_FRAME_BYTES];
    msg[0] = b'd';
    // CopyData's payload is fixed: one tag + three LSNs + one timestamp + one reply byte = 34 bytes,
    // and the self-inclusive length adds its own 4 bytes but excludes the tag.
    msg[1..5].copy_from_slice(&38_u32.to_be_bytes());
    msg[5] = b'r';
    msg[6..14].copy_from_slice(&s.write.as_u64().to_be_bytes());
    msg[14..22].copy_from_slice(&s.flush.as_u64().to_be_bytes());
    msg[22..30].copy_from_slice(&s.apply.as_u64().to_be_bytes());
    msg[30..38].copy_from_slice(&pg_epoch_micros().to_be_bytes());
    msg[38] = u8::from(s.reply_requested);
    msg
}

/// Take one framed backend message (`tag` + 4-byte self-inclusive length + body) from `buf`, or
/// `None` if a full message is not yet buffered.
fn take_message(buf: &mut BytesMut) -> Option<(u8, Bytes)> {
    // The header is a compile-time width, so one `first_chunk` proves the tag and the length field
    // together — `None` is a buffer too short to hold them. Only the body that follows is
    // variable-width, which is what the runtime check below is for.
    let [tag, len_be @ ..] = *buf.first_chunk::<5>()?;
    let len = usize::try_from(u32::from_be_bytes(len_be)).ok()?;
    let total = len.checked_add(1)?; // tag + (length field + body)
    if buf.len() < total {
        return None;
    }
    let msg = buf.split_to(total).freeze();
    Some((tag, msg.slice(5..)))
}

/// `X/Y` upper-hex LSN — the only form `START_REPLICATION` accepts (not the 16-hex `Display`).
fn lsn_xy(lsn: Lsn) -> String {
    let v = lsn.as_u64();
    format!("{:X}/{:X}", v >> 32, v & 0xFFFF_FFFF)
}

fn fixed<const N: usize>(b: &[u8], what: &str) -> anyhow::Result<[u8; N]> {
    b.try_into()
        .map_err(|_| anyhow!("{what}: expected {N} bytes, got {}", b.len()))
}

fn read_lsn(b: &[u8]) -> anyhow::Result<Lsn> {
    Ok(Lsn::new(u64::from_be_bytes(fixed(b, "read_lsn")?)))
}
fn read_i64(b: &[u8]) -> anyhow::Result<i64> {
    Ok(i64::from_be_bytes(fixed(b, "read_i64")?))
}
fn read_i32(b: &[u8]) -> anyhow::Result<i32> {
    Ok(i32::from_be_bytes(fixed(b, "read_i32")?))
}

/// Decode the single text boolean returned by `pg_try_advisory_lock_shared`.
fn data_row_bool(body: &[u8]) -> anyhow::Result<bool> {
    let count = u16::from_be_bytes(fixed(
        body.get(..2).unwrap_or_default(),
        "DataRow column count",
    )?);
    anyhow::ensure!(count == 1, "guard DataRow has {count} columns, expected 1");
    let len = read_i32(
        body.get(2..6)
            .context("guard DataRow is missing its value length")?,
    )?;
    anyhow::ensure!(
        len == 1,
        "guard DataRow boolean has length {len}, expected 1"
    );
    match body.get(6).copied() {
        Some(b't') => Ok(true),
        Some(b'f') => Ok(false),
        other => bail!("guard DataRow is not a text boolean: {other:?}"),
    }
}

/// The `Int32` sub-type of an Authentication ('R') body. `take_message` frames on the 4-byte length
/// header only, so the body it hands back may be shorter than the sub-type field — a truncated frame
/// is a protocol error the handshake reports, never an out-of-bounds slice that aborts the extractor.
fn auth_sub_type(body: &[u8]) -> anyhow::Result<i32> {
    let head = body
        .get(..4)
        .with_context(|| format!("short Authentication message ({} bytes)", body.len()))?;
    read_i32(head)
}

/// The `'M'` (human message) field of an ErrorResponse/NoticeResponse body.
fn error_message(body: &[u8]) -> String {
    // The body is a run of NUL-terminated `(type byte, text)` fields closed by a bare NUL, so a
    // split on NUL yields exactly one field per element and the terminator surfaces as the first
    // empty one. A frame truncated mid-field simply ends the iterator on the same span the byte
    // cursor would have stopped at.
    body.split(|&b| b == 0)
        .take_while(|field| !field.is_empty())
        .find_map(|field| match field {
            [b'M', text @ ..] => Some(String::from_utf8_lossy(text).into_owned()),
            _ => None,
        })
        .unwrap_or_else(|| "(no message)".to_string())
}

fn parse_dsn(dsn: &str) -> anyhow::Result<(String, u16, String, String)> {
    let cfg: tokio_postgres::Config = dsn.parse().context("parse source DSN")?;
    let Some(tokio_postgres::config::Host::Tcp(host)) = cfg.get_hosts().first() else {
        bail!("replication DSN needs a TCP host");
    };
    let port = cfg.get_ports().first().copied().unwrap_or(5432);
    let user = cfg
        .get_user()
        .ok_or_else(|| anyhow!("replication DSN needs a user"))?
        .to_string();
    let database = cfg
        .get_dbname()
        .ok_or_else(|| anyhow!("replication DSN needs a dbname"))?
        .to_string();
    Ok((host.clone(), port, user, database))
}

#[cfg(test)]
#[path = "replication_test.rs"]
mod tests;
