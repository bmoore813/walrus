//! Hand-rolled pgoutput (`proto_version` 2) decoder. Sync + pure: no tokio, no I/O.
//!
//! The decoder is tested family-by-family against the golden vectors in
//! `tests/pgoutput_vectors.rs`, from [`Reader`] primitives and stream framing through all supported
//! transaction and row-change messages.

pub mod error;
pub mod reader;
pub mod typmod;

pub use error::DecodeError;
pub use reader::Reader;

use bytes::Bytes;
use common::{Lsn, PgColumn, PgRelation, ReplicaIdentity, TupleValue};

/// Message types that carry a 4-byte per-message xid immediately after the tag — but ONLY inside a
/// streamed block (proto §7): Relation, Type, Insert, Update, Delete, Truncate, Message.
const XID_PREFIXED: &[u8] = b"RYIUDTM";

/// Convert an Int32 protocol length/count to the platform index width without wrapping.
fn wire_usize(raw: u32) -> usize {
    usize::try_from(raw).unwrap_or(usize::MAX)
}

/// Whether we are inside a Stream Start..Stop block. The per-message xid prefix (proto §7) exists
/// **only** while this is true; Stream Start/Stop toggle it. It is threaded through
/// [`parse_stream`] so context carries across messages.
#[derive(Debug, Default, Clone, Copy)]
pub struct StreamCtx {
    /// Whether a Stream Start has been seen without its matching Stream Stop. While true, every
    /// message carries an xid prefix, so the decoder must read one.
    pub in_stream: bool,
}

/// The old-image submessage tag: `'K'` = key columns only (DEFAULT identity), `'O'` = the whole old
/// row (FULL identity).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OldTupleKind {
    /// `'K'` — only the replica-identity key columns are present; every other value is absent.
    Key,
    /// `'O'` — the complete old row, which the source sends only under `REPLICA IDENTITY FULL`.
    Full,
}

impl TryFrom<u8> for OldTupleKind {
    type Error = DecodeError;

    /// Classify the old-image submessage tag that precedes an Update/Delete tuple.
    ///
    /// # Errors
    ///
    /// Returns [`DecodeError::BadTupleFormat`] carrying `tag` for any byte other than `b'K'` or
    /// `b'O'`; the frame is then unparseable, so this is terminal rather than skippable.
    fn try_from(tag: u8) -> Result<Self, Self::Error> {
        match tag {
            b'K' => Ok(Self::Key),
            b'O' => Ok(Self::Full),
            other => {
                std::hint::cold_path();
                Err(DecodeError::BadTupleFormat { byte: other })
            }
        }
    }
}

/// One decoded pgoutput message, covering relation/type metadata, row changes, truncate/logical
/// messages, streamed transactions, and the two-phase family.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Message {
    /// `'B'`: Int64 final LSN, Int64 commit ts (µs since 2000-01-01), Int32 xid.
    Begin {
        /// Transaction's final WAL position, supplied before its row messages.
        final_lsn: Lsn,
        /// Commit timestamp in microseconds since 2000-01-01.
        commit_ts: i64,
        /// Top-level PostgreSQL transaction ID.
        xid: u32,
    },
    /// `'C'`: Int8 flags, Int64 commit LSN, Int64 end LSN, Int64 commit ts.
    Commit {
        /// Protocol flags reserved by PostgreSQL.
        flags: u8,
        /// WAL position where the transaction committed.
        commit_lsn: Lsn,
        /// WAL position immediately after the commit record.
        end_lsn: Lsn,
        /// Commit timestamp in microseconds since 2000-01-01.
        commit_ts: i64,
    },
    /// `'O'`: Int64 commit LSN, String origin name.
    Origin {
        /// WAL position of the origin record.
        commit_lsn: Lsn,
        /// Logical replication origin name.
        name: String,
    },
    /// `'R'`: the table shape (OID, namespace, name, replica identity, columns). `xid` is `Some`
    /// only inside a streamed block.
    Relation {
        /// Active streamed transaction, when the relation message is streamed.
        xid: Option<u32>,
        /// Complete relation identity and ordered column shape.
        relation: PgRelation,
    },
    /// `'Y'`: a non-builtin type announcement (e.g. our `mood` enum).
    Type {
        /// Active streamed transaction, when the type message is streamed.
        xid: Option<u32>,
        /// PostgreSQL type OID.
        oid: u32,
        /// Schema containing the announced type.
        namespace: String,
        /// Unqualified PostgreSQL type name.
        name: String,
    },
    /// `'I'`: Int32 relation OID, `Byte1('N')`, then the new TupleData.
    Insert {
        /// Active streamed transaction, when the row change is streamed.
        xid: Option<u32>,
        /// OID of the changed relation.
        relation_oid: u32,
        /// New row values in relation-column order.
        new: Vec<TupleValue>,
    },
    /// `'U'`: rel OID, then EITHER a (`'K'`|`'O'`) old tuple + `'N'`, OR straight to `'N'` (no old
    /// image — a non-key UPDATE under DEFAULT identity), then the new tuple.
    Update {
        /// Active streamed transaction, when the row change is streamed.
        xid: Option<u32>,
        /// OID of the changed relation.
        relation_oid: u32,
        /// Kind of old-row image present, or `None` when absent.
        old_kind: Option<OldTupleKind>,
        /// Old row or key values, when pgoutput supplied them.
        old: Option<Vec<TupleValue>>,
        /// New row values in relation-column order.
        new: Vec<TupleValue>,
    },
    /// `'D'`: rel OID, then a (`'K'`|`'O'`) old tuple (always present — how the transformer locates the
    /// row to remove).
    Delete {
        /// Active streamed transaction, when the row change is streamed.
        xid: Option<u32>,
        /// OID of the changed relation.
        relation_oid: u32,
        /// Whether the old payload is a key or full row.
        old_kind: OldTupleKind,
        /// Old key or row values in relation-column order.
        old: Vec<TupleValue>,
    },
    /// `'T'`: Int32 rel-count, Int8 option bits (`1`=CASCADE, `2`=RESTART IDENTITY), then the rel
    /// OIDs. Carries **no tuple / no PK** — handled as a separate wipe step downstream (transformer §5.5).
    Truncate {
        /// Active streamed transaction, when the truncate is streamed.
        xid: Option<u32>,
        /// Whether PostgreSQL applied `CASCADE`.
        cascade: bool,
        /// Whether PostgreSQL applied `RESTART IDENTITY`.
        restart_identity: bool,
        /// OIDs of all relations truncated by the statement.
        relations: Vec<u32>,
    },
    /// `'M'`: Int8 flags (bit 1 = transactional), Int64 message LSN, String prefix, Int32 content
    /// length + content bytes. A non-transactional message is emitted immediately, even ahead of
    /// its transaction's Begin (used for the idle heartbeat).
    Message {
        /// Active streamed transaction, when the logical message is streamed.
        xid: Option<u32>,
        /// Whether the message participates in its transaction's outcome.
        transactional: bool,
        /// WAL position assigned to the logical message.
        lsn: Lsn,
        /// Application-defined logical-message prefix.
        prefix: String,
        /// Opaque logical-message content.
        content: Bytes,
    },
    /// `'S'`: Int32 **top-level** xid, Int8 first-segment flag (1 = first block for this xid).
    /// Opens a streamed block — sets `ctx.in_stream`.
    StreamStart {
        /// Top-level transaction whose segment is starting.
        xid: u32,
        /// Whether this is the transaction's first streamed segment.
        first_segment: bool,
    },
    /// `'E'`: no payload. Closes the current streamed block — clears `ctx.in_stream`.
    StreamStop,
    /// `'c'`: Int32 xid, Int8 flags, Int64 commit LSN, Int64 end LSN, Int64 commit ts.
    StreamCommit {
        /// Top-level transaction being committed.
        xid: u32,
        /// Protocol flags reserved by PostgreSQL.
        flags: u8,
        /// WAL position where the streamed transaction committed.
        commit_lsn: Lsn,
        /// WAL position immediately after the commit record.
        end_lsn: Lsn,
        /// Commit timestamp in microseconds since 2000-01-01.
        commit_ts: i64,
    },
    /// `'A'`: Int32 top xid, Int32 sub xid. Under `streaming 'on'` there are **no** trailing LSN/ts
    /// fields (those exist only under `streaming 'parallel'`, v4, which walrus never enables).
    /// `sub != top` is a rolled-back savepoint inside a *committing* transaction (§9b).
    StreamAbort {
        /// Top-level streamed transaction containing the rollback.
        top_xid: u32,
        /// Subtransaction rolled back, equal to `top_xid` for a full abort.
        sub_xid: u32,
    },

    // ---- two-phase (v3) frames. walrus runs at v2 and never enables `two_phase`, so it NEVER sees
    // these in production — but the decoder must still parse them without misaligning the cursor, so
    // a stray byte fails loudly *at that byte* rather than silently corrupting the stream. There is
    // no runtime handling for these anywhere; the decoder is simply complete. ----
    /// `'b'`: Int64 prepare LSN, Int64 end LSN, Int64 prepare ts, Int32 xid, String gid.
    BeginPrepare {
        /// WAL position where prepare begins.
        prepare_lsn: Lsn,
        /// WAL position immediately after the prepare record.
        end_lsn: Lsn,
        /// Prepare timestamp in microseconds since 2000-01-01.
        prepare_ts: i64,
        /// PostgreSQL transaction ID.
        xid: u32,
        /// Global transaction identifier.
        gid: String,
    },
    /// `'P'`: Int8 flags, then prepare LSN, end LSN, prepare ts, xid, gid.
    Prepare {
        /// Protocol flags reserved by PostgreSQL.
        flags: u8,
        /// WAL position where the transaction was prepared.
        prepare_lsn: Lsn,
        /// WAL position immediately after the prepare record.
        end_lsn: Lsn,
        /// Prepare timestamp in microseconds since 2000-01-01.
        prepare_ts: i64,
        /// PostgreSQL transaction ID.
        xid: u32,
        /// Global transaction identifier.
        gid: String,
    },
    /// `'K'`: Int8 flags, commit LSN, end LSN, commit ts, xid, gid. **Top-level message** — NOT the
    /// old-KEY submessage marker (that `'K'` is read only inside the Update/Delete arms; same byte,
    /// different parser position, no collision).
    CommitPrepared {
        /// Protocol flags reserved by PostgreSQL.
        flags: u8,
        /// WAL position where the prepared transaction committed.
        commit_lsn: Lsn,
        /// WAL position immediately after the commit record.
        end_lsn: Lsn,
        /// Commit timestamp in microseconds since 2000-01-01.
        commit_ts: i64,
        /// PostgreSQL transaction ID.
        xid: u32,
        /// Global transaction identifier.
        gid: String,
    },
    /// `'r'`: Int8 flags, end LSN, rollback end LSN, prepare ts, rollback ts, xid, gid.
    RollbackPrepared {
        /// Protocol flags reserved by PostgreSQL.
        flags: u8,
        /// End WAL position recorded by the prepared transaction.
        end_lsn: Lsn,
        /// WAL position immediately after the rollback record.
        rollback_end_lsn: Lsn,
        /// Original prepare timestamp in microseconds since 2000-01-01.
        prepare_ts: i64,
        /// Rollback timestamp in microseconds since 2000-01-01.
        rollback_ts: i64,
        /// PostgreSQL transaction ID.
        xid: u32,
        /// Global transaction identifier.
        gid: String,
    },
    /// `'p'`: the streamed variant of Prepare (same fields as `'P'`).
    StreamPrepare {
        /// Protocol flags reserved by PostgreSQL.
        flags: u8,
        /// WAL position where the streamed transaction was prepared.
        prepare_lsn: Lsn,
        /// WAL position immediately after the prepare record.
        end_lsn: Lsn,
        /// Prepare timestamp in microseconds since 2000-01-01.
        prepare_ts: i64,
        /// PostgreSQL transaction ID.
        xid: u32,
        /// Global transaction identifier.
        gid: String,
    },
}

/// Move-cost budget for the one-message-per-decoded-WAL-record hot path (`own-move-large`).
///
/// Measured with `size_of::<Message>()` on the supported 64-bit targets. If this trips, shrink the
/// type, box the growing variant, or raise the measured budget deliberately in review.
const MESSAGE_MAX_BYTES: usize = 88;
const _: () = assert!(size_of::<Message>() <= MESSAGE_MAX_BYTES);

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    size_of::<Option<Vec<TupleValue>>>() == size_of::<Vec<TupleValue>>(),
    "Option<Vec<TupleValue>> lost its niche; revisit the message representation"
);

impl Message {
    /// For a [`Message::StreamAbort`]: `Some(true)` when the WHOLE transaction aborted (`top == sub`,
    /// §9a — drop everything), `Some(false)` for a rolled-back savepoint inside a committing txn
    /// (`top != sub`, §9b — discard only the sub-xid's rows). `None` for any other message.
    pub fn is_whole_txn_abort(&self) -> Option<bool> {
        match self {
            Message::StreamAbort { top_xid, sub_xid } => Some(top_xid == sub_xid),
            // Wildcard is deliberate: Message is #[non_exhaustive], and this helper ignores other families.
            _ => None,
        }
    }
}

/// Consume the fixed `'N'` marker that precedes a new tuple; a mismatch is an upstream framing
/// error (a misaligned parse).
fn expect_n(reader: &mut Reader<'_>) -> Result<(), DecodeError> {
    let b = reader.byte1()?;
    if b != b'N' {
        // A well-formed walsender stream never leaves the protocol's closed framing set.
        // `cold_path` is stable on the Rust 1.96.0 toolchain pinned by this workspace.
        std::hint::cold_path();
        return Err(DecodeError::BadTupleFormat { byte: b });
    }
    Ok(())
}

/// Decode a `TupleData`: `Int16` column-count, then per column a one-byte format tag —
/// `'n'` → [`TupleValue::Null`], `'u'` → [`TupleValue::UnchangedToast`] (value **not** on the
/// wire), `'t'` → [`TupleValue::Text`] (Int32 length + UTF-8 bytes), `'b'` →
/// [`TupleValue::Binary`] (Int32 length + bytes). An unexpected tag means the cursor misaligned →
/// [`DecodeError::BadTupleFormat`] (fail loud, never guess). Shared by Insert/Update/Delete.
///
/// # Errors
///
/// Returns [`DecodeError::UnexpectedEof`] for truncated data, [`DecodeError::BadTupleFormat`] for an
/// invalid marker, or [`DecodeError::Utf8`] for invalid textual values.
pub fn parse_tuple(reader: &mut Reader<'_>) -> Result<Vec<TupleValue>, DecodeError> {
    let ncols = reader.int16()?;
    let mut cols = Vec::with_capacity(usize::from(ncols));
    for _ in 0..ncols {
        let value = match reader.byte1()? {
            b'n' => TupleValue::Null,
            b'u' => TupleValue::UnchangedToast, // one byte total — no length, no value
            b't' => {
                let len = wire_usize(reader.int32()?);
                // `t` is the value's *text* representation; interpreting it (numeric? enum label?)
                // is the type layer's job (pg-to-arrow). Validate UTF-8 on the borrowed frame, then
                // allocate only the owned String that must outlive it.
                TupleValue::Text(reader.str(len)?.to_string())
            }
            b'b' => {
                let len = wire_usize(reader.int32()?);
                TupleValue::Binary(reader.take(len)?)
            }
            other => {
                std::hint::cold_path();
                return Err(DecodeError::BadTupleFormat { byte: other });
            }
        };
        cols.push(value);
    }
    Ok(cols)
}

/// Parse one message off `reader` (advancing it), for use by [`parse_stream`]. Stream context is
/// consulted for every message that can carry the streamed xid prefix; Begin/Commit/Origin are
/// never xid-prefixed —
/// they *are* the transaction frame.
fn parse_one(reader: &mut Reader<'_>, ctx: &mut StreamCtx) -> Result<Message, DecodeError> {
    // Changing this count changes how the same streamed bytes are framed and decoded.
    const {
        assert!(
            XID_PREFIXED.len() == 7,
            "proto v2 §7 requires exactly 7 xid-prefixed tags (RYIUDTM) or streamed bytes misalign"
        );
    }

    let tag = reader.byte1()?;
    // The per-message (sub-transaction) xid prefix exists only while streaming (proto §7/§9b). The
    // same bytes therefore parse differently in vs. out of a stream. Begin/Commit/Origin are the
    // txn frame itself and are never prefixed.
    //
    // `ctx.in_stream` leads deliberately (`opt-likely-hint`): only a txn over
    // `logical_decoding_work_mem` streams, so on the overwhelmingly common non-streamed path this
    // one bool short-circuits the test. `<[u8]>::contains` is specialised to a memchr scan, and
    // leading with it paid that scan for every decoded message only to discard the answer. Both
    // operands are pure reads, so the order is purely which path is laid out short.
    let xid = if ctx.in_stream && XID_PREFIXED.contains(&tag) {
        Some(reader.int32()?)
    } else {
        None
    };
    match tag {
        b'B' => Ok(Message::Begin {
            final_lsn: reader.lsn()?,
            commit_ts: reader.int64()?,
            xid: reader.int32()?,
        }),
        b'C' => Ok(Message::Commit {
            flags: reader.byte1()?,
            commit_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            commit_ts: reader.int64()?,
        }),
        b'O' => Ok(Message::Origin {
            commit_lsn: reader.lsn()?,
            name: reader.string()?,
        }),
        b'R' => {
            let oid = reader.int32()?;
            let schema = reader.string()?;
            let name = reader.string()?;
            let ident_byte = reader.byte1()?;
            let replica_identity = ReplicaIdentity::try_from(ident_byte)
                .map_err(|_| DecodeError::BadReplicaIdentity { byte: ident_byte })?;
            let ncols = reader.int16()?;
            let mut columns = Vec::with_capacity(usize::from(ncols));
            for _ in 0..ncols {
                let flags = reader.byte1()?;
                let col_name = reader.string()?;
                let type_oid = reader.int32()?;
                let type_modifier = typmod::atttypmod(reader.int32()?);
                columns.push(PgColumn {
                    name: col_name,
                    type_oid,
                    type_modifier,
                    is_key: flags & 1 != 0,
                });
            }
            Ok(Message::Relation {
                xid,
                relation: PgRelation {
                    oid,
                    schema,
                    name,
                    replica_identity,
                    columns,
                },
            })
        }
        b'Y' => Ok(Message::Type {
            xid,
            oid: reader.int32()?,
            namespace: reader.string()?,
            name: reader.string()?,
        }),
        b'I' => {
            let relation_oid = reader.int32()?;
            // A fixed `'N'` marker precedes the new tuple; a mismatch is an upstream framing error.
            let marker = reader.byte1()?;
            if marker != b'N' {
                std::hint::cold_path();
                return Err(DecodeError::BadTupleFormat { byte: marker });
            }
            Ok(Message::Insert {
                xid,
                relation_oid,
                new: parse_tuple(reader)?,
            })
        }
        b'U' => {
            let relation_oid = reader.int32()?;
            // Branch on the byte AFTER the OID: 'K'/'O' → an old image (then a 'N' before the new
            // tuple); 'N' → no old image, and the 'N' we just read IS the new-tuple marker.
            let (old_kind, old) = match reader.byte1()? {
                b'N' => (None, None),
                tag => {
                    let kind = OldTupleKind::try_from(tag)?;
                    let old = parse_tuple(reader)?;
                    expect_n(reader)?;
                    (Some(kind), Some(old))
                }
            };
            Ok(Message::Update {
                xid,
                relation_oid,
                old_kind,
                old,
                new: parse_tuple(reader)?,
            })
        }
        b'D' => {
            let relation_oid = reader.int32()?;
            let old_kind: OldTupleKind = reader.byte1()?.try_into()?;
            Ok(Message::Delete {
                xid,
                relation_oid,
                old_kind,
                old: parse_tuple(reader)?,
            })
        }
        b'T' => {
            let nrel = wire_usize(reader.int32()?);
            let opt = reader.byte1()?;
            // Fixed-count array: the count IS the length; no per-element framing, no tuple.
            let relations = (0..nrel)
                .map(|_| reader.int32())
                .collect::<Result<Vec<_>, _>>()?;
            Ok(Message::Truncate {
                xid,
                cascade: opt & 1 != 0,
                restart_identity: opt & 2 != 0,
                relations,
            })
        }
        b'M' => {
            let flags = reader.byte1()?;
            let lsn = reader.lsn()?;
            let prefix = reader.string()?;
            let len = wire_usize(reader.int32()?);
            let content = reader.take(len)?;
            Ok(Message::Message {
                xid,
                transactional: flags & 1 != 0,
                lsn,
                prefix,
                content,
            })
        }
        b'S' => {
            let start_xid = reader.int32()?;
            let first_segment = match reader.byte1()? {
                0 => false,
                1 => true,
                byte => return Err(DecodeError::BadStreamStartFlag { byte }),
            };
            ctx.in_stream = true; // opens the streamed block — the next change reads a sub-xid prefix
            Ok(Message::StreamStart {
                xid: start_xid,
                first_segment,
            })
        }
        b'E' => {
            ctx.in_stream = false; // closes the block
            Ok(Message::StreamStop)
        }
        b'c' => Ok(Message::StreamCommit {
            xid: reader.int32()?,
            flags: reader.byte1()?,
            commit_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            commit_ts: reader.int64()?,
        }),
        b'A' => Ok(Message::StreamAbort {
            top_xid: reader.int32()?,
            sub_xid: reader.int32()?,
        }),
        // Two-phase (v3). Field orders differ per message — cross-checked against the reference
        // decoder. `'K'` here is Commit Prepared (top-level), distinct from the old-KEY marker.
        b'b' => Ok(Message::BeginPrepare {
            prepare_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            prepare_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        b'P' => Ok(Message::Prepare {
            flags: reader.byte1()?,
            prepare_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            prepare_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        b'K' => Ok(Message::CommitPrepared {
            flags: reader.byte1()?,
            commit_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            commit_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        b'r' => Ok(Message::RollbackPrepared {
            flags: reader.byte1()?,
            end_lsn: reader.lsn()?,
            rollback_end_lsn: reader.lsn()?,
            prepare_ts: reader.int64()?,
            rollback_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        b'p' => Ok(Message::StreamPrepare {
            flags: reader.byte1()?,
            prepare_lsn: reader.lsn()?,
            end_lsn: reader.lsn()?,
            prepare_ts: reader.int64()?,
            xid: reader.int32()?,
            gid: reader.string()?,
        }),
        // The arms above are the protocol's whole tag set at v2+v3. Reaching here means a misaligned
        // cursor or a peer speaking something walrus never negotiated — never a live walsender.
        other => {
            std::hint::cold_path();
            Err(DecodeError::UnknownMessage { byte: other })
        }
    }
}

/// Decode exactly one **complete** message from `reader`: parse one message, then reject any
/// trailing unconsumed bytes (a truncated or misaligned message).
///
/// # Errors
///
/// Returns the concrete [`DecodeError`] for a malformed message, or
/// [`DecodeError::TrailingBytes`] when a valid message does not consume the full payload.
pub fn parse_message(reader: &mut Reader<'_>, ctx: &mut StreamCtx) -> Result<Message, DecodeError> {
    let msg = parse_one(reader, ctx)?;
    let unconsumed = reader.remaining();
    if unconsumed != 0 {
        // Every well-formed frame is consumed whole; leftovers mean the payload and the parser
        // disagree about the message's width.
        std::hint::cold_path();
        return Err(DecodeError::TrailingBytes {
            unconsumed: reader::u32c(unconsumed),
        });
    }
    Ok(msg)
}

/// Split a raw walsender byte stream into messages, skipping the single `0x0a` that
/// `pg_recvlogical` inserts between self-delimiting messages, threading `ctx` across them.
///
/// # Errors
///
/// Returns the first [`DecodeError`] produced by a truncated, unknown, misframed, or state-invalid
/// message in the stream.
pub fn parse_stream(data: &[u8], ctx: &mut StreamCtx) -> Result<Vec<Message>, DecodeError> {
    let mut reader = Reader::new(data);
    let mut out = Vec::new();
    while reader.remaining() > 0 {
        // Skip exactly one separator, and only at a message boundary (the top of the loop), so a
        // `0x0a` byte *inside* a value is never mistaken for a separator.
        if reader.peek() == Some(0x0a) {
            reader.byte1()?;
            continue;
        }
        out.push(parse_one(&mut reader, ctx)?);
    }
    Ok(out)
}

#[cfg(test)]
#[path = "mod_test.rs"]
mod tests;
