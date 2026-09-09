#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::let_underscore_must_use,
    reason = "integration test — unwrap/expect fine in setup + helpers"
)]
#![allow(
    clippy::panic,
    clippy::unreachable,
    reason = "golden helpers: macros assert fixture/protocol invariants"
)]
//! Golden-vector fixtures for the pgoutput decoder — a faithful, byte-for-byte port of
//! `docs/examples/proto-version/test_decode_pgoutput.py::VECTORS`.
//!
//! The tests exercise every supported message family plus `parse_stream` framing.

use common::{ReplicaIdentity, TupleValue};
use extractor::pgoutput::{
    DecodeError, Message, OldTupleKind, Reader, StreamCtx, parse_message, parse_stream, parse_tuple,
};
use std::fmt::Write as _;

/// A ported row of `VECTORS`: bytes in, the golden render line out.
#[derive(Clone, Copy, Debug)]
struct Vector {
    name: &'static str,
    /// Message bytes, hex-encoded (lowercase, as captured).
    hex: &'static str,
    /// Were we inside a Stream Start..Stop block when this message arrived? (Only then does a
    /// per-message xid prefix exist.)
    streaming: bool,
    /// The golden render line, verbatim from the Python reference.
    expected: &'static str,
}

/// Every entry of `test_decode_pgoutput.py::VECTORS`, ported verbatim (names, hex, streaming flag,
/// expected line). `concat!` mirrors Python's adjacent-string-literal concatenation.
const VECTORS: &[Vector] = &[
    // ---- non-streamed transaction control + DML (REPLICA IDENTITY DEFAULT) ----
    Vector {
        name: "begin",
        hex: "42000000000199bac80002f8dc466a6b4f000002ed",
        streaming: false,
        expected: "BEGIN         final_lsn=0/199BAC8 commit_ts=2026-07-05T13:55:11.294287+00:00 xid=749",
    },
    Vector {
        name: "type_enum",
        hex: "59000040067075626c6963006d6f6f6400",
        streaming: false,
        expected: "TYPE          oid=16390 public.mood",
    },
    Vector {
        name: "relation_orders",
        hex: concat!(
            "520000400d7075626c6963006f7264657273006400050169640000000017ffffffff007374617475",
            "730000000019ffffffff00616d6f756e7400000006a4000a0006006665656c696e67000000400",
            "6ffffffff006e6f74650000000019ffffffff"
        ),
        streaming: false,
        expected: concat!(
            "RELATION      oid=16397 public.orders replident='d' cols=[KEY id(oid=23,mod=-1), ",
            "status(oid=25,mod=-1), amount(oid=1700,mod=655366), feeling(oid=16390,mod=-1), ",
            "note(oid=25,mod=-1)]"
        ),
    },
    Vector {
        name: "insert",
        hex: concat!(
            "490000400d4e000574000000013174000000036e6577740000000531392e3939740000000568617070",
            "7974000000056669727374"
        ),
        streaming: false,
        expected: "INSERT        rel=16397 new=['1', 'new', '19.99', 'happy', 'first']",
    },
    Vector {
        name: "update_pk_change",
        hex: concat!(
            "550000400d4b00057400000001316e6e6e6e4e00057400000003313030740000000773686970706564",
            "740000000532392e39397400000005686170707974000000056669727374"
        ),
        streaming: false,
        expected: concat!(
            "UPDATE        rel=16397 old[K]=['1', NULL, NULL, NULL, NULL] ",
            "new=['100', 'shipped', '29.99', 'happy', 'first']"
        ),
    },
    Vector {
        name: "delete_key",
        hex: "440000400d4b000574000000033130306e6e6e6e",
        streaming: false,
        expected: "DELETE        rel=16397 old[K]=['100', NULL, NULL, NULL, NULL]",
    },
    Vector {
        name: "commit",
        hex: "4300000000000199bac8000000000199baf80002f8dc466a6b4f",
        streaming: false,
        expected: concat!(
            "COMMIT        flags=0 commit_lsn=0/199BAC8 end_lsn=0/199BAF8 ",
            "commit_ts=2026-07-05T13:55:11.294287+00:00"
        ),
    },
    // ---- the REPLICA IDENTITY contrast ----
    Vector {
        name: "update_no_key_change",
        hex: "550000410b4e00057400000001357400000001626e6e74000000016e",
        streaming: false,
        expected: "UPDATE        rel=16651 new=['5', 'b', NULL, NULL, 'n']",
    },
    Vector {
        name: "update_full_identity",
        hex: concat!(
            "55000041194f00037400000001377400000001777400000001314e0003740000000137740000000177",
            "740000000132"
        ),
        streaming: false,
        expected: "UPDATE        rel=16665 old[O]=['7', 'w', '1'] new=['7', 'w', '2']",
    },
    Vector {
        name: "delete_full_identity",
        hex: "44000041194f0003740000000137740000000177740000000132",
        streaming: false,
        expected: "DELETE        rel=16665 old[O]=['7', 'w', '2']",
    },
    // ---- truncate option bits, logical messages ----
    Vector {
        name: "truncate_plain",
        hex: "5400000001000000410b",
        streaming: false,
        expected: "TRUNCATE      opts=none rels=[16651]",
    },
    Vector {
        name: "truncate_cascade_restart",
        hex: "54000000010300004119",
        streaming: false,
        expected: "TRUNCATE      opts=CASCADE|RESTART IDENTITY rels=[16665]",
    },
    Vector {
        name: "message_non_transactional",
        hex: "4d000000000001fb4be077616c727573000000000668622d6e6f6e",
        streaming: false,
        expected: "MESSAGE       non-transactional lsn=0/1FB4BE0 prefix='walrus' content=b'hb-non'",
    },
    Vector {
        name: "message_transactional",
        hex: "4d010000000001fb4ba077616c727573000000000668622d74786e",
        streaming: false,
        expected: "MESSAGE       transactional lsn=0/1FB4BA0 prefix='walrus' content=b'hb-txn'",
    },
    // ---- streaming frames (v2+) ----
    Vector {
        name: "stream_start_first",
        hex: "53000002f101",
        streaming: false,
        expected: "STREAM START  xid=753 first_segment=1",
    },
    Vector {
        name: "stream_stop",
        hex: "45",
        streaming: true,
        expected: "STREAM STOP",
    },
    Vector {
        name: "stream_commit",
        hex: "63000002f5000000000001e095800000000001e095b80002f8dc50e8dd86",
        streaming: true,
        expected: concat!(
            "STREAM COMMIT xid=757 flags=0 commit_lsn=0/1E09580 end_lsn=0/1E095B8 ",
            "commit_ts=2026-07-05T13:58:07.353222+00:00"
        ),
    },
    Vector {
        name: "stream_abort_subtransaction",
        hex: "41000002f5000002f6",
        streaming: true,
        expected: "STREAM ABORT  top_xid=757 sub_xid=758  <- SUBTRANSACTION rollback",
    },
    Vector {
        name: "stream_abort_whole_txn",
        hex: "410000036200000362",
        streaming: true,
        expected: "STREAM ABORT  top_xid=866 sub_xid=866  <- WHOLE-TXN abort",
    },
    Vector {
        name: "streamed_insert_carries_xid",
        hex: "49000002f10000410b4e00057400000001317400000001736e6e74000000027879",
        streaming: true,
        expected: "INSERT        xid=753 rel=16651 new=['1', 's', NULL, NULL, 'xy']",
    },
    Vector {
        name: "unchanged_toast_update",
        hex: "550000410b4e00057400000001397400000001796e6e75",
        streaming: false,
        expected: "UPDATE        rel=16651 new=['9', 'y', NULL, NULL, <unchanged-TOAST>]",
    },
    // ---- schema evolution / composite key / generated columns (captured) ----
    Vector {
        name: "relation_composite_pk",
        hex: concat!(
            "520000636e7075626c696300637573746f6d6572730064000301726567696f6e0000000019ffffffff",
            "0169640000000017ffffffff006e616d650000000019ffffffff"
        ),
        streaming: false,
        expected: concat!(
            "RELATION      oid=25454 public.customers replident='d' ",
            "cols=[KEY region(oid=25,mod=-1), KEY id(oid=23,mod=-1), name(oid=25,mod=-1)]"
        ),
    },
    Vector {
        name: "update_composite_pk_partial",
        hex: concat!(
            "550000636e4b0003740000000275737400000001316e4e000374000000026575740000000131740000",
            "000141"
        ),
        streaming: false,
        expected: "UPDATE        rel=25454 old[K]=['us', '1', NULL] new=['eu', '1', 'A']",
    },
    Vector {
        name: "relation_full_identity_all_keys",
        hex: concat!(
            "52000063757075626c6963006974656d73006600030169640000000017ffffffff016c6162656c0000",
            "000019ffffffff017174790000000017ffffffff"
        ),
        streaming: false,
        expected: concat!(
            "RELATION      oid=25461 public.items replident='f' ",
            "cols=[KEY id(oid=23,mod=-1), KEY label(oid=25,mod=-1), KEY qty(oid=23,mod=-1)]"
        ),
    },
    Vector {
        name: "insert_generated_column_omitted",
        hex: "49000063754e000374000000013274000000016774000000023130",
        streaming: false,
        expected: "INSERT        rel=25461 new=['2', 'g', '10']",
    },
    Vector {
        name: "relation_after_add_column",
        hex: concat!(
            "52000063677075626c6963006f7264657273006400060169640000000017ffffffff00737461747573",
            "0000000019ffffffff00616d6f756e7400000006a4000a0006006665656c696e670000006361ffffff",
            "ff006e6f74650000000019ffffffff0065787472610000000017ffffffff"
        ),
        streaming: false,
        expected: concat!(
            "RELATION      oid=25447 public.orders replident='d' cols=[KEY id(oid=23,mod=-1), ",
            "status(oid=25,mod=-1), amount(oid=1700,mod=655366), feeling(oid=25441,mod=-1), ",
            "note(oid=25,mod=-1), extra(oid=23,mod=-1)]"
        ),
    },
    // ---- two-phase commit messages (v3; CONSTRUCTED — walrus is v2-only, never sees these,
    //      but the decoder must parse them; xid=100, gid='gtx') ----
    Vector {
        name: "begin_prepare",
        hex: "62000000000100000000000000010001000002f8dc000000000000006467747800",
        streaming: false,
        expected: concat!(
            "BEGIN PREPARE prepare_lsn=0/1000000 end_lsn=0/1000100 ",
            "ts=2026-07-05T13:35:29.914880+00:00 xid=100 gid='gtx'"
        ),
    },
    Vector {
        name: "prepare",
        hex: "5000000000000100000000000000010001000002f8dc000000000000006467747800",
        streaming: false,
        expected: concat!(
            "PREPARE       prepare_lsn=0/1000000 end_lsn=0/1000100 ",
            "ts=2026-07-05T13:35:29.914880+00:00 xid=100 gid='gtx'"
        ),
    },
    Vector {
        name: "commit_prepared",
        hex: "4b00000000000100020000000000010003000002f8dc000000010000006467747800",
        streaming: false,
        expected: concat!(
            "COMMIT PREP   commit_lsn=0/1000200 end_lsn=0/1000300 ",
            "ts=2026-07-05T13:35:29.914881+00:00 xid=100 gid='gtx'"
        ),
    },
    Vector {
        name: "rollback_prepared",
        hex: concat!(
            "7200000000000100020000000000010004000002f8dc000000000002f8dc0000000200000064",
            "67747800"
        ),
        streaming: false,
        expected: concat!(
            "ROLLBACK PREP end_lsn=0/1000200 rollback_lsn=0/1000400 ",
            "prepare_ts=2026-07-05T13:35:29.914880+00:00 rollback_ts=2026-07-05T13:35:29.914882",
            "+00:00 xid=100 gid='gtx'"
        ),
    },
];

/// Find a vector by name.
fn lookup(name: &str) -> &'static Vector {
    VECTORS
        .iter()
        .find(|v| v.name == name)
        .unwrap_or_else(|| panic!("no vector named {name}"))
}

/// Test-only convenience: hex → one complete `Message` (rejects trailing bytes).
fn decode(hex: &str, streaming: bool) -> Message {
    let bytes = hex::decode(hex).expect("vector hex is valid");
    let mut ctx = StreamCtx {
        in_stream: streaming,
    };
    let mut reader = Reader::new(&bytes);
    parse_message(&mut reader, &mut ctx).expect("vector decodes")
}

/// `X/Y` upper-hex LSN, matching `decode_pgoutput.py::lsn`.
fn fmt_lsn(v: u64) -> String {
    format!("{:X}/{:X}", v >> 32, v & 0xFFFF_FFFF)
}

/// µs-since-2000 → Python `datetime.isoformat()` (`YYYY-MM-DDThh:mm:ss[.ffffff]+00:00`). The test
/// crate reproduces Python's rendering to diff against the golden line; production uses `ExtractorMeta`'s
/// RFC-3339 `Z` stamp. Formatted by hand (not jiff's `Display`, which trims trailing
/// fractional zeros — `.914880` would render `.91488`).
fn fmt_ts(micros_since_2000: i64) -> String {
    let ts = jiff::Timestamp::from_microsecond(micros_since_2000 + common::PG_EPOCH_UNIX_MICROS)
        .expect("timestamp in range");
    let dt = ts.to_zoned(jiff::tz::TimeZone::UTC).datetime();
    let micros = u32::try_from(dt.subsec_nanosecond() / 1000).unwrap();
    let base = format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}",
        dt.year(),
        dt.month(),
        dt.day(),
        dt.hour(),
        dt.minute(),
        dt.second()
    );
    if micros == 0 {
        format!("{base}+00:00")
    } else {
        format!("{base}.{micros:06}+00:00")
    }
}

/// Test-only renderer reproducing `decode_pgoutput.py::render` line-for-line.
fn render(m: &Message) -> String {
    match m {
        Message::Begin {
            final_lsn,
            commit_ts,
            xid,
        } => format!(
            "BEGIN         final_lsn={} commit_ts={} xid={}",
            fmt_lsn(final_lsn.as_u64()),
            fmt_ts(*commit_ts),
            xid
        ),
        Message::Commit {
            flags,
            commit_lsn,
            end_lsn,
            commit_ts,
        } => format!(
            "COMMIT        flags={} commit_lsn={} end_lsn={} commit_ts={}",
            flags,
            fmt_lsn(commit_lsn.as_u64()),
            fmt_lsn(end_lsn.as_u64()),
            fmt_ts(*commit_ts)
        ),
        Message::Origin { commit_lsn, name } => format!(
            "ORIGIN        commit_lsn={} name='{}'",
            fmt_lsn(commit_lsn.as_u64()),
            name
        ),
        Message::Relation { xid, relation } => {
            let pre = match xid {
                Some(x) => format!("xid={x} "),
                None => String::new(),
            };
            let replident = match relation.replica_identity {
                ReplicaIdentity::Default => 'd',
                ReplicaIdentity::Nothing => 'n',
                ReplicaIdentity::Full => 'f',
                ReplicaIdentity::Index => 'i',
            };
            let cols = relation
                .columns
                .iter()
                .map(|c| {
                    format!(
                        "{}{}(oid={},mod={})",
                        if c.is_key { "KEY " } else { "" },
                        c.name,
                        c.type_oid,
                        c.type_modifier
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!(
                "RELATION      {pre}oid={} {}.{} replident='{replident}' cols=[{cols}]",
                relation.oid, relation.schema, relation.name
            )
        }
        Message::Type {
            xid,
            oid,
            namespace,
            name,
        } => {
            let pre = match xid {
                Some(x) => format!("xid={x} "),
                None => String::new(),
            };
            format!("TYPE          {pre}oid={oid} {namespace}.{name}")
        }
        Message::Insert {
            xid,
            relation_oid,
            new,
        } => {
            let pre = match xid {
                Some(x) => format!("xid={x} "),
                None => String::new(),
            };
            format!(
                "INSERT        {pre}rel={relation_oid} new={}",
                render_tuple(new)
            )
        }
        Message::Update {
            xid,
            relation_oid,
            old_kind,
            old,
            new,
        } => {
            let pre = match xid {
                Some(x) => format!("xid={x} "),
                None => String::new(),
            };
            let oldstr = match (old_kind, old) {
                (Some(k), Some(o)) => format!(" old[{}]={}", old_kind_char(*k), render_tuple(o)),
                _ => String::new(),
            };
            format!(
                "UPDATE        {pre}rel={relation_oid}{oldstr} new={}",
                render_tuple(new)
            )
        }
        Message::Delete {
            xid,
            relation_oid,
            old_kind,
            old,
        } => {
            let pre = match xid {
                Some(x) => format!("xid={x} "),
                None => String::new(),
            };
            format!(
                "DELETE        {pre}rel={relation_oid} old[{}]={}",
                old_kind_char(*old_kind),
                render_tuple(old)
            )
        }
        Message::Truncate {
            xid,
            cascade,
            restart_identity,
            relations,
        } => {
            let pre = match xid {
                Some(x) => format!("xid={x} "),
                None => String::new(),
            };
            let mut opts: Vec<&str> = Vec::new();
            if *cascade {
                opts.push("CASCADE");
            }
            if *restart_identity {
                opts.push("RESTART IDENTITY");
            }
            let opts_str = if opts.is_empty() {
                "none".to_string()
            } else {
                opts.join("|")
            };
            let rels = relations
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(", ");
            format!("TRUNCATE      {pre}opts={opts_str} rels=[{rels}]")
        }
        Message::Message {
            xid,
            transactional,
            lsn,
            prefix,
            content,
        } => {
            let pre = match xid {
                Some(x) => format!("xid={x} "),
                None => String::new(),
            };
            let kind = if *transactional {
                "transactional"
            } else {
                "non-transactional"
            };
            format!(
                "MESSAGE       {pre}{kind} lsn={} prefix='{prefix}' content={}",
                fmt_lsn(lsn.as_u64()),
                fmt_bytes(content)
            )
        }
        Message::StreamStart { xid, first_segment } => {
            format!(
                "STREAM START  xid={xid} first_segment={}",
                if *first_segment { 1 } else { 0 }
            )
        }
        Message::StreamStop => "STREAM STOP".to_string(),
        Message::StreamCommit {
            xid,
            flags,
            commit_lsn,
            end_lsn,
            commit_ts,
        } => format!(
            "STREAM COMMIT xid={xid} flags={flags} commit_lsn={} end_lsn={} commit_ts={}",
            fmt_lsn(commit_lsn.as_u64()),
            fmt_lsn(end_lsn.as_u64()),
            fmt_ts(*commit_ts)
        ),
        Message::StreamAbort { top_xid, sub_xid } => {
            let kind = if top_xid == sub_xid {
                "WHOLE-TXN abort"
            } else {
                "SUBTRANSACTION rollback"
            };
            format!("STREAM ABORT  top_xid={top_xid} sub_xid={sub_xid}  <- {kind}")
        }
        Message::BeginPrepare {
            prepare_lsn,
            end_lsn,
            prepare_ts,
            xid,
            gid,
        } => format!(
            "BEGIN PREPARE prepare_lsn={} end_lsn={} ts={} xid={xid} gid='{gid}'",
            fmt_lsn(prepare_lsn.as_u64()),
            fmt_lsn(end_lsn.as_u64()),
            fmt_ts(*prepare_ts)
        ),
        Message::Prepare {
            prepare_lsn,
            end_lsn,
            prepare_ts,
            xid,
            gid,
            ..
        } => format!(
            "PREPARE       prepare_lsn={} end_lsn={} ts={} xid={xid} gid='{gid}'",
            fmt_lsn(prepare_lsn.as_u64()),
            fmt_lsn(end_lsn.as_u64()),
            fmt_ts(*prepare_ts)
        ),
        Message::CommitPrepared {
            commit_lsn,
            end_lsn,
            commit_ts,
            xid,
            gid,
            ..
        } => format!(
            "COMMIT PREP   commit_lsn={} end_lsn={} ts={} xid={xid} gid='{gid}'",
            fmt_lsn(commit_lsn.as_u64()),
            fmt_lsn(end_lsn.as_u64()),
            fmt_ts(*commit_ts)
        ),
        Message::RollbackPrepared {
            end_lsn,
            rollback_end_lsn,
            prepare_ts,
            rollback_ts,
            xid,
            gid,
            ..
        } => format!(
            "ROLLBACK PREP end_lsn={} rollback_lsn={} prepare_ts={} rollback_ts={} xid={xid} gid='{gid}'",
            fmt_lsn(end_lsn.as_u64()),
            fmt_lsn(rollback_end_lsn.as_u64()),
            fmt_ts(*prepare_ts),
            fmt_ts(*rollback_ts)
        ),
        Message::StreamPrepare {
            prepare_lsn,
            end_lsn,
            prepare_ts,
            xid,
            gid,
            ..
        } => format!(
            "STREAM PREPARE prepare_lsn={} end_lsn={} ts={} xid={xid} gid='{gid}'",
            fmt_lsn(prepare_lsn.as_u64()),
            fmt_lsn(end_lsn.as_u64()),
            fmt_ts(*prepare_ts)
        ),
        _ => unreachable!("every current Message variant is rendered"),
    }
}

/// The old-image kind as its wire char: `Key` → `'K'`, `Full` → `'O'`.
const fn old_kind_char(k: OldTupleKind) -> char {
    match k {
        OldTupleKind::Key => 'K',
        OldTupleKind::Full => 'O',
    }
}

/// Bytes as a Python-style byte-string literal (`b'hb-non'`), escaping the non-printables Python
/// escapes, to diff against the golden line.
fn fmt_bytes(b: &[u8]) -> String {
    let mut s = String::from("b'");
    for &byte in b {
        match byte {
            b'\\' => s.push_str("\\\\"),
            b'\'' => s.push_str("\\'"),
            b'\n' => s.push_str("\\n"),
            b'\r' => s.push_str("\\r"),
            b'\t' => s.push_str("\\t"),
            0x20..=0x7e => s.push(char::from(byte)),
            _ => {
                let _ = write!(&mut s, "\\x{byte:02x}");
            }
        }
    }
    s.push('\'');
    s
}

/// Render a tuple like `decode_pgoutput.py::render_tuple`.
fn render_tuple(cols: &[TupleValue]) -> String {
    let items: Vec<String> = cols
        .iter()
        .map(|c| match c {
            TupleValue::Null => "NULL".to_string(),
            TupleValue::UnchangedToast => "<unchanged-TOAST>".to_string(),
            TupleValue::Text(s) => {
                let shown = if s.chars().count() > 40 {
                    let prefix: String = s.chars().take(37).collect();
                    format!("{prefix}...")
                } else {
                    s.clone()
                };
                format!("'{shown}'")
            }
            TupleValue::Binary(b) => format!("0x{}", hex::encode(b)),
        })
        .collect();
    format!("[{}]", items.join(", "))
}

#[test]
fn begin_decodes() {
    let v = lookup("begin");
    assert_eq!(render(&decode(v.hex, v.streaming)), v.expected);
}

#[test]
fn commit_decodes() {
    let v = lookup("commit");
    assert_eq!(render(&decode(v.hex, v.streaming)), v.expected);
}

#[test]
fn commit_vectors_keep_order_lsn_distinct_from_feedback_end_lsn() {
    for name in ["commit", "stream_commit"] {
        let message = decode(lookup(name).hex, lookup(name).streaming);
        let (commit_lsn, end_lsn) = match message {
            Message::Commit {
                commit_lsn,
                end_lsn,
                ..
            }
            | Message::StreamCommit {
                commit_lsn,
                end_lsn,
                ..
            } => (commit_lsn, end_lsn),
            other => panic!("expected commit boundary for {name}, got {other:?}"),
        };
        assert!(
            end_lsn > commit_lsn,
            "{name}: feedback must advance beyond the commit record's identity LSN"
        );
    }
}

#[test]
fn parse_stream_skips_newline_separators() {
    // begin \n commit \n → [Begin, Commit]; the 0x0a bytes are separators, not data.
    let mut raw = hex::decode(lookup("begin").hex).unwrap();
    raw.push(0x0a);
    raw.extend_from_slice(&hex::decode(lookup("commit").hex).unwrap());
    raw.push(0x0a);

    let mut ctx = StreamCtx::default();
    let msgs = parse_stream(&raw, &mut ctx).unwrap();
    assert_eq!(msgs.len(), 2);
    match &msgs[0] {
        Message::Begin { xid, .. } => assert_eq!(*xid, 749),
        other => panic!("expected Begin, got {other:?}"),
    }
    assert!(matches!(&msgs[1], Message::Commit { .. }));
}

#[test]
fn unknown_type_byte_errors_not_panics() {
    let bytes = b"Zxxxx";
    let mut ctx = StreamCtx::default();
    let mut reader = Reader::new(bytes);
    assert!(matches!(
        parse_message(&mut reader, &mut ctx),
        Err(DecodeError::UnknownMessage { byte: b'Z' })
    ));
}

#[test]
fn parse_message_rejects_trailing_bytes() {
    let mut bytes = hex::decode(lookup("begin").hex).unwrap();
    bytes.push(0xff); // one stray byte past a complete Begin
    let mut ctx = StreamCtx::default();
    let mut reader = Reader::new(&bytes);
    assert!(matches!(
        parse_message(&mut reader, &mut ctx),
        Err(DecodeError::TrailingBytes { unconsumed: 1 })
    ));
}

#[test]
fn relation_and_type_vectors_render() {
    for name in [
        "type_enum",
        "relation_orders",
        "relation_composite_pk",
        "relation_full_identity_all_keys",
        "relation_after_add_column",
    ] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn numeric_typmod_encodes_precision_scale() {
    use extractor::pgoutput::typmod::{atttypmod, numeric_precision_scale};
    assert_eq!(atttypmod(0xFFFF_FFFF), -1);
    assert_eq!(numeric_precision_scale(655366), Some((10, 2)));
    assert_eq!(numeric_precision_scale(-1), None);

    // orders.amount is numeric(10,2): type_oid 1700, atttypmod 655366.
    match decode(lookup("relation_orders").hex, false) {
        Message::Relation { relation, .. } => {
            let amount = relation
                .columns
                .iter()
                .find(|c| c.name == "amount")
                .unwrap();
            assert_eq!(amount.type_oid, 1700);
            assert_eq!(amount.type_modifier, 655366);
            assert_eq!(numeric_precision_scale(amount.type_modifier), Some((10, 2)));
        }
        other => panic!("expected Relation, got {other:?}"),
    }
}

#[test]
fn relation_marks_only_key_columns() {
    match decode(lookup("relation_orders").hex, false) {
        Message::Relation { relation, xid } => {
            assert_eq!(xid, None, "a non-streamed Relation has no xid prefix");
            assert_eq!(relation.to_key_columns(), vec!["id"]);
            assert_eq!(relation.replica_identity, ReplicaIdentity::Default);
        }
        other => panic!("expected Relation, got {other:?}"),
    }
}

#[test]
fn full_identity_flags_every_column_key() {
    match decode(lookup("relation_full_identity_all_keys").hex, false) {
        Message::Relation { relation, .. } => {
            assert_eq!(relation.replica_identity, ReplicaIdentity::Full);
            assert!(
                relation.columns.iter().all(|c| c.is_key),
                "REPLICA IDENTITY FULL flags every column as key"
            );
        }
        other => panic!("expected Relation, got {other:?}"),
    }
}

#[test]
fn insert_vectors_render() {
    for name in ["insert", "insert_generated_column_omitted"] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn insert_new_tuple_has_no_null_or_toast_here() {
    // orders insert: all 5 columns are Text; none Null/UnchangedToast.
    match decode(lookup("insert").hex, false) {
        Message::Insert { new, .. } => {
            assert_eq!(new.len(), 5);
            assert!(new.iter().all(|v| matches!(v, TupleValue::Text(_))));
        }
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn generated_stored_column_is_omitted_from_tuple() {
    // items insert has 3 columns (id, label, qty) — the GENERATED STORED col is absent.
    match decode(lookup("insert_generated_column_omitted").hex, false) {
        Message::Insert { new, .. } => assert_eq!(new.len(), 3),
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn parse_tuple_distinguishes_null_and_unchanged_toast() {
    // Int16 ncols=2, then 'n', 'u' — a real NULL vs an unchanged-TOAST placeholder.
    let bytes = [0x00, 0x02, b'n', b'u'];
    let mut reader = Reader::new(&bytes);
    let cols = parse_tuple(&mut reader).unwrap();
    assert_eq!(cols, vec![TupleValue::Null, TupleValue::UnchangedToast]);
    assert_ne!(cols[0], cols[1]);
}

#[test]
fn parse_tuple_rejects_unknown_format_byte() {
    let bytes = [0x00, 0x01, b'x']; // ncols=1, bad format tag 'x'
    let mut reader = Reader::new(&bytes);
    assert!(matches!(
        parse_tuple(&mut reader),
        Err(DecodeError::BadTupleFormat { byte: b'x' })
    ));
}

#[test]
fn update_delete_vectors_render() {
    for name in [
        "update_pk_change",
        "delete_key",
        "update_no_key_change",
        "update_full_identity",
        "delete_full_identity",
        "update_composite_pk_partial",
        "unchanged_toast_update",
    ] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn non_key_update_has_no_old_image() {
    // update_no_key_change: the byte after the OID was 'N', not 'K' → no old image.
    match decode(lookup("update_no_key_change").hex, false) {
        Message::Update { old_kind, old, .. } => {
            assert_eq!(old_kind, None);
            assert_eq!(old, None);
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn pk_changing_update_carries_key_only_old_image() {
    match decode(lookup("update_pk_change").hex, false) {
        Message::Update { old_kind, old, .. } => {
            assert_eq!(old_kind, Some(OldTupleKind::Key));
            let old = old.unwrap();
            assert_eq!(old[0], TupleValue::Text("1".to_string()));
            assert!(
                old[1..].iter().all(|v| *v == TupleValue::Null),
                "DEFAULT identity ships key columns only; the rest are NULL padding"
            );
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn full_identity_update_carries_whole_old_row() {
    match decode(lookup("update_full_identity").hex, false) {
        Message::Update { old_kind, old, .. } => {
            assert_eq!(old_kind, Some(OldTupleKind::Full));
            assert_eq!(
                old.unwrap(),
                vec![
                    TupleValue::Text("7".to_string()),
                    TupleValue::Text("w".to_string()),
                    TupleValue::Text("1".to_string()),
                ]
            );
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn composite_pk_old_image_carries_all_key_columns() {
    // only region changed, but the old KEY image still ships BOTH key columns; non-key is NULL.
    match decode(lookup("update_composite_pk_partial").hex, false) {
        Message::Update { old_kind, old, .. } => {
            assert_eq!(old_kind, Some(OldTupleKind::Key));
            assert_eq!(
                old.unwrap(),
                vec![
                    TupleValue::Text("us".to_string()),
                    TupleValue::Text("1".to_string()),
                    TupleValue::Null,
                ]
            );
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn null_and_unchanged_toast_are_distinct() {
    match decode(lookup("unchanged_toast_update").hex, false) {
        Message::Update { new, .. } => {
            assert_eq!(new[2], TupleValue::Null);
            assert_eq!(new[4], TupleValue::UnchangedToast);
            assert_ne!(new[2], new[4]);
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn truncate_and_message_vectors_render() {
    for name in [
        "truncate_plain",
        "truncate_cascade_restart",
        "message_non_transactional",
        "message_transactional",
    ] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn truncate_option_bits() {
    match decode(lookup("truncate_plain").hex, false) {
        Message::Truncate {
            cascade,
            restart_identity,
            relations,
            ..
        } => {
            assert_eq!((cascade, restart_identity), (false, false));
            assert_eq!(relations, vec![16651]);
        }
        other => panic!("expected Truncate, got {other:?}"),
    }
    match decode(lookup("truncate_cascade_restart").hex, false) {
        Message::Truncate {
            cascade,
            restart_identity,
            relations,
            ..
        } => {
            assert_eq!((cascade, restart_identity), (true, true));
            assert_eq!(relations, vec![16665]);
        }
        other => panic!("expected Truncate, got {other:?}"),
    }
}

#[test]
fn message_transactional_flag() {
    match decode(lookup("message_non_transactional").hex, false) {
        Message::Message {
            transactional,
            content,
            ..
        } => {
            assert!(!transactional);
            // content preserved as raw bytes, not utf-8-lossily decoded.
            assert_eq!(content.as_ref(), b"hb-non".as_slice());
        }
        other => panic!("expected Message, got {other:?}"),
    }
    match decode(lookup("message_transactional").hex, false) {
        Message::Message {
            transactional,
            content,
            ..
        } => {
            assert!(transactional);
            assert_eq!(content.as_ref(), b"hb-txn".as_slice());
        }
        other => panic!("expected Message, got {other:?}"),
    }
}

#[test]
fn stream_vectors_render() {
    for name in [
        "stream_start_first",
        "stream_stop",
        "stream_commit",
        "stream_abort_subtransaction",
        "stream_abort_whole_txn",
        "streamed_insert_carries_xid",
    ] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

/// FLAGSHIP: a rolled-back savepoint is NOT a whole-txn abort — discard only sub_xid's rows.
#[test]
fn subtransaction_abort_is_not_whole_txn() {
    let m = decode(lookup("stream_abort_subtransaction").hex, true);
    assert_eq!(m.is_whole_txn_abort(), Some(false));
    match &m {
        Message::StreamAbort { top_xid, sub_xid } => assert_eq!((*top_xid, *sub_xid), (757, 758)),
        other => panic!("expected StreamAbort, got {other:?}"),
    }
}

#[test]
fn whole_txn_abort_flagged() {
    let m = decode(lookup("stream_abort_whole_txn").hex, true);
    assert_eq!(m.is_whole_txn_abort(), Some(true));
    match &m {
        Message::StreamAbort { top_xid, sub_xid } => {
            assert_eq!((*top_xid, *sub_xid), (866, 866));
        }
        other => panic!("expected StreamAbort, got {other:?}"),
    }
}

#[test]
fn stream_start_stop_toggle_context() {
    let mut ctx = StreamCtx::default();
    assert!(!ctx.in_stream);

    // Stream Start opens the streamed context.
    let start_bytes = hex::decode(lookup("stream_start_first").hex).unwrap();
    let start = parse_message(&mut Reader::new(&start_bytes), &mut ctx).unwrap();
    assert!(matches!(
        start,
        Message::StreamStart {
            xid: 753,
            first_segment: true
        }
    ));
    assert!(ctx.in_stream, "Start opened the streamed context");

    // A streamed change now reads its sub-xid prefix.
    let ins_bytes = hex::decode(lookup("streamed_insert_carries_xid").hex).unwrap();
    match parse_message(&mut Reader::new(&ins_bytes), &mut ctx).unwrap() {
        Message::Insert { xid, .. } => assert_eq!(xid, Some(753)),
        other => panic!("expected Insert, got {other:?}"),
    }

    // Stream Stop closes it; a subsequent change would have no prefix.
    parse_message(&mut Reader::new(b"E"), &mut ctx).unwrap();
    assert!(!ctx.in_stream, "Stop closed the context");
}

#[test]
fn parse_stream_preserves_streaming_context_across_messages() {
    let mut raw = hex::decode(lookup("stream_start_first").hex).unwrap();
    raw.push(0x0a);
    raw.extend_from_slice(&hex::decode(lookup("streamed_insert_carries_xid").hex).unwrap());
    raw.push(0x0a);
    raw.push(b'E');
    raw.push(0x0a);

    let mut ctx = StreamCtx::default();
    let msgs = parse_stream(&raw, &mut ctx).unwrap();
    assert_eq!(msgs.len(), 3);
    assert!(matches!(msgs[0], Message::StreamStart { xid: 753, .. }));
    match &msgs[1] {
        Message::Insert { xid, .. } => assert_eq!(*xid, Some(753)),
        other => panic!("expected Insert, got {other:?}"),
    }
    assert!(matches!(msgs[2], Message::StreamStop));
}

#[test]
fn two_phase_vectors_render() {
    for name in [
        "begin_prepare",
        "prepare",
        "commit_prepared",
        "rollback_prepared",
    ] {
        let v = lookup(name);
        assert_eq!(render(&decode(v.hex, v.streaming)), v.expected, "{name}");
    }
}

#[test]
fn two_phase_messages_carry_gid_and_xid() {
    // Proves each field order (esp. rollback_prepared's two-ts/two-end-LSN layout) is exact.
    for name in [
        "begin_prepare",
        "prepare",
        "commit_prepared",
        "rollback_prepared",
    ] {
        let (xid, gid) = match decode(lookup(name).hex, false) {
            Message::BeginPrepare { xid, gid, .. }
            | Message::Prepare { xid, gid, .. }
            | Message::CommitPrepared { xid, gid, .. }
            | Message::RollbackPrepared { xid, gid, .. } => (xid, gid),
            other => panic!("{name}: expected a two-phase message, got {other:?}"),
        };
        assert_eq!(xid, 100, "{name} xid");
        assert_eq!(gid, "gtx", "{name} gid");
    }
}

#[test]
fn commit_prepared_k_disambiguated_from_update_key_marker() {
    // Top-level 'K' is Commit Prepared...
    assert!(matches!(
        decode(lookup("commit_prepared").hex, false),
        Message::CommitPrepared { .. }
    ));
    // ...while the 'K' inside an Update is the old-KEY marker. Same byte, different position.
    match decode(lookup("update_pk_change").hex, false) {
        Message::Update { old_kind, .. } => assert_eq!(old_kind, Some(OldTupleKind::Key)),
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn unknown_top_level_byte_still_errors() {
    // Adding the two-phase arms must not turn a truly unknown byte into a silent success.
    let mut ctx = StreamCtx::default();
    assert!(matches!(
        parse_message(&mut Reader::new(b"Zxxxx"), &mut ctx),
        Err(DecodeError::UnknownMessage { byte: b'Z' })
    ));
}

/// The decoder now covers the full catalog (v1 + v2 streaming + v3 two-phase): EVERY ported vector
/// round-trips to its golden line.
#[test]
fn all_vectors_render_to_golden() {
    assert_eq!(VECTORS.len(), 30, "expected the full ported VECTORS set");
    for v in VECTORS {
        assert_eq!(
            render(&decode(v.hex, v.streaming)),
            v.expected,
            "{}",
            v.name
        );
    }
}
