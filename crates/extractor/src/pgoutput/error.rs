//! The pgoutput decoder's structured error taxonomy.

/// Everything that can go wrong decoding a pgoutput message. Variants are *structured* (not
/// stringly-typed) so callers can branch on them throughout the decoder and consume loop.
/// This taxonomy is still growing; new variants must remain additive for downstream crates.
///
/// Comparable like the [`Message`](super::Message) it is the alternative to, so a decode result can
/// be asserted whole (`assert_eq!(parse_message(..), Err(DecodeError::TrailingBytes { .. }))`) rather
/// than only pattern-matched. Every payload is a scalar, so `Eq` is exact.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum DecodeError {
    /// Widths match pgoutput's `Int32` frame bound, keeping this per-byte error path compact.
    #[error("unexpected end of message: needed {needed}B at offset {offset}, {remaining} left")]
    UnexpectedEof {
        /// Number of bytes required by the pending field.
        needed: u32,
        /// Byte offset where the field begins.
        offset: u32,
        /// Number of unread bytes available in the frame.
        remaining: u32,
    },
    /// The leading type byte is not one this decoder knows. Either the source speaks a newer
    /// pgoutput version, or the stream is misaligned.
    #[error("unknown message type byte {byte:#04x}")]
    UnknownMessage {
        /// Unrecognized leading message tag.
        byte: u8,
    },
    /// A `TupleData` column marker was none of `n`/`u`/`t`/`b`, which almost always means the
    /// previous field was read at the wrong width.
    #[error("bad TupleData format byte {byte:#04x} (misaligned parse?)")]
    BadTupleFormat {
        /// Invalid tuple-column or old-image tag.
        byte: u8,
    },
    /// A Relation message carried a `relreplident` byte outside the documented set.
    #[error("invalid replica identity byte {byte:#04x}")]
    BadReplicaIdentity {
        /// Invalid `relreplident` protocol byte.
        byte: u8,
    },
    /// A Stream Start first-segment flag must be encoded as the exact protocol byte `0` or `1`.
    #[error("invalid Stream Start first-segment flag byte {byte:#04x}")]
    BadStreamStartFlag {
        /// Invalid first-segment flag byte.
        byte: u8,
    },
    /// A `String` field was not valid UTF-8. pgoutput strings are always text, so this is corruption
    /// rather than a supported encoding.
    #[error("invalid UTF-8 in String field")]
    Utf8(#[from] std::str::Utf8Error),
    /// The message decoded successfully but did not consume its whole payload — a decoder bug, and
    /// the reason every parse checks its own frame rather than trusting the length.
    #[error("{unconsumed} trailing bytes after a complete message")]
    TrailingBytes {
        /// Bytes left after decoding the complete message shape.
        unconsumed: u32,
    },
}

#[cfg(target_pointer_width = "64")]
const _: () = assert!(
    std::mem::size_of::<DecodeError>() == 24,
    "DecodeError crosses the pgoutput frame decode path"
);
