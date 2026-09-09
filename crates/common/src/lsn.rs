//! The [`Lsn`] newtype: a Postgres Log Sequence Number as a single `u64`.
//!
//! Every watermark, manifest bound, and provenance field in walrus is an LSN. Postgres shows them
//! two ways — the human `X/Y` form (`0/199BAC8`) and, in walrus's own JSON / control tables, a
//! **zero-padded 16-hex** string (`00000000019A2B3C`) chosen precisely so a *text* sort equals a
//! *numeric* sort. This type parses both forms, prints the padded form, orders numerically, and
//! serialises as the padded string — the ordering contract the whole `(commit_lsn, lsn)` pipeline
//! relies on.

use std::fmt;
use std::mem::{align_of, size_of};
use std::str::FromStr;

/// A Postgres Log Sequence Number as a single `u64`.
///
/// Canonical text form is **uppercase, zero-padded 16-hex** ([`Display`](fmt::Display)), chosen so
/// lexical order equals numeric order. `Ord` derives from the inner `u64`, so it *is* numeric
/// order.
///
/// The transparent representation guarantees that the SQLx encoding delegation stays
/// layout-identical to the single `u64` value it forwards as `i64` wire bits.
///
/// # Examples
///
/// Both accepted dialects name the same position, and printing always gives the padded form:
///
/// ```
/// use common::Lsn;
///
/// let from_postgres: Lsn = "0/199BAC8".parse()?;
/// let from_control_db: Lsn = "000000000199BAC8".parse()?;
///
/// assert_eq!(from_postgres, from_control_db);
/// assert_eq!(from_postgres.as_u64(), 0x199BAC8);
/// assert_eq!(from_postgres.to_string(), "000000000199BAC8");
/// # Ok::<(), common::lsn::LsnParseError>(())
/// ```
///
/// The padding is what buys the ordering contract: sorting the *text* sorts the WAL positions.
///
/// ```
/// use common::Lsn;
///
/// let mut positions = [Lsn::new(0x100), Lsn::ZERO, Lsn::new(0xFF)];
/// positions.sort_by_key(Lsn::to_string);
///
/// assert_eq!(positions, [Lsn::ZERO, Lsn::new(0xFF), Lsn::new(0x100)]);
/// ```
#[repr(transparent)]
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Lsn(u64);

const _: () = assert!(
    size_of::<Lsn>() == size_of::<u64>() && align_of::<Lsn>() == align_of::<u64>(),
    "Lsn must stay layout-identical to u64 for SQLx encoding"
);

impl Lsn {
    /// The zero LSN — orders below every nonzero LSN.
    ///
    /// This is an associated constant, not a function, so it has no call site to inline.
    pub const ZERO: Lsn = Lsn(0);

    /// Wrap a raw `u64` WAL position.
    #[must_use]
    #[inline]
    pub const fn new(raw: u64) -> Self {
        Lsn(raw)
    }

    /// The raw `u64` WAL position.
    #[must_use]
    #[inline]
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Reinterpret this unsigned LSN as the signed integer used by SQLx's binary encoder.
    #[cfg(any(feature = "sqlx", test))]
    const fn to_sqlx_i64_bits(self) -> i64 {
        // `cast_signed`, not `as i64`: pg_lsn and int8 share the same 64-bit wire representation,
        // so the reinterpretation is the point. The named method states that, needs no scoped
        // `cast_possible_wrap` allow, and matches `pgoutput::typmod`/`reader`.
        self.0.cast_signed()
    }

    /// Recover an unsigned LSN from SQLx's signed view of the same wire bits.
    #[cfg(any(feature = "sqlx", test))]
    const fn from_sqlx_i64_bits(raw: i64) -> Self {
        // The inverse reinterpretation — see `to_sqlx_i64_bits` for why this is not `TryFrom`.
        Self(raw.cast_unsigned())
    }

    /// Retreat this position by `bytes`, **saturating at 0**.
    ///
    /// A named method, not `Sub<u64>`: the clamp is a policy decision (the retention floor must
    /// never go negative and must never wrap), and a reader seeing `lsn - lag` would not expect it.
    ///
    /// # Examples
    ///
    /// ```
    /// use common::Lsn;
    ///
    /// assert_eq!(Lsn::new(300).saturating_sub_bytes(100), Lsn::new(200));
    /// // A floor further back than the head clamps at zero instead of wrapping.
    /// assert_eq!(Lsn::new(100).saturating_sub_bytes(300), Lsn::ZERO);
    /// ```
    #[must_use = "returns the retreated position; it does not move anything"]
    pub const fn saturating_sub_bytes(self, bytes: u64) -> Self {
        Lsn(self.0.saturating_sub(bytes))
    }
}

impl From<u64> for Lsn {
    /// Wrap a raw `u64` WAL position. [`Lsn::new`] stays for `const` contexts ([`Lsn::ZERO`]).
    fn from(raw: u64) -> Self {
        Lsn(raw)
    }
}

impl From<Lsn> for u64 {
    /// The raw `u64` WAL position. [`Lsn::as_u64`] stays for `const` contexts.
    fn from(lsn: Lsn) -> Self {
        lsn.0
    }
}

/// WAL byte distance between two positions.
///
/// **Defined for the ordered case only** (`self >= rhs`) — which is what the `(commit_lsn, lsn)`
/// contract and the control DB's `CHECK (transformed_lsn <= raw_appended_lsn)` guarantee. A
/// violation is a bug: it trips a `debug_assert!` in tests and saturates to 0 in release rather than
/// wrapping. Callers whose inputs are genuinely unordered must bound an operand into that domain
/// first — `head.max(floor) - floor`, not a branch on the same comparison (see the transformer's
/// `phase_a::raw_append_lag_bytes`).
impl std::ops::Sub<Lsn> for Lsn {
    type Output = u64;

    /// # Panics
    ///
    /// In debug builds, panics if `self < rhs`: the ordered domain above is a caller invariant, so a
    /// violation is a bug worth failing on where it happens rather than a distance to hand back.
    /// Release builds carry no assertion and saturate to 0 instead of wrapping.
    fn sub(self, rhs: Lsn) -> u64 {
        debug_assert!(
            self.0 >= rhs.0,
            "Lsn subtraction is defined for the ordered case only"
        );
        self.0.saturating_sub(rhs.0)
    }
}

/// The reference form, so a borrow site need not copy.
impl std::ops::Sub<&Lsn> for &Lsn {
    type Output = u64;

    /// # Panics
    ///
    /// Carries the by-value operator's debug-only ordered-domain assertion, which this forwards to.
    fn sub(self, rhs: &Lsn) -> u64 {
        *self - *rhs
    }
}

/// Advance a WAL position by `bytes`.
///
/// **Saturates at [`u64::MAX`]** rather than wrapping or panicking, so the operator's overflow
/// behaviour is stated where a reader meets it — as the `Sub` above states its own clamp. This end
/// needs no named spelling the way [`Lsn::saturating_sub_bytes`] does: a real WAL position is
/// nowhere near 2^64, so the ceiling guards a corrupt operand instead of encoding the reachable
/// policy decision the retention floor makes.
impl std::ops::Add<u64> for Lsn {
    type Output = Lsn;

    fn add(self, bytes: u64) -> Lsn {
        Lsn(self.0.saturating_add(bytes))
    }
}

/// The in-place spelling of the [`Add`](std::ops::Add) above, saturation included — a compound
/// assignment must never mean something its binary form does not.
impl std::ops::AddAssign<u64> for Lsn {
    fn add_assign(&mut self, bytes: u64) {
        *self = *self + bytes;
    }
}

/// Failure to parse either the `X/Y` or the 16-hex form of an [`Lsn`].
///
/// Carries the value bundle its sibling leaf errors carry ([`crate::sql::IdentError`],
/// [`WorkerThreadsError`](crate::runtime::WorkerThreadsError)): both fields are plain owned data, and
/// [`Lsn`] is already [`PartialEq`], so `Result<Lsn, LsnParseError>` compares as a whole.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid LSN {input:?}: {reason}")]
pub struct LsnParseError {
    /// The text that was rejected, preserved verbatim so the message can quote it.
    pub input: String,
    /// Why it was rejected, as a fixed phrase — one of a closed set the parser owns, never
    /// user-supplied text.
    pub reason: &'static str,
}

/// Parse one hexadecimal half of the `X/Y` form (each half fits a `u32`; empty / non-hex / a `+`
/// sign / an over-wide half all reject).
fn parse_hex_u32(part: &str) -> Option<u32> {
    if part.is_empty() || !part.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    u32::from_str_radix(part, 16).ok()
}

/// Parse the bare-hex form: 1–16 significant hex digits (leading zeros allowed), rejecting
/// non-hex, empty, a sign, and anything wider than `u64`.
fn parse_hex_u64(s: &str) -> Option<u64> {
    if s.is_empty() || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    // More than 16 significant hex digits cannot fit a u64 — a caller bug, not a truncation.
    if s.trim_start_matches('0').len() > 16 {
        return None;
    }
    u64::from_str_radix(s, 16).ok()
}

impl FromStr for Lsn {
    type Err = LsnParseError;

    /// Accepts `"0/199BAC8"` (two hex halves, `(high << 32) | low`) and `"00000000019A2B3C"`
    /// (bare 1–16 hex, with or without leading zeros).
    ///
    /// # Errors
    ///
    /// Returns [`LsnParseError`] carrying `s` verbatim when the text is neither form: an `X/Y`
    /// half that is empty, non-hex, signed, or wider than a `u32`, or a bare value that is empty,
    /// non-hex, signed, or wider than 16 significant hex digits.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let reject = |reason: &'static str| LsnParseError {
            input: s.to_string(),
            reason,
        };
        if let Some((hi, lo)) = s.split_once('/') {
            let high =
                parse_hex_u32(hi).ok_or_else(|| reject("X/Y half is not a valid hex u32"))?;
            let low = parse_hex_u32(lo).ok_or_else(|| reject("X/Y half is not a valid hex u32"))?;
            Ok(Lsn((u64::from(high) << 32) | u64::from(low)))
        } else {
            parse_hex_u64(s)
                .map(Lsn)
                .ok_or_else(|| reject("not a 1–16 digit hex value"))
        }
    }
}

impl fmt::Display for Lsn {
    /// Always 16 uppercase hex digits, zero-padded — e.g. `00000000019A2B3C`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016X}", self.0)
    }
}

impl fmt::Debug for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Lsn({:016X})", self.0)
    }
}

/// Lowercase hexadecimal, unpadded by default.
///
/// Forwards the whole formatter so alternate form, width, precision, and zero-fill flags behave
/// exactly as they do for the inner `u64`. [`Display`](fmt::Display) remains the canonical padded
/// walrus rendering.
impl fmt::LowerHex for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::LowerHex::fmt(&self.0, f)
    }
}

/// Uppercase hexadecimal, unpadded by default.
///
/// Formatter flags forward to the inner `u64`; unlike [`Display`](fmt::Display), the default form
/// intentionally omits leading zeroes.
impl fmt::UpperHex for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::UpperHex::fmt(&self.0, f)
    }
}

/// Octal, unpadded by default, with all formatter flags forwarded to the inner `u64`.
impl fmt::Octal for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Octal::fmt(&self.0, f)
    }
}

/// Binary, unpadded by default, with all formatter flags forwarded to the inner `u64`.
impl fmt::Binary for Lsn {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Binary::fmt(&self.0, f)
    }
}

impl serde::Serialize for Lsn {
    /// Emit the canonical padded string (never a bare JSON number) so the on-disk form sorts as
    /// text exactly as it sorts numerically.
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> serde::Deserialize<'de> for Lsn {
    /// Read a string in either accepted dialect via [`FromStr`] — so a malformed WAL position is a
    /// deserialization error and no out-of-grammar [`Lsn`] is ever constructed.
    ///
    /// Hand-written rather than `#[serde(try_from = "String")]`, which would need a `TryFrom<String>`
    /// impl: a second owned-input parser aliasing [`FromStr`], the grammar's single entry point, to
    /// reach this exact read-a-`String`-then-parse path. The paired `into = "String"` is declined for
    /// a separate reason: it would allocate a `String` per LSN, where the
    /// [`Serialize`](serde::Serialize) above renders straight into the serializer via `collect_str`
    /// — and `lsn` plus `commit_lsn` ride every row of the provenance document.
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s: String = serde::Deserialize::deserialize(d)?;
        s.parse::<Lsn>().map_err(serde::de::Error::custom)
    }
}

/// Postgres `pg_lsn` support (feature `sqlx`), delegating to sqlx's `PgLsn` so an [`Lsn`] binds and
/// decodes as a native `pg_lsn` — which sorts as a WAL position, matching this newtype's ordering.
#[cfg(feature = "sqlx")]
mod sqlx_support {
    use super::Lsn;
    use sqlx::postgres::{PgArgumentBuffer, PgTypeInfo, PgValueRef};
    use sqlx::{Decode, Encode, Postgres, Type, TypeInfo};

    impl Type<Postgres> for Lsn {
        fn type_info() -> PgTypeInfo {
            // sqlx 0.8 has no built-in pg_lsn (OID 3220); resolve the type by name.
            PgTypeInfo::with_name("pg_lsn")
        }
        fn compatible(ty: &PgTypeInfo) -> bool {
            // Match by name so a catalog-resolved pg_lsn column (fetched by OID) is accepted the
            // same as our `with_name` declaration — the default PartialEq would reject that.
            ty.name().eq_ignore_ascii_case("pg_lsn")
        }
    }

    impl Encode<'_, Postgres> for Lsn {
        fn encode_by_ref(
            &self,
            buf: &mut PgArgumentBuffer,
        ) -> Result<sqlx::encode::IsNull, sqlx::error::BoxDynError> {
            // pg_lsn's binary wire format is an 8-byte big-endian integer — identical to int8, so
            // reuse i64's encoder. The helper's `cast_signed` preserves the bit pattern;
            // `TryFrom` would reject every valid LSN above i64::MAX.
            <i64 as Encode<Postgres>>::encode_by_ref(&self.to_sqlx_i64_bits(), buf)
        }
    }

    impl<'r> Decode<'r, Postgres> for Lsn {
        fn decode(value: PgValueRef<'r>) -> Result<Self, sqlx::error::BoxDynError> {
            let raw = <i64 as Decode<Postgres>>::decode(value)?;
            Ok(Lsn::from_sqlx_i64_bits(raw))
        }
    }
}

#[cfg(test)]
#[path = "lsn_test.rs"]
mod tests;
