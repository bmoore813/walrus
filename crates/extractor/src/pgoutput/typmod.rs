//! `atttypmod` decoding. On the wire it's an `Int32`; `0xFFFFFFFF` is the `-1` "no modifier"
//! sentinel. For `numeric` it packs precision/scale as `((p << 16) | s) + 4`.

/// Decode a raw wire `atttypmod` into its signed value (`0xFFFFFFFF` → `-1`).
///
/// # Examples
///
/// ```
/// use extractor::pgoutput::typmod::atttypmod;
///
/// assert_eq!(atttypmod(0xFFFF_FFFF), -1);
/// assert_eq!(atttypmod(655_366), 655_366);
/// ```
#[must_use]
pub const fn atttypmod(raw: u32) -> i32 {
    raw.cast_signed()
}

/// Recover `numeric(p, s)` from a decoded `atttypmod`. `< 4` (including the `-1` sentinel) means
/// unconstrained → `None`; otherwise subtract 4 first, then unpack. e.g. `numeric(10, 2)` →
/// `atttypmod 655366` → `Some((10, 2))`.
///
/// # Examples
///
/// ```
/// use extractor::pgoutput::typmod::{atttypmod, numeric_precision_scale};
///
/// assert_eq!(numeric_precision_scale(655_366), Some((10, 2)));
/// // The `-1` sentinel — an unconstrained `numeric` — has nothing to unpack.
/// assert_eq!(numeric_precision_scale(atttypmod(0xFFFF_FFFF)), None);
/// ```
#[must_use]
pub fn numeric_precision_scale(typmod: i32) -> Option<(u16, u16)> {
    if typmod < 4 {
        return None;
    }
    let packed = u32::try_from(typmod - 4).ok()?;
    let precision = u16::try_from((packed >> 16) & 0xFFFF).ok()?;
    let scale = u16::try_from(packed & 0xFFFF).ok()?;
    Some((precision, scale))
}

#[cfg(test)]
#[path = "typmod_test.rs"]
mod tests;
