use super::*;
use proptest::prelude::*;

#[test]
fn lsn_is_layout_identical_to_its_inner_u64() {
    assert_eq!(std::mem::size_of::<Lsn>(), std::mem::size_of::<u64>());
    assert_eq!(std::mem::align_of::<Lsn>(), std::mem::align_of::<u64>());
}

#[test]
fn parses_x_slash_y_form() {
    assert_eq!("0/199BAC8".parse::<Lsn>().unwrap().as_u64(), 0x199BAC8);
    assert_eq!("1/0".parse::<Lsn>().unwrap().as_u64(), 1u64 << 32);
    assert_eq!(
        "2/3B423D8".parse::<Lsn>().unwrap().as_u64(),
        (2u64 << 32) | 0x3B423D8
    );
    // low half is not zero-padded by Postgres and both halves are hex.
    assert_eq!(
        "A/B".parse::<Lsn>().unwrap().as_u64(),
        (0xA_u64 << 32) | 0xB
    );
}

#[test]
fn parses_bare_16_hex_form() {
    assert_eq!(
        "00000000019A2B3C".parse::<Lsn>().unwrap().as_u64(),
        0x19A2B3C
    );
    // leading zeros are optional.
    assert_eq!("19A2B3C".parse::<Lsn>().unwrap().as_u64(), 0x19A2B3C);
    assert_eq!("0".parse::<Lsn>().unwrap(), Lsn::ZERO);
    assert_eq!(
        "FFFFFFFFFFFFFFFF".parse::<Lsn>().unwrap().as_u64(),
        u64::MAX
    );
    // lowercase parses too (Display is the canonical uppercase form).
    assert_eq!("deadbeef".parse::<Lsn>().unwrap().as_u64(), 0xDEADBEEF);
}

#[test]
fn display_is_zero_padded_16_upper_hex() {
    assert_eq!(Lsn::new(0x19A2B3C).to_string(), "00000000019A2B3C");
    assert_eq!(Lsn::ZERO.to_string(), "0000000000000000");
    assert_eq!(Lsn::new(u64::MAX).to_string(), "FFFFFFFFFFFFFFFF");
    assert_eq!(Lsn::new(0xDEADBEEF).to_string().len(), 16);
    // matches the design's walrus_extractor_meta sample.
    assert_eq!(Lsn::new(0x1B4C000).to_string(), "0000000001B4C000");
}

#[test]
fn hex_specifiers_forward_to_the_inner_u64() {
    let lsn = Lsn::new(0x019A_2B3C);
    assert_eq!(format!("{lsn:x}"), "19a2b3c");
    assert_eq!(format!("{lsn:X}"), "19A2B3C");
    assert_eq!(format!("{lsn:#018X}"), "0x00000000019A2B3C");
    assert_eq!(format!("{lsn:#010x}"), "0x019a2b3c");
}

#[test]
fn octal_and_binary_specifiers_forward_to_the_inner_u64() {
    assert_eq!(format!("{:o}", Lsn::new(8)), "10");
    assert_eq!(format!("{:#b}", Lsn::new(5)), "0b101");
}

#[test]
fn display_stays_padded_and_differs_from_raw_upper_hex() {
    let lsn = Lsn::new(0x019A_2B3C);
    assert_eq!(lsn.to_string(), "00000000019A2B3C");
    assert_eq!(lsn.to_string().len(), 16);
    assert_ne!(format!("{lsn}"), format!("{lsn:X}"));
    assert_eq!(serde_json::to_string(&lsn).unwrap(), "\"00000000019A2B3C\"");
}

#[test]
fn lower_hex_output_still_parses_back() {
    let lsn = Lsn::new(0x019A_2B3C);
    assert_eq!(format!("{lsn:x}").parse::<Lsn>().unwrap(), lsn);
}

#[test]
fn round_trips_through_display_and_from_str() {
    for raw in [0u64, 1, 0x199BAC8, 0x1B4C000, 0xFEDCBA9876543210, u64::MAX] {
        let lsn = Lsn::new(raw);
        assert_eq!(lsn.to_string().parse::<Lsn>().unwrap(), lsn);
    }
    // parse(X/Y) -> display -> parse is the identity on the value.
    let a = "0/199BAC8".parse::<Lsn>().unwrap();
    assert_eq!(a.to_string().parse::<Lsn>().unwrap(), a);
}

#[test]
fn u64_round_trips_through_from() {
    let lsn = Lsn::from(0x0000_0000_019A_2B3C_u64);
    assert_eq!(lsn, Lsn::new(0x0000_0000_019A_2B3C));
    assert_eq!(u64::from(lsn), 0x0000_0000_019A_2B3C);
    // `From` and the const constructors agree — neither is the "real" one.
    assert_eq!(u64::from(Lsn::ZERO), Lsn::ZERO.as_u64());
}

#[test]
fn bit_pattern_roundtrips_above_i64_max() {
    let lsn = Lsn::new(u64::MAX - 7);
    let encoded = lsn.to_sqlx_i64_bits();
    assert!(
        encoded < 0,
        "the high unsigned bit is preserved in the signed wire value"
    );
    assert_eq!(Lsn::from_sqlx_i64_bits(encoded), lsn);
}

#[test]
#[allow(clippy::op_ref, reason = "exercise the explicit borrowed Sub impl")]
fn subtraction_is_the_byte_distance() {
    assert_eq!(Lsn::new(500) - Lsn::new(200), 300);
    assert_eq!(Lsn::new(200) - Lsn::new(200), 0);
    assert_eq!(&Lsn::new(500) - &Lsn::new(200), 300);
}

/// The other half of the `Sub` contract: outside its documented domain (`self >= rhs`) the
/// operator is a caller bug, and the `debug_assert!` names it rather than hand back a distance a
/// caller would read as real. Without debug assertions that check is compiled out and the same
/// call saturates to 0, so the `cfg` gate sits on the *test*: `--release` skips it, never fails it.
#[cfg(debug_assertions)]
#[test]
#[should_panic(expected = "Lsn subtraction is defined for the ordered case only")]
fn unordered_subtraction_trips_the_debug_assert() {
    let _distance = Lsn::new(100) - Lsn::new(300);
}

#[test]
fn addition_advances_a_position() {
    assert_eq!(Lsn::new(100) + 24, Lsn::new(124));
    let mut lsn = Lsn::new(100);
    lsn += 24;
    assert_eq!(lsn, Lsn::new(124));
}

/// The documented ceiling: `+` clamps at `u64::MAX` instead of wrapping or panicking, and the
/// compound form means exactly the same thing.
#[test]
fn addition_saturates_instead_of_wrapping() {
    assert_eq!(Lsn::new(u64::MAX) + 1, Lsn::new(u64::MAX));
    let mut lsn = Lsn::new(u64::MAX - 1);
    lsn += 5;
    assert_eq!(lsn, Lsn::new(u64::MAX));
}

#[test]
fn saturating_sub_bytes_bottoms_out_at_zero() {
    assert_eq!(Lsn::new(300).saturating_sub_bytes(100), Lsn::new(200));
    assert_eq!(Lsn::new(100).saturating_sub_bytes(300), Lsn::ZERO);
}

#[test]
fn serde_round_trips_as_padded_string() {
    let lsn = Lsn::new(0x19A2B3C);
    let json = serde_json::to_string(&lsn).unwrap();
    assert_eq!(
        json, "\"00000000019A2B3C\"",
        "serialises as a padded STRING"
    );

    let back: Lsn = serde_json::from_str(&json).unwrap();
    assert_eq!(back, lsn);

    // deserialize also accepts the X/Y dialect, since it routes through FromStr.
    let from_xy: Lsn = serde_json::from_str("\"0/199BAC8\"").unwrap();
    assert_eq!(from_xy.as_u64(), 0x199BAC8);

    // a bare JSON number is NOT a valid Lsn (the padded-string contract).
    assert!(serde_json::from_str::<Lsn>("26879180").is_err());
}

/// The load-bearing invariant: sorting the *text* form equals sorting the *numeric* value.
#[test]
fn text_sort_equals_numeric_sort() {
    let vals = [
        Lsn::new(0xFF),
        Lsn::new(0x1),
        Lsn::ZERO,
        Lsn::new(0x100),
        Lsn::new(0x19A2B3C),
        Lsn::new(u64::MAX),
        Lsn::new(0x10),
        Lsn::new(0x1B4C000),
        Lsn::new(0xDEADBEEF),
    ];

    let mut by_numeric = vals;
    by_numeric.sort_by_key(|l| l.as_u64());

    let mut by_text = vals;
    by_text.sort_by_key(Lsn::to_string);

    let mut by_ord = vals;
    by_ord.sort();

    assert_eq!(by_text, by_numeric, "text sort must equal numeric sort");
    assert_eq!(by_ord, by_numeric, "derived Ord must be numeric");
    assert!(Lsn::ZERO < Lsn::new(1));
}

#[test]
fn rejects_garbage_and_overlong_input() {
    for bad in [
        "",                  // empty
        "xyz",               // non-hex
        "0/",                // empty low half
        "/0",                // empty high half
        "0/GG",              // non-hex low half
        "1FFFFFFFFFFFFFFFF", // 17 significant hex — overflows u64
        " 0/1",              // leading whitespace
        "+199",              // sign is not a hex digit
        "1FFFFFFFF/0",       // high half wider than u32
        "0/1FFFFFFFF",       // low half wider than u32
    ] {
        assert!(
            bad.parse::<Lsn>().is_err(),
            "expected {bad:?} to be rejected"
        );
    }
}

/// A rejection is a comparable value, not just an `is_err()`: both `Lsn` and `LsnParseError` are
/// `PartialEq`, so the whole `Result` is asserted at once — which pins the *reason* each form fails
/// under, and the verbatim input the message quotes back to an operator.
#[test]
fn a_rejection_compares_as_a_whole_result() {
    assert_eq!(
        "xyz".parse::<Lsn>(),
        Err(LsnParseError {
            input: "xyz".to_string(),
            reason: "not a 1–16 digit hex value",
        })
    );
    assert_eq!(
        "0/GG".parse::<Lsn>(),
        Err(LsnParseError {
            input: "0/GG".to_string(),
            reason: "X/Y half is not a valid hex u32",
        })
    );
}

// `round_trips_through_display_and_from_str` and `text_sort_equals_numeric_sort` state their claims
// over a handful of hand-picked positions. Both are really claims about *every* position — the
// padded form exists only because they hold — and 64 bits is the one space an example set cannot
// cover. Generating the input is what turns those samples into properties.
//
// `cases` is stated here rather than inherited from `PROPTEST_CASES`, so every run exercises the
// same number of positions; each case is one `format!` and one parse, so 2× the default costs
// microseconds. Failure persistence is off deliberately — a red case must not drop a
// `proptest-regressions/` file into a tree that does not ignore one. The shrunk position proptest
// prints IS the reproduction: paste it into the example above it and the case is pinned forever.
proptest! {
    #![proptest_config(ProptestConfig {
        cases: 512,
        failure_persistence: None,
        ..ProptestConfig::default()
    })]

    /// `round_trips_through_display_and_from_str` over the whole space, including the upper half
    /// that only `bit_pattern_roundtrips_above_i64_max` reaches by hand.
    #[test]
    fn display_round_trips_through_from_str(raw in any::<u64>()) {
        let lsn = Lsn::new(raw);

        let parsed = lsn.to_string().parse::<Lsn>();

        prop_assert_eq!(parsed, Ok(lsn));
    }

    /// The rendering the ordering below rests on: exactly 16 hex digits, never lowercase, for every
    /// position — and the lowercase spelling Postgres prints still parses back to the same one.
    #[test]
    fn display_is_16_upper_hex_digits_and_parses_in_either_case(raw in any::<u64>()) {
        let text = Lsn::new(raw).to_string();

        prop_assert_eq!(text.len(), 16);
        prop_assert!(text.bytes().all(|b| b.is_ascii_hexdigit() && !b.is_ascii_lowercase()));
        prop_assert_eq!(text.to_lowercase().parse::<Lsn>(), Ok(Lsn::new(raw)));
    }

    /// The load-bearing invariant `text_sort_equals_numeric_sort` samples with nine values: for
    /// *every* pair, comparing the padded text agrees with comparing the numbers, so a control-table
    /// or JSON sort over the text is a WAL-position sort.
    #[test]
    fn text_order_equals_numeric_order(a in any::<u64>(), b in any::<u64>()) {
        let (left, right) = (Lsn::new(a), Lsn::new(b));

        let by_text = left.to_string().cmp(&right.to_string());

        prop_assert_eq!(by_text, a.cmp(&b));
        prop_assert_eq!(left.cmp(&right), a.cmp(&b));
    }

    /// The other accepted dialect, over every pair of halves: Postgres's `X/Y` is two unpadded hex
    /// `u32`s meaning `(high << 32) | low`.
    #[test]
    fn x_slash_y_form_parses_as_its_two_halves(high in any::<u32>(), low in any::<u32>()) {
        let text = format!("{high:X}/{low:X}");

        let parsed = text.parse::<Lsn>();

        prop_assert_eq!(parsed, Ok(Lsn::new((u64::from(high) << 32) | u64::from(low))));
    }
}
