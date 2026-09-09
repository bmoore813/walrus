use super::*;

#[test]
fn interval_years_months_days_time_split() {
    // 1 year → 12 mon, + 2 mons = 14; 3 days; 04:05:06.5 → micros.
    let expected_micros = (4 * 3600 + 5 * 60 + 6) * 1_000_000 + 500_000;
    assert_eq!(
        parse_interval("1 year 2 mons 3 days 04:05:06.5").unwrap(),
        (14, 3, expected_micros)
    );
}

#[test]
fn fractional_seconds_scale_by_position_and_truncate_past_micros() {
    // The fractional run is scaled by its digit count rather than zero-padded into a buffer, so pin
    // all three edges: a short run, an exact six digits, and a longer run (truncated, never rounded).
    assert_eq!(parse_interval("00:00:00.5").unwrap(), (0, 0, 500_000));
    assert_eq!(parse_interval("00:00:00.000001").unwrap(), (0, 0, 1));
    assert_eq!(parse_interval("00:00:00.1234567").unwrap(), (0, 0, 123_456));
    // No fractional part at all is the same zero, through the empty-run branch.
    assert_eq!(parse_interval("00:00:01").unwrap(), (0, 0, 1_000_000));
}

#[test]
fn interval_1_month_ne_30_days_ne_720_hours() {
    // The three fields stay independent: none of these collapses into another.
    let a = parse_interval("1 mon").unwrap();
    let b = parse_interval("30 days").unwrap();
    let c = parse_interval("720:00:00").unwrap();
    assert_eq!(a, (1, 0, 0));
    assert_eq!(b, (0, 30, 0));
    assert_eq!(c, (0, 0, 720 * 3_600_000_000));
    assert_ne!(a, b);
    assert_ne!(b, c);
    assert_ne!(a, c);
}

#[test]
fn interval_keeps_a_negative_clock_and_ago_negates_every_field() {
    assert_eq!(parse_interval("-00:00:01").unwrap(), (0, 0, -1_000_000));
    // verbose `ago` negates every field.
    assert_eq!(parse_interval("@ 1 day ago").unwrap(), (0, -1, 0));
}

#[test]
fn timetz_positive_and_negative_offsets() {
    let micros = (12 * 3600 + 34 * 60 + 56) * 1_000_000 + 789_000;
    assert_eq!(
        parse_timetz("12:34:56.789+05:30").unwrap(),
        (micros, 19_800)
    );
    assert_eq!(
        parse_timetz("12:34:56-08").unwrap(),
        ((12 * 3600 + 34 * 60 + 56) * 1_000_000, -28_800)
    );
    // whole-hour east offset, no minutes.
    assert_eq!(parse_timetz("00:00:00+00").unwrap(), (0, 0));
}

#[test]
fn interval_number_without_a_following_unit_is_rejected() {
    // A `<number> <unit>` pair takes its unit from the *next* token, so a number at end of input has
    // nothing to pair with. Pin both shapes — a lone number and one trailing a complete pair — so the
    // token walk keeps failing instead of silently dropping the unattached value.
    assert!(parse_interval("5").is_err());
    assert!(parse_interval("1 day 2").is_err());
    // `@`/`ago` are decorations, never units: consuming one as a unit would make this parse.
    assert!(parse_interval("1 ago").is_err());
}

#[test]
fn interval_and_timetz_reject_garbage() {
    assert!(parse_interval("1 fortnight").is_err());
    assert!(parse_timetz("12:34:56").is_err()); // no offset
}

#[test]
fn interval_hours_overflow_is_a_value_parse_error_not_a_panic() {
    let out = parse_interval("9223372036854775807 hours");
    assert!(matches!(out, Err(Error::ValueParse(_))), "got {out:?}");
}

#[test]
fn interval_ago_negation_overflow_is_rejected() {
    let out = parse_interval("-9223372036854775808 days ago");
    assert!(matches!(out, Err(Error::ValueParse(_))), "got {out:?}");
}

#[test]
fn clock_token_overflow_returns_a_value_parse_error() {
    let out = parse_interval("9223372036854775807:00:00");
    assert!(matches!(out, Err(Error::ValueParse(_))), "got {out:?}");
}

#[test]
fn timetz_offset_overflow_returns_a_value_parse_error() {
    let out = parse_timetz("12:00:00+2147483647:00");
    assert!(matches!(out, Err(Error::ValueParse(_))), "got {out:?}");
}
