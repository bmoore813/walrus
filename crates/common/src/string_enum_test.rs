use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TestParseError {
    column: &'static str,
    input: String,
}

impl TestParseError {
    fn new(column: &'static str, input: &str) -> Self {
        Self {
            column,
            input: input.to_string(),
        }
    }
}

string_enum! {
    /// Attributes and explicit visibility ride through the same arm.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum Signal {
        error = TestParseError;
        column = "test.signal";
        Green => "green",
        Amber => "amber",
        Red => "red", // trailing comma must be accepted by the `$(,)?` in the matcher
    }
}

string_enum! {
    // No attributes and `$vis` matches nothing; typed rejection inputs remain explicit.
    enum Bare {
        error = TestParseError;
        column = "test.bare";
        One => "one",
    }
}

string_enum! {
    /// Doc comment (a `#[doc = ".."]` meta), an explicit derive list, an explicit visibility,
    /// and a trailing comma in the variant table — all of it must take the first arm.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
    pub enum AllOptions {
        error = TestParseError;
        column = "test.all_options";
        First => "first",
        Second => "second",
    }
}

impl Copy for Bare {}

impl Clone for Bare {
    fn clone(&self) -> Self {
        *self
    }
}

#[test]
fn round_trips_every_variant_both_directions() {
    for (v, s) in [
        (Signal::Green, "green"),
        (Signal::Amber, "amber"),
        (Signal::Red, "red"),
    ] {
        assert_eq!(v.as_str(), s);
        assert_eq!(s.parse::<Signal>(), Ok(v));
    }
}

#[test]
fn rejection_keeps_the_supplied_column_and_input_as_data() {
    let err = "chartreuse".parse::<Signal>().unwrap_err();
    assert_eq!(err.column, "test.signal");
    assert_eq!(err.input, "chartreuse");
}

#[test]
fn defining_crate_helper_preserves_typed_error_inputs() {
    let error = unknown_variant("test.direct", "chartreuse", TestParseError::new);

    assert_eq!(error.column, "test.direct");
    assert_eq!(error.input, "chartreuse");
}

#[test]
fn generated_enum_is_copy_and_eq() {
    let a = Signal::Amber;
    let b = a; // `a` is still usable => Copy, which `as_str(self)` requires.
    assert_eq!(a, b);
    assert_eq!(format!("{a:?}"), "Amber");
}

#[test]
fn caller_derive_reaches_the_generated_enum() {
    use std::collections::HashSet;

    let values: HashSet<Signal> = [Signal::Green, Signal::Amber].into_iter().collect();
    assert_eq!(values.len(), 2);
}

#[test]
fn private_enum_without_attributes_still_round_trips() {
    let value = "one".parse::<Bare>().unwrap();
    assert_eq!(Bare::as_str(value), "one");
}

#[test]
fn every_optional_piece_takes_the_real_arm() {
    assert_eq!(AllOptions::Second.as_str(), "second");
    assert_eq!("first".parse::<AllOptions>(), Ok(AllOptions::First));
}
