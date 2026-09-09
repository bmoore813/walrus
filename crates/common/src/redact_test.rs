use super::*;

const SECRET: &str = "postgres://walrus:hunter2@control-pg/walrus";

#[test]
fn both_formatters_print_the_placeholder() {
    let wrapped = Redacted::new(SECRET.to_owned());

    assert_eq!(format!("{wrapped:?}"), REDACTED);
    assert_eq!(format!("{wrapped}"), REDACTED);
}

/// `write_str` ignores width and precision, which is the point: a formatting flag must not become a
/// way to read a secret one character at a time, and `{:.3}` on the value would print `pos`.
#[test]
fn width_and_precision_cannot_reach_the_secret() {
    let wrapped = Redacted::new(SECRET.to_owned());

    assert_eq!(format!("{wrapped:.3}"), REDACTED);
    assert_eq!(format!("{wrapped:>60}"), REDACTED);
    assert_eq!(format!("{wrapped:>60?}"), REDACTED);
}

#[test]
fn expose_is_the_way_back_to_the_value() {
    let wrapped = Redacted::new(SECRET.to_owned());

    assert_eq!(wrapped.expose(), SECRET);
}

/// The wrapper exists for exactly this: the structs holding walrus's secrets all derive `Debug`, so
/// one `?cfg` anywhere would ship every field to the aggregator. Wrapping the field is what makes
/// that impossible rather than merely unattempted.
#[test]
fn a_deriving_struct_cannot_leak_a_wrapped_field() {
    #[derive(Debug)]
    struct Holder {
        instance: String,
        dsn: Redacted<String>,
    }

    let holder = Holder {
        instance: "walrus-extractor-0".to_owned(),
        dsn: Redacted::new(SECRET.to_owned()),
    };

    let rendered = format!("{holder:?}");

    assert_eq!(holder.dsn.expose(), SECRET);
    assert!(!rendered.contains("hunter2"), "{rendered}");
    assert!(rendered.contains(&holder.instance), "{rendered}");
    assert!(rendered.contains(REDACTED), "{rendered}");
}

/// Wrapping a config field must change no operator-facing key or value shape — `transparent` is
/// what keeps `WALRUS_CONTROL_DB_URL=…` deserializing as it did before.
#[test]
fn deserialization_is_transparent() {
    let parsed: Redacted<String> = serde_json::from_str(&format!("\"{SECRET}\"")).unwrap();

    assert_eq!(parsed.expose(), SECRET);
}

/// The three config `Default` impls build their empty DSN this way, and `validate()` then rejects
/// it — so the default has to be the empty string and not something the bounds check would accept.
#[test]
fn the_default_is_the_wrapped_default() {
    assert!(Redacted::<String>::default().expose().is_empty());
}

#[test]
fn both_owned_and_borrowed_strings_convert() {
    let owned: Redacted<String> = SECRET.to_owned().into();
    let borrowed: Redacted<String> = SECRET.into();

    assert_eq!(owned, borrowed);
    assert_eq!(borrowed.expose(), SECRET);
}
