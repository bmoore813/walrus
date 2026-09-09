#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test - filesystem and process calls build isolated rustc fixtures"
)]
//! Compile-fail coverage for walrus's inline const invariants.

use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

#[test]
fn production_sites_contain_inline_const_guards() {
    let sites = [
        (
            "crates/common/src/error.rs",
            include_str!("../src/error.rs"),
            "ExitCode::Internal must stay in 0..125",
        ),
        (
            "crates/walrus-config/src/extractor.rs",
            include_str!("../../walrus-config/src/extractor.rs"),
            "MAX_DURATION must be nonzero",
        ),
        (
            "crates/extractor/src/health.rs",
            include_str!("../../extractor/src/health.rs"),
            "Phase::Bootstrapping must stay byte 0",
        ),
        (
            "crates/transformer/src/health.rs",
            include_str!("../../transformer/src/health.rs"),
            "TransformerPhase::Quarantined must stay byte 2",
        ),
        (
            "crates/extractor/src/pgoutput/mod.rs",
            include_str!("../../extractor/src/pgoutput/mod.rs"),
            "proto v2 §7 requires exactly 7 xid-prefixed tags (RYIUDTM)",
        ),
    ];

    for (path, source, message_stem) in sites {
        assert!(
            source.contains("const {") && source.contains(message_stem),
            "{path} must contain an inline const guard with diagnostic stem {message_stem:?}"
        );
    }
}

#[test]
fn invalid_const_values_are_rejected() {
    let cases = [
        (
            "exit_code_300",
            r#"
#[repr(i32)]
enum ExitCode {
    Internal = 300,
}

fn convert() {
    const {
        assert!(
            (ExitCode::Internal as i32) >= 0 && (ExitCode::Internal as i32) < 125,
            "ExitCode::Internal must stay in 0..125 or the process status contract breaks"
        );
    }
}
"#,
            "ExitCode::Internal must stay in 0..125",
        ),
        (
            "bootstrapping_byte_2",
            r#"
#[repr(u8)]
enum Phase {
    Bootstrapping = 2,
    Ready = 1,
}

fn decode() {
    const {
        assert!(
            Phase::Bootstrapping as u8 == 0,
            "Phase::Bootstrapping must stay byte 0 because AtomicPhase defaults to zero"
        );
        assert!(
            Phase::Ready as u8 == 1,
            "Phase::Ready must stay byte 1 so AtomicPhase store and decode agree"
        );
    }
}
"#,
            "Phase::Bootstrapping must stay byte 0",
        ),
        (
            "quarantined_byte_3",
            r#"
#[repr(u8)]
enum TransformerPhase {
    Bootstrapping = 0,
    Ready = 1,
    Quarantined = 3,
}

fn decode() {
    const {
        assert!(
            TransformerPhase::Quarantined as u8 == 2,
            "TransformerPhase::Quarantined must stay byte 2 or clear_quarantine's compare_exchange swaps the wrong phase"
        );
    }
}
"#,
            "TransformerPhase::Quarantined must stay byte 2",
        ),
        (
            "short_xid_prefix",
            r#"
const XID_PREFIXED: &[u8] = b"RYIUDT";

fn parse() {
    const {
        assert!(
            XID_PREFIXED.len() == 7,
            "proto v2 §7 requires exactly 7 xid-prefixed tags (RYIUDTM) or streamed bytes misalign"
        );
    }
}
"#,
            "proto v2 §7 requires exactly 7 xid-prefixed tags (RYIUDTM)",
        ),
    ];

    let directory = fixture_directory();
    std::fs::create_dir(&directory).expect("create process-unique rustc fixture directory");
    let rustc = std::env::var_os("RUSTC").unwrap_or_else(|| OsString::from("rustc"));
    let mut results = Vec::with_capacity(cases.len());

    for (name, source, message_stem) in cases {
        let source_path = directory.join(format!("{name}.rs"));
        let output_path = directory.join(format!("lib{name}.rlib"));
        std::fs::write(&source_path, source).expect("write isolated rustc fixture");
        let output = Command::new(&rustc)
            .arg("--edition=2024")
            .arg("--crate-type=lib")
            .arg("--crate-name")
            .arg(name)
            .arg(&source_path)
            .arg("-o")
            .arg(output_path)
            .output()
            .expect("invoke the active rustc");
        results.push((
            name,
            output.status.success(),
            String::from_utf8_lossy(&output.stderr).into_owned(),
            message_stem,
        ));
    }

    std::fs::remove_dir_all(&directory).expect("remove only the rustc fixture directory");

    for (name, succeeded, stderr, message_stem) in results {
        assert!(!succeeded, "{name} unexpectedly compiled successfully");
        assert!(
            stderr.contains("E0080"),
            "{name} failed for the wrong reason (missing E0080):\n{stderr}"
        );
        assert!(
            stderr.contains(message_stem),
            "{name} did not report its invariant diagnostic {message_stem:?}:\n{stderr}"
        );
    }
}

fn fixture_directory() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock must be after the Unix epoch")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "walrus-const-invariant-{}-{nonce}",
        std::process::id()
    ))
}
