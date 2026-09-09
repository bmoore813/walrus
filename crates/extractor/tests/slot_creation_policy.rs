//! Architectural guard for the one-slot contract.
//!
//! Reload/export code may open ordinary SQL connections and exported snapshots, but logical-slot
//! creation stays centralized in `slot.rs` so a table reload cannot accidentally consume another
//! finite source slot.

#![allow(
    clippy::disallowed_methods,
    clippy::expect_used,
    reason = "synchronous source-policy test: filesystem failures should stop the test with their path"
)]

use std::path::{Path, PathBuf};

const CREATE_FN: &str = "pg_create_logical_replication_slot";

fn rust_sources(root: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(root).expect("read extractor source directory") {
        let path = entry.expect("read extractor source entry").path();
        if path.is_dir() {
            rust_sources(&path, out);
        } else if path.extension().is_some_and(|ext| ext == "rs") {
            out.push(path);
        }
    }
}

#[test]
fn logical_slot_creation_is_owned_only_by_slot_module() {
    let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let slot_module = src.join("slot.rs");
    let mut sources = Vec::new();
    rust_sources(&src, &mut sources);

    let mut owners = Vec::new();
    for path in sources {
        let body = std::fs::read_to_string(&path).expect("read extractor Rust source");
        if body.contains(CREATE_FN) {
            owners.push(path);
        }
    }

    assert_eq!(
        owners,
        vec![slot_module],
        "logical replication slot creation must remain centralized; reload workers use imported SQL snapshots"
    );
}
