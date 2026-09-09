#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "integration test — unwrap/expect are setup assertions; the synchronous manifest and \
              source scans are repository-policy checks, not runtime I/O"
)]
//! Guard for `obs-library-facade`: a library emits through the `tracing` facade, and only the
//! process that owns `main` installs a subscriber.
//!
//! walrus keeps exactly one deliberate exception to the rule's manifest half. `common` is the
//! shared bootstrap both binaries call, so it — and nothing else — depends on `tracing-subscriber`
//! and holds the single install in `crates/common/src/telemetry.rs`; every other crate, including
//! the extractor and transformer *libraries* that sit under the two `main`s, only emits events. The exception
//! is what erodes, and nothing in the toolchain notices when it does: `tracing-subscriber` compiles
//! wherever it is added, a second `try_init()` returns an `Err` a caller may discard, and a library
//! that installs its own subscriber silently takes format, level filter and destination away from
//! whoever owns `main`. The symptom is missing logs at runtime, never a red build — so the
//! manifests and the call sites are asserted here as text.

use std::path::{Path, PathBuf};

/// The one production module allowed to build and install a global subscriber.
const INSTALL_SITE: &str = "crates/common/src/telemetry.rs";

/// The one member manifest allowed to depend on a subscriber crate: it owns [`INSTALL_SITE`].
const SUBSCRIBER_OWNER: &str = "crates/common/Cargo.toml";

/// The install itself — `SubscriberInitExt::try_init`, which is also what registers the
/// `log` -> `tracing` bridge (see the `tracing-log` feature in [`SUBSCRIBER_OWNER`]).
const INSTALL_CALL: &str = ".try_init(";

/// Spellings that build or install a process-wide subscriber or logger.
const INSTALL_NEEDLES: [&str; 6] = [
    "tracing_subscriber::",
    INSTALL_CALL,
    "set_global_default(",
    "env_logger::",
    "set_boxed_logger(",
    "set_logger(",
];

/// A call into walrus's own install. `tracing` and `log` are facades a library may depend on and
/// emit through; this function is the one thing only a binary root may reach.
const INIT_CALL: &str = "init_tracing(";
/// Its declaration, which is not a call — [`INSTALL_SITE`] has to be able to define it.
const INIT_DEFINITION: &str = "fn init_tracing(";

/// Dependency names that install (or bridge into) a subscriber or logger. Plain `tracing` and `log`
/// are absent on purpose: they are the facade, and every crate here is welcome to emit through one.
const SUBSCRIBER_CRATES: [&str; 7] = [
    "tracing-subscriber",
    "console-subscriber",
    "tracing-log",
    "env_logger",
    "env-logger",
    "fern",
    "log4rs",
];

/// The manifest tables whose entries link into the shipped binaries. `[dev-dependencies]` is left
/// out deliberately: the rule allows a subscriber in tests, and nothing a consumer links sees it.
const SHIPPED_TABLES: [&str; 2] = ["[dependencies]", "[build-dependencies]"];

/// Workspace root — this crate's manifest dir is `<root>/crates/common`.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../..")
}

/// Every file under `dir` that `keep` accepts, recursively, skipping build and VCS output.
fn collect(dir: &Path, keep: fn(&Path) -> bool, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).expect("read a workspace directory") {
        let path = entry.expect("read a workspace directory entry").path();
        if path.is_dir() {
            let name = path.file_name().and_then(|name| name.to_str());
            if !matches!(name, Some("target" | ".git")) {
                collect(&path, keep, out);
            }
        } else if keep(&path) {
            out.push(path);
        }
    }
}

fn is_manifest(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == "Cargo.toml")
}

/// Production crate source: the `*_test.rs` unit-test siblings are excluded, because a test may
/// install its own scoped subscriber — that is the rule's own carve-out.
fn is_production_source(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".rs") && !name.ends_with("_test.rs"))
}

/// Every `crates/*/src/**/*.rs` that ships.
fn production_sources(root: &Path) -> Vec<PathBuf> {
    let mut sources = Vec::new();
    for entry in std::fs::read_dir(root.join("crates")).expect("read the workspace crates") {
        let src = entry.expect("read a crate entry").path().join("src");
        if src.is_dir() {
            collect(&src, is_production_source, &mut sources);
        }
    }
    sources.sort();
    sources
}

/// Every workspace member manifest, found rather than listed, so member number seven is covered
/// the day it lands instead of the day someone remembers this file.
fn member_manifests(root: &Path) -> Vec<PathBuf> {
    let mut manifests = Vec::new();
    collect(&root.join("crates"), is_manifest, &mut manifests);
    collect(&root.join("tests"), is_manifest, &mut manifests);
    manifests.sort();
    manifests
}

fn display_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

/// A crate's binary root: the file that owns `main`, and with it the subscriber choice.
fn is_binary_root(relative: &str) -> bool {
    relative.ends_with("/src/main.rs")
}

/// The body of `[section]` in `manifest`, up to the next table header at column 0.
fn section<'a>(manifest: &'a str, header: &str) -> Option<&'a str> {
    let mut offset = 0;
    let mut body_start = None;
    for line in manifest.split_inclusive('\n') {
        if line.trim_end_matches(['\r', '\n']) == header {
            body_start = Some(offset + line.len());
            break;
        }
        offset += line.len();
    }

    let body = &manifest[body_start?..];
    let mut body_end = body.len();
    let mut offset = 0;
    for line in body.split_inclusive('\n') {
        if line.starts_with('[') {
            body_end = offset;
            break;
        }
        offset += line.len();
    }
    Some(&body[..body_end])
}

/// Whether a TOML table body assigns `key`. A comment line starts with `#` and a feature entry
/// starts with `"`, so neither can answer for a dependency of that name — which is what keeps
/// `tracing-subscriber`'s own `"tracing-log"` feature from reading as a `tracing-log` dependency.
fn declares(body: &str, key: &str) -> bool {
    body.lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix(key))
        .any(|rest| rest.trim_start().starts_with('='))
}

fn manifest_offences(relative: &str, manifest: &str) -> Vec<String> {
    if relative == SUBSCRIBER_OWNER {
        return Vec::new();
    }

    let mut offences = Vec::new();
    for table in SHIPPED_TABLES {
        let Some(body) = section(manifest, table) else {
            continue;
        };
        for dependency in SUBSCRIBER_CRATES {
            if declares(body, dependency) {
                offences.push(format!(
                    "{relative}: {table} names {dependency}; a library only emits — move it to \
                     [dev-dependencies], or call the install {SUBSCRIBER_OWNER} already owns"
                ));
            }
        }
    }
    offences
}

fn source_offences(relative: &str, source: &str) -> Vec<String> {
    let mut offences = Vec::new();
    for (index, line) in source.lines().enumerate() {
        if line.trim_start().starts_with("//") {
            continue;
        }
        let line_number = index + 1;

        if relative != INSTALL_SITE {
            for needle in INSTALL_NEEDLES {
                if line.contains(needle) {
                    offences.push(format!(
                        "{relative}:{line_number}: builds or installs a subscriber ({needle}); \
                         only {INSTALL_SITE} may, and only a binary main may ask it to"
                    ));
                }
            }
        }

        if line.contains(INIT_CALL) && !line.contains(INIT_DEFINITION) && !is_binary_root(relative)
        {
            offences.push(format!(
                "{relative}:{line_number}: calls {INIT_CALL} outside a binary main; a library \
                 emits, and leaves format, filter and destination to whoever owns main"
            ));
        }
    }
    offences
}

#[test]
fn only_the_bootstrap_crate_depends_on_a_subscriber() {
    let root = workspace_root();
    let mut scanned = Vec::new();
    let mut offences = Vec::new();
    for path in member_manifests(&root) {
        let relative = display_path(&root, &path);
        let manifest = std::fs::read_to_string(&path).expect("read a member manifest");
        offences.extend(manifest_offences(&relative, &manifest));
        scanned.push(relative);
    }

    assert!(
        scanned.len() >= 6 && scanned.iter().any(|path| path == SUBSCRIBER_OWNER),
        "the manifest scan must reach every workspace member: {scanned:?}"
    );
    assert!(
        offences.is_empty(),
        "a subscriber dependency outside {SUBSCRIBER_OWNER} lets a library take the install away \
         from the binary that owns main:\n{}",
        offences.join("\n")
    );
}

/// Anti-vacuity for the manifest scan: the needle spelling still matches the dependency it hunts.
#[test]
fn the_bootstrap_crate_still_owns_the_subscriber_dependency() {
    let path = workspace_root().join(SUBSCRIBER_OWNER);
    let manifest = std::fs::read_to_string(path).expect("read the bootstrap manifest");
    let body = section(&manifest, "[dependencies]").expect("[dependencies] table");

    assert!(
        declares(body, "tracing-subscriber"),
        "{SUBSCRIBER_OWNER} owns the one subscriber dependency, so the scan's spelling must match"
    );
    assert!(
        declares(body, "console-subscriber"),
        "{SUBSCRIBER_OWNER} owns the optional console layer beside the normal subscriber"
    );
}

#[test]
fn no_production_source_installs_a_subscriber() {
    let root = workspace_root();
    let sources = production_sources(&root);
    assert!(!sources.is_empty(), "the production source scan is empty");

    let mut offences = Vec::new();
    for path in sources {
        let relative = display_path(&root, &path);
        let source = std::fs::read_to_string(&path).expect("read a production source");
        offences.extend(source_offences(&relative, &source));
    }

    assert!(
        offences.is_empty(),
        "only {INSTALL_SITE} may install a subscriber, and only a binary main may call it:\n{}",
        offences.join("\n")
    );
}

/// The other half of "one install site": the site itself, and both calls into it, must still be
/// there. A `main` that quietly stops calling it loses every event in the process, and no other
/// assertion in the suite goes red.
#[test]
fn the_install_site_and_both_binary_roots_still_wire_it_up() {
    let root = workspace_root();
    let path = root.join(INSTALL_SITE);
    let install = std::fs::read_to_string(path).expect("read the install site");

    assert!(
        install.contains(INSTALL_CALL),
        "{INSTALL_SITE} no longer installs a subscriber — the one process-wide install has moved"
    );

    for binary_root in [
        "crates/transformer/src/main.rs",
        "crates/extractor/src/main.rs",
    ] {
        let main_rs = root.join(binary_root);
        let source = std::fs::read_to_string(main_rs).expect("read a binary root");
        assert!(
            source.contains(INIT_CALL),
            "{binary_root} must install the subscriber before anything downstream can log"
        );
    }
}

#[test]
fn the_source_scan_rejects_a_planted_library_install() {
    let planted = concat!(
        "use tracing_subscriber::fmt;\n",
        "pub fn connect(url: &str) {\n",
        "    fmt().try_init().ok();\n",
        "    common::init_tracing(&Default::default()).ok();\n",
        "}\n",
    );

    let offences = source_offences("crates/transformer/src/lib.rs", planted);
    let report = offences.join("\n");

    assert_eq!(offences.len(), 3, "{report}");
    assert!(report.contains("crates/transformer/src/lib.rs:1"));
    assert!(report.contains("crates/transformer/src/lib.rs:3"));
    assert!(report.contains("crates/transformer/src/lib.rs:4"));
}

#[test]
fn the_source_scan_accepts_the_shapes_walrus_actually_has() {
    let install_site = concat!(
        "use tracing_subscriber::EnvFilter;\n",
        "pub fn init_tracing(cfg: &TelemetryConfig) -> crate::Result<()> {\n",
        "    tracing_subscriber::registry().with(filter).try_init()\n",
        "}\n",
    );
    let binary_root = concat!(
        "// `init_tracing` runs before any event has a subscriber to reach.\n",
        "fn main() {\n",
        "    common::init_tracing(&cfg.telemetry).ok();\n",
        "}\n",
    );
    let library = concat!(
        "/// Contrast `crate::telemetry::init_tracing`, whose install is idempotent.\n",
        "pub fn init() {}\n",
    );

    let empty = Vec::<String>::new();

    assert_eq!(source_offences(INSTALL_SITE, install_site), empty);
    assert_eq!(
        source_offences("crates/transformer/src/main.rs", binary_root),
        empty
    );
    assert_eq!(
        source_offences("crates/common/src/metrics.rs", library),
        empty
    );
}

#[test]
fn the_manifest_scan_reads_the_shipped_tables_only() {
    let shipped = concat!(
        "[dependencies]\n",
        "tracing = { workspace = true }\n",
        "tracing-subscriber = { workspace = true, features = [\n",
        "    \"tracing-log\",\n",
        "] }\n",
        "\n",
        "[dev-dependencies]\n",
        "env_logger = \"0.11\"\n",
    );
    let dev_only = concat!(
        "[dependencies]\n",
        "tracing = { workspace = true }\n",
        "[dev-dependencies]\n",
        "tracing-subscriber = \"0.3\"\n",
    );

    let empty = Vec::<String>::new();
    let offences = manifest_offences("crates/transformer/Cargo.toml", shipped);

    assert_eq!(offences.len(), 1, "{}", offences.join("\n"));
    assert!(offences[0].contains("tracing-subscriber"));
    assert_eq!(
        manifest_offences("crates/transformer/Cargo.toml", dev_only),
        empty
    );
    assert_eq!(manifest_offences(SUBSCRIBER_OWNER, shipped), empty);
}
