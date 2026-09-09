//! Guard for the mutable-conversion policy: walrus deliberately has no generic mutable write target
//! because it has no fixed-length byte sink. If that changes, update this test with the new invariant.

use std::{
    io::Read,
    path::{Path, PathBuf},
};

/// Build the bound marker at runtime so this file does not match itself.
fn needle() -> String {
    ["As", "Mut", "<"].concat()
}

fn rs_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        if path.is_dir() {
            if path.file_name().is_some_and(|name| name == "target") {
                continue;
            }
            rs_files(&path, out)?;
        } else if path.extension().is_some_and(|extension| extension == "rs") {
            out.push(path);
        }
    }
    Ok(())
}

#[test]
fn no_asmut_bounds_anywhere_in_crates() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
    let mut files = Vec::new();
    rs_files(&root.join("crates"), &mut files).expect("walk Rust source tree");
    assert!(
        !files.is_empty(),
        "the source walk found nothing — bad root path"
    );

    let needle = needle();
    let mut hits = Vec::new();
    for path in files {
        let mut file = std::fs::OpenOptions::new()
            .read(true)
            .open(&path)
            .expect("open Rust source file");
        let mut source = String::new();
        file.read_to_string(&mut source)
            .expect("read Rust source file");
        for (line_index, line) in source.lines().enumerate() {
            if line.contains(&needle) {
                hits.push(format!("{}:{}", path.display(), line_index + 1));
            }
        }
    }

    assert!(
        hits.is_empty(),
        "generic mutable bound(s) appeared: {hits:?}\n\
         walrus has no fixed-length byte sink; document the new invariant before adding one",
    );
}
