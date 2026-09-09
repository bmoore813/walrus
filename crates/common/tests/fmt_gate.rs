//! Guard for `lint-rustfmt-check`: formatting stays a machine-checked gate, not a convention.
//!
//! CI's `cargo fmt --all --check` is what turns an unformatted diff into a red build, and the
//! justfile's `fmt` recipe is its local mirror. Neither surface is reachable from a Rust source
//! file, so nothing in the tree notices when the step is renamed away, loses `--check` (which
//! rewrites the working tree and then exits 0), narrows to a single package, or when the pinned
//! toolchain stops shipping the component the command needs — the gate simply stops gating, and
//! every later diff is unchecked. This test is what goes red instead.
//!
//! `--all` is not decoration. Without it the scope of a `cargo fmt` run is whatever package the
//! invocation's directory resolves to, so the gate's coverage would be a property of where CI
//! happens to run the step; naming the flag makes "every workspace member" the contract.

const CI_WORKFLOW: &str = include_str!("../../../.github/workflows/ci.yml");
const JUSTFILE: &str = include_str!("../../../justfile");
const TOOLCHAIN: &str = include_str!("../../../rust-toolchain.toml");

/// The `cargo fmt` invocation a line *runs*, or `None` when the line merely names one. The
/// workflow titles its step after the command it runs, so a `name:` key is prose and must not
/// answer for the gate; `#` opens a comment in the workflow and in the justfile alike.
fn cargo_fmt_command(line: &str) -> Option<&str> {
    let line = line.trim();
    let line = line.strip_prefix("- ").unwrap_or(line);
    if line.starts_with('#') || line.starts_with("name:") {
        return None;
    }

    let command = line.strip_prefix("run:").unwrap_or(line).trim();
    command.starts_with("cargo fmt").then_some(command)
}

/// What a surface owes rustfmt: check every workspace member, and fail on a diff instead of
/// silently rewriting it.
fn fmt_gate(surface: &str) -> Result<(), &'static str> {
    let Some(command) = surface.lines().find_map(cargo_fmt_command) else {
        return Err("nothing runs cargo fmt");
    };

    if !command.contains("--check") {
        Err("cargo fmt runs without --check")
    } else if !command.contains("--all") {
        Err("the check is not workspace-wide")
    } else {
        Ok(())
    }
}

/// The body of `just <recipe>`: the lines indented under its header, up to the next line at column
/// zero. `None` when no recipe by that name exists.
fn recipe_body<'a>(justfile: &'a str, recipe: &str) -> Option<&'a str> {
    let mut offset = 0;
    let mut body_start: Option<usize> = None;

    for line in justfile.split_inclusive('\n') {
        if let Some(start) = body_start {
            if !line.starts_with([' ', '\t']) && !line.trim().is_empty() {
                return Some(&justfile[start..offset]);
            }
        } else if line.trim_end().strip_suffix(':') == Some(recipe) {
            body_start = Some(offset + line.len());
        }
        offset += line.len();
    }

    body_start.map(|start| &justfile[start..])
}

/// Whether the pinned toolchain installs rustfmt. CI's `rustup toolchain install` reads this file,
/// so a missing component turns the gate into a "rustfmt is not installed" error rather than a
/// formatting verdict.
fn ships_rustfmt(toolchain: &str) -> Result<(), &'static str> {
    let declared = toolchain.lines().find_map(|line| {
        let (key, value) = line.split_once('=')?;
        (key.trim() == "components").then_some(value)
    });

    let Some(components) = declared else {
        return Err("the pin declares no components");
    };

    if components.contains("\"rustfmt\"") {
        Ok(())
    } else {
        Err("the components omit rustfmt")
    }
}

#[test]
fn ci_fails_the_build_on_an_unformatted_workspace() {
    assert_eq!(fmt_gate(CI_WORKFLOW), Ok(()));
}

#[test]
fn the_just_recipe_mirrors_the_ci_gate() {
    let recipe = recipe_body(JUSTFILE, "fmt").expect("no `fmt` recipe in the justfile");

    assert_eq!(fmt_gate(recipe), Ok(()));
}

#[test]
fn the_pinned_toolchain_ships_rustfmt() {
    assert_eq!(ships_rustfmt(TOOLCHAIN), Ok(()));
}

#[test]
fn the_fmt_gate_policy_rejects_fabricated_surfaces() {
    let cases = [
        ("      - run: cargo fmt --all --check\n", Ok(())),
        ("    cargo fmt --all --check\n", Ok(())),
        (
            "      - run: cargo clippy --all-targets\n",
            Err("nothing runs cargo fmt"),
        ),
        // A step titled after a command it does not run, and a commented-out recipe line.
        (
            "      - name: cargo fmt --all --check\n",
            Err("nothing runs cargo fmt"),
        ),
        (
            "    # cargo fmt --all --check\n",
            Err("nothing runs cargo fmt"),
        ),
        (
            "      - run: cargo fmt --all\n",
            Err("cargo fmt runs without --check"),
        ),
        (
            "      - run: cargo fmt --check\n",
            Err("the check is not workspace-wide"),
        ),
    ];

    for (surface, expected) in cases {
        assert_eq!(fmt_gate(surface), expected, "surface:\n{surface}");
    }
}

#[test]
fn a_recipe_body_stops_at_the_next_recipe() {
    let justfile = concat!(
        "# Baseline gates (mirror CI).\n",
        "fmt:\n",
        "    cargo fmt --all --check\n",
        "\n",
        "clippy:\n",
        "    cargo clippy --all-targets --all-features -- -D warnings\n",
    );

    assert_eq!(
        recipe_body(justfile, "fmt"),
        Some("    cargo fmt --all --check\n\n")
    );
    assert_eq!(recipe_body(justfile, "test"), None);
}

#[test]
fn the_toolchain_policy_rejects_fabricated_pins() {
    let cases = [
        ("components = [\"rustfmt\", \"clippy\"]\n", Ok(())),
        (
            "channel = \"1.95.0\"\n",
            Err("the pin declares no components"),
        ),
        (
            "# components = [\"rustfmt\"]\n",
            Err("the pin declares no components"),
        ),
        (
            "components = [\"clippy\"]\n",
            Err("the components omit rustfmt"),
        ),
    ];

    for (toolchain, expected) in cases {
        assert_eq!(
            ships_rustfmt(toolchain),
            expected,
            "toolchain:\n{toolchain}"
        );
    }
}
