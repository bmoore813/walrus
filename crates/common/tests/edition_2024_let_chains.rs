//! Guard for `pat-if-let-chains`: the workspace keeps the edition that makes conditional `let`
//! chains legal, and the audited nests stay flattened into them.
//!
//! Neither half has a lint behind it. A nested `if let A = x { if let B = y { … } }` compiles
//! forever, and an edition downgrade would surface as a syntax error somewhere far from the
//! manifest that caused it. So both are asserted as source text: the workspace manifest pins
//! edition 2024 with the `rust-version` that ships the feature, and each rewritten site still
//! reads as one `if let ... && ...` condition.

const ROOT: &str = include_str!("../../../Cargo.toml");
const CONTROL_DB: &str = include_str!("../../control/src/db.rs");
const EXTRACTOR_CONSUME: &str = include_str!("../../extractor/src/consume.rs");
const ARROW_BATCH: &str = include_str!("../../pg-to-arrow/src/batch.rs");

#[test]
fn workspace_is_edition_2024_for_if_let_chains() {
    assert!(ROOT.contains("edition = \"2024\" # if-let chains"));
    assert!(ROOT.contains("resolver = \"2\""));
    assert!(ROOT.contains("rust-version = \"1.96\""));
}

#[test]
fn the_audited_nests_are_conditional_chains() {
    assert!(
        CONTROL_DB
            .contains("if let sqlx::Error::Database(db) = &e\n            && db.code().as_deref()")
    );
    assert!(EXTRACTOR_CONSUME.contains(
        "if let Some(batcher) = self.batchers.remove(&oid)\n            && batcher.has_open_txn()"
    ));
    assert!(ARROW_BATCH.contains(
        "if let DataType::List(item) = field.data_type()\n        && let DataType::Struct(fs)"
    ));
    assert!(ARROW_BATCH.contains("&& let Some(bound) = fs.first()"));
    assert!(ARROW_BATCH.contains("if let Some(t) = scratch.find('T')\n        && let Some(sign)"));

    let audited = [CONTROL_DB, EXTRACTOR_CONSUME, ARROW_BATCH].concat();
    assert!(audited.matches("&& let ").count() >= 3);
}
