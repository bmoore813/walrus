//! Typed rejections for control-plane text-column enums.
//!
//! `file_manifest.kind`/`.status` and `table_reload.flavor`/`.status` are text columns with SQL
//! `CHECK` constraints; their Rust enums are the second line of defence. `replication_state.status`
//! has no CHECK, so there the enum is the *only* line. Either way a value outside the known
//! vocabulary is a data-integrity bug, so the exact rejected text is preserved as data.

/// A control-plane enum rejected a text value read from the database.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("unknown {column} value {input:?}")]
pub struct ParseEnumError {
    /// Exact checked control-DB column, such as `file_manifest.kind`.
    pub column: &'static str,
    /// The exact text that was rejected, preserved verbatim.
    pub input: String,
}

impl ParseEnumError {
    /// Preserve a rejected enum value together with the exact column that rejected it.
    #[must_use]
    pub fn new(column: &'static str, input: &str) -> Self {
        Self {
            column,
            input: input.to_string(),
        }
    }
}

#[cfg(test)]
#[path = "parse_test.rs"]
mod tests;
