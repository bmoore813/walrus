//! Shared SQL string helpers for the crates that build DuckDB/Postgres statements by hand.

use std::borrow::Cow;
use std::fmt::{self, Write as _};

mod private {
    pub trait Sealed {}
}

/// Escape a string for interpolation as a **single-quoted SQL string literal** by doubling every
/// `'`. The caller supplies the surrounding quotes (`format!("'{}'", sql_literal(s))`) — or
/// substitutes the result into a template whose placeholder already sits inside quotes.
///
/// Returns [`Cow::Borrowed`] when there is nothing to escape and [`Cow::Owned`] only when a `'` was
/// actually doubled.
///
/// This is literal escaping only; it is **not** identifier quoting (that doubles `"`).
///
/// # Examples
///
/// ```
/// use common::sql::sql_literal;
/// assert_eq!(sql_literal("O'Brien"), "O''Brien");
/// assert_eq!(sql_literal("plain"), "plain");
/// ```
#[must_use]
pub fn sql_literal(s: &str) -> Cow<'_, str> {
    if s.contains('\'') {
        Cow::Owned(s.replace('\'', "''"))
    } else {
        Cow::Borrowed(s)
    }
}

/// SQL rendering for the borrowed text walrus interpolates into hand-built statements.
///
/// `str` belongs to `core`, so walrus cannot give it an inherent method; an extension trait is the
/// orphan-rule-safe way to reach method syntax at the statement-building call sites. `&String` and
/// `Cow<'_, str>` receivers reach it through deref.
///
/// This trait is sealed: `.to_quoted_literal()` is callable from anywhere `common` is a dependency,
/// but only `common` can implement it. The method *name* is the safety claim — "this is already a
/// complete, escaped literal" — so a foreign impl on another receiver could keep the name while
/// emitting unescaped text into a statement, reintroducing exactly the per-call-site escaping drift
/// this module exists to collapse.
///
/// ```compile_fail
/// use common::sql::SqlStrExt;
///
/// struct Unescaped(String);
///
/// impl SqlStrExt for Unescaped {
///     fn to_quoted_literal(&self) -> String {
///         format!("'{}'", self.0)
///     }
/// }
/// ```
pub trait SqlStrExt: private::Sealed {
    /// Render `self` as a **complete** single-quoted SQL string literal: [`sql_literal`]'s escaping
    /// plus the surrounding `'`.
    ///
    /// Use this where the value stands alone in a statement (`… IS 'text'`, `current_setting('x')`);
    /// use [`sql_literal`] where the template already carries the quotes, since this method would
    /// double them.
    ///
    /// Always allocates a new `String` — hence `to_`, matching `str::to_uppercase` and unlike
    /// [`sql_literal`], which borrows when there is nothing to escape.
    ///
    /// # Examples
    ///
    /// ```
    /// use common::sql::SqlStrExt;
    /// assert_eq!("O'Brien".to_quoted_literal(), "'O''Brien'");
    /// assert_eq!("plain".to_quoted_literal(), "'plain'");
    /// ```
    #[must_use]
    fn to_quoted_literal(&self) -> String;
}

/// The one receiver. The seal impl sits with the real impl so the pair is added or removed together.
impl private::Sealed for str {}

impl SqlStrExt for str {
    fn to_quoted_literal(&self) -> String {
        format!("'{}'", sql_literal(self))
    }
}

/// Why a string cannot be represented as a SQL identifier.
///
/// This taxonomy is still growing — the accepted shape is a policy (a NAMEDATALEN cap, control
/// characters) that can tighten, not a closed algebra — so new variants must remain additive for
/// downstream crates.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum IdentError {
    /// SQL identifiers cannot be empty.
    #[error("must not be empty")]
    Empty,
    /// A NUL would terminate the identifier on the wire.
    #[error("value {0:?} contains an interior NUL")]
    InteriorNul(String),
}

/// Why a column cannot be emitted as a bare SQL name.
///
/// Walrus deliberately uses one portable spelling for columns in PostgreSQL and DuckDB: lowercase
/// ASCII snake_case, excluding reserved words. Keeping this stricter than the databases' quoted-
/// identifier grammar means generated SQL never needs double quotes around a column name and never
/// changes a name through case folding.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[non_exhaustive]
pub enum ColumnNameError {
    /// The name is not a lowercase ASCII identifier of the form `[a-z_][a-z0-9_]*`.
    #[error("{0:?} is not lowercase snake_case (`[a-z_][a-z0-9_]*`)")]
    NotSnakeCase(String),
    /// A reserved word cannot be used bare in every statement Walrus generates.
    #[error("{0:?} is a reserved SQL word")]
    Reserved(String),
}

/// A column name that is safe to interpolate without identifier quotes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SqlColumn<'a>(&'a str);

impl<'a> SqlColumn<'a> {
    /// Validate a portable, bare column name.
    ///
    /// # Errors
    ///
    /// Returns [`ColumnNameError::NotSnakeCase`] unless `name` matches
    /// `[a-z_][a-z0-9_]*`, or [`ColumnNameError::Reserved`] when PostgreSQL or DuckDB would require
    /// the name to be quoted.
    pub fn new(name: &'a str) -> Result<Self, ColumnNameError> {
        let mut bytes = name.bytes();
        let starts_like_identifier = bytes
            .next()
            .is_some_and(|byte| byte == b'_' || byte.is_ascii_lowercase());
        if !starts_like_identifier
            || !bytes.all(|byte| byte == b'_' || byte.is_ascii_lowercase() || byte.is_ascii_digit())
        {
            return Err(ColumnNameError::NotSnakeCase(name.to_string()));
        }
        if RESERVED_COLUMN_NAMES.binary_search(&name).is_ok() {
            return Err(ColumnNameError::Reserved(name.to_string()));
        }
        Ok(Self(name))
    }

    /// Return the validated bare name.
    #[must_use]
    pub const fn as_str(self) -> &'a str {
        self.0
    }
}

impl fmt::Display for SqlColumn<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.0)
    }
}

// Union of DuckDB's reserved keywords and PostgreSQL 14–17's `RESERVED_KEYWORD` and
// `TYPE_FUNC_NAME_KEYWORD` entries. The latter two are excluded from PostgreSQL's bare `ColId`
// grammar. Keep this sorted so validation is a binary search and additions are easy to review
// against `duckdb_keywords()` and PostgreSQL's `src/include/parser/kwlist.h`.
const RESERVED_COLUMN_NAMES: [&str; 109] = [
    "all",
    "analyse",
    "analyze",
    "and",
    "any",
    "array",
    "as",
    "asc",
    "asymmetric",
    "authorization",
    "binary",
    "both",
    "case",
    "cast",
    "check",
    "collate",
    "collation",
    "column",
    "concurrently",
    "constraint",
    "create",
    "cross",
    "current_catalog",
    "current_date",
    "current_role",
    "current_schema",
    "current_time",
    "current_timestamp",
    "current_user",
    "default",
    "deferrable",
    "desc",
    "describe",
    "distinct",
    "do",
    "else",
    "end",
    "except",
    "false",
    "fetch",
    "for",
    "foreign",
    "freeze",
    "from",
    "full",
    "grant",
    "group",
    "having",
    "ilike",
    "in",
    "initially",
    "inner",
    "intersect",
    "into",
    "is",
    "isnull",
    "join",
    "lateral",
    "leading",
    "left",
    "like",
    "limit",
    "localtime",
    "localtimestamp",
    "natural",
    "not",
    "notnull",
    "null",
    "offset",
    "on",
    "only",
    "or",
    "order",
    "outer",
    "overlaps",
    "pivot",
    "pivot_longer",
    "pivot_wider",
    "placing",
    "primary",
    "qualify",
    "references",
    "returning",
    "right",
    "select",
    "session_user",
    "show",
    "similar",
    "some",
    "summarize",
    "symmetric",
    "system_user",
    "table",
    "tablesample",
    "then",
    "to",
    "trailing",
    "true",
    "union",
    "unique",
    "unpivot",
    "user",
    "using",
    "variadic",
    "verbose",
    "when",
    "where",
    "window",
    "with",
];

/// A validated quoted SQL identifier: a table, schema, publication, or role name.
///
/// Construction rejects values that cannot safely reach the wire. [`Display`](fmt::Display)
/// always renders the SQL-standard double-quoted form, doubling interior double quotes. Contrast
/// [`SqlColumn`], which enforces the bare lowercase column policy, and [`sql_literal`], which
/// escapes a value rather than a name.
///
/// # Examples
///
/// ```
/// use common::sql::SqlIdent;
/// assert_eq!(SqlIdent::new("plain")?.to_string(), "\"plain\"");
/// assert_eq!(SqlIdent::new("a\"b")?.to_string(), "\"a\"\"b\"");
/// assert!(SqlIdent::new("").is_err());
/// # Ok::<(), common::sql::IdentError>(())
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SqlIdent(String);

impl SqlIdent {
    /// Validate a raw identifier and preserve it for allocation-free quoting by [`Display`](fmt::Display).
    ///
    /// # Errors
    ///
    /// Returns [`IdentError::Empty`] for an empty name or [`IdentError::InteriorNul`] when the name
    /// contains a NUL byte.
    pub fn new(s: &str) -> Result<Self, IdentError> {
        if s.is_empty() {
            return Err(IdentError::Empty);
        }
        if s.contains('\0') {
            return Err(IdentError::InteriorNul(s.to_string()));
        }
        Ok(Self(s.to_string()))
    }

    /// Return the unquoted identifier for comparison with catalog values.
    #[must_use]
    pub fn as_raw(&self) -> &str {
        &self.0
    }
}

impl TryFrom<&str> for SqlIdent {
    type Error = IdentError;

    /// The standard spelling of [`SqlIdent::new`], so a call site can write `name.try_into()?` and a
    /// generic bound can name this conversion.
    ///
    /// `TryFrom<&str>` rather than [`FromStr`](std::str::FromStr): an identifier *is* the text it
    /// admits, not a rendering of some other value — and [`Display`](fmt::Display) emits the
    /// **quoted** form, so a `FromStr` here would not round-trip its own output.
    ///
    /// # Errors
    ///
    /// Returns [`IdentError::Empty`] for an empty name or [`IdentError::InteriorNul`] when the name
    /// contains a NUL byte.
    fn try_from(s: &str) -> Result<Self, Self::Error> {
        SqlIdent::new(s)
    }
}

impl fmt::Display for SqlIdent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_char('"')?;
        for ch in self.0.chars() {
            if ch == '"' {
                f.write_char('"')?;
            }
            f.write_char(ch)?;
        }
        f.write_char('"')
    }
}

#[cfg(test)]
#[path = "sql_test.rs"]
mod tests;
