//! `string_enum!`: one variant table -> an enum plus its two-way `&'static str` mapping.
//!
//! walrus stores several closed sets as plain Postgres `text` (`file_manifest.kind`/`.status`,
//! `table_reload.flavor`/`.status`, `replication_state.status`). Each needs the same pair of impls,
//! and hand-writing them means every legal string is typed twice — once in `as_str`, once in
//! `FromStr` — so the two directions can drift. This macro makes the variant table the single
//! source of both.
//!
//! Generated code reaches the typed constructor adapter through `common`'s crate-root
//! `__private` namespace. That namespace is a macro implementation detail with no stability
//! expectation; the caller-selected error type and inputs remain unchanged.

/// Construct the caller-selected error for an unknown `string_enum!` value.
///
/// Generated code reaches this adapter through `$crate::__private::unknown_variant`, so the item
/// resolves in `common` even when the macro expands in another crate. The adapter deliberately
/// preserves the caller's error taxonomy: it forwards the exact column and input to `make_error`
/// and returns `E`.
///
/// `#[cold]` because this is reachable only from the generated `from_str`'s catch-all: every value
/// the control DB's `CHECK` constraints permit matches a variant arm above it. Being generic, the
/// adapter is monomorphized into each generated parser, so without the hint every one of them
/// inlines its error type's owned-`String` construction next to the string-compare chain.
#[cold]
#[must_use]
pub fn unknown_variant<E>(
    column: &'static str,
    input: &str,
    make_error: impl FnOnce(&'static str, &str) -> E,
) -> E {
    make_error(column, input)
}

/// Declare a `text`-column enum and its exact persisted strings in one table.
///
/// The export attribute below publishes this at `common`'s root, so a caller reaches it by ordinary
/// path import — never the order-sensitive `#[macro_use] extern crate` of the 2015 edition.
///
/// The declared `error` type becomes the generated `FromStr`'s `Err`, so it must offer an
/// associated `new(column: &'static str, input: &str) -> Self` for the rejection path to call.
/// walrus passes `control::ParseEnumError`; the example hides an equivalent stand-in, because
/// `common` sits *below* `control` in the dependency DAG and cannot name the real one.
///
/// # Examples
///
/// ```
/// use common::string_enum;
/// # #[derive(Debug, PartialEq, Eq)]
/// # pub struct ParseEnumError {
/// #     column: &'static str,
/// #     input: String,
/// # }
/// # impl ParseEnumError {
/// #     fn new(column: &'static str, input: &str) -> Self {
/// #         Self { column, input: input.to_string() }
/// #     }
/// # }
///
/// string_enum! {
///     /// Doc comments are captured and re-emitted onto the generated enum.
///     #[derive(Debug, Clone, Copy, PartialEq, Eq)]
///     pub enum ManifestKind {
///         error = ParseEnumError;
///         column = "file_manifest.kind";
///         Snapshot => "snapshot",
///         Stream   => "stream",
///     }
/// }
///
/// assert_eq!(ManifestKind::Snapshot.as_str(), "snapshot");
/// assert_eq!("stream".parse::<ManifestKind>(), Ok(ManifestKind::Stream));
/// assert!("archived".parse::<ManifestKind>().is_err());
/// ```
#[macro_export]
macro_rules! string_enum {
    (
        // `:meta` captures complete attribute bodies, including desugared `///` docs and caller
        // derives. Re-emitting them as caller tokens lets `sqlx::Type` resolve in `control`.
        $(#[$attr:meta])*
        // `:vis` alone may match nothing; one arm covers public, restricted, and private enums.
        $vis:vis enum $name:ident {
            // `:path` preserves the caller's typed error. The column and persisted values use
            // `:literal`, not `:expr`, so expressions and macros are rejected at the call site.
            error = $error:path;
            column = $column:literal;
            // `:ident` precisely names enum variants; `$(,)?` accepts one trailing comma.
            $($variant:ident => $text:literal),* $(,)?
        }
    ) => {
        $(#[$attr])*
        $vis enum $name {
            $(
                #[doc = concat!("Persisted as `", $text, "`.")]
                $variant,
            )*
        }

        impl $name {
            /// The exact string persisted in the control DB.
            #[must_use]
            $vis const fn as_str(self) -> &'static str {
                match self {
                    $($name::$variant => $text,)*
                }
            }
        }

        impl ::std::str::FromStr for $name {
            type Err = $error;

            /// Parse one of the exact strings persisted in the control DB — the inverse of
            /// [`Self::as_str`].
            ///
            /// # Errors
            ///
            /// Returns the caller-selected error type, built from the declared column and the
            /// verbatim input, when `s` is not one of the variant table's persisted strings.
            fn from_str(s: &str) -> ::std::result::Result<Self, Self::Err> {
                match s {
                    $($text => Ok($name::$variant),)*
                    // Bare `crate::` resolves in the caller and produces E0425; `$crate` is common.
                    other => Err($crate::__private::unknown_variant(
                        $column,
                        other,
                        <$error>::new,
                    )),
                }
            }
        }
    };

    // Fallback arm. This must stay last: moving the catch-all above the real arm would shadow every
    // valid invocation. This is the one sanctioned token-tree slurp because the captured tokens
    // are never re-parsed; they only replace an internal matcher error with a useful message.
    // Declarative macros cannot attach a span, so the whole invocation is highlighted, and lexer
    // errors such as unbalanced delimiters fail before this arm can match.
    ($($bad:tt)*) => { // tt-fallback-ok — the macro-fragment guard exempts this line
        compile_error!(concat!(
            "string_enum! expects `#[attrs] <vis> enum Name { error = ErrorType; ",
            "column = \"db.column\"; Variant => \"db_string\", ... }`; ",
            "every variant needs a `=> \"literal\"` giving the exact persisted string"
        ));
    };
}

#[cfg(test)]
#[path = "string_enum_test.rs"]
mod tests;
