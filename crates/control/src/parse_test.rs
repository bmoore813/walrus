use super::*;

#[test]
fn display_quotes_the_rejected_input() {
    let error = ParseEnumError::new("file_manifest.kind", "snapshottt");
    assert_eq!(error.column, "file_manifest.kind");
    assert_eq!(error.input, "snapshottt");
    assert_eq!(
        error.to_string(),
        "unknown file_manifest.kind value \"snapshottt\""
    );
}

#[test]
fn display_is_unambiguous_for_whitespace_and_empty_input() {
    let whitespace = ParseEnumError::new("table_reload.status", " complete ");
    assert_eq!(
        whitespace.to_string(),
        "unknown table_reload.status value \" complete \""
    );

    let empty = ParseEnumError::new("file_manifest.status", "");
    assert_eq!(empty.to_string(), "unknown file_manifest.status value \"\"");
}
