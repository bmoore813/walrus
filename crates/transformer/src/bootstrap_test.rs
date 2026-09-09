use super::*;

#[tokio::test]
async fn missing_probe_proves_an_in_memory_store_is_readable() {
    let store = object_store::memory::InMemory::new();

    assert!(verify_s3_read(&store).await.is_ok());
}

#[test]
fn head_failure_keeps_the_bootstrap_operation_and_store_error() {
    let failure = object_store::Error::Generic {
        store: "failing-store",
        source: Box::new(std::io::Error::other("injected head failure")),
    };
    match classify_s3_read(Err(failure)) {
        Err(TransformerError::ObjectStore { op, source }) => {
            assert_eq!(op, "staging bucket not readable");
            assert!(source.to_string().contains("injected head failure"));
        }
        other => panic!("expected a classified object-store error, got {other:?}"),
    }
}
