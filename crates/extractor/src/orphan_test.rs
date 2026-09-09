use super::*;
use object_store::PutPayload;
use object_store::memory::InMemory;

const UUID: &str = "c0a8012e-7a6f-4b48-9b5b-6023f5f1bb2d";

fn data_key(epoch: i64, table: &str) -> Path {
    Path::from(format!(
        "{epoch}/public/{table}/0000000000000100-{UUID}.parquet"
    ))
}

#[test]
fn classifier_deletes_only_exact_old_unreferenced_staged_keys() {
    let epoch = EpochNo(7);
    let key = data_key(7, "orders");
    let referenced = HashSet::from([key.to_string()]);

    assert_eq!(
        classify_epoch_object(epoch, &key, 1, 100, &referenced),
        ObjectDisposition::RetainReferenced
    );
    assert_eq!(
        classify_epoch_object(epoch, &key, 100, 100, &HashSet::new()),
        ObjectDisposition::RetainYoung,
        "the exact grace boundary is retained"
    );
    assert_eq!(
        classify_epoch_object(epoch, &key, 99, 100, &HashSet::new()),
        ObjectDisposition::DeleteOrphan
    );

    for ignored in [
        data_key(8, "orders"),
        Path::from("_walrus/canary/pod-a"),
        Path::from("ducklake/prod/main/ducklake-1.parquet"),
        Path::from(format!("7/public/orders/not-an-lsn-{UUID}.parquet")),
        Path::from("7/public/orders/0000000000000100-not-a-uuid.parquet"),
    ] {
        assert_eq!(
            classify_epoch_object(epoch, &ignored, 1, 100, &HashSet::new()),
            ObjectDisposition::IgnoreUnknown,
            "ignored {ignored}"
        );
    }
}

#[test]
fn manifest_uri_inventory_must_match_the_configured_bucket() {
    assert_eq!(
        referenced_keys(
            "walrus",
            &["s3://walrus/7/public/orders/file.parquet".into()]
        )
        .unwrap(),
        HashSet::from(["7/public/orders/file.parquet".to_string()])
    );
    assert!(
        referenced_keys(
            "walrus",
            &["s3://another/7/public/orders/file.parquet".into()]
        )
        .is_err(),
        "an incomplete reference inventory must fail closed"
    );
}

#[tokio::test]
async fn in_memory_sweep_deletes_only_the_matching_unreferenced_object() {
    let store = InMemory::new();
    let orphan = data_key(7, "orphan");
    let referenced_key = data_key(7, "referenced");
    let unfamiliar = Path::from("7/ducklake/data/ducklake-file.parquet");
    let other_epoch = data_key(8, "other_epoch");
    let system = Path::from("_walrus/canary/pod-a");
    for key in [&orphan, &referenced_key, &unfamiliar, &other_epoch, &system] {
        store.put(key, PutPayload::from_static(b"x")).await.unwrap();
    }

    let referenced = HashSet::from([referenced_key.to_string()]);
    let stats = sweep_epoch_at(&store, EpochNo(7), &referenced, i64::MAX)
        .await
        .unwrap();

    assert_eq!(stats.scanned, 3);
    assert_eq!(stats.deleted, 1);
    assert_eq!(stats.referenced, 1);
    assert_eq!(stats.ignored, 1);
    assert!(store.head(&orphan).await.is_err());
    for retained in [&referenced_key, &unfamiliar, &other_epoch, &system] {
        store.head(retained).await.unwrap();
    }
}
