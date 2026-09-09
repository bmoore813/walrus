use super::*;

fn failure(table: &str) -> WorkerFailure {
    WorkerFailure {
        schema: "public".to_string(),
        table: table.to_string(),
        error: TransformerError::Quarantine {
            table: format!("public.{table}"),
            reason: "lossy cast".to_string(),
        },
    }
}

#[tokio::test]
async fn first_failure_names_its_table_and_cancels_the_token() {
    let token = CancellationToken::new();
    let (tx, rx) = failure_channel(3);
    report(&tx, failure("orders"));
    drop(tx);
    let drain_token = token.clone();
    let drain = async move { drain_token.cancelled().await };

    let got = supervise(rx, &token, drain)
        .await
        .expect("a worker failure was reported");

    assert_eq!(got.schema, "public");
    assert_eq!(got.table, "orders");
    assert_eq!(got.to_table_key(), "public.orders");
    assert!(matches!(got.error, TransformerError::Quarantine { .. }));
    assert!(token.is_cancelled());
}

#[tokio::test]
async fn a_clean_drain_reports_no_failure() {
    let token = CancellationToken::new();
    let (tx, rx) = failure_channel(0);
    drop(tx);

    let got = supervise(rx, &token, async {}).await;

    assert!(got.is_none());
    assert!(!token.is_cancelled());
}

#[tokio::test]
async fn report_never_parks_a_worker_on_a_full_channel() {
    let (tx, mut rx) = failure_channel(1);
    report(&tx, failure("orders"));
    report(&tx, failure("shipments"));

    assert_eq!(rx.try_recv().unwrap().table, "orders");
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
