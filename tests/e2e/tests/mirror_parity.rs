#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "acceptance test — failures should stop at the exact setup or parity boundary"
)]
//! Acceptance proof for the reusable source-to-DuckLake parity framework.
//!
//! `just acceptance` owns an isolated Compose stack and runs this target. The test keeps the target
//! table quiescent after each sentinel, waits on control-plane watermarks rather than sleeping, and
//! compares the source with the public DuckLake view after every named scenario step.
#![cfg(feature = "it")]

use e2e::{Harness, ScenarioStep, TableExpectation, TableId, TableParity};
use std::time::Duration;

const DEADLINE: Duration = Duration::from_secs(180);

#[derive(Debug)]
struct StreamTrace {
    ops: Vec<String>,
    row_lsns: Vec<String>,
    commit_lsn: String,
    xid: String,
    batch_id: String,
}

fn stream_values(harness: &Harness, table: &str, id: i64, expression: &str) -> Vec<String> {
    harness
        .duckdb_rows(
            table,
            &format!(
                "SELECT CAST({expression} AS VARCHAR) FROM {table}_raw \
                 WHERE id = {id} \
                   AND json_extract_string(walrus_extractor_meta, '$.kind') = 'stream' \
                 ORDER BY _walrus_lsn"
            ),
        )
        .expect("read stream trace from DuckLake")
}

fn assert_stream_trace(
    harness: &Harness,
    table: &str,
    id: i64,
    expected_ops: &[&str],
) -> StreamTrace {
    let ops = stream_values(harness, table, id, "_walrus_op");
    assert_eq!(
        ops,
        expected_ops
            .iter()
            .map(|op| (*op).to_string())
            .collect::<Vec<_>>(),
        "wrong raw operation sequence for {table}.id={id}"
    );

    let row_lsns = stream_values(harness, table, id, "_walrus_lsn");
    assert_eq!(row_lsns.len(), expected_ops.len());
    assert!(
        row_lsns.iter().all(|lsn| lsn != "0000000000000000"),
        "every raw change has a nonzero row LSN: {row_lsns:?}"
    );
    assert!(
        row_lsns.windows(2).all(|pair| pair[0] < pair[1]),
        "row LSNs preserve the per-key statement order: {row_lsns:?}"
    );

    let one_common_value = |field: &str, expression: &str| {
        let values = stream_values(harness, table, id, expression);
        assert_eq!(
            values.len(),
            expected_ops.len(),
            "missing {field} on {table}.id={id}: {values:?}"
        );
        assert!(
            !values[0].is_empty() && values.iter().all(|value| value == &values[0]),
            "all changes must share one {field} on {table}.id={id}: {values:?}"
        );
        values[0].clone()
    };

    let commit_lsn = one_common_value("commit LSN", "_walrus_commit_lsn");
    assert_ne!(commit_lsn, "0000000000000000");
    StreamTrace {
        ops,
        row_lsns,
        commit_lsn,
        xid: one_common_value("xid", "json_extract_string(walrus_extractor_meta, '$.xid')"),
        batch_id: one_common_value(
            "batch ID",
            "json_extract_string(walrus_extractor_meta, '$.batch_id')",
        ),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "requires the compose backing services; run `just acceptance`"]
async fn dml_comments_and_column_evolution_keep_exact_logical_parity() {
    let target = TableId::new("public", "mirror_parity");
    let mut harness = Harness::start_scenario(
        "DROP TABLE IF EXISTS public.mirror_parity; \
         CREATE TABLE public.mirror_parity ( \
             id bigint PRIMARY KEY, \
             status text NOT NULL, \
             amount numeric(12,2), \
             active boolean, \
             legacy text \
         ); \
         COMMENT ON TABLE public.mirror_parity IS 'initial mirror table'; \
         COMMENT ON COLUMN public.mirror_parity.status IS 'initial workflow state'; \
         INSERT INTO public.mirror_parity (id, status, amount, active, legacy) VALUES \
             (1, 'created', 10.25, true, 'keep-until-drop'), \
             (2, 'delete-me', NULL, false, NULL), \
             (3, 'nullable', -3.50, NULL, 'old');",
    )
    .await
    .expect("start isolated source, extractor, transformer, and DuckLake");

    let dml = ScenarioStep::new(
        "same-transaction key churn",
        "BEGIN; \
         INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
             VALUES (4, 'inserted', 99.99, true, 'new'); \
         UPDATE public.mirror_parity \
             SET status = 'updated', amount = 11.75, active = false WHERE id = 1; \
         DELETE FROM public.mirror_parity WHERE id = 2; \
         INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
             VALUES (5, 'first-lifetime', 5.00, true, 'A'); \
         INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
             VALUES (2, 'replacement', 2.00, true, 'replacement'); \
         DELETE FROM public.mirror_parity WHERE id = 5; \
         DELETE FROM public.mirror_parity WHERE id = 2; \
         INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
             VALUES (5, 'final-lifetime', 5.50, false, 'B'); \
         COMMIT;",
        "INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
         VALUES (100, 'dml-sentinel', 0.00, true, 'sentinel')",
    )
    .converge_on(target.clone())
    .expect(TableExpectation::Present(TableParity::auto(
        "public",
        "mirror_parity",
    )));
    harness.run_step(&dml, DEADLINE).await.expect("DML parity");

    let delete_ending = assert_stream_trace(&harness, "mirror_parity", 2, &["d", "i", "d"]);
    let insert_ending = assert_stream_trace(&harness, "mirror_parity", 5, &["i", "d", "i"]);
    assert_eq!(
        delete_ending.commit_lsn, insert_ending.commit_lsn,
        "both interleaved lifecycles came from one source transaction"
    );
    assert_eq!(delete_ending.xid, insert_ending.xid);
    assert_eq!(
        delete_ending.batch_id, insert_ending.batch_id,
        "the extractor preserved both lifecycles in one physical batch"
    );
    assert_eq!(delete_ending.ops, ["d", "i", "d"]);
    assert_eq!(insert_ending.ops, ["i", "d", "i"]);
    assert_eq!(delete_ending.row_lsns.len(), 3);
    assert_eq!(insert_ending.row_lsns.len(), 3);

    let update_comments = ScenarioStep::new(
        "update table and column comments",
        "COMMENT ON TABLE public.mirror_parity IS 'customer''s live orders'; \
         COMMENT ON COLUMN public.mirror_parity.status IS 'workflow state v2';",
        "INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
         VALUES (103, 'comment-update-sentinel', 3.03, true, NULL)",
    )
    .converge_on(target.clone())
    .expect(TableExpectation::Present(TableParity::auto(
        "public",
        "mirror_parity",
    )));
    harness
        .run_step(&update_comments, DEADLINE)
        .await
        .expect("updated COMMENT parity");

    let remove_comments = ScenarioStep::new(
        "remove table and column comments",
        "COMMENT ON TABLE public.mirror_parity IS NULL; \
         COMMENT ON COLUMN public.mirror_parity.status IS NULL;",
        "INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
         VALUES (104, 'comment-remove-sentinel', 4.04, false, NULL)",
    )
    .converge_on(target.clone())
    .expect(TableExpectation::Present(TableParity::auto(
        "public",
        "mirror_parity",
    )));
    harness
        .run_step(&remove_comments, DEADLINE)
        .await
        .expect("removed COMMENT parity");

    let add_column = ScenarioStep::new(
        "add column",
        "ALTER TABLE public.mirror_parity ADD COLUMN note text; \
         UPDATE public.mirror_parity SET note = 'backfilled' WHERE id IN (1, 3);",
        "INSERT INTO public.mirror_parity (id, status, amount, active, legacy, note) \
         VALUES (101, 'add-column-sentinel', 1.01, NULL, NULL, 'new shape')",
    )
    .converge_on(target.clone())
    .expect(TableExpectation::Present(TableParity::auto(
        "public",
        "mirror_parity",
    )));
    harness
        .run_step(&add_column, DEADLINE)
        .await
        .expect("ADD COLUMN parity");

    let drop_column = ScenarioStep::new(
        "drop column",
        "ALTER TABLE public.mirror_parity DROP COLUMN note; \
         UPDATE public.mirror_parity SET status = 'after-drop' WHERE id = 3;",
        "INSERT INTO public.mirror_parity (id, status, amount, active, legacy) \
         VALUES (102, 'drop-column-sentinel', 2.02, false, 'trailing-column-removed')",
    )
    .converge_on(target)
    .expect(TableExpectation::Present(TableParity::auto(
        "public",
        "mirror_parity",
    )));
    harness
        .run_step(&drop_column, DEADLINE)
        .await
        .expect("DROP COLUMN parity");

    harness
        .assert_managed_inventory()
        .await
        .expect("source, registry, and DuckLake table inventories agree");
}
