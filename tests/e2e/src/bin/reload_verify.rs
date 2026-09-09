//! Post-timing correctness verifier for `scripts/bench-reload.sh`.
//!
//! It compares benchmark tables with one full outer join on their primary key, so verification is
//! exact without routing millions of rows through the shell or Python. The transformer is stopped before
//! invocation; this utility attaches the same remote DuckLake catalog read-only.

use anyhow::{Context, Result};
use std::io::Write as _;

const TABLE_NAMESPACE: uuid::Uuid =
    uuid::Uuid::from_u128(0x4f02_efc2_39b3_4d9d_a860_22af_7291_8cc8);

fn sql_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn sql_ident(value: &str) -> Result<String> {
    let mut chars = value.chars();
    anyhow::ensure!(
        chars
            .next()
            .is_some_and(|ch| ch == '_' || ch.is_ascii_alphabetic())
            && chars.all(|ch| ch == '_' || ch.is_ascii_alphanumeric()),
        "benchmark table name is not a simple SQL identifier: {value:?}"
    );
    Ok(format!("\"{value}\""))
}

fn env_or(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let extension_dir = args
        .next()
        .context("usage: reload_verify <extension-dir> <table> [table ...]")?;
    let tables = args.collect::<Vec<_>>();
    anyhow::ensure!(
        !tables.is_empty(),
        "at least one benchmark table is required"
    );

    let source_url = env_or(
        "WALRUS_SOURCE_DB_URL",
        "postgres://postgres:postgres@localhost:5432/walrus",
    );
    let catalog_url = env_or(
        "WALRUS_DUCKLAKE__CATALOG_URL",
        "postgres://postgres:postgres@localhost:5433/walrus_ducklake",
    );
    let metadata_schema = env_or("WALRUS_DUCKLAKE__METADATA_SCHEMA", "walrus_reload_bench");
    let conn = duckdb::Connection::open_in_memory().context("open verifier DuckDB")?;
    conn.execute_batch(&format!(
        "SET extension_directory = {};\n\
         LOAD json; LOAD httpfs; LOAD aws; LOAD postgres; LOAD ducklake;\n\
         SET autoinstall_known_extensions = false; SET autoload_known_extensions = false;\n\
         CREATE OR REPLACE SECRET reload_verify_s3 (\n\
           TYPE s3, PROVIDER config, KEY_ID 'minioadmin', SECRET 'minioadmin',\n\
           REGION 'us-east-1', ENDPOINT 'localhost:9000', URL_STYLE 'path', USE_SSL false\n\
         );\n\
         ATTACH {} AS source (TYPE POSTGRES, READ_ONLY);\n\
         CREATE OR REPLACE SECRET reload_verify_catalog (TYPE postgres, URI {});\n\
         ATTACH 'ducklake:postgres:' AS walrus (\n\
           META_SECRET 'reload_verify_catalog', METADATA_SCHEMA {}, META_SCHEMA {},\n\
           CREATE_IF_NOT_EXISTS false, READ_ONLY\n\
         );",
        sql_literal(&extension_dir),
        sql_literal(&source_url),
        sql_literal(&catalog_url),
        sql_literal(&metadata_schema),
        sql_literal(&metadata_schema),
    ))
    .context("load extensions and attach source/DuckLake")?;

    let mut source_rows = 0_i64;
    let mut mirror_rows = 0_i64;
    let mut difference_rows = 0_i64;
    for table in &tables {
        let source_table = sql_ident(table)?;
        let current_table = sql_ident(&format!("{table}_current"))?;
        let key = format!("public\0{table}");
        let schema = sql_ident(&format!(
            "_walrus_{}",
            uuid::Uuid::new_v5(&TABLE_NAMESPACE, key.as_bytes()).simple()
        ))?;
        let (table_source_rows, table_mirror_rows, table_difference_rows) = conn
            .query_row(
                &format!(
                    "SELECT count(source_id), count(mirror_id),\n\
                            count(*) FILTER (WHERE source_id IS NULL OR mirror_id IS NULL\n\
                              OR source_payload IS DISTINCT FROM mirror_payload)\n\
                     FROM (SELECT id AS source_id, payload AS source_payload\n\
                           FROM source.public.{source_table}) AS source_rows\n\
                     FULL OUTER JOIN\n\
                          (SELECT id AS mirror_id, payload AS mirror_payload\n\
                           FROM walrus.{schema}.{current_table}) AS mirror_rows\n\
                       ON source_id = mirror_id"
                ),
                [],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, i64>(2)?,
                    ))
                },
            )
            .with_context(|| format!("compare source and mirror for public.{table}"))?;
        source_rows += table_source_rows;
        mirror_rows += table_mirror_rows;
        difference_rows += table_difference_rows;
    }

    let result = serde_json::json!({
        "tables": tables,
        "source_rows": source_rows,
        "mirror_rows": mirror_rows,
        "difference_rows": difference_rows,
    });
    writeln!(std::io::stdout().lock(), "{result}").context("write verification result")?;
    anyhow::ensure!(
        difference_rows == 0 && source_rows == mirror_rows,
        "mirror differs from source: {result}"
    );
    Ok(())
}
