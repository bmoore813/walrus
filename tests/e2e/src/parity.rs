//! Reusable, black-box source-to-DuckLake parity assertions and scenario steps.
//!
//! The verifier deliberately does not call the transformer's transform code. It reads the live source
//! catalog, the durable schema registry, and the public DuckLake view independently, then lets one
//! transient DuckDB connection compare both databases with multiset semantics.

use super::{DUCKLAKE_SCHEMA, Harness, HarnessConfig, TABLE_NAMESPACE};
use anyhow::{Context, Result};
use common::Tier;
use common::sql::{SqlColumn, SqlIdent, SqlStrExt};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

const DIFFERENCE_SAMPLE_LIMIT: usize = 10;

/// A schema-qualified source table identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct TableId {
    /// Source PostgreSQL schema name.
    pub schema: String,
    /// Unqualified source table name.
    pub table: String,
}

impl TableId {
    /// Construct a schema-qualified source table identity.
    pub fn new(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            schema: schema.into(),
            table: table.into(),
        }
    }
}

impl fmt::Display for TableId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.schema, self.table)
    }
}

/// One canonical field in an explicit source-to-DuckLake projection.
///
/// Expressions are trusted test SQL. The source expression is evaluated with alias `s`, and the
/// DuckLake expression with alias `d`. Both are cast to `duck_type` before comparison.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompareField {
    /// Stable label used when reporting a parity mismatch.
    pub logical_name: String,
    /// Trusted PostgreSQL expression evaluated against source alias `s`.
    pub source_expression: String,
    /// Trusted DuckDB expression evaluated against DuckLake alias `d`.
    pub lake_expression: String,
    /// DuckDB type both expressions are cast to before comparison.
    pub duck_type: String,
}

impl CompareField {
    /// Construct one explicitly mapped comparison field.
    pub fn new(
        logical_name: impl Into<String>,
        source_expression: impl Into<String>,
        lake_expression: impl Into<String>,
        duck_type: impl Into<String>,
    ) -> Self {
        Self {
            logical_name: logical_name.into(),
            source_expression: source_expression.into(),
            lake_expression: lake_expression.into(),
            duck_type: duck_type.into(),
        }
    }
}

/// How a table's source and DuckLake rows are projected into a common comparison shape.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Projection {
    /// Derive one comparison field per one-to-one scalar registry descriptor.
    AutoScalar,
    /// Caller-provided canonical expressions for decomposed or otherwise special mappings.
    Explicit(Vec<CompareField>),
}

/// Logical parity settings for one present table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableParity {
    /// Source table and corresponding DuckLake object identity.
    pub id: TableId,
    /// Projection used to normalize rows for comparison.
    pub projection: Projection,
    /// Whether table and column comments must also match.
    pub compare_comments: bool,
}

impl TableParity {
    /// Compare a table through fields derived from its scalar registry descriptors.
    pub fn auto(schema: impl Into<String>, table: impl Into<String>) -> Self {
        Self {
            id: TableId::new(schema, table),
            projection: Projection::AutoScalar,
            compare_comments: true,
        }
    }

    /// Compare a table through caller-provided canonical field expressions.
    pub const fn explicit(id: TableId, fields: Vec<CompareField>) -> Self {
        Self {
            id,
            projection: Projection::Explicit(fields),
            compare_comments: true,
        }
    }

    #[must_use]
    /// Disable table and column comment comparison for this expectation.
    pub const fn without_comments(mut self) -> Self {
        self.compare_comments = false;
        self
    }
}

/// The table state a scenario expects after convergence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TableExpectation {
    /// The table must exist and match the supplied parity contract.
    Present(TableParity),
    /// The table must be absent from the public DuckLake schema.
    Absent(TableId),
}

/// One source mutation, its later WAL barrier, and the parity assertions to run after convergence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScenarioStep {
    /// Stable scenario-step name used in diagnostics.
    pub name: String,
    /// Trusted source SQL applied before the barrier.
    pub mutation_sql: String,
    /// Trusted source SQL whose commit establishes the WAL convergence floor.
    pub barrier_sql: String,
    /// Tables whose durable watermarks must reach the convergence floor.
    pub convergence_tables: Vec<TableId>,
    /// Table states asserted after convergence.
    pub expectations: Vec<TableExpectation>,
}

impl ScenarioStep {
    /// Construct a scenario step with no convergence tables or expectations yet.
    pub fn new(
        name: impl Into<String>,
        mutation_sql: impl Into<String>,
        barrier_sql: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            mutation_sql: mutation_sql.into(),
            barrier_sql: barrier_sql.into(),
            convergence_tables: Vec::new(),
            expectations: Vec::new(),
        }
    }

    #[must_use]
    /// Add a table whose transformed watermark gates convergence.
    pub fn converge_on(mut self, table: TableId) -> Self {
        self.convergence_tables.push(table);
        self
    }

    #[must_use]
    /// Add a table-state assertion evaluated after convergence.
    pub fn expect(mut self, expectation: TableExpectation) -> Self {
        self.expectations.push(expectation);
        self
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct SourceColumn {
    name: String,
    type_oid: u32,
    type_modifier: i32,
    comment: Option<String>,
}

impl Harness {
    /// Apply one named scenario step, wait on durable table watermarks, and assert its expectations.
    ///
    /// # Errors
    ///
    /// Returns an error when the step has no convergence target or expectation, source SQL or a
    /// watermark query fails, the deadline expires, a child has exited, or any parity assertion
    /// fails. Source mutations committed before an error are not rolled back.
    pub async fn run_step(&mut self, step: &ScenarioStep, deadline: Duration) -> Result<()> {
        anyhow::ensure!(
            !step.convergence_tables.is_empty(),
            "scenario step {:?} has no convergence table",
            step.name
        );
        anyhow::ensure!(
            !step.expectations.is_empty(),
            "scenario step {:?} has no parity expectation",
            step.name
        );

        let floor = self
            .source_wal_lsn()
            .await
            .with_context(|| format!("{}: capture WAL floor", step.name))?;
        if !step.mutation_sql.trim().is_empty() {
            self.source_batch(&step.mutation_sql)
                .await
                .with_context(|| format!("{}: apply source mutation", step.name))?;
        }
        self.source_batch(&step.barrier_sql)
            .await
            .with_context(|| format!("{}: commit source barrier", step.name))?;

        for table in &step.convergence_tables {
            self.await_table_transformed_past(&table.schema, &table.table, floor, deadline)
                .await
                .with_context(|| format!("{}: await {table}", step.name))?;
        }
        self.ensure_children_running()
            .with_context(|| format!("{}: service health", step.name))?;
        for expectation in &step.expectations {
            match expectation {
                TableExpectation::Present(spec) => self
                    .assert_logical_parity(spec)
                    .await
                    .with_context(|| format!("{}: compare {}", step.name, spec.id))?,
                TableExpectation::Absent(table) => self
                    .assert_table_absent(table)
                    .await
                    .with_context(|| format!("{}: assert {table} absent", step.name))?,
            }
        }
        Ok(())
    }

    /// Assert schema, comments, row count, and bidirectional row multiset parity for one table.
    ///
    /// # Errors
    ///
    /// Returns an error if source, registry, or DuckLake reads fail, the source table has no visible
    /// columns, or any schema, comment, count, or row multiset differs. This assertion is read-only.
    pub async fn assert_logical_parity(&self, spec: &TableParity) -> Result<()> {
        let source_columns = source_columns(self, &spec.id).await?;
        anyhow::ensure!(
            !source_columns.is_empty(),
            "source table {} does not exist or has no visible columns",
            spec.id
        );
        let registry = latest_registry(self, &spec.id).await?;
        assert_registry_matches_source(&spec.id, &source_columns, &registry)?;
        let source_table_comment = if spec.compare_comments {
            Some(source_table_comment(self, &spec.id).await?)
        } else {
            None
        };

        let conn = parity_reader(&self.config)?;
        let fields = comparison_fields(spec, &source_columns, &registry)?;
        assert_view_schema(&conn, &spec.id, &fields)?;
        if let Some(source_table_comment) = source_table_comment {
            assert_comments(
                &conn,
                &spec.id,
                source_table_comment.as_deref(),
                &source_columns,
            )?;
        }
        assert_rows_equal(&conn, &spec.id, &fields)
    }

    /// Assert the relation is absent from source PostgreSQL and from both public and internal
    /// DuckLake namespaces. This is ready for a future green DROP TABLE scenario.
    ///
    /// # Errors
    ///
    /// Returns an error if a source or DuckLake catalog query fails, or if the relation remains in
    /// any checked namespace. This assertion is read-only.
    pub async fn assert_table_absent(&self, table: &TableId) -> Result<()> {
        anyhow::ensure!(
            !source_table_exists(self, table).await?,
            "source table {table} still exists"
        );
        let conn = parity_reader(&self.config)?;
        anyhow::ensure!(
            !duck_object_exists(&conn, &table.schema, &format!("{}_current", table.table))?,
            "DuckLake public view {table}_current still exists"
        );
        let internal = internal_schema(table);
        anyhow::ensure!(
            !duck_object_exists(&conn, &internal, &table.table)?,
            "DuckLake internal mirror {internal}.{} still exists",
            table.table
        );
        Ok(())
    }

    /// Assert set equality across eligible published source tables, live current-epoch registry
    /// entries, and public DuckLake `_current` views.
    ///
    /// # Errors
    ///
    /// Returns an error if any inventory query fails or the source, registry, and public DuckLake
    /// object sets differ. This assertion is read-only.
    pub async fn assert_managed_inventory(&self) -> Result<()> {
        let source: BTreeSet<TableId> = sqlx::query_as::<_, (String, String)>(
            "SELECT DISTINCT pt.schemaname, pt.tablename \
             FROM pg_publication_tables pt \
             JOIN pg_namespace n ON n.nspname = pt.schemaname \
             JOIN pg_class c ON c.relnamespace = n.oid AND c.relname = pt.tablename \
             WHERE pt.pubname = 'walrus_pub' \
               AND NOT (pt.schemaname = 'public' AND pt.tablename IN \
                 ('walrus_heartbeat', 'walrus_ddl_audit', \
                  'walrus_reload_signal', 'walrus_reload_event')) \
               AND c.relkind IN ('r', 'p') \
               AND EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid \
                           AND i.indisprimary AND i.indisvalid AND i.indisready AND i.indislive) \
             ORDER BY pt.schemaname, pt.tablename",
        )
        .fetch_all(&self.source)
        .await?
        .into_iter()
        .map(|(schema, table)| TableId { schema, table })
        .collect();

        let registry: BTreeSet<TableId> =
            control::read_all_latest_registry(&self.control, self.epoch.into())
                .await?
                .into_iter()
                .filter(|row| {
                    !extractor::relcache::is_internal_table(&row.source_schema, &row.source_table)
                })
                .filter(|row| registry_column_array(&row.columns).is_some_and(|c| !c.is_empty()))
                .map(|row| TableId::new(row.source_schema, row.source_table))
                .collect();

        let conn = parity_reader(&self.config)?;
        let views: BTreeSet<TableId> = {
            let mut stmt = conn.prepare(
                "SELECT schema_name, view_name FROM duckdb_views() \
                 WHERE database_name = 'walrus' AND NOT internal \
                   AND schema_name NOT LIKE '\\_walrus\\_%' ESCAPE '\\' \
                   AND ends_with(view_name, '_current') \
                 ORDER BY schema_name, view_name",
            )?;
            let rows = stmt.query_map([], |row| {
                let schema: String = row.get(0)?;
                let view: String = row.get(1)?;
                Ok(TableId::new(
                    schema,
                    view.strip_suffix("_current").unwrap_or(&view),
                ))
            })?;
            rows.collect::<std::result::Result<_, _>>()?
        };

        anyhow::ensure!(
            source == registry && registry == views,
            "managed table inventory differs\nsource only: {:?}\nregistry only vs source: {:?}\n\
             registry only vs DuckLake: {:?}\nDuckLake only: {:?}",
            source.difference(&registry).collect::<Vec<_>>(),
            registry.difference(&source).collect::<Vec<_>>(),
            registry.difference(&views).collect::<Vec<_>>(),
            views.difference(&registry).collect::<Vec<_>>()
        );
        Ok(())
    }

    fn ensure_children_running(&mut self) -> Result<()> {
        anyhow::ensure!(
            self.extractor.try_wait()?.is_none(),
            "walrus-extractor exited; log tail:\n{}",
            self.extractor_log_tail(30)
        );
        anyhow::ensure!(
            self.transformer.try_wait()?.is_none(),
            "walrus-transformer exited"
        );
        Ok(())
    }
}

async fn source_table_exists(harness: &Harness, table: &TableId) -> Result<bool> {
    Ok(sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p'))",
    )
    .bind(&table.schema)
    .bind(&table.table)
    .fetch_one(&harness.source)
    .await?)
}

async fn source_columns(harness: &Harness, table: &TableId) -> Result<Vec<SourceColumn>> {
    let rows = sqlx::query_as::<_, (String, i64, i32, Option<String>)>(
        "SELECT a.attname, a.atttypid::bigint, a.atttypmod, d.description \
         FROM pg_class c \
         JOIN pg_namespace n ON n.oid = c.relnamespace \
         JOIN pg_attribute a ON a.attrelid = c.oid \
         LEFT JOIN pg_description d ON d.objoid = c.oid AND d.objsubid = a.attnum \
         WHERE n.nspname = $1 AND c.relname = $2 AND c.relkind IN ('r', 'p') \
           AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated = '' \
         ORDER BY a.attnum",
    )
    .bind(&table.schema)
    .bind(&table.table)
    .fetch_all(&harness.source)
    .await?;
    rows.into_iter()
        .map(|(name, oid, type_modifier, comment)| {
            Ok(SourceColumn {
                name,
                type_oid: u32::try_from(oid).context("source type OID exceeds u32")?,
                type_modifier,
                comment,
            })
        })
        .collect()
}

async fn latest_registry(harness: &Harness, table: &TableId) -> Result<control::RegistryRow> {
    let version = control::read_latest_version(
        &harness.control,
        harness.epoch.into(),
        &table.schema,
        &table.table,
    )
    .await?
    .with_context(|| format!("no registry entry for {table}"))?;
    control::read_registry(
        &harness.control,
        harness.epoch.into(),
        &table.schema,
        &table.table,
        version,
    )
    .await?
    .with_context(|| format!("registry entry for {table} v{version} disappeared"))
}

fn assert_registry_matches_source(
    table: &TableId,
    source: &[SourceColumn],
    registry: &control::RegistryRow,
) -> Result<()> {
    let values = registry_column_array(&registry.columns).with_context(|| {
        format!("registry columns for {table} are neither an array nor a relation envelope")
    })?;
    let mut captured = Vec::with_capacity(values.len());
    for value in values {
        let name = value["name"]
            .as_str()
            .context("registry column has no string name")?;
        let oid = value["type_oid"]
            .as_u64()
            .context("registry column has no integer type_oid")?;
        let modifier = value["type_modifier"]
            .as_i64()
            .context("registry column has no integer type_modifier")?;
        captured.push((
            name,
            u32::try_from(oid).context("registry type OID exceeds u32")?,
            i32::try_from(modifier).context("registry type modifier exceeds i32")?,
        ));
    }
    let current: Vec<(&str, u32, i32)> = source
        .iter()
        .map(|column| (column.name.as_str(), column.type_oid, column.type_modifier))
        .collect();
    anyhow::ensure!(
        captured == current,
        "schema registry for {table} differs from source\nregistry: {captured:?}\nsource: {current:?}"
    );
    Ok(())
}

/// Historical registry fixtures store the column snapshot directly, while production relation
/// messages serialize the whole `PgRelation` and put the same array under `columns`.
fn registry_column_array(value: &serde_json::Value) -> Option<&[serde_json::Value]> {
    value
        .as_array()
        .or_else(|| value.get("columns").and_then(serde_json::Value::as_array))
        .map(Vec::as_slice)
}

fn comparison_fields(
    spec: &TableParity,
    source: &[SourceColumn],
    registry: &control::RegistryRow,
) -> Result<Vec<CompareField>> {
    if let Projection::Explicit(fields) = &spec.projection {
        anyhow::ensure!(
            !fields.is_empty(),
            "{} has an empty explicit projection",
            spec.id
        );
        return Ok(fields.clone());
    }

    let descriptors: BTreeMap<&str, &common::TypeDescriptor> = registry
        .descriptors
        .iter()
        .map(|descriptor| (descriptor.column.as_str(), descriptor))
        .collect();
    anyhow::ensure!(
        descriptors.len() == registry.descriptors.len(),
        "{} registry has duplicate descriptors",
        spec.id
    );
    source
        .iter()
        .map(|column| {
            let descriptor = descriptors
                .get(column.name.as_str())
                .with_context(|| format!("{} has no descriptor for {}", spec.id, column.name))?;
            anyhow::ensure!(
                descriptor.tier != Tier::Two
                    && descriptor.recombine.is_none()
                    && descriptor.emit.len() == 1,
                "{}.{} uses a decomposed/special mapping; supply Projection::Explicit",
                spec.id,
                column.name
            );
            let (emit_name, emit_type) = descriptor.emit[0]
                .rsplit_once(':')
                .context("malformed registry emit descriptor")?;
            anyhow::ensure!(
                emit_name == column.name,
                "{}.{} emits as {emit_name:?}; supply Projection::Explicit",
                spec.id,
                column.name
            );
            let duck_type = if emit_type.starts_with("DECIMAL(") {
                emit_type
            } else {
                descriptor.duckdb.as_str()
            };
            let ident = SqlColumn::new(&column.name)?;
            Ok(CompareField::new(
                &column.name,
                format!("s.{ident}"),
                format!("d.{ident}"),
                duck_type,
            ))
        })
        .collect()
}

fn assert_view_schema(
    conn: &duckdb::Connection,
    table: &TableId,
    fields: &[CompareField],
) -> Result<()> {
    let view = format!("{}_current", table.table);
    let mut stmt = conn.prepare(
        "SELECT column_name, data_type FROM duckdb_columns() \
         WHERE database_name = 'walrus' AND schema_name = ? AND table_name = ? \
         ORDER BY column_index",
    )?;
    let actual: Vec<(String, String)> = stmt
        .query_map(duckdb::params![table.schema, view], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    anyhow::ensure!(
        !actual.is_empty(),
        "DuckLake public view {table}_current is missing"
    );

    let expected: Vec<(String, String)> = fields
        .iter()
        .map(|field| {
            anyhow::ensure!(
                !field.duck_type.contains(';'),
                "comparison type for {} contains a statement separator",
                field.logical_name
            );
            let normalized: String = conn.query_row(
                &format!("SELECT typeof(CAST(NULL AS {}))", field.duck_type),
                [],
                |row| row.get(0),
            )?;
            Ok((field.logical_name.clone(), normalized))
        })
        .collect::<Result<_>>()?;
    anyhow::ensure!(
        actual == expected,
        "DuckLake schema for {table} differs\nexpected: {expected:?}\nactual: {actual:?}"
    );
    Ok(())
}

async fn source_table_comment(harness: &Harness, table: &TableId) -> Result<Option<String>> {
    Ok(sqlx::query_scalar(
        "SELECT obj_description(c.oid, 'pg_class') \
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace \
         WHERE n.nspname = $1 AND c.relname = $2",
    )
    .bind(&table.schema)
    .bind(&table.table)
    .fetch_one(&harness.source)
    .await?)
}

fn assert_comments(
    conn: &duckdb::Connection,
    table: &TableId,
    source_table_comment: Option<&str>,
    source_columns: &[SourceColumn],
) -> Result<()> {
    let internal = internal_schema(table);
    let lake_table_comment: Option<String> = conn
        .query_row(
            "SELECT comment FROM duckdb_tables() \
             WHERE database_name = 'walrus' AND schema_name = ? AND table_name = ?",
            duckdb::params![internal, table.table],
            |row| row.get(0),
        )
        .with_context(|| format!("read DuckLake table comment for {table}"))?;
    anyhow::ensure!(
        source_table_comment == lake_table_comment.as_deref(),
        "table comment mismatch for {table}: source={source_table_comment:?}, \
         DuckLake={lake_table_comment:?}"
    );

    let mut stmt = conn.prepare(
        "SELECT column_name, comment FROM duckdb_columns() \
         WHERE database_name = 'walrus' AND schema_name = ? AND table_name = ? \
         ORDER BY column_index",
    )?;
    let lake_comments: BTreeMap<String, Option<String>> = stmt
        .query_map(duckdb::params![internal, table.table], |row| {
            Ok((row.get(0)?, row.get(1)?))
        })?
        .collect::<std::result::Result<_, _>>()?;
    for column in source_columns {
        let actual = lake_comments
            .get(&column.name)
            .with_context(|| format!("DuckLake mirror {table} has no column {}", column.name))?;
        anyhow::ensure!(
            actual == &column.comment,
            "column comment mismatch for {table}.{}: source={:?}, DuckLake={actual:?}",
            column.name,
            column.comment
        );
    }
    Ok(())
}

fn assert_rows_equal(
    conn: &duckdb::Connection,
    table: &TableId,
    fields: &[CompareField],
) -> Result<()> {
    let source_projection = projection_sql(fields, true)?;
    let lake_projection = projection_sql(fields, false)?;
    let schema = SqlIdent::new(&table.schema)?;
    let source_table = SqlIdent::new(&table.table)?;
    let view = SqlIdent::new(&format!("{}_current", table.table))?;
    let ctes = format!(
        "WITH source_rows AS (SELECT {source_projection} FROM source_pg.{schema}.{source_table} AS s), \
         lake_rows AS (SELECT {lake_projection} FROM walrus.{schema}.{view} AS d)"
    );
    let source_count = query_i64(conn, &format!("{ctes} SELECT count(*) FROM source_rows"))?;
    let lake_count = query_i64(conn, &format!("{ctes} SELECT count(*) FROM lake_rows"))?;
    let missing = query_i64(
        conn,
        &format!(
            "{ctes} SELECT count(*) FROM \
             (SELECT * FROM source_rows EXCEPT ALL SELECT * FROM lake_rows)"
        ),
    )?;
    let extra = query_i64(
        conn,
        &format!(
            "{ctes} SELECT count(*) FROM \
             (SELECT * FROM lake_rows EXCEPT ALL SELECT * FROM source_rows)"
        ),
    )?;
    if missing == 0 && extra == 0 && source_count == lake_count {
        return Ok(());
    }

    let missing_sample = difference_sample(conn, &ctes, fields, "source_rows", "lake_rows")?;
    let extra_sample = difference_sample(conn, &ctes, fields, "lake_rows", "source_rows")?;
    anyhow::bail!(
        "row parity failed for {table}: source={source_count}, DuckLake={lake_count}, \
         missing={missing}, extra={extra}\nmissing sample: {missing_sample:?}\n\
         extra sample: {extra_sample:?}"
    )
}

fn projection_sql(fields: &[CompareField], source: bool) -> Result<String> {
    anyhow::ensure!(!fields.is_empty(), "row comparison has no fields");
    fields
        .iter()
        .map(|field| {
            anyhow::ensure!(
                !field.duck_type.contains(';'),
                "comparison type for {} contains a statement separator",
                field.logical_name
            );
            let expression = if source {
                &field.source_expression
            } else {
                &field.lake_expression
            };
            let name = SqlColumn::new(&field.logical_name)?;
            Ok(format!(
                "CAST({expression} AS {}) AS {name}",
                field.duck_type
            ))
        })
        .collect::<Result<Vec<_>>>()
        .map(|columns| columns.join(", "))
}

fn difference_sample(
    conn: &duckdb::Connection,
    ctes: &str,
    fields: &[CompareField],
    left: &str,
    right: &str,
) -> Result<Vec<String>> {
    let rendered = fields
        .iter()
        .map(|field| {
            let ident = SqlColumn::new(&field.logical_name)?;
            Ok(format!(
                "{} || '=' || coalesce(CAST({ident} AS VARCHAR), '<NULL>')",
                field.logical_name.to_quoted_literal()
            ))
        })
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let sql = format!(
        "{ctes}, difference AS \
         (SELECT * FROM {left} EXCEPT ALL SELECT * FROM {right}) \
         SELECT concat_ws(' | ', {rendered}) FROM difference LIMIT {DIFFERENCE_SAMPLE_LIMIT}"
    );
    let mut stmt = conn.prepare(&sql)?;
    let rows = stmt.query_map([], |row| row.get(0))?;
    Ok(rows.collect::<std::result::Result<_, _>>()?)
}

fn query_i64(conn: &duckdb::Connection, sql: &str) -> Result<i64> {
    Ok(conn.query_row(sql, [], |row| row.get(0))?)
}

fn duck_object_exists(conn: &duckdb::Connection, schema: &str, object: &str) -> Result<bool> {
    let count: i64 = conn.query_row(
        "SELECT count(*) FROM (\
           SELECT schema_name, table_name AS object_name FROM duckdb_tables() \
           WHERE database_name = 'walrus' \
           UNION ALL \
           SELECT schema_name, view_name AS object_name FROM duckdb_views() \
           WHERE database_name = 'walrus'\
         ) WHERE schema_name = ? AND object_name = ?",
        duckdb::params![schema, object],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn internal_schema(table: &TableId) -> String {
    let key = format!("{}\0{}", table.schema, table.table);
    format!(
        "_walrus_{}",
        uuid::Uuid::new_v5(&TABLE_NAMESPACE, key.as_bytes()).simple()
    )
}

fn parity_reader(config: &HarnessConfig) -> Result<duckdb::Connection> {
    let conn = duckdb::Connection::open_in_memory().context("open parity DuckDB connection")?;
    let s3_endpoint = config
        .s3_endpoint
        .trim_start_matches("http://")
        .trim_start_matches("https://");
    let use_ssl = config.s3_endpoint.starts_with("https://");
    conn.execute_batch(&format!(
        "INSTALL json; INSTALL httpfs; INSTALL aws; INSTALL postgres; INSTALL ducklake; \
         LOAD json; LOAD httpfs; LOAD aws; LOAD postgres; LOAD ducklake; \
         CREATE OR REPLACE SECRET parity_s3 (TYPE s3, PROVIDER config, \
           KEY_ID 'minioadmin', SECRET 'minioadmin', REGION 'us-east-1', \
           ENDPOINT {}, URL_STYLE 'path', USE_SSL {use_ssl}); \
         CREATE OR REPLACE SECRET parity_catalog (TYPE postgres, URI {}); \
         ATTACH 'ducklake:postgres:' AS walrus (META_SECRET 'parity_catalog', \
           METADATA_SCHEMA {ducklake_schema}, META_SCHEMA {ducklake_schema}, \
           CREATE_IF_NOT_EXISTS false, READ_ONLY); \
         ATTACH {} AS source_pg (TYPE postgres, READ_ONLY);",
        s3_endpoint.to_quoted_literal(),
        config.catalog_url.to_quoted_literal(),
        config.source_url.to_quoted_literal(),
        ducklake_schema = DUCKLAKE_SCHEMA.to_quoted_literal(),
    ))
    .context("attach source PostgreSQL and DuckLake for parity")?;
    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, MutexGuard};

    static DUCKDB_TEST_LOCK: Mutex<()> = Mutex::new(());

    fn duckdb_test_guard() -> MutexGuard<'static, ()> {
        DUCKDB_TEST_LOCK
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn comparison_connection() -> duckdb::Connection {
        let conn = duckdb::Connection::open_in_memory().unwrap();
        conn.execute_batch(
            "ATTACH ':memory:' AS source_pg; \
             ATTACH ':memory:' AS walrus; \
             CREATE SCHEMA source_pg.public; \
             CREATE SCHEMA walrus.public;",
        )
        .unwrap();
        conn
    }

    fn fields() -> Vec<CompareField> {
        vec![
            CompareField::new("id", "s.id", "d.id", "BIGINT"),
            CompareField::new("status", "s.status", "d.status", "VARCHAR"),
        ]
    }

    #[test]
    fn bidirectional_multiset_diff_detects_one_missing_duplicate() {
        let _guard = duckdb_test_guard();
        let conn = comparison_connection();
        conn.execute_batch(
            "CREATE TABLE source_pg.public.fixture(id BIGINT, status VARCHAR); \
             CREATE TABLE walrus.public.fixture_current(id BIGINT, status VARCHAR); \
             INSERT INTO source_pg.public.fixture VALUES (1, 'same'), (1, 'same'), (2, NULL); \
             INSERT INTO walrus.public.fixture_current VALUES (1, 'same'), (2, NULL);",
        )
        .unwrap();

        let error = assert_rows_equal(&conn, &TableId::new("public", "fixture"), &fields())
            .unwrap_err()
            .to_string();
        assert!(error.contains("source=3, DuckLake=2, missing=1, extra=0"));
        assert!(error.contains("id=1 | status=same"));
    }

    #[test]
    fn bidirectional_multiset_diff_accepts_equal_null_rows_in_any_order() {
        let _guard = duckdb_test_guard();
        let conn = comparison_connection();
        conn.execute_batch(
            "CREATE TABLE source_pg.public.fixture(id BIGINT, status VARCHAR); \
             CREATE TABLE walrus.public.fixture_current(id BIGINT, status VARCHAR); \
             INSERT INTO source_pg.public.fixture VALUES (1, 'same'), (2, NULL); \
             INSERT INTO walrus.public.fixture_current VALUES (2, NULL), (1, 'same');",
        )
        .unwrap();

        assert_rows_equal(&conn, &TableId::new("public", "fixture"), &fields()).unwrap();
    }

    #[test]
    fn schema_check_reports_order_and_type_drift() {
        let _guard = duckdb_test_guard();
        let conn = comparison_connection();
        conn.execute_batch(
            "CREATE TABLE walrus.public.fixture_current(status VARCHAR, id INTEGER);",
        )
        .unwrap();

        let error = assert_view_schema(&conn, &TableId::new("public", "fixture"), &fields())
            .unwrap_err()
            .to_string();
        assert!(error.contains("DuckLake schema for public.fixture differs"));
        assert!(error.contains("actual: [(\"status\", \"VARCHAR\"), (\"id\", \"INTEGER\")]"));
    }

    #[test]
    fn registry_column_snapshot_accepts_both_durable_shapes() {
        let columns = serde_json::json!([{"name": "id"}]);
        let relation = serde_json::json!({"columns": [{"name": "id"}]});

        assert_eq!(registry_column_array(&columns).unwrap().len(), 1);
        assert_eq!(registry_column_array(&relation).unwrap().len(), 1);
        assert!(registry_column_array(&serde_json::json!({})).is_none());
    }
}
