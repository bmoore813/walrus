# End-to-end and parity tests

The `e2e` crate drives the real `walrus-extractor` and `walrus-transformer` binaries against source
PostgreSQL, control/catalog PostgreSQL, MinIO, and a PostgreSQL-backed DuckLake. Tests are ignored
and feature-gated so ordinary workspace tests do not require Docker.

## Exact parity acceptance suite

Run the self-contained suite with:

```sh
just acceptance
```

The command creates a uniquely named Compose project with dynamically assigned host ports, applies
the source/control migrations, migrates DuckLake, starts both binaries, runs the scenarios serially,
and removes only that project's containers and volumes. Set `WALRUS_E2E_KEEP_STACK=1` to retain the
backing containers after a run for debugging; the command printed at exit removes them later.

The parity verifier checks the logical read contract rather than DuckLake's implementation details:

- source table, current schema-registry entry, and public `<table>_current` view agree;
- ordered user columns and mapped DuckLake types agree;
- source table/column comments match the backing DuckLake mirror metadata; and
- source and DuckLake rows have no count, value, null, missing, extra, or multiplicity difference.

PostgreSQL-only indexes, constraints, defaults, DuckLake raw tables, and Walrus's applied-LSN columns
are intentionally excluded.

## Adding a scenario

Create the table and initial rows before the replication slot with `Harness::start_scenario`. Express
each later change as a `ScenarioStep` containing:

1. mutation SQL;
2. a separate, later sentinel write;
3. the tables whose transformed watermarks must pass the pre-mutation WAL floor; and
4. `TableExpectation::Present` or `TableExpectation::Absent` assertions.

Use `TableParity::auto` for scalar columns. It derives the canonical target types from the durable
registry and rejects decomposed mappings rather than guessing. For ranges, intervals, geometric
values, or another special representation, use `TableParity::explicit` with `CompareField` source
and DuckLake expressions. Expressions use source alias `s` and DuckLake alias `d`.

Keep each target table quiescent after its sentinel until comparison completes. Waiting for another
table does not provide a cross-table transactional snapshot.

The starter DROP COLUMN step removes a trailing column. A middle-column drop that shifts another
column into its ordinal position is currently quarantined by the product because it is
indistinguishable from a rename without durable column-lineage evidence.

`COMMENT ON TABLE` and `COMMENT ON COLUMN` are checked across baseline capture, replacement, and
`IS NULL` removal by the mirror-parity scenario. An online drop of a tracked table is currently an
unsupported identity change; the verifier's absent-table expectation is ready for that future case.
