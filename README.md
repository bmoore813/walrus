# walrus

A Postgres WAL → DuckLake replication service in Rust, built for Kubernetes. It consumes the
Postgres logical-replication WAL over one hand-rolled pgoutput stream (`proto_version 2`,
`streaming 'on'`), stages changes as Apache Arrow / Parquet in S3, appends them
into a shared PostgreSQL-catalogued DuckLake, and transforms each `<table>_raw` CDC log into a
current-state mirror on a user-chosen cadence. DuckLake stores the durable data as Parquet in S3;
the transformer's embedded DuckDB connections are transient execution engines, not database files.

See [How Walrus replicates PostgreSQL tables](docs/replication-overview.md) for a developer overview
of parallel exports, large-transaction streaming, batch reconciliation, and reloads on one slot.

## Shape (see the design doc for detail)

- **`walrus-extractor`** — *non-negotiable job: take work off the WAL and write it to storage,
  fast, so the slot can't run away.* Reads the WAL in memory, batches, converts Postgres → Arrow →
  Parquet, dumps to S3, records file locations + LSN ranges in a control table, and advances the
  replication slot only after that's durable. Flushes when **any** limit trips — cadence
  (`max_fill_ms`), memory footprint (`max_bytes`), or record count (`max_rows`). It moves change
  events verbatim; it does not reconcile them.
- **`walrus-transformer`** — *non-negotiable job: reconcile that work into the exact shape the data has
  in Postgres — accuracy over latency, not real-time.* Polls the control table on a cadence,
  pulls Parquet from S3, **appends each CDC row verbatim into a `<table>_raw` log** (keeping
  `walrus_extractor_meta`), then **transforms that log into `<table>`** — dedup-to-latest by
  PK/LSN, then `MERGE` upsert/delete — a current-state mirror that matches the source table. Each
  source table has an isolated internal DuckLake schema containing raw, mirror, ledger, and
  watermark state; readers use the stable `<source_schema>.<table>_current` view.

## Configuration

Both services require `WALRUS_CONFIG` to point to the same versioned YAML document. The checked-in
[`walrus.yaml`](walrus.yaml) is a complete local-development example; Kubernetes mounts the
non-secret [`deploy/k8s/base/walrus.yaml`](deploy/k8s/base/walrus.yaml) into both pods and supplies
database credentials separately.

Flat `WALRUS_*` environment variables override YAML. `WALRUS_CONTROL_DB_URL`,
`WALRUS_OBJECT_STORE__*`, and `WALRUS_TELEMETRY__*` override `common`; every other key overrides
the active service section. Unknown YAML keys and unknown active-service overrides are startup
errors.

Licensed under [MIT](LICENSE).
