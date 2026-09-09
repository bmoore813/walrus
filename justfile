# justfile — everyday walrus dev commands. Run `just <recipe>`; `just --list` shows them all.
# Recipes are shell by default (`just` is not `make`).

compose := "docker compose -f deploy/docker/docker-compose.yml"

# List available recipes.
default:
    @just --list

# Boot the dev stack (source-pg, control-pg, minio + bucket) and block until healthy.
up:
    {{compose}} up --wait

# Tear the stack down, removing containers *and* volumes.
down:
    {{compose}} down -v

# Baseline gates (mirror CI).
fmt:
    cargo fmt --all --check

clippy:
    RUSTFLAGS="--cfg tokio_unstable" cargo clippy --all-targets --all-features -- -D warnings

test:
    cargo test --workspace

# Fast repository-shape checks for unit-test module wiring and naming.
hygiene:
    bash scripts/test-hygiene.sh --check

# Require every production Rust module to open with discoverable module-level documentation.
check-docs:
    bash scripts/check-module-docs.sh --check

# Feature-gated integration tests.
it:
    cargo test --workspace --features it

# Self-contained logical-parity acceptance suite. It owns an isolated Compose project, launches the
# real extractor/transformer binaries through the Rust harness, and tears down its containers and volumes.
acceptance:
    bash scripts/run-acceptance.sh

# Create or migrate the local PostgreSQL-backed DuckLake catalog. Normal transformer startup refuses
# automatic catalog migrations; production runs the same binary command as an explicit release step.
ducklake-migrate:
    WALRUS_CONFIG=walrus.yaml \
    WALRUS_CONTROL_DB_URL=postgres://postgres:postgres@localhost:5433/walrus_control \
    WALRUS_INSTANCE=walrus-transformer-0 \
    WALRUS_OBJECT_STORE__BUCKET=walrus \
    WALRUS_OBJECT_STORE__ENDPOINT=http://localhost:9000 \
    WALRUS_DUCKLAKE__CATALOG_URL=postgres://postgres:postgres@localhost:5433/walrus_ducklake \
    WALRUS_DUCKLAKE__DATA_PATH=s3://walrus/ducklake/tests/ \
    WALRUS_DUCKLAKE__INSTALL_EXTENSIONS=true \
    AWS_ACCESS_KEY_ID=minioadmin AWS_SECRET_ACCESS_KEY=minioadmin \
    cargo run -p transformer --bin walrus-transformer -- --migrate-ducklake-catalog

# Real DuckLake contract: PostgreSQL catalog + MinIO-backed Parquet, including transform, rebuild,
# pruning, per-table file maintenance, catalog retention procedures, and the public read view.
ducklake-it:
    WALRUS_DUCKLAKE_CATALOG_URL=postgres://postgres:postgres@localhost:5433/walrus_ducklake \
    cargo test -p transformer --test ducklake -- --ignored --test-threads=1

# Criterion micro-benches: extractor decode, Arrow batch building, transformer transform, and Phase-A append.
# Run on a quiet machine; results print to stdout. Never a CI gate (shared
# runners are too noisy) — CI only compile-checks the bench targets via `clippy --all-targets`.
# Baselines *and* the profiling workflow — how to get from "this bench is slow" to a flamegraph —
# live in docs/benchmarks.md. The transformer targets build DuckDB (`bundled`), so a cold first run
# compiles for ~20 min before it measures anything; the 1M-row transform grid takes minutes.
bench:
    cargo bench -p extractor -p pg-to-arrow -p transformer

# Record a named baseline for every bench (criterion stores it under `target/criterion/**/<name>`).
# `--` hands the flag to the bench binary (criterion), not to cargo.
bench-baseline name="main":
    cargo bench -p extractor -p pg-to-arrow -p transformer -- --save-baseline {{name}}

# Re-run the benches against a saved baseline: criterion prints the per-bench delta and whether it
# clears its noise threshold. Usage: `just bench-baseline before`, apply the change, then
# `just bench-compare before` — the only honest read on hardware that drifts between runs.
bench-compare name="main":
    cargo bench -p extractor -p pg-to-arrow -p transformer -- --baseline {{name}}

# End-to-end release measurement. The structured run bundle under target/perf includes the workload
# knobs, host/toolchain identity, one-second samples, CPU-seconds/1k rows and peak process RSS.
perf-e2e scenario="mixed":
    PERF_MODE=measure bash scripts/bench-e2e.sh {{scenario}}

# Compatibility name for the original benchmark entry point; it delegates to the structured run.
bench-e2e scenario="mixed":
    @just perf-e2e {{scenario}}

# One-slot table-reload benchmark. Defaults to the worker-scaling matrix over a warm 10M-row
# narrow fixture; the table matrix holds its requested-table workload fixed while varying only the
# concurrent-table cap. Use `wide`, `tables`, `chunks`, or `all` explicitly. An external pre-change
# extractor can be included with `BASELINE_EXTRACTOR_BIN=/absolute/path/to/walrus-extractor`.
perf-reload fixture="narrow" matrix="workers":
    bash scripts/bench-reload.sh {{fixture}} {{matrix}}

# Compatibility alias for callers that group every performance entry point under `bench-*`.
bench-reload fixture="narrow" matrix="workers":
    @just perf-reload {{fixture}} {{matrix}}

# Direction-aware comparison. It refuses different workloads, CPU/OS, toolchains or build modes.
perf-compare baseline candidate:
    python3 scripts/perf_report.py compare {{baseline}} {{candidate}}

# Profile one Criterion target, excluding Cargo/compilation from the captured process.
# Examples: `just profile-bench extractor decode parse_tuple 10`, `just profile-bench transformer transform`.
profile-bench package bench filter="" seconds="10":
    bash scripts/profile-bench.sh {{package}} {{bench}} '{{filter}}' {{seconds}}

# Profile exactly one service while the complete real pipeline is under load.
profile-e2e target="transformer" scenario="mixed":
    PERF_MODE=cpu PERF_TARGET={{target}} bash scripts/bench-e2e.sh {{scenario}}

# Allocation profile for exactly one service. Timing/CPU numbers in this bundle are diagnostic only.
profile-heap target="transformer" scenario="mixed":
    PERF_MODE=heap PERF_TARGET={{target}} bash scripts/bench-e2e.sh {{scenario}}

# Tokio task/resource diagnostics for exactly one service. Connect the UI to 127.0.0.1:6669.
profile-async target="transformer" scenario="mixed":
    PERF_MODE=async PERF_TARGET={{target}} bash scripts/bench-e2e.sh {{scenario}}

# Request a single-table rebuild through the source WAL. The extractor observes this committed event,
# records the durable control request, and coordinates the fenced export. `request_id` is optional;
# pass the UUID printed by an earlier invocation to make a command retry idempotent. Runs psql inside
# source-pg (the host needs no postgres-client). Just args are positional, so both
# `just reload public.orders`, `just reload public.orders request_id=<uuid>`, and the explicit
# `just reload table='public.orders' flavor=reload request_id=<uuid>` work; key= prefixes are
# stripped. `flavor` remains accepted for CLI compatibility, but the unified source-WAL protocol
# intentionally supports only a rebuilding `reload`.
reload table flavor='reload' request_id='':
    #!/usr/bin/env bash
    set -euo pipefail
    t="{{table}}"; t="${t#table=}"
    f="{{flavor}}"
    r="{{request_id}}"; r="${r#request_id=}"
    if [[ "$f" == request_id=* && -z "$r" ]]; then
      r="${f#request_id=}"
      f="reload"
    else
      f="${f#flavor=}"
    fi
    if [[ "$f" != "reload" ]]; then
      echo "reload_event supports only flavor=reload; resync cannot be mapped without changing semantics" >&2
      exit 2
    fi
    if [[ "$t" != *.* ]]; then
      echo "table must be schema-qualified (for example public.orders)" >&2
      exit 2
    fi
    source_schema="${t%%.*}"
    source_table="${t#*.}"
    if [[ -z "$source_schema" || -z "$source_table" || "$source_table" == *.* ]]; then
      echo "table must contain exactly one non-empty schema/table separator" >&2
      exit 2
    fi
    {{compose}} exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 \
      -v request_id="$r" -v source_schema="$source_schema" -v source_table="$source_table" \
      <<'SQL'
    WITH request AS (
      SELECT COALESCE(NULLIF(:'request_id', '')::uuid, gen_random_uuid()) AS request_id
    ), payload AS (
      SELECT request_id AS event_id, request_id, 'request'::text AS event_kind,
             'table'::text AS scope, :'source_schema'::text AS source_schema,
             :'source_table'::text AS source_table
      FROM request
    ), inserted AS (
      INSERT INTO public.walrus_reload_event
        (event_id, request_id, event_kind, scope, source_schema, source_table)
      SELECT event_id, request_id, event_kind, scope, source_schema, source_table
      FROM payload
      ON CONFLICT (event_id) DO NOTHING
      RETURNING request_id, event_kind, scope, source_schema, source_table, wal_insert_lsn
    )
    SELECT request_id, event_kind, scope, source_schema, source_table, wal_insert_lsn,
           'inserted' AS result
    FROM inserted
    UNION ALL
    SELECT event.request_id, event.event_kind, event.scope, event.source_schema,
           event.source_table, event.wal_insert_lsn, 'already_exists' AS result
    FROM public.walrus_reload_event AS event
    JOIN payload USING (event_id)
    WHERE NOT EXISTS (SELECT 1 FROM inserted)
      AND event.request_id = payload.request_id
      AND event.event_kind = payload.event_kind
      AND event.scope = payload.scope
      AND event.source_schema = payload.source_schema
      AND event.source_table = payload.source_table;
    \if :ROW_COUNT
    \else
      DO $$ BEGIN RAISE EXCEPTION 'request_id already belongs to a different reload request'; END $$;
    \endif
    SQL

# Request one coordinated rebuild of every published user table through the source WAL. The extractor
# expands this committed parent request from the target array frozen in the same row. Reuse
# `request_id` to retry safely.
reload-all request_id='':
    #!/usr/bin/env bash
    set -euo pipefail
    r="{{request_id}}"; r="${r#request_id=}"
    publication_name="${WALRUS_PUBLICATION_NAME:-walrus_pub}"
    {{compose}} exec -T source-pg psql -U postgres -d walrus -v ON_ERROR_STOP=1 \
      -v request_id="$r" -v publication_name="$publication_name" \
      <<'SQL'
    WITH request AS (
      SELECT COALESCE(NULLIF(:'request_id', '')::uuid, gen_random_uuid()) AS request_id
    ), targets AS (
      SELECT COALESCE(
        jsonb_agg(
          jsonb_build_object('schema', schemaname, 'table', tablename)
          ORDER BY schemaname, tablename
        ),
        '[]'::jsonb
      ) AS targets
      FROM pg_publication_tables
      WHERE pubname = :'publication_name'
        AND NOT (
          schemaname = 'public'
          AND tablename IN (
            'walrus_heartbeat', 'walrus_ddl_audit',
            'walrus_reload_signal', 'walrus_reload_event'
          )
        )
    ), payload AS (
      SELECT request_id AS event_id, request_id, 'request'::text AS event_kind,
             'all_published'::text AS scope, targets
      FROM request CROSS JOIN targets
    ), inserted AS (
      INSERT INTO public.walrus_reload_event (event_id, request_id, event_kind, scope, targets)
      SELECT event_id, request_id, event_kind, scope, targets FROM payload
      ON CONFLICT (event_id) DO NOTHING
      RETURNING request_id, event_kind, scope, targets, wal_insert_lsn
    )
    SELECT request_id, event_kind, scope, targets, wal_insert_lsn, 'inserted' AS result
    FROM inserted
    UNION ALL
    SELECT event.request_id, event.event_kind, event.scope, event.targets,
           event.wal_insert_lsn, 'already_exists' AS result
    FROM public.walrus_reload_event AS event
    JOIN payload USING (event_id)
    WHERE NOT EXISTS (SELECT 1 FROM inserted)
      AND event.request_id = payload.request_id
      AND event.event_kind = payload.event_kind
      AND event.scope = payload.scope
      AND event.source_schema IS NULL
      AND event.source_table IS NULL
      AND event.targets = payload.targets;
    \if :ROW_COUNT
    \else
      DO $$ BEGIN RAISE EXCEPTION 'request_id already belongs to a different reload request'; END $$;
    \endif
    SQL

# Connectivity smoke: both Postgres instances ready + MinIO health + the walrus bucket exists.
# Postgres checks run inside the containers (the host needs no postgres-client); MinIO health is
# hit on the published port.
smoke:
    {{compose}} exec -T source-pg pg_isready -U postgres -d walrus
    {{compose}} exec -T control-pg pg_isready -U postgres -d walrus_control
    {{compose}} exec -T control-pg pg_isready -U postgres -d walrus_ducklake
    curl -sf http://localhost:9000/minio/health/live
    {{compose}} exec -T createbucket mc ls local/walrus
