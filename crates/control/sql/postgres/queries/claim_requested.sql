WITH candidates AS MATERIALIZED (
    SELECT candidate.reload_id
    FROM public.walrus_table_reload AS candidate
    WHERE candidate.epoch = $1 AND candidate.status = 'requested'
      -- A spent integrity-recovery budget is a durable stop, not permission for the exporter to
      -- keep producing replacement generations the transformer is forbidden to publish.
      AND NOT EXISTS (
          SELECT 1 FROM public.walrus_table_integrity_recovery AS recovery
          WHERE recovery.epoch = candidate.epoch
            AND recovery.source_schema = candidate.source_schema
            AND recovery.source_table = candidate.source_table
            AND recovery.status = 'quarantined'
      )
      -- Do not start the next queued source request until the transformer has published the current one.
      AND NOT EXISTS (
          SELECT 1 FROM public.walrus_table_reload AS active
          WHERE active.epoch = candidate.epoch
            AND active.source_schema = candidate.source_schema
            AND active.source_table = candidate.source_table
            AND active.status IN ('exporting', 'export_complete', 'publishing')
      )
      AND (
          -- A legacy direct request keeps its historical priority/duplicate semantics.
          candidate.source_request_id IS NULL
          OR (
              NOT EXISTS (
                  SELECT 1 FROM public.walrus_table_reload AS legacy
                  WHERE legacy.epoch = candidate.epoch
                    AND legacy.source_schema = candidate.source_schema
                    AND legacy.source_table = candidate.source_table
                    AND legacy.status = 'requested'
                    AND legacy.source_request_id IS NULL
              )
              -- Exactly the oldest source-WAL request for a table may enter the active state.
              AND NOT EXISTS (
                  SELECT 1 FROM public.walrus_table_reload AS earlier
                  WHERE earlier.epoch = candidate.epoch
                    AND earlier.source_schema = candidate.source_schema
                    AND earlier.source_table = candidate.source_table
                    AND earlier.status = 'requested'
                    AND earlier.source_request_id IS NOT NULL
                    AND earlier.reload_id < candidate.reload_id
              )
          )
      )
    ORDER BY candidate.reload_id
    LIMIT $4
    FOR UPDATE OF candidate SKIP LOCKED
), claimed AS (
    UPDATE public.walrus_table_reload AS reload
    SET status = 'exporting',
        lease_holder = $2,
        exporter_generation = reload.exporter_generation + 1,
        lease_expiry = statement_timestamp() + make_interval(secs => $3),
        updated_at = now()
    FROM candidates
    WHERE reload.reload_id = candidates.reload_id
      AND reload.status = 'requested'
    RETURNING reload.reload_id, reload.epoch, reload.source_schema, reload.source_table,
              reload.flavor, reload.status,
              reload.source_request_id, reload.parent_request_id,
              reload.request_scope,
              reload.chunk_no, reload.cursor_pk,
              reload.start_lsn, reload.first_lsn, reload.final_lsn,
              reload.schema_version, reload.restart_count, reload.lease_holder,
              reload.exporter_generation,
              reload.export_snapshot IS NOT NULL AS has_export_plan, reload.error
)
SELECT *
FROM claimed
ORDER BY reload_id
