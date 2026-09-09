WITH candidates AS MATERIALIZED (
    SELECT candidate.reload_id
    FROM public.walrus_table_reload AS candidate
    WHERE candidate.epoch = $1
      AND candidate.status = 'exporting'
      AND (($5 AND candidate.lease_holder = $2)
           OR candidate.lease_expiry <= statement_timestamp())
    ORDER BY candidate.reload_id
    LIMIT $4
    FOR UPDATE OF candidate SKIP LOCKED
), adopted AS (
    UPDATE public.walrus_table_reload AS reload
    SET lease_holder = $2,
        exporter_generation = reload.exporter_generation + 1,
        lease_expiry = statement_timestamp() + make_interval(secs => $3),
        updated_at = now()
    FROM candidates
    WHERE reload.reload_id = candidates.reload_id
      AND reload.status = 'exporting'
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
FROM adopted
ORDER BY reload_id
