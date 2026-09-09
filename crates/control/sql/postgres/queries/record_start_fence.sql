WITH authorized AS MATERIALIZED (
    SELECT set_config('walrus.reload_marker_protocol', '2', true) AS protocol
), candidate AS MATERIALIZED (
    SELECT reload_id
    FROM public.walrus_table_reload
    CROSS JOIN authorized
    WHERE reload_id = $1
      AND authorized.protocol = '2'
      AND (
        (
          status = 'exporting'
          AND (start_lsn IS NULL OR start_lsn = $2)
          AND (schema_version IS NULL OR schema_version = $3)
        )
        OR EXISTS (
          SELECT 1
          FROM public.walrus_table_reload_marker existing
          WHERE existing.reload_id = public.walrus_table_reload.reload_id
            AND existing.marker_kind = 'baseline'
            AND existing.lsn = $2
            AND existing.schema_version = $3
        )
      )
      AND (COALESCE(source_request_id, parent_request_id) IS NULL
           OR COALESCE(source_request_id, parent_request_id) = $4)
      AND source_schema = $5
      AND source_table = $6
    FOR UPDATE
), marker AS MATERIALIZED (
    INSERT INTO public.walrus_table_reload_marker
        (reload_id, marker_kind, lsn, schema_version)
    SELECT reload_id, 'baseline', $2, $3
    FROM candidate
    ON CONFLICT (reload_id, marker_kind) DO UPDATE
    SET lsn = EXCLUDED.lsn
    WHERE public.walrus_table_reload_marker.lsn = EXCLUDED.lsn
      AND public.walrus_table_reload_marker.schema_version = EXCLUDED.schema_version
    RETURNING reload_id
), frozen AS MATERIALIZED (
    UPDATE public.walrus_table_reload AS r
    SET start_lsn = $2,
        schema_version = COALESCE(r.schema_version, $3),
        updated_at = now()
    FROM marker
    WHERE r.reload_id = marker.reload_id
      AND r.status = 'exporting'
    RETURNING r.reload_id
), deauthorized AS MATERIALIZED (
    -- `record_start_fence` accepts a caller-owned transaction.  Do not leave the
    -- trigger capability live for the rest of that transaction/pooled session.
    -- The aggregates produce one row even when the guarded operation produced
    -- none, and make clearing happen strictly after both writes.
    SELECT pg_catalog.set_config('walrus.reload_marker_protocol', '', true) AS protocol
    FROM (SELECT count(*) FROM marker) AS marker_result
    CROSS JOIN (SELECT count(*) FROM frozen) AS frozen_result
)
SELECT marker.reload_id AS "reload_id?"
FROM deauthorized
LEFT JOIN marker ON true
WHERE deauthorized.protocol = ''
