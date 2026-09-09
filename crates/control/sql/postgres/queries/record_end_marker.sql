WITH authorized AS MATERIALIZED (
    SELECT set_config('walrus.reload_marker_protocol', '2', true) AS protocol
), recorded AS MATERIALIZED (
    INSERT INTO public.walrus_table_reload_marker
        (reload_id, marker_kind, lsn, schema_version)
    SELECT reload_id, 'end', $2, schema_version
    FROM public.walrus_table_reload
         CROSS JOIN authorized
    WHERE reload_id = $1
      AND authorized.protocol = '2'
      AND schema_version IS NOT NULL
      AND schema_version = $3
      AND (COALESCE(source_request_id, parent_request_id) IS NULL
           OR COALESCE(source_request_id, parent_request_id) = $4)
      AND source_schema = $5
      AND source_table = $6
      AND (
        (
          status = 'exporting'
          AND start_lsn IS NOT NULL
          AND start_lsn <= $2
        )
        OR EXISTS (
          SELECT 1 FROM public.walrus_table_reload_marker m
          WHERE m.reload_id = public.walrus_table_reload.reload_id
            AND m.marker_kind = 'end'
            AND m.lsn = $2
            AND m.schema_version = public.walrus_table_reload.schema_version
        )
      )
    ON CONFLICT (reload_id, marker_kind) DO UPDATE
    SET lsn = EXCLUDED.lsn
    WHERE public.walrus_table_reload_marker.lsn = EXCLUDED.lsn
      AND public.walrus_table_reload_marker.schema_version = EXCLUDED.schema_version
    RETURNING reload_id
), deauthorized AS MATERIALIZED (
    SELECT pg_catalog.set_config('walrus.reload_marker_protocol', '', true) AS protocol
    FROM (SELECT count(*) FROM recorded) AS operation_result
)
SELECT recorded.reload_id AS "reload_id?"
FROM deauthorized
LEFT JOIN recorded ON true
WHERE deauthorized.protocol = ''
