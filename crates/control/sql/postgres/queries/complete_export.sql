WITH authorized AS MATERIALIZED (
  SELECT set_config('walrus.reload_export_complete_protocol', '2', true) AS protocol
), completed AS MATERIALIZED (
  UPDATE public.walrus_table_reload
  SET status = 'export_complete', final_lsn = $2, updated_at = now()
  FROM authorized
  WHERE reload_id = $1 AND status = 'exporting'
    AND authorized.protocol = '2'
    AND lease_holder = $3
    AND exporter_generation = $4
    AND lease_expiry > statement_timestamp()
    AND start_lsn IS NOT NULL
    AND export_snapshot IS NOT NULL
    AND export_sealed_at IS NOT NULL
    AND export_file_count = chunk_no
    AND export_row_count IS NOT NULL
    AND $2 >= start_lsn
    AND EXISTS (
      SELECT 1 FROM public.walrus_table_reload_marker
      WHERE reload_id = $1 AND marker_kind = 'baseline'
        AND lsn = public.walrus_table_reload.start_lsn
        AND schema_version = public.walrus_table_reload.schema_version
    )
    AND EXISTS (
      SELECT 1 FROM public.walrus_table_reload_marker
      WHERE reload_id = $1 AND marker_kind = 'end'
        AND lsn = $2
        AND schema_version = public.walrus_table_reload.schema_version
    )
  RETURNING reload_id
), deauthorized AS MATERIALIZED (
  SELECT pg_catalog.set_config('walrus.reload_export_complete_protocol', '', true) AS protocol
  FROM (SELECT count(*) FROM completed) AS operation_result
)
SELECT completed.reload_id
FROM deauthorized
LEFT JOIN completed ON true
WHERE deauthorized.protocol = ''
