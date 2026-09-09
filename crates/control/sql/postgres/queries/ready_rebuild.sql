SELECT r.reload_id, r.epoch, r.source_schema, r.source_table,
       r.flavor, r.status,
       r.source_request_id, r.parent_request_id,
       r.request_scope,
       r.chunk_no, r.cursor_pk,
       r.start_lsn, r.first_lsn, r.final_lsn,
       r.schema_version, r.restart_count, r.lease_holder, r.exporter_generation,
       r.export_snapshot IS NOT NULL AS has_export_plan, r.error
FROM public.walrus_table_reload r
WHERE r.epoch = $1 AND r.source_schema = $2 AND r.source_table = $3
  AND r.status = 'export_complete'
  AND r.start_lsn IS NOT NULL AND r.final_lsn IS NOT NULL AND r.schema_version IS NOT NULL
  AND r.export_sealed_at IS NOT NULL
  AND r.final_lsn >= r.start_lsn
  AND EXISTS (
    SELECT 1 FROM public.walrus_table_reload_marker m
    WHERE m.reload_id = r.reload_id AND m.marker_kind = 'baseline'
      AND m.lsn = r.start_lsn AND m.schema_version = r.schema_version
  )
  AND EXISTS (
    SELECT 1 FROM public.walrus_table_reload_marker m
    WHERE m.reload_id = r.reload_id AND m.marker_kind = 'end'
      AND m.lsn = r.final_lsn AND m.schema_version = r.schema_version
  )
ORDER BY r.reload_id DESC
LIMIT 1
