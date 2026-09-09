SELECT reload_id, epoch, source_schema, source_table,
       flavor, status,
       source_request_id, parent_request_id,
       request_scope,
       chunk_no, cursor_pk,
       start_lsn, first_lsn, final_lsn,
       schema_version, restart_count, lease_holder, exporter_generation,
       export_snapshot IS NOT NULL AS has_export_plan, error
FROM public.walrus_table_reload
WHERE reload_id = $1
