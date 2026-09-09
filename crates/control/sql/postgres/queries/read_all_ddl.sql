SELECT id, epoch, source_audit_id, source_schema, source_table,
       c_lsn, c_event, c_tag, schema_version, c_rel_oid,
       c_columns, c_dropped, c_ddl_text, c_table_comment
FROM public.walrus_ddl_manifest
WHERE epoch = $1
ORDER BY c_lsn, id
