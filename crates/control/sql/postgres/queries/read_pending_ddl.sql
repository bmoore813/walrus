SELECT id, epoch, source_audit_id, source_schema, source_table,
       c_lsn, c_event, c_tag, schema_version, c_rel_oid,
       c_columns, c_dropped, c_ddl_text, c_table_comment
FROM public.walrus_ddl_manifest
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND c_lsn > $4
ORDER BY c_lsn, id
