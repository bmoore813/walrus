INSERT INTO public.walrus_ddl_manifest AS durable
    (epoch, source_audit_id, source_schema, source_table, c_lsn, c_event, c_tag,
     schema_version, c_rel_oid, c_columns, c_dropped, c_ddl_text, c_table_comment)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
ON CONFLICT (epoch, source_audit_id) DO UPDATE
SET source_audit_id = EXCLUDED.source_audit_id
WHERE ROW(
        durable.source_schema,
        durable.source_table,
        durable.c_lsn,
        durable.c_event,
        durable.c_tag,
        durable.schema_version,
        durable.c_rel_oid,
        durable.c_columns,
        durable.c_dropped,
        durable.c_ddl_text,
        durable.c_table_comment
    ) IS NOT DISTINCT FROM ROW(
        EXCLUDED.source_schema,
        EXCLUDED.source_table,
        EXCLUDED.c_lsn,
        EXCLUDED.c_event,
        EXCLUDED.c_tag,
        EXCLUDED.schema_version,
        EXCLUDED.c_rel_oid,
        EXCLUDED.c_columns,
        EXCLUDED.c_dropped,
        EXCLUDED.c_ddl_text,
        EXCLUDED.c_table_comment
    )
RETURNING durable.id
