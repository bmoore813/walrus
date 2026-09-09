WITH watermark AS (
  SELECT applied.c_lsn, applied.id
  FROM public.walrus_ddl_manifest applied
  WHERE applied.epoch = $1 AND applied.id = $4
  UNION ALL
  SELECT '0/0'::pg_lsn, 0::bigint
  WHERE NOT EXISTS (
    SELECT 1 FROM public.walrus_ddl_manifest applied
    WHERE applied.epoch = $1 AND applied.id = $4
  )
)
SELECT pending.id, pending.epoch, pending.source_audit_id,
       pending.source_schema, pending.source_table, pending.c_lsn,
       pending.c_event, pending.c_tag, pending.schema_version, pending.c_rel_oid,
       pending.c_columns, pending.c_dropped, pending.c_ddl_text,
       pending.c_table_comment
FROM public.walrus_ddl_manifest pending
CROSS JOIN watermark
WHERE pending.epoch = $1
  AND pending.source_schema = $2
  AND pending.source_table = $3
  AND (pending.c_lsn, pending.id) > (watermark.c_lsn, watermark.id)
  AND pending.schema_version <= $5
ORDER BY pending.c_lsn DESC, pending.id DESC
LIMIT 1
