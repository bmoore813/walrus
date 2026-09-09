SELECT MAX(schema_version)
FROM public.walrus_ddl_manifest
WHERE epoch = $1
  AND source_schema = $2
  AND source_table = $3
  AND c_lsn <= $4
