WITH authorized AS MATERIALIZED (
  SELECT set_config('walrus.manifest_delete_protocol', '2', true) AS protocol
), deleted AS MATERIALIZED (
  DELETE FROM public.walrus_file_manifest
  WHERE reload_id = $1
    AND (SELECT protocol = '2' FROM authorized)
  RETURNING id
), deauthorized AS MATERIALIZED (
  SELECT pg_catalog.set_config('walrus.manifest_delete_protocol', '', true) AS protocol
  FROM (SELECT count(*) FROM deleted) AS operation_result
)
SELECT (SELECT count(*)::bigint FROM deleted) AS deleted_count
FROM deauthorized
WHERE deauthorized.protocol = ''
