WITH authorized AS MATERIALIZED (
  SELECT set_config('walrus.manifest_delete_protocol', '2', true) AS protocol
), requested AS MATERIALIZED (
  SELECT DISTINCT unnest($1::bigint[]) AS id
), requested_group_ids AS MATERIALIZED (
  SELECT DISTINCT manifest.stream_group_id AS id
  FROM requested
  JOIN public.walrus_file_manifest AS manifest ON manifest.id = requested.id
  WHERE manifest.stream_group_id IS NOT NULL
), locked_groups AS MATERIALIZED (
  SELECT g.id, g.expected_files, g.row_count, g.final_schema_version, g.file_shape, g.status
  FROM public.walrus_stream_manifest_group AS g
  JOIN requested_group_ids AS requested ON requested.id = g.id
  ORDER BY g.id
  FOR UPDATE OF g
), locked_children AS MATERIALIZED (
  SELECT child.id, child.stream_group_id, child.stream_group_ordinal, child.kind,
         child.status, child.row_count, child.lsn_start, child.lsn_end,
         child.schema_version
  FROM public.walrus_file_manifest AS child
  JOIN locked_groups AS g ON g.id = child.stream_group_id
  ORDER BY child.id
  FOR UPDATE OF child
), group_stats AS MATERIALIZED (
  SELECT g.id, g.expected_files, g.row_count, g.final_schema_version, g.file_shape, g.status,
         count(child.id)::bigint AS actual_files,
         count(child.id) FILTER (WHERE requested.id IS NOT NULL)::bigint AS requested_files,
         COALESCE(sum(child.row_count), 0)::bigint AS actual_rows,
         count(DISTINCT child.stream_group_ordinal)::bigint AS distinct_ordinals,
         min(child.stream_group_ordinal) AS min_ordinal,
         max(child.stream_group_ordinal) AS max_ordinal,
         bool_and(child.kind IN ('stream', 'spill')) AS kinds_valid,
         bool_and(child.status = 'ready') AS statuses_valid,
         max(child.schema_version) AS max_child_schema_version,
         jsonb_agg(
           jsonb_build_object(
             'kind', child.kind,
             'row_count', child.row_count,
             'lsn_start', child.lsn_start - '0/0'::pg_lsn,
             'lsn_end', child.lsn_end - '0/0'::pg_lsn,
             'schema_version', child.schema_version
           )
           ORDER BY child.kind, child.row_count, child.lsn_start, child.lsn_end,
                    child.schema_version
         ) AS actual_shape
  FROM locked_groups AS g
  LEFT JOIN locked_children AS child ON child.stream_group_id = g.id
  LEFT JOIN requested ON requested.id = child.id
  GROUP BY g.id, g.expected_files, g.row_count, g.final_schema_version, g.file_shape, g.status
), invalid_group AS MATERIALIZED (
  SELECT 1
  FROM group_stats
  WHERE status <> 'ready'
     OR expected_files <> actual_files
     OR expected_files <> requested_files
     OR row_count <> actual_rows
     OR distinct_ordinals <> expected_files
     OR min_ordinal <> 0
     OR max_ordinal <> expected_files - 1
     OR kinds_valid IS NOT TRUE
     OR statuses_valid IS NOT TRUE
     OR final_schema_version <= 0
     OR max_child_schema_version > final_schema_version
     OR file_shape <> actual_shape
  LIMIT 1
), applied_groups AS MATERIALIZED (
  UPDATE public.walrus_stream_manifest_group AS g
  SET status = 'applied', applied_at = now()
  WHERE g.id IN (SELECT id FROM locked_groups)
    AND g.status = 'ready'
    AND NOT EXISTS (SELECT 1 FROM invalid_group)
  RETURNING g.id
), deleted AS MATERIALIZED (
  DELETE FROM public.walrus_file_manifest AS manifest
  USING requested
  WHERE manifest.id = requested.id
    AND (SELECT protocol = '2' FROM authorized)
    AND NOT EXISTS (SELECT 1 FROM invalid_group)
    AND manifest.status = 'ready'
    AND (
      manifest.stream_group_id IS NULL
      OR manifest.stream_group_id IN (SELECT id FROM applied_groups)
    )
  RETURNING manifest.id
), deauthorized AS MATERIALIZED (
  SELECT pg_catalog.set_config('walrus.manifest_delete_protocol', '', true) AS protocol
  FROM (SELECT count(*) FROM applied_groups) AS applied_result
  CROSS JOIN (SELECT count(*) FROM deleted) AS deleted_result
)
SELECT (SELECT count(*)::bigint FROM deleted) AS deleted_count
FROM deauthorized
WHERE deauthorized.protocol = ''
