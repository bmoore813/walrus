WITH authorized AS MATERIALIZED (
  SELECT set_config('walrus.manifest_delete_protocol', '2', true) AS protocol
), requested_group_ids AS MATERIALIZED (
  SELECT DISTINCT unnest($5::bigint[]) AS id
), candidate_groups AS MATERIALIZED (
  -- These parents were locked in ascending id by the preceding transaction statement. This fresh
  -- READ COMMITTED snapshot deliberately observes a group that became applied/superseded while
  -- that statement waited, so terminal childless groups are idempotent no-ops below.
  SELECT g.id, g.expected_files, g.row_count, g.final_schema_version, g.file_shape, g.status
  FROM public.walrus_stream_manifest_group AS g
  JOIN requested_group_ids AS requested ON requested.id = g.id
), locked_files AS MATERIALIZED (
  SELECT manifest.id, manifest.stream_group_id, manifest.stream_group_ordinal,
         manifest.kind, manifest.status, manifest.row_count, manifest.lsn_start,
         manifest.lsn_end, manifest.schema_version
  FROM public.walrus_file_manifest AS manifest
  WHERE manifest.epoch = $1
    AND manifest.source_schema = $2
    AND manifest.source_table = $3
    AND manifest.kind <> 'reload'
    AND manifest.lsn_end <= $4
    AND (
      manifest.stream_group_id IS NULL
      OR manifest.stream_group_id IN (SELECT id FROM requested_group_ids)
    )
  ORDER BY manifest.id
  FOR UPDATE OF manifest
), group_stats AS MATERIALIZED (
  SELECT g.id, g.expected_files, g.row_count, g.final_schema_version, g.file_shape, g.status,
         count(file.id)::bigint AS actual_files,
         COALESCE(sum(file.row_count), 0)::bigint AS actual_rows,
         count(DISTINCT file.stream_group_ordinal)::bigint AS distinct_ordinals,
         min(file.stream_group_ordinal) AS min_ordinal,
         max(file.stream_group_ordinal) AS max_ordinal,
         bool_and(file.kind IN ('stream', 'spill')) AS kinds_valid,
         bool_and(file.status = g.status) AS statuses_valid,
         max(file.schema_version) AS max_child_schema_version,
         COALESCE(
           jsonb_agg(
             jsonb_build_object(
               'kind', file.kind,
               'row_count', file.row_count,
               'lsn_start', file.lsn_start - '0/0'::pg_lsn,
               'lsn_end', file.lsn_end - '0/0'::pg_lsn,
               'schema_version', file.schema_version
             )
             ORDER BY file.kind, file.row_count, file.lsn_start, file.lsn_end,
                      file.schema_version
           ) FILTER (WHERE file.id IS NOT NULL),
           '[]'::jsonb
         ) AS actual_shape
  FROM candidate_groups AS g
  LEFT JOIN locked_files AS file ON file.stream_group_id = g.id
  GROUP BY g.id, g.expected_files, g.row_count, g.final_schema_version, g.file_shape, g.status
), validation AS MATERIALIZED (
  SELECT count(*) FILTER (
    WHERE status NOT IN ('ready', 'failed', 'applied', 'superseded')
       OR (status IN ('applied', 'superseded') AND actual_files <> 0)
       OR (
         status IN ('ready', 'failed')
         AND (
           expected_files <> actual_files
           OR row_count <> actual_rows
           OR final_schema_version <= 0
           OR file_shape <> actual_shape
           OR (
             expected_files > 0
             AND (
               distinct_ordinals <> expected_files
               OR min_ordinal <> 0
               OR max_ordinal <> expected_files - 1
               OR kinds_valid IS NOT TRUE
               OR statuses_valid IS NOT TRUE
               OR max_child_schema_version > final_schema_version
             )
           )
         )
       )
  )::bigint AS invalid_groups
  FROM group_stats
), superseded_groups AS MATERIALIZED (
  UPDATE public.walrus_stream_manifest_group AS group_receipt
  SET status = 'superseded', applied_at = now()
  WHERE group_receipt.id IN (SELECT id FROM candidate_groups)
    AND group_receipt.status IN ('ready', 'failed')
    AND (SELECT invalid_groups FROM validation) = 0
  RETURNING group_receipt.id
), deleted AS MATERIALIZED (
  DELETE FROM public.walrus_file_manifest AS manifest
  USING locked_files AS locked
  WHERE manifest.id = locked.id
    AND (SELECT protocol = '2' FROM authorized)
    AND (SELECT invalid_groups FROM validation) = 0
  RETURNING manifest.id
), deauthorized AS MATERIALIZED (
  SELECT pg_catalog.set_config('walrus.manifest_delete_protocol', '', true) AS protocol
  FROM (SELECT count(*) FROM superseded_groups) AS superseded_result
  CROSS JOIN (SELECT count(*) FROM deleted) AS deleted_result
)
SELECT (SELECT invalid_groups FROM validation) AS invalid_groups,
       (SELECT count(*)::bigint FROM deleted) AS deleted_count,
       (SELECT count(*)::bigint FROM superseded_groups) AS superseded_group_count
FROM deauthorized
WHERE deauthorized.protocol = ''
