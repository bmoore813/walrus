WITH requested AS MATERIALIZED (
    SELECT *
    FROM unnest(
        $1::bigint[], $2::bigint[], $3::text[], $4::text[], $5::text[], $6::bigint[]
    ) AS item(id, epoch, source_schema, source_table, commit_lsn, final_schema_version)
), locked_groups AS MATERIALIZED (
    SELECT g.id, g.expected_files, g.row_count, g.final_schema_version,
           g.file_shape, g.status
    FROM public.walrus_stream_manifest_group g
    JOIN requested item
      ON item.id = g.id
     AND item.epoch = g.epoch
     AND item.source_schema = g.source_schema
     AND item.source_table = g.source_table
     AND item.commit_lsn::pg_lsn = g.commit_lsn
     AND item.final_schema_version = g.final_schema_version
    ORDER BY g.id
    FOR UPDATE OF g
), group_stats AS MATERIALIZED (
    SELECT g.id, g.expected_files, g.row_count, g.final_schema_version,
           g.file_shape, g.status, count(child.id)::bigint AS actual_files
    FROM locked_groups g
    LEFT JOIN public.walrus_file_manifest child ON child.stream_group_id = g.id
    GROUP BY g.id, g.expected_files, g.row_count, g.final_schema_version,
             g.file_shape, g.status
), validation AS MATERIALIZED (
    SELECT
      (SELECT count(*) FROM requested) <> (SELECT count(*) FROM locked_groups)
      OR EXISTS (
          SELECT 1 FROM group_stats
          WHERE expected_files <> 0
             OR row_count <> 0
             OR file_shape <> '[]'::jsonb
             OR status <> 'ready'
             OR actual_files <> 0
             OR final_schema_version <= 0
      ) AS invalid
), completed AS (
    UPDATE public.walrus_stream_manifest_group g
    SET status = 'applied', applied_at = now()
    WHERE g.id IN (SELECT id FROM locked_groups)
      AND NOT (SELECT invalid FROM validation)
    RETURNING g.id
)
SELECT (SELECT invalid FROM validation) AS invalid,
       (SELECT count(*)::bigint FROM completed) AS completed_count
