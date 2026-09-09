WITH requested_files AS MATERIALIZED (
    SELECT unnest($1::bigint[]) AS id
), requested_barriers AS MATERIALIZED (
    SELECT unnest($2::bigint[]) AS id
), file_rows AS MATERIALIZED (
    SELECT manifest.id, manifest.stream_group_id
    FROM public.walrus_file_manifest manifest
    JOIN requested_files requested ON requested.id = manifest.id
), target_groups AS MATERIALIZED (
    SELECT stream_group_id AS id FROM file_rows WHERE stream_group_id IS NOT NULL
    UNION
    SELECT id FROM requested_barriers
), locked_groups AS MATERIALIZED (
    SELECT parent.id
    FROM public.walrus_stream_manifest_group parent
    JOIN target_groups target ON target.id = parent.id
    ORDER BY parent.id
    FOR UPDATE OF parent
)
SELECT (SELECT count(*)::bigint FROM requested_files) AS requested_file_count,
       (SELECT count(*)::bigint FROM file_rows) AS found_file_count,
       (SELECT count(*)::bigint FROM target_groups) AS target_group_count,
       (SELECT count(*)::bigint FROM locked_groups) AS locked_group_count
