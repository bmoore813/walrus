WITH publication AS MATERIALIZED (
    SELECT r.epoch, r.source_schema, r.source_table, r.final_lsn
    FROM public.walrus_table_reload r
    JOIN public.walrus_table_ownership o
      ON o.epoch = r.epoch
     AND o.source_schema = r.source_schema
     AND o.source_table = r.source_table
    WHERE r.reload_id = $1
      AND r.status = 'publishing'
      AND r.publication_nonce = $2
      AND r.publisher_owner_pod = $3
      AND r.publisher_fencing_token = $4
      AND o.owner_pod = $3
      AND o.fencing_token = $4
      AND o.lease_expiry > statement_timestamp()
), group_parents AS MATERIALIZED (
    SELECT parent.*
    FROM public.walrus_stream_manifest_group parent
    JOIN publication p
      ON p.epoch = parent.epoch
     AND p.source_schema = parent.source_schema
     AND p.source_table = parent.source_table
     AND parent.commit_lsn <= p.final_lsn
    WHERE parent.status IN ('ready', 'failed')
       OR EXISTS (
           SELECT 1 FROM public.walrus_file_manifest child WHERE child.stream_group_id = parent.id
       )
), group_stats AS MATERIALIZED (
    SELECT parent.id, parent.epoch, parent.source_schema, parent.source_table,
           parent.commit_lsn, parent.commit_ts, parent.top_xid, parent.expected_files,
           parent.row_count, parent.final_schema_version, parent.file_shape, parent.status,
           count(child.id)::bigint AS actual_files,
           COALESCE(sum(child.row_count), 0)::bigint AS actual_rows,
           count(DISTINCT child.stream_group_ordinal)::bigint AS distinct_ordinals,
           min(child.stream_group_ordinal) AS min_ordinal,
           max(child.stream_group_ordinal) AS max_ordinal,
           bool_and(child.kind IN ('stream', 'spill'))
             FILTER (WHERE child.id IS NOT NULL) AS kinds_valid,
           bool_and(child.status = parent.status)
             FILTER (WHERE child.id IS NOT NULL) AS statuses_valid,
           max(child.schema_version) AS max_child_schema_version,
           COALESCE(
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
             ) FILTER (WHERE child.id IS NOT NULL),
             '[]'::jsonb
           ) AS actual_shape
    FROM group_parents parent
    LEFT JOIN public.walrus_file_manifest child ON child.stream_group_id = parent.id
    GROUP BY parent.id, parent.epoch, parent.source_schema, parent.source_table,
             parent.commit_lsn, parent.commit_ts, parent.top_xid, parent.expected_files,
             parent.row_count, parent.final_schema_version, parent.file_shape, parent.status
), checked_groups AS MATERIALIZED (
    SELECT stats.*,
           stats.status = 'ready'
           AND stats.expected_files = stats.actual_files
           AND stats.row_count = stats.actual_rows
           AND stats.file_shape = stats.actual_shape
           AND stats.final_schema_version > 0
           AND (
             (stats.expected_files = 0 AND stats.row_count = 0 AND stats.actual_files = 0)
             OR
             (stats.expected_files > 0
              AND stats.row_count > 0
              AND stats.distinct_ordinals = stats.expected_files
              AND stats.min_ordinal = 0
              AND stats.max_ordinal = stats.expected_files - 1
              AND stats.kinds_valid IS TRUE
              AND stats.statuses_valid IS TRUE
              AND stats.max_child_schema_version <= stats.final_schema_version)
           ) AS unit_valid
    FROM group_stats stats
), ungrouped AS MATERIALIZED (
    SELECT manifest.*
    FROM public.walrus_file_manifest manifest
    JOIN publication p
      ON p.epoch = manifest.epoch
     AND p.source_schema = manifest.source_schema
     AND p.source_table = manifest.source_table
     AND manifest.lsn_end <= p.final_lsn
    WHERE manifest.stream_group_id IS NULL
), units AS (
    SELECT true AS is_group, id AS group_id, NULL::bigint AS singleton_id,
           commit_lsn AS first_lsn, id AS first_id,
           GREATEST(expected_files, actual_files, 1::bigint) AS unit_cost,
           unit_valid
    FROM checked_groups
    UNION ALL
    SELECT false, NULL::bigint, id, lsn_end, id, 1::bigint, status = 'ready'
    FROM ungrouped
), ranked_units AS (
    SELECT units.*,
           COALESCE(
             sum(unit_cost) OVER (
               ORDER BY first_lsn, is_group, first_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
             ),
             0
           ) AS work_before,
           COALESCE(
             bool_or(NOT unit_valid) OVER (
               ORDER BY first_lsn, is_group, first_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
             ),
             false
           ) AS invalid_before
    FROM units
), selected_units AS MATERIALIZED (
    SELECT * FROM ranked_units
    WHERE work_before < $5
      AND NOT invalid_before
      AND (unit_valid OR work_before = 0)
), claimed_work AS MATERIALIZED (
  SELECT group_row.expected_files = 0 AS is_schema_barrier,
       child.id, group_row.epoch, group_row.source_schema, group_row.source_table,
       child.s3_uri, child.kind, child.row_count, child.object_size, child.sha256,
       child.lsn_start, group_row.commit_lsn AS lsn_end, child.schema_version,
       child.status, child.reload_id, group_row.id AS stream_group_id,
       child.stream_group_ordinal, group_row.commit_ts AS stream_commit_ts,
       group_row.top_xid AS stream_top_xid,
       group_row.expected_files AS stream_group_expected_files,
       group_row.row_count AS stream_group_row_count,
       group_row.final_schema_version AS stream_group_final_schema_version,
       group_row.status AS stream_group_status, group_row.unit_valid,
       unit.first_lsn AS work_lsn, 1::int AS work_kind, unit.first_id AS work_id
  FROM selected_units unit
  JOIN checked_groups group_row ON unit.is_group AND group_row.id = unit.group_id
  LEFT JOIN public.walrus_file_manifest child ON child.stream_group_id = group_row.id
  UNION ALL
  SELECT false,
       file.id, file.epoch, file.source_schema, file.source_table, file.s3_uri, file.kind,
       file.row_count, file.object_size, file.sha256, file.lsn_start, file.lsn_end,
       file.schema_version, file.status, file.reload_id, NULL::bigint, NULL::bigint,
       NULL::text, NULL::bigint, NULL::bigint, NULL::bigint, NULL::bigint,
       NULL::text, file.status = 'ready',
       unit.first_lsn, 0::int, unit.first_id
  FROM selected_units unit
  JOIN ungrouped file ON NOT unit.is_group AND file.id = unit.singleton_id
)
SELECT auth.claim_authorized,
       claimed_work.work_kind IS NOT NULL AS claim_has_work,
       claimed_work.*
FROM (SELECT EXISTS(SELECT 1 FROM publication) AS claim_authorized) auth
LEFT JOIN claimed_work ON true
ORDER BY claimed_work.work_lsn, claimed_work.work_kind, claimed_work.work_id, claimed_work.id
