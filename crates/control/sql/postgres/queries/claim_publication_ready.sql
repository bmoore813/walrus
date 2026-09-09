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
),
candidates AS MATERIALIZED (
    SELECT m.id, m.epoch, m.source_schema, m.source_table, m.s3_uri, m.kind,
           m.row_count, m.object_size, m.sha256, m.lsn_start, m.lsn_end,
           m.schema_version, m.status, m.reload_id, m.stream_group_id,
           m.stream_group_ordinal, g.commit_ts AS stream_commit_ts,
           g.top_xid AS stream_top_xid,
           g.expected_files AS stream_group_expected_files,
           g.row_count AS stream_group_row_count,
           g.final_schema_version AS stream_group_final_schema_version
    FROM public.walrus_file_manifest m
    JOIN publication p
      ON p.epoch = m.epoch
     AND p.source_schema = m.source_schema
     AND p.source_table = m.source_table
    LEFT JOIN public.walrus_stream_manifest_group g ON g.id = m.stream_group_id
    WHERE m.status = 'ready'
      AND m.lsn_end <= p.final_lsn
      AND (m.stream_group_id IS NULL OR g.status = 'ready')
),
units AS (
    SELECT stream_group_id,
           CASE WHEN stream_group_id IS NULL THEN id END AS singleton_id,
           min(lsn_end) AS first_lsn,
           min(id) AS first_id,
           count(*) AS file_count
    FROM candidates
    GROUP BY stream_group_id, CASE WHEN stream_group_id IS NULL THEN id END
),
ranked_units AS (
    SELECT units.*,
           COALESCE(
             sum(file_count) OVER (
               ORDER BY first_lsn, first_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
             ),
             0
           ) AS files_before
    FROM units
),
selected_units AS (
    SELECT * FROM ranked_units WHERE files_before < $5
)
SELECT c.*
FROM candidates c
JOIN selected_units s
  ON (c.stream_group_id = s.stream_group_id)
  OR (c.stream_group_id IS NULL AND c.id = s.singleton_id)
ORDER BY c.lsn_end, c.id
