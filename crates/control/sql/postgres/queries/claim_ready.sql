WITH eligible AS MATERIALIZED (
    SELECT m.id, m.lsn_end, m.stream_group_id
    FROM public.walrus_file_manifest m
    LEFT JOIN public.walrus_stream_manifest_group g ON g.id = m.stream_group_id
    WHERE m.epoch = $1
      AND m.source_schema = $2
      AND m.source_table = $3
      AND m.status = 'ready'
      AND (m.stream_group_id IS NULL OR g.status = 'ready')
      AND NOT EXISTS (
          SELECT 1 FROM public.walrus_table_reload r
          WHERE r.epoch = m.epoch
            AND r.source_schema = m.source_schema
            AND r.source_table = m.source_table
            AND r.status IN ('requested', 'exporting', 'export_complete', 'publishing')
      )
      AND NOT EXISTS (
          SELECT 1 FROM public.walrus_table_integrity_recovery recovery
          WHERE recovery.epoch = m.epoch
            AND recovery.source_schema = m.source_schema
            AND recovery.source_table = m.source_table
            AND recovery.status IN ('retrying', 'quarantined')
      )
), prefix AS MATERIALIZED (
    SELECT id, lsn_end, stream_group_id
    FROM eligible
    ORDER BY lsn_end, id
    LIMIT $4
), selected AS (
    -- LIMIT is a scheduling hint, never an atomicity boundary. If it touches one child of a
    -- protocol-v2 group, return every still-ready child in that per-table group.
    SELECT DISTINCT e.id
    FROM eligible e
    JOIN prefix p
      ON p.id = e.id
      OR (p.stream_group_id IS NOT NULL AND p.stream_group_id = e.stream_group_id)
)
SELECT m.id, m.epoch, m.source_schema, m.source_table, m.s3_uri, m.kind, m.row_count,
       m.object_size, m.sha256,
       m.lsn_start, m.lsn_end, m.schema_version,
       m.status, m.reload_id, m.stream_group_id, m.stream_group_ordinal,
       g.commit_ts AS stream_commit_ts,
       g.top_xid AS stream_top_xid,
       g.expected_files AS stream_group_expected_files,
       g.row_count AS stream_group_row_count,
       g.final_schema_version AS stream_group_final_schema_version
FROM public.walrus_file_manifest m
JOIN selected s ON s.id = m.id
LEFT JOIN public.walrus_stream_manifest_group g ON g.id = m.stream_group_id
ORDER BY m.lsn_end, m.id
