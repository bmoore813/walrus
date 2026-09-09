SELECT MAX(work_lsn) AS "max_lsn_end: Lsn"
FROM (
  SELECT lsn_end AS work_lsn
  FROM public.walrus_file_manifest
  WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND status = 'ready'
  UNION ALL
  SELECT commit_lsn
  FROM public.walrus_stream_manifest_group
  WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
    AND status = 'ready'
) AS ready_work
