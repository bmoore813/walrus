SELECT parent.id
FROM public.walrus_stream_manifest_group AS parent
WHERE parent.epoch = $1
  AND parent.source_schema = $2
  AND parent.source_table = $3
  AND parent.commit_lsn <= $4
  AND (
    parent.status IN ('ready', 'failed')
    OR EXISTS (
      SELECT 1
      FROM public.walrus_file_manifest AS child
      WHERE child.stream_group_id = parent.id
    )
  )
ORDER BY parent.id
FOR UPDATE OF parent
