SELECT EXISTS (
  SELECT 1
  FROM public.walrus_file_manifest manifest
  WHERE manifest.epoch = $1
    AND manifest.source_schema = $2
    AND manifest.source_table = $3
  UNION ALL
  SELECT 1
  FROM public.walrus_stream_manifest_group parent
  WHERE parent.epoch = $1
    AND parent.source_schema = $2
    AND parent.source_table = $3
    AND parent.status IN ('ready', 'failed')
) AS work_exists
