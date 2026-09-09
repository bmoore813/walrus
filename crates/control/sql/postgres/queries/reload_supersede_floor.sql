SELECT start_lsn AS "first_lsn: Lsn"
FROM public.walrus_table_reload
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
  AND status IN ('requested', 'exporting', 'export_complete', 'publishing')
  AND start_lsn IS NOT NULL
ORDER BY reload_id DESC
LIMIT 1
