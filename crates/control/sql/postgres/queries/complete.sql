UPDATE public.walrus_table_reload
SET status = 'complete', updated_at = now()
WHERE reload_id = $1
  AND status = 'export_complete'
  -- Compatibility only for rows that predate exporter fencing/publication. Protocol-v2 attempts
  -- have a positive generation and may reach complete only through finish_reload_publication.sql.
  AND exporter_generation = 0
