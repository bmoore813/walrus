UPDATE public.walrus_table_reload
SET status = 'requested', lease_holder = NULL, lease_expiry = NULL, updated_at = now()
WHERE reload_id = $1 AND lease_holder = $2 AND exporter_generation = $3
  AND status = 'exporting'
  AND start_lsn IS NULL
  AND chunk_no = 0
  AND cursor_pk IS NULL
