UPDATE public.walrus_table_reload
SET status = 'failed', error = $2, updated_at = now()
WHERE reload_id = $1 AND status = 'exporting'
