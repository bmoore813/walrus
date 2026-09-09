UPDATE public.walrus_table_reload
SET lease_expiry = statement_timestamp() + make_interval(secs => $3), updated_at = now()
WHERE reload_id = $1 AND lease_holder = $2 AND exporter_generation = $4
  AND status = 'exporting'
  AND lease_expiry > statement_timestamp()
