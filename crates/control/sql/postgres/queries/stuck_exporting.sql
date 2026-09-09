SELECT reload_id, lease_holder
FROM public.walrus_table_reload
WHERE epoch = $1 AND status = 'exporting'
  AND lease_expiry IS NOT NULL AND lease_expiry <= statement_timestamp()
ORDER BY reload_id
