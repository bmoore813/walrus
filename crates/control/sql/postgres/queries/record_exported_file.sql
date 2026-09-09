UPDATE public.walrus_table_reload
SET chunk_no = chunk_no + 1,
    first_lsn = COALESCE(first_lsn, start_lsn),
    updated_at = now()
WHERE reload_id = $1
  AND status = 'exporting'
  AND start_lsn = $2
  AND schema_version = $3
  AND lease_holder = $4
  AND exporter_generation = $5
  AND lease_expiry > statement_timestamp()
  AND export_snapshot IS NOT NULL
  AND export_sealed_at IS NULL
  AND cursor_pk IS NULL
RETURNING chunk_no
