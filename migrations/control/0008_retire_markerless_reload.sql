-- The unified publisher accepts only attempts proven by one durable baseline/end marker pair.
-- A pre-upgrade export_complete attempt has no proof that all chunks came from one source snapshot,
-- so fail it visibly and purge its baseline files instead of retaining a markerless compatibility
-- publication path. A fresh request will use the fenced protocol.

WITH unsafe_attempt AS (
  UPDATE public.walrus_table_reload
  SET status = 'failed',
      error = 'upgrade invalidated markerless export; issue a fresh fenced reload',
      updated_at = now()
  WHERE status = 'export_complete'
    AND start_lsn IS NULL
  RETURNING reload_id
)
DELETE FROM public.walrus_file_manifest AS manifest
USING unsafe_attempt
WHERE manifest.reload_id = unsafe_attempt.reload_id;
