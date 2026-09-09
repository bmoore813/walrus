SELECT r.reload_id, r.epoch, r.source_schema, r.source_table, r.status,
       r.start_lsn, r.final_lsn, r.schema_version,
       r.publication_nonce, r.publisher_owner_pod, r.publisher_fencing_token
FROM public.walrus_table_reload r
WHERE r.reload_id = $1
  AND r.status IN ('publishing', 'complete')
  AND r.start_lsn IS NOT NULL
  AND r.final_lsn IS NOT NULL
  AND r.schema_version IS NOT NULL
  AND r.publication_nonce IS NOT NULL
  AND r.publisher_owner_pod IS NOT NULL
  AND r.publisher_fencing_token IS NOT NULL
