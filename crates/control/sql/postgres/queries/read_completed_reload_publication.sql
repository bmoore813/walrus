SELECT EXISTS (
  SELECT 1
  FROM public.walrus_table_reload r
  JOIN public.walrus_transformer_checkpoint c
    ON c.epoch = r.epoch
   AND c.source_schema = r.source_schema
   AND c.source_table = r.source_table
  WHERE r.reload_id = $1
    AND r.epoch = $2
    AND r.source_schema = $3
    AND r.source_table = $4
    AND r.status = 'complete'
    AND r.publication_nonce = $5
    AND r.start_lsn = $6
    AND r.final_lsn = $7
    AND r.schema_version = $8
    AND r.publisher_owner_pod = $9
    AND r.publisher_fencing_token = $10
    AND c.raw_appended_lsn >= r.final_lsn
    AND c.transformed_lsn >= r.final_lsn
) AS already_complete
