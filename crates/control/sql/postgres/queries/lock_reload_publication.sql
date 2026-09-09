WITH locked_ownership AS MATERIALIZED (
  SELECT o.epoch, o.source_schema, o.source_table, o.owner_pod, o.fencing_token
  FROM public.walrus_table_ownership o
  WHERE o.epoch = $5
    AND o.source_schema = $6
    AND o.source_table = $7
    AND o.owner_pod = $3
    AND o.fencing_token = $4
    AND o.lease_expiry > statement_timestamp()
  FOR UPDATE OF o
), locked_reload AS MATERIALIZED (
  SELECT r.epoch, r.source_schema, r.source_table, r.start_lsn, r.final_lsn,
         r.schema_version, r.publication_nonce, r.publisher_owner_pod,
         r.publisher_fencing_token
  FROM locked_ownership o
  JOIN public.walrus_table_reload r
    ON r.epoch = o.epoch
   AND r.source_schema = o.source_schema
   AND r.source_table = o.source_table
  WHERE r.reload_id = $1
    AND r.status = 'publishing'
    AND r.publication_nonce = $2
    AND r.publisher_owner_pod = $3
    AND r.publisher_fencing_token = $4
  FOR UPDATE OF r
)
SELECT * FROM locked_reload
