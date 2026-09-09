WITH locked_ownership AS MATERIALIZED (
    SELECT o.epoch, o.source_schema, o.source_table
    FROM public.walrus_table_ownership o
    WHERE o.epoch = $5
      AND o.source_schema = $6
      AND o.source_table = $7
      AND o.owner_pod = $3
      AND o.fencing_token = $4
      AND o.lease_expiry > statement_timestamp()
    FOR UPDATE OF o
), fenced AS MATERIALIZED (
    SELECT r.reload_id, r.epoch, r.source_schema, r.source_table, r.final_lsn,
           r.publication_nonce
    FROM locked_ownership o
    JOIN public.walrus_table_reload r
      ON r.epoch = o.epoch
     AND r.source_schema = o.source_schema
     AND r.source_table = o.source_table
    WHERE r.reload_id = $1
      AND r.epoch = $5
      AND r.source_schema = $6
      AND r.source_table = $7
      AND r.status = 'publishing'
      AND r.publication_nonce = $2
      AND r.start_lsn = $8
      AND r.final_lsn = $9
      AND r.schema_version = $10
      AND r.publisher_owner_pod = $3
      AND r.publisher_fencing_token = $4
      AND r.publisher_owner_pod = $11
      AND r.publisher_fencing_token = $12
    FOR UPDATE OF r
), sealed AS MATERIALIZED (
    SELECT f.*
    FROM fenced f
    JOIN public.walrus_manifest_publication_fence seal
      ON seal.epoch = f.epoch
     AND seal.source_schema = f.source_schema
     AND seal.source_table = f.source_table
     AND seal.sealed_reload_id = f.reload_id
     AND seal.sealed_publication_nonce = f.publication_nonce
     AND seal.sealed_through_lsn = f.final_lsn
    FOR UPDATE OF seal
), advanced AS (
    UPDATE public.walrus_transformer_checkpoint c
    SET raw_appended_lsn = f.final_lsn,
        transformed_lsn = f.final_lsn,
        updated_at = now()
    FROM sealed f
    WHERE c.epoch = f.epoch
      AND c.source_schema = f.source_schema
      AND c.source_table = f.source_table
      AND c.raw_appended_lsn <= f.final_lsn
      AND c.transformed_lsn <= f.final_lsn
    RETURNING c.epoch, c.source_schema, c.source_table
)
SELECT EXISTS (
  SELECT 1
  FROM sealed f
  JOIN advanced c
    ON c.epoch = f.epoch
   AND c.source_schema = f.source_schema
   AND c.source_table = f.source_table
) AS prepared
