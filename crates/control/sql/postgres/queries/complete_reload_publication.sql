WITH finished AS (
    UPDATE public.walrus_table_reload r
    SET status = 'complete', updated_at = now()
    WHERE r.reload_id = $1
      AND r.epoch = $2
      AND r.source_schema = $3
      AND r.source_table = $4
      AND r.status = 'publishing'
      AND r.publication_nonce = $5
      AND r.start_lsn = $6
      AND r.final_lsn = $7
      AND r.schema_version = $8
      AND r.publisher_owner_pod = $9
      AND r.publisher_fencing_token = $10
    RETURNING r.reload_id, r.epoch, r.source_schema, r.source_table
), recovered AS (
    UPDATE public.walrus_table_integrity_recovery recovery
    SET status = 'recovered', updated_at = now()
    FROM finished done
    WHERE recovery.epoch = done.epoch
      AND recovery.source_schema = done.source_schema
      AND recovery.source_table = done.source_table
      AND recovery.status = 'retrying'
      AND recovery.recovery_reload_id = done.reload_id
    RETURNING recovery.epoch
)
SELECT EXISTS (SELECT 1 FROM finished) AS transitioned
