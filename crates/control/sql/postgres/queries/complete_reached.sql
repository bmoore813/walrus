UPDATE public.walrus_table_reload r
SET status = 'complete', updated_at = now()
FROM public.walrus_transformer_checkpoint c
WHERE r.epoch = $1 AND r.source_schema = $2 AND r.source_table = $3
  AND r.status = 'export_complete'
  -- Compatibility only for rows that predate exporter fencing/publication. Protocol-v2 attempts
  -- have a positive generation and may reach complete only through finish_reload_publication.sql.
  AND r.exporter_generation = 0
  AND c.epoch = r.epoch AND c.source_schema = r.source_schema
  AND c.source_table = r.source_table
  AND r.final_lsn <= c.transformed_lsn
RETURNING r.reload_id
