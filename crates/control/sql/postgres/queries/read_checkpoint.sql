SELECT epoch, source_schema, source_table,
       raw_appended_lsn AS "raw_appended_lsn: Lsn",
       transformed_lsn AS "transformed_lsn: Lsn"
FROM public.walrus_transformer_checkpoint
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3
