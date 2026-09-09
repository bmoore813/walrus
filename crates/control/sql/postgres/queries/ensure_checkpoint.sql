INSERT INTO public.walrus_transformer_checkpoint
    (epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn)
VALUES ($1, $2, $3, '0/0'::pg_lsn, '0/0'::pg_lsn)
ON CONFLICT (epoch, source_schema, source_table) DO NOTHING
