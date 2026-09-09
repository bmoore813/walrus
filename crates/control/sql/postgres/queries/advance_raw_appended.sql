INSERT INTO public.walrus_transformer_checkpoint
    (epoch, source_schema, source_table, raw_appended_lsn, transformed_lsn)
VALUES ($1, $2, $3, $4, '0/0'::pg_lsn)
ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
    SET raw_appended_lsn =
            GREATEST(public.walrus_transformer_checkpoint.raw_appended_lsn, EXCLUDED.raw_appended_lsn),
        updated_at = now()
