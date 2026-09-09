INSERT INTO public.walrus_table_reload
  (epoch, source_schema, source_table, flavor, parent_request_id)
VALUES ($1, $2, $3, $4, $5)
RETURNING reload_id
