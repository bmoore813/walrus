INSERT INTO public.walrus_table_reload
    (epoch, source_schema, source_table, flavor,
     source_request_id, parent_request_id, request_scope)
VALUES ($1, $2, $3, $4, $5, $6, $7)
ON CONFLICT (epoch, source_request_id, source_schema, source_table)
  WHERE source_request_id IS NOT NULL
DO UPDATE
SET source_request_id = EXCLUDED.source_request_id
WHERE public.walrus_table_reload.parent_request_id IS NOT DISTINCT FROM EXCLUDED.parent_request_id
  AND public.walrus_table_reload.request_scope = EXCLUDED.request_scope
  AND public.walrus_table_reload.flavor = EXCLUDED.flavor
RETURNING reload_id
