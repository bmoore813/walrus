INSERT INTO public.walrus_table_reload
    (epoch, source_schema, source_table, flavor, status, restart_count,
     lease_holder, lease_expiry, exporter_generation, parent_request_id, request_scope)
SELECT epoch, source_schema, source_table, flavor, 'exporting', $2,
       lease_holder, lease_expiry, exporter_generation,
       COALESCE(source_request_id, parent_request_id), request_scope
FROM public.walrus_table_reload
WHERE reload_id = $1
RETURNING reload_id
