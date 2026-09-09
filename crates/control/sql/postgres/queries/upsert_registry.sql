INSERT INTO public.walrus_schema_registry AS durable
    (epoch, source_schema, source_table, schema_version, descriptors, columns)
VALUES ($1, $2, $3, $4, $5, $6)
ON CONFLICT (epoch, source_schema, source_table, schema_version) DO UPDATE
SET schema_version = EXCLUDED.schema_version
WHERE ROW(durable.descriptors, durable.columns)
      IS NOT DISTINCT FROM ROW(EXCLUDED.descriptors, EXCLUDED.columns)
RETURNING durable.schema_version
