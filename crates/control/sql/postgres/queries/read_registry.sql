SELECT epoch, source_schema, source_table, schema_version,
       descriptors AS "descriptors: Json<Vec<TypeDescriptor>>",
       columns AS "columns: serde_json::Value"
FROM public.walrus_schema_registry
WHERE epoch = $1 AND source_schema = $2 AND source_table = $3 AND schema_version = $4
