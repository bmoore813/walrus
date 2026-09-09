INSERT INTO "{table}_raw" ({destination_columns}, _walrus_op, _walrus_commit_lsn, _walrus_lsn, _walrus_extractor_processed_at)
SELECT {source_columns}, json_extract_string(walrus_extractor_meta, '$.op'), {commit_lsn_expr}, json_extract_string(walrus_extractor_meta, '$.lsn'), json_extract_string(walrus_extractor_meta, '$.extractor_processed_at')
FROM read_parquet('{uri}'){on_conflict}
