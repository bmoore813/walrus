UPDATE public.walrus_table_reload
SET chunk_no = $2,
    cursor_pk = $3,
    first_lsn = COALESCE(first_lsn, $4),
    schema_version = COALESCE(schema_version, $5),
    updated_at = now()
WHERE reload_id = $1 AND status = 'exporting' AND chunk_no = $2::bigint - 1
  -- Protocol v2 permanently retires keyset progress. Old binaries cannot mint a positive exporter
  -- generation, and newly claimed attempts must use the fenced snapshot/range/file protocol.
  AND exporter_generation = 0
  AND start_lsn IS NOT NULL
  AND start_lsn = $4
  AND schema_version = $5
  -- A rolling legacy exporter may start only before the parallel file counter has advanced. Once
  -- either mode records progress, cursor_pk distinguishes it and the other mode cannot mix in.
  AND (cursor_pk IS NOT NULL OR chunk_no = 0)
