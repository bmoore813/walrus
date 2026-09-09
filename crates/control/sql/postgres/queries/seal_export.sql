WITH range_stats AS MATERIALIZED (
  SELECT count(*) AS range_count,
         count(*) FILTER (WHERE status = 'complete') AS complete_count,
         count(*) FILTER (WHERE exporter_generation <> $3) AS stale_count,
         COALESCE(sum(file_count) FILTER (WHERE status = 'complete'), 0)::bigint AS file_count,
         COALESCE(sum(row_count) FILTER (WHERE status = 'complete'), 0)::bigint AS row_count
  FROM public.walrus_table_reload_export_range
  WHERE reload_id = $1
), manifest_stats AS MATERIALIZED (
  SELECT count(manifest.id) AS file_count,
         COALESCE(sum(manifest.row_count), 0)::bigint AS row_count,
         count(*) FILTER (
           WHERE manifest.status <> 'ready'
              OR manifest.kind <> 'reload'
              OR manifest.epoch <> reload.epoch
              OR manifest.source_schema <> reload.source_schema
              OR manifest.source_table <> reload.source_table
              OR manifest.lsn_start <> $4
              OR manifest.lsn_end <> $4
              OR manifest.schema_version <> $5
         ) AS invalid_count
  FROM public.walrus_table_reload AS reload
  LEFT JOIN public.walrus_file_manifest AS manifest
    ON manifest.reload_id = reload.reload_id
  WHERE reload.reload_id = $1
), candidate AS MATERIALIZED (
  SELECT reload.reload_id,
         ranges.file_count,
         ranges.row_count
  FROM public.walrus_table_reload AS reload
  CROSS JOIN range_stats AS ranges
  CROSS JOIN manifest_stats AS manifests
  WHERE reload.reload_id = $1
    AND reload.status = 'exporting'
    AND reload.lease_holder = $2
    AND reload.exporter_generation = $3
    AND reload.lease_expiry > statement_timestamp()
    AND reload.start_lsn = $4
    AND reload.schema_version = $5
    AND reload.cursor_pk IS NULL
    AND reload.export_snapshot IS NOT NULL
    AND reload.export_range_count = ranges.range_count
    AND ranges.range_count = ranges.complete_count
    AND ranges.stale_count = 0
    AND public.walrus_reload_export_plan_is_attested(
      reload.reload_id,
      reload.exporter_generation,
      reload.export_range_count
    )
    AND ranges.file_count = manifests.file_count
    AND ranges.row_count = manifests.row_count
    AND manifests.invalid_count = 0
    AND reload.chunk_no = manifests.file_count
    AND (
      reload.export_sealed_at IS NULL
      OR (reload.export_file_count = manifests.file_count
          AND reload.export_row_count = manifests.row_count)
    )
)
UPDATE public.walrus_table_reload AS reload
SET export_sealed_at = COALESCE(reload.export_sealed_at, now()),
    export_file_count = candidate.file_count,
    export_row_count = candidate.row_count,
    updated_at = now()
FROM candidate
WHERE reload.reload_id = candidate.reload_id
RETURNING candidate.file_count, candidate.row_count
