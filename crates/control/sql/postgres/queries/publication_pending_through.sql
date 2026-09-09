SELECT
  (SELECT count(*)
   FROM public.walrus_file_manifest m
   WHERE m.epoch = r.epoch
     AND m.source_schema = r.source_schema
     AND m.source_table = r.source_table
     AND m.lsn_end <= r.final_lsn
     AND m.stream_group_id IS NULL)
  +
  (SELECT count(*)
   FROM public.walrus_stream_manifest_group g
   WHERE g.epoch = r.epoch
     AND g.source_schema = r.source_schema
     AND g.source_table = r.source_table
     AND g.commit_lsn <= r.final_lsn
     AND (
       g.status IN ('ready', 'failed')
       OR EXISTS (
         SELECT 1 FROM public.walrus_file_manifest child WHERE child.stream_group_id = g.id
       )
     )) AS pending,
  EXISTS (
    SELECT 1
    FROM public.walrus_file_manifest m
    WHERE m.epoch = r.epoch
      AND m.source_schema = r.source_schema
      AND m.source_table = r.source_table
      AND m.stream_group_id IS NULL
      AND m.lsn_start <= r.final_lsn
      AND m.lsn_end > r.final_lsn
  ) AS ungrouped_straddler
FROM public.walrus_table_reload r
JOIN public.walrus_table_ownership o
  ON o.epoch = r.epoch
 AND o.source_schema = r.source_schema
 AND o.source_table = r.source_table
WHERE r.reload_id = $1
  AND r.status = 'publishing'
  AND r.publication_nonce = $2
  AND r.publisher_owner_pod = $3
  AND r.publisher_fencing_token = $4
  AND o.owner_pod = $3
  AND o.fencing_token = $4
  AND o.lease_expiry > statement_timestamp()
