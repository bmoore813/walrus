SELECT reload_id,
       marker_kind AS "marker_kind: ReloadMarkerKind",
       lsn AS "lsn: Lsn",
       schema_version
FROM public.walrus_table_reload_marker
WHERE reload_id = $1
ORDER BY CASE marker_kind WHEN 'baseline' THEN 0 ELSE 1 END
