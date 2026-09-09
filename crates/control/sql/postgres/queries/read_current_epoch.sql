SELECT epoch, slot_name, created_lsn AS "created_lsn: Lsn", status, catalog_fence_version
FROM public.walrus_replication_state
ORDER BY epoch DESC
LIMIT 1
