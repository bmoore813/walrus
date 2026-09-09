INSERT INTO public.walrus_replication_state
  (epoch, slot_name, created_lsn, status, catalog_fence_version)
SELECT COALESCE(MAX(epoch), 0) + 1, $1, $2, $3, 0
FROM public.walrus_replication_state
RETURNING epoch
