INSERT INTO public.walrus_replication_state
  (epoch, slot_name, created_lsn, status, catalog_fence_version)
VALUES ($1, $2, $3, $4, 0)
