-- 0007_bootstrap_reconcile.sql -- bind a new epoch to the all-table request that builds it.

ALTER TABLE public.walrus_replication_state
  ADD COLUMN bootstrap_request_id uuid,
  ADD COLUMN bootstrap_expected_tables bigint,
  ADD COLUMN bootstrap_targets jsonb,
  ADD CONSTRAINT replication_state_bootstrap_request_pair CHECK (
    (bootstrap_request_id IS NULL AND bootstrap_expected_tables IS NULL
     AND bootstrap_targets IS NULL)
    OR
    (bootstrap_request_id IS NOT NULL AND bootstrap_expected_tables IS NOT NULL
     AND bootstrap_targets IS NOT NULL AND jsonb_typeof(bootstrap_targets) = 'array'
     AND bootstrap_expected_tables >= 0
     AND bootstrap_expected_tables = jsonb_array_length(bootstrap_targets))
  );

CREATE UNIQUE INDEX walrus_replication_state_bootstrap_request_idx
  ON public.walrus_replication_state (bootstrap_request_id)
  WHERE bootstrap_request_id IS NOT NULL;
