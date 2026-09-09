-- Distinguish epochs whose initial schema registry and start LSN were captured atomically under
-- SHARE locks from historical epochs that read those two facts in separate, racy operations.
-- Existing rows intentionally remain version zero: the extractor will open a reconciled successor.
ALTER TABLE public.walrus_replication_state
  ADD COLUMN catalog_fence_version integer NOT NULL DEFAULT 0,
  ADD CONSTRAINT replication_state_catalog_fence_version_supported
    CHECK (catalog_fence_version IN (0, 1)),
  ADD CONSTRAINT replication_state_status_check
    CHECK (status IN ('bootstrapping', 'streaming', 'total_restart'));

COMMENT ON COLUMN public.walrus_replication_state.created_lsn IS
  'writer-drained catalog/start LSN for the generation (slot consistent point for legacy rows)';

-- A protocol-v1 bootstrap target list is an identity set, not merely a JSON array with the right
-- length. This immutable helper is shared by the insertion and promotion guards. It deliberately
-- rejects malformed members and duplicate schema/table identities; quoted PostgreSQL identifiers
-- remain case-sensitive and are therefore compared byte-for-byte.
CREATE FUNCTION public.walrus_bootstrap_target_inventory_valid(
  candidate_expected_tables bigint,
  candidate_targets         jsonb
)
RETURNS boolean
LANGUAGE sql
IMMUTABLE
AS $$
  WITH target_element AS MATERIALIZED (
    SELECT element.value
    FROM pg_catalog.jsonb_array_elements(
      CASE
        WHEN pg_catalog.jsonb_typeof(candidate_targets) = 'array' THEN candidate_targets
        ELSE '[]'::jsonb
      END
    ) AS element(value)
  ), valid_target AS MATERIALIZED (
    SELECT element.value ->> 'schema' AS source_schema,
           element.value ->> 'table' AS source_table
    FROM target_element AS element
    WHERE pg_catalog.jsonb_typeof(element.value) = 'object'
      AND pg_catalog.jsonb_typeof(element.value -> 'schema') = 'string'
      AND pg_catalog.jsonb_typeof(element.value -> 'table') = 'string'
      AND element.value ->> 'schema' <> ''
      AND element.value ->> 'table' <> ''
  ), unique_target AS MATERIALIZED (
    SELECT target.source_schema, target.source_table
    FROM valid_target AS target
    GROUP BY target.source_schema, target.source_table
  )
  SELECT COALESCE(
    candidate_expected_tables IS NOT NULL
    AND candidate_expected_tables >= 0
    AND pg_catalog.jsonb_typeof(candidate_targets) = 'array'
    AND (SELECT count(*) FROM target_element) = candidate_expected_tables
    AND (SELECT count(*) FROM valid_target) = candidate_expected_tables
    AND (SELECT count(*) FROM unique_target) = candidate_expected_tables,
    false
  )
$$;

-- Keep the bootstrap cutover predicate in one database function so both the supported Rust path
-- and the row trigger evaluate exactly the same proof. Counts alone are insufficient: a fabricated
-- child for the wrong table could replace a missing target while preserving the expected count.
-- Parse and validate the frozen JSON identities, select only the newest DDL-restart attempt for
-- each child identity, and require bidirectional set equality. The empty inventory remains a valid
-- bootstrap and promotes without children.
CREATE FUNCTION public.walrus_bootstrap_generation_ready(
  candidate_epoch                 bigint,
  candidate_request_id            uuid,
  candidate_expected_tables       bigint,
  candidate_targets               jsonb,
  candidate_catalog_fence_version integer
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
  WITH target_element AS MATERIALIZED (
    SELECT element.value
    FROM pg_catalog.jsonb_array_elements(
      CASE
        WHEN pg_catalog.jsonb_typeof(candidate_targets) = 'array' THEN candidate_targets
        ELSE '[]'::jsonb
      END
    ) AS element(value)
  ), expected_target AS MATERIALIZED (
    SELECT element.value ->> 'schema' AS source_schema,
           element.value ->> 'table' AS source_table
    FROM target_element AS element
    WHERE pg_catalog.jsonb_typeof(element.value) = 'object'
      AND pg_catalog.jsonb_typeof(element.value -> 'schema') = 'string'
      AND pg_catalog.jsonb_typeof(element.value -> 'table') = 'string'
  ), unique_expected_target AS MATERIALIZED (
    SELECT target.source_schema, target.source_table
    FROM expected_target AS target
    GROUP BY target.source_schema, target.source_table
  ), latest_child AS MATERIALIZED (
    SELECT DISTINCT ON (reload.source_schema, reload.source_table)
           reload.reload_id, reload.source_schema, reload.source_table,
           reload.request_scope, reload.status
    FROM public.walrus_table_reload AS reload
    WHERE reload.epoch = candidate_epoch
      AND reload.parent_request_id = candidate_request_id
    ORDER BY reload.source_schema, reload.source_table, reload.reload_id DESC
  ), registered_target AS MATERIALIZED (
    SELECT registry.source_schema, registry.source_table
    FROM public.walrus_schema_registry AS registry
    WHERE registry.epoch = candidate_epoch
    GROUP BY registry.source_schema, registry.source_table
  )
  SELECT COALESCE(
    candidate_catalog_fence_version = 1
    AND candidate_request_id IS NOT NULL
    AND public.walrus_bootstrap_target_inventory_valid(
      candidate_expected_tables,
      candidate_targets
    )
    AND (SELECT count(*) FROM latest_child) = candidate_expected_tables
    AND (SELECT count(*) FROM registered_target) = candidate_expected_tables
    AND NOT EXISTS (
      SELECT 1
      FROM latest_child AS child
      WHERE child.request_scope <> 'all_published'
         OR child.status <> 'complete'
    )
    AND NOT EXISTS (
      SELECT 1
      FROM unique_expected_target AS target
      LEFT JOIN latest_child AS child
        ON child.source_schema = target.source_schema
       AND child.source_table = target.source_table
      WHERE child.reload_id IS NULL
    )
    AND NOT EXISTS (
      SELECT 1
      FROM latest_child AS child
      LEFT JOIN unique_expected_target AS target
        ON target.source_schema = child.source_schema
       AND target.source_table = child.source_table
      WHERE target.source_schema IS NULL
    )
    AND NOT EXISTS (
      SELECT 1
      FROM unique_expected_target AS target
      LEFT JOIN registered_target AS registered
        ON registered.source_schema = target.source_schema
       AND registered.source_table = target.source_table
      WHERE registered.source_schema IS NULL
    )
    AND NOT EXISTS (
      SELECT 1
      FROM registered_target AS registered
      LEFT JOIN unique_expected_target AS target
        ON target.source_schema = registered.source_schema
       AND target.source_table = registered.source_table
      WHERE target.source_schema IS NULL
    ),
    false
  )
$$;

-- `catalog_fence_version = 1` asserts that created_lsn and the target registry were captured in
-- one writer-drained source/control protocol. A generic INSERT cannot prove that. The current extractor
-- sets this transaction-local tripwire only inside its atomic bump_bootstrap_epoch statement; old
-- binaries and generic helpers remain limited to provenance zero, which startup never resumes.
CREATE FUNCTION public.walrus_guard_replication_state_insert_provenance()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.catalog_fence_version NOT IN (0, 1) THEN
    RAISE EXCEPTION 'unsupported replication catalog-fence protocol %',
                    NEW.catalog_fence_version
      USING ERRCODE = '23514',
            CONSTRAINT = 'replication_state_catalog_fence_version_supported';
  END IF;

  IF NEW.catalog_fence_version = 1
     AND (
       NEW.status <> 'bootstrapping'
       OR NEW.bootstrap_request_id IS NULL
       OR NEW.created_lsn <= '0/0'::pg_lsn
       OR NEW.slot_name = ''
       OR NOT public.walrus_bootstrap_target_inventory_valid(
         NEW.bootstrap_expected_tables,
         NEW.bootstrap_targets
       )
     ) THEN
    RAISE EXCEPTION
      'catalog-fence protocol 1 requires one well-formed bootstrapping inventory and source LSN'
      USING ERRCODE = '23514', CONSTRAINT = 'replication_state_catalog_fence_shape';
  END IF;

  IF NEW.catalog_fence_version = 1
     AND pg_catalog.current_setting('walrus.catalog_fence_protocol', true)
         IS DISTINCT FROM '1' THEN
    RAISE EXCEPTION
      'catalog-fence protocol 1 generations require the atomic bootstrap insertion protocol'
      USING ERRCODE = '23514',
            CONSTRAINT = 'replication_state_catalog_fence_insert_protocol';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER replication_state_insert_provenance
BEFORE INSERT ON public.walrus_replication_state
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_replication_state_insert_provenance();

-- The v1 row is inserted before its frozen registry rows in the same control transaction. Validate
-- their exact table identity set at the deferred boundary: neither a same-count wrong table nor an
-- unexpected extra registry entry can attest the source inventory. Multiple schema versions for
-- one table intentionally collapse to one identity. An empty target set requires an empty epoch
-- registry and remains valid.
CREATE FUNCTION public.walrus_bootstrap_registry_inventory_valid(
  candidate_epoch           bigint,
  candidate_expected_tables bigint,
  candidate_targets         jsonb
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
  WITH expected_target AS MATERIALIZED (
    SELECT element.value ->> 'schema' AS source_schema,
           element.value ->> 'table' AS source_table
    FROM pg_catalog.jsonb_array_elements(
      CASE
        WHEN pg_catalog.jsonb_typeof(candidate_targets) = 'array' THEN candidate_targets
        ELSE '[]'::jsonb
      END
    ) AS element(value)
  ), registered_target AS MATERIALIZED (
    SELECT registry.source_schema, registry.source_table
    FROM public.walrus_schema_registry AS registry
    WHERE registry.epoch = candidate_epoch
    GROUP BY registry.source_schema, registry.source_table
  )
  SELECT COALESCE(
    public.walrus_bootstrap_target_inventory_valid(
      candidate_expected_tables,
      candidate_targets
    )
    AND (SELECT count(*) FROM registered_target) = candidate_expected_tables
    AND NOT EXISTS (
      SELECT 1
      FROM expected_target AS target
      LEFT JOIN registered_target AS registered
        ON registered.source_schema = target.source_schema
       AND registered.source_table = target.source_table
      WHERE registered.source_schema IS NULL
    )
    AND NOT EXISTS (
      SELECT 1
      FROM registered_target AS registered
      LEFT JOIN expected_target AS target
        ON target.source_schema = registered.source_schema
       AND target.source_table = registered.source_table
      WHERE target.source_schema IS NULL
    ),
    false
  )
$$;

CREATE FUNCTION public.walrus_guard_replication_state_registry_inventory()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.catalog_fence_version = 1
     AND NOT public.walrus_bootstrap_registry_inventory_valid(
       NEW.epoch,
       NEW.bootstrap_expected_tables,
       NEW.bootstrap_targets
     ) THEN
    RAISE EXCEPTION
      'catalog-fence protocol 1 requires its exact frozen schema registry in the insertion transaction'
      USING ERRCODE = '23514',
            CONSTRAINT = 'replication_state_catalog_registry_inventory';
  END IF;
  RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER replication_state_catalog_registry_inventory
AFTER INSERT ON public.walrus_replication_state
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
WHEN (NEW.catalog_fence_version = 1)
EXECUTE FUNCTION public.walrus_guard_replication_state_registry_inventory();

-- Registry rows are immutable schema history. Exact upsert replays may execute PostgreSQL's no-op
-- UPDATE path, but no semantic field or creation timestamp may change, and removal requires an
-- explicit transaction-local maintenance tripwire. This applies to legacy and fenced epochs alike.
CREATE FUNCTION public.walrus_guard_schema_registry_semantics()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
      RAISE EXCEPTION 'schema registry history is semantically immutable'
        USING ERRCODE = '23514', CONSTRAINT = 'schema_registry_semantics_immutable';
    END IF;
    RETURN NEW;
  END IF;

  IF pg_catalog.current_setting('walrus.schema_registry_maintenance', true)
       IS DISTINCT FROM '1-delete' THEN
    RAISE EXCEPTION 'schema registry history is append-only and cannot be removed with %', TG_OP
      USING ERRCODE = '23514', CONSTRAINT = 'schema_registry_removal_guard';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NULL;
END
$$;

CREATE TRIGGER schema_registry_semantics_immutable
BEFORE UPDATE ON public.walrus_schema_registry
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_schema_registry_semantics();

CREATE TRIGGER schema_registry_delete_guard
BEFORE DELETE ON public.walrus_schema_registry
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_schema_registry_semantics();

CREATE TRIGGER schema_registry_truncate_guard
BEFORE TRUNCATE ON public.walrus_schema_registry
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_schema_registry_semantics();

-- Once a v1 generation exists, later structural versions are valid only for identities in its
-- frozen publication inventory. This permits arbitrary schema_version history for those tables but
-- prevents a Relation message or generic API from silently expanding the epoch's topology.
CREATE FUNCTION public.walrus_guard_schema_registry_catalog_membership()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  parent public.walrus_replication_state%ROWTYPE;
BEGIN
  SELECT state.* INTO parent
  FROM public.walrus_replication_state AS state
  WHERE state.epoch = NEW.epoch
  FOR KEY SHARE;

  IF FOUND
     AND parent.catalog_fence_version = 1
     AND NOT EXISTS (
       SELECT 1
       FROM pg_catalog.jsonb_array_elements(parent.bootstrap_targets) AS element(value)
       WHERE element.value ->> 'schema' = NEW.source_schema
         AND element.value ->> 'table' = NEW.source_table
     ) THEN
    RAISE EXCEPTION
      'schema registry identity %.% is outside epoch % frozen catalog',
      NEW.source_schema, NEW.source_table, NEW.epoch
      USING ERRCODE = '23514', CONSTRAINT = 'schema_registry_catalog_membership';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER schema_registry_catalog_membership
BEFORE INSERT ON public.walrus_schema_registry
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_schema_registry_catalog_membership();

-- The extractor trusts this row before it reads any WAL. Treat the slot identity, fence LSN,
-- provenance, immutable bootstrap request, and creation timestamp as one append-only generation
-- identity. Only the narrow operational status state machine may update an existing row.
CREATE FUNCTION public.walrus_guard_replication_state_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    IF pg_catalog.current_setting('walrus.replication_state_maintenance', true)
         IS DISTINCT FROM '1-delete' THEN
      RAISE EXCEPTION 'replication generations are append-only and cannot be removed with %', TG_OP
        USING ERRCODE = '23514', CONSTRAINT = 'replication_state_removal_guard';
    END IF;
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    END IF;
    RETURN NULL;
  END IF;

  IF (to_jsonb(NEW) - 'status') IS DISTINCT FROM (to_jsonb(OLD) - 'status') THEN
    RAISE EXCEPTION 'replication generation identity and catalog provenance are immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'replication_state_identity_immutable';
  END IF;

  IF NOT (
    OLD.status = NEW.status
    OR (OLD.status = 'bootstrapping' AND NEW.status = 'streaming')
    OR (
      OLD.status IN ('bootstrapping', 'streaming', 'total_restart')
      AND NEW.status = 'total_restart'
    )
  ) THEN
    RAISE EXCEPTION 'illegal replication generation status transition % -> %',
                    OLD.status, NEW.status
      USING ERRCODE = '23514', CONSTRAINT = 'replication_state_status_transition';
  END IF;

  IF OLD.status = 'bootstrapping'
     AND NEW.status = 'streaming'
     AND NOT public.walrus_bootstrap_generation_ready(
       OLD.epoch,
       OLD.bootstrap_request_id,
       OLD.bootstrap_expected_tables,
       OLD.bootstrap_targets,
       OLD.catalog_fence_version
     ) THEN
    RAISE EXCEPTION
      'bootstrap generation may stream only after every exact frozen target latest child completes'
      USING ERRCODE = '23514', CONSTRAINT = 'replication_state_bootstrap_promotion_guard';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER replication_state_identity_immutable
BEFORE UPDATE ON public.walrus_replication_state
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_replication_state_identity();

CREATE TRIGGER replication_state_delete_guard
BEFORE DELETE ON public.walrus_replication_state
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_replication_state_identity();

CREATE TRIGGER replication_state_truncate_guard
BEFORE TRUNCATE ON public.walrus_replication_state
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_replication_state_identity();
