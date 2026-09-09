-- Reload fences are durable protocol evidence, not editable scheduler state. 0009 attested the
-- final publication transition, but the transformer also trusts the earlier F/schema/snapshot/H header
-- and its two marker rows. Freeze each field as soon as it is established and require the narrow
-- protocol path for every first write so a stale or buggy embedded statement cannot manufacture a
-- different reconciliation window.

-- Freeze marker writers before parent writers. PostgreSQL's start-fence plan takes a RowShare lock
-- on the parent, a RowExclusive lock on the marker table, and only then upgrades the parent table
-- to RowExclusive for the data-modifying CTE. Locking the parent first here could let that writer
-- hold the marker while waiting for its parent upgrade, then deadlock when this migration requests
-- the marker. Marker-first blocks the writer before that partial acquisition; ShareRowExclusive on
-- the parent remains compatible with its earlier RowShare lock. The locks are held through this
-- transactional migration, so validation and trigger installation see one stable evidence set.
LOCK TABLE public.walrus_table_reload_marker, public.walrus_table_reload
IN SHARE ROW EXCLUSIVE MODE;

-- Take the range-table lock only after the parent. Frozen-plan creation writes the parent before
-- inserting its children, while range completion already holds its child lock before consulting
-- the parent. This order lets either in-flight path drain without creating a parent/child lock
-- inversion, then keeps the plan stable while existing seals are validated below.
LOCK TABLE public.walrus_table_reload_export_range
IN SHARE ROW EXCLUSIVE MODE;

-- Recompute the physical-plan proof from durable rows. The Rust planner rejects malformed input
-- early, but the database is the trust boundary for the seal: an embedded/stale statement or a
-- privileged writer must not be able to bless a plan with a gap, overlap, swapped ordinal, early
-- open end, or finite final end. A valid plan is exactly one full-scan row, or a zero-based CTID
-- chain whose first block is zero and whose sole open end belongs to its final row.
CREATE FUNCTION public.walrus_reload_export_plan_is_attested(
  expected_reload_id bigint,
  expected_exporter_generation bigint,
  expected_range_count bigint
)
RETURNS boolean
LANGUAGE sql
STABLE
AS $$
  WITH ordered AS MATERIALIZED (
    SELECT export_range.exporter_generation,
           export_range.range_no,
           export_range.full_scan,
           export_range.start_block,
           export_range.end_block,
           export_range.status,
           row_number() OVER (ORDER BY export_range.range_no) AS ordinal,
           count(*) OVER () AS observed_range_count,
           lag(export_range.end_block) OVER (ORDER BY export_range.range_no) AS previous_end
    FROM public.walrus_table_reload_export_range AS export_range
    WHERE export_range.reload_id = expected_reload_id
  )
  SELECT COALESCE(
    count(*)::bigint = expected_range_count
    AND count(*) > 0
    AND bool_and(exporter_generation = expected_exporter_generation)
    AND bool_and(status = 'complete')
    AND (
      (
        count(*) = 1
        AND bool_and(
          full_scan
          AND range_no = 0
          AND start_block IS NULL
          AND end_block IS NULL
        )
      )
      OR
      bool_and(
        NOT full_scan
        AND range_no = ordinal - 1
        AND start_block IS NOT NULL
        AND start_block >= 0
        AND CASE
              WHEN ordinal = 1 THEN start_block = 0
              ELSE previous_end IS NOT NULL AND start_block = previous_end
            END
        AND CASE
              WHEN ordinal = observed_range_count THEN end_block IS NULL
              ELSE end_block IS NOT NULL AND end_block > start_block
            END
      )
    ),
    false
  )
  FROM ordered
$$;

-- Do not silently grandfather a malformed seal created between the protocol-v2 rollout and this
-- migration. Generation-zero rows are legacy attempts without a frozen range plan.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.walrus_table_reload AS reload
    WHERE reload.exporter_generation > 0
      AND reload.export_sealed_at IS NOT NULL
      AND NOT public.walrus_reload_export_plan_is_attested(
        reload.reload_id,
        reload.exporter_generation,
        reload.export_range_count
      )
  ) THEN
    RAISE EXCEPTION
      'reload evidence migration found a sealed protocol-v2 export with a malformed range plan';
  END IF;
END
$$;

CREATE FUNCTION public.walrus_guard_table_reload_initial_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status NOT IN ('requested', 'exporting')
     OR NEW.chunk_no <> 0
     OR NEW.cursor_pk IS NOT NULL
     OR NEW.first_lsn IS NOT NULL
     OR NEW.start_lsn IS NOT NULL
     OR NEW.final_lsn IS NOT NULL
     OR NEW.schema_version IS NOT NULL
     OR NEW.export_snapshot IS NOT NULL
     OR NEW.export_snapshot_xmin IS NOT NULL
     OR NEW.export_snapshot_xmax IS NOT NULL
     OR NEW.export_range_count IS NOT NULL
     OR NEW.export_sealed_at IS NOT NULL
     OR NEW.export_file_count IS NOT NULL
     OR NEW.export_row_count IS NOT NULL
     OR NEW.publication_nonce IS NOT NULL
     OR NEW.publisher_owner_pod IS NOT NULL
     OR NEW.publisher_fencing_token IS NOT NULL
     OR NEW.publishing_at IS NOT NULL THEN
    RAISE EXCEPTION 'a new reload attempt must begin without fabricated boundary evidence'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_initial_evidence_pristine';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_initial_evidence
BEFORE INSERT ON public.walrus_table_reload
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_table_reload_initial_evidence();

CREATE FUNCTION public.walrus_guard_table_reload_durable_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF ROW(
       NEW.reload_id, NEW.epoch, NEW.source_schema, NEW.source_table, NEW.flavor,
       NEW.source_request_id, NEW.parent_request_id, NEW.request_scope,
       NEW.restart_count, NEW.requested_at
     ) IS DISTINCT FROM ROW(
       OLD.reload_id, OLD.epoch, OLD.source_schema, OLD.source_table, OLD.flavor,
       OLD.source_request_id, OLD.parent_request_id, OLD.request_scope,
       OLD.restart_count, OLD.requested_at
     ) THEN
    RAISE EXCEPTION 'reload request identity is immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_request_identity_immutable';
  END IF;

  IF NEW.exporter_generation < OLD.exporter_generation
     OR (NEW.exporter_generation IS DISTINCT FROM OLD.exporter_generation
         AND NEW.status <> 'exporting') THEN
    RAISE EXCEPTION 'reload exporter generation is monotonic and may change only while exporting'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_exporter_generation_immutable';
  END IF;
  IF NEW.chunk_no < OLD.chunk_no THEN
    RAISE EXCEPTION 'reload exported-file count cannot move backward'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_chunk_no_monotonic';
  END IF;

  IF OLD.start_lsn IS NOT NULL AND NEW.start_lsn IS DISTINCT FROM OLD.start_lsn THEN
    RAISE EXCEPTION 'reload start LSN is immutable once fenced'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_start_lsn_immutable';
  END IF;
  IF OLD.schema_version IS NOT NULL
     AND NEW.schema_version IS DISTINCT FROM OLD.schema_version THEN
    RAISE EXCEPTION 'reload schema version is immutable once fenced'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_schema_version_immutable';
  END IF;
  IF OLD.first_lsn IS NOT NULL AND NEW.first_lsn IS DISTINCT FROM OLD.first_lsn THEN
    RAISE EXCEPTION 'reload first-file LSN is immutable once recorded'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_first_lsn_immutable';
  END IF;
  IF OLD.final_lsn IS NOT NULL AND NEW.final_lsn IS DISTINCT FROM OLD.final_lsn THEN
    RAISE EXCEPTION 'reload final LSN is immutable once fenced'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_final_lsn_immutable';
  END IF;

  IF OLD.export_snapshot IS NOT NULL
     AND ROW(
       NEW.export_snapshot, NEW.export_snapshot_xmin,
       NEW.export_snapshot_xmax, NEW.export_range_count
     ) IS DISTINCT FROM ROW(
       OLD.export_snapshot, OLD.export_snapshot_xmin,
       OLD.export_snapshot_xmax, OLD.export_range_count
     ) THEN
    RAISE EXCEPTION 'reload snapshot and range-plan header are immutable once frozen'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_snapshot_header_immutable';
  END IF;
  IF OLD.export_sealed_at IS NOT NULL
     AND ROW(NEW.export_sealed_at, NEW.export_file_count, NEW.export_row_count)
       IS DISTINCT FROM
         ROW(OLD.export_sealed_at, OLD.export_file_count, OLD.export_row_count) THEN
    RAISE EXCEPTION 'reload export seal totals are immutable once recorded'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_export_seal_immutable';
  END IF;

  IF OLD.export_sealed_at IS NOT NULL
     AND ROW(NEW.chunk_no, NEW.cursor_pk, NEW.first_lsn)
       IS DISTINCT FROM ROW(OLD.chunk_no, OLD.cursor_pk, OLD.first_lsn) THEN
    RAISE EXCEPTION 'reload progress is immutable after its export is sealed'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_sealed_progress_immutable';
  END IF;

  IF OLD.start_lsn IS NULL AND NEW.start_lsn IS NOT NULL THEN
    IF current_setting('walrus.reload_marker_protocol', true) IS DISTINCT FROM '2'
       OR NEW.status <> 'exporting'
       OR NEW.schema_version IS NULL THEN
      RAISE EXCEPTION 'reload F/schema may be set only by the protocol-v2 baseline-marker path'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_start_fence_protocol_v2';
    END IF;
  END IF;
  IF OLD.schema_version IS NULL AND NEW.schema_version IS NOT NULL THEN
    IF current_setting('walrus.reload_marker_protocol', true) IS DISTINCT FROM '2'
       OR NEW.status <> 'exporting'
       OR NEW.start_lsn IS NULL THEN
      RAISE EXCEPTION 'reload F/schema may be set only by the protocol-v2 baseline-marker path'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_start_fence_protocol_v2';
    END IF;
  END IF;
  IF OLD.first_lsn IS NULL AND NEW.first_lsn IS NOT NULL
     AND NEW.first_lsn IS DISTINCT FROM NEW.start_lsn THEN
    RAISE EXCEPTION 'reload first-file LSN must equal its frozen start fence'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_first_lsn_matches_start';
  END IF;

  IF OLD.export_snapshot IS NULL AND NEW.export_snapshot IS NOT NULL THEN
    IF current_setting('walrus.reload_export_plan_protocol', true) IS DISTINCT FROM '2'
       OR NEW.status <> 'exporting'
       OR NEW.start_lsn IS NULL
       OR NEW.schema_version IS NULL THEN
      RAISE EXCEPTION 'reload snapshot header may be set only by the protocol-v2 frozen-plan path'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_snapshot_header_protocol_v2';
    END IF;
  END IF;
  IF OLD.export_sealed_at IS NULL AND NEW.export_sealed_at IS NOT NULL THEN
    IF current_setting('walrus.reload_export_seal_protocol', true) IS DISTINCT FROM '2'
       OR NEW.status <> 'exporting'
       OR NEW.export_snapshot IS NULL
       OR NOT public.walrus_reload_export_plan_is_attested(
         NEW.reload_id,
         NEW.exporter_generation,
         NEW.export_range_count
       ) THEN
      RAISE EXCEPTION
        'reload export totals require a protocol-v2 seal over one exact complete range plan'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_export_plan_attestation';
    END IF;
  END IF;
  IF OLD.final_lsn IS NULL AND NEW.final_lsn IS NOT NULL THEN
    IF current_setting('walrus.reload_export_complete_protocol', true) IS DISTINCT FROM '2'
       OR NEW.status <> 'export_complete'
       OR NEW.start_lsn IS NULL
       OR NEW.schema_version IS NULL
       OR NEW.export_sealed_at IS NULL
       OR NEW.final_lsn < NEW.start_lsn THEN
      RAISE EXCEPTION 'reload H may be set only by the protocol-v2 export-completion path'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_final_lsn_protocol_v2';
    END IF;
  END IF;

  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_durable_evidence
BEFORE UPDATE ON public.walrus_table_reload
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_table_reload_durable_evidence();

-- Validate existing marker evidence before installing the forward guards. An upgrade must stop on
-- contradictory bounds instead of blessing them as immutable.
DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM public.walrus_table_reload_marker AS marker
    JOIN public.walrus_table_reload AS reload USING (reload_id)
    WHERE marker.schema_version IS DISTINCT FROM reload.schema_version
       OR reload.start_lsn IS NULL
       OR (marker.marker_kind = 'baseline' AND marker.lsn IS DISTINCT FROM reload.start_lsn)
       OR (marker.marker_kind = 'end' AND marker.lsn < reload.start_lsn)
       OR (marker.marker_kind = 'end' AND reload.final_lsn IS NOT NULL
           AND marker.lsn IS DISTINCT FROM reload.final_lsn)
       OR (marker.marker_kind = 'end' AND reload.exporter_generation > 0
           AND (reload.export_snapshot IS NULL OR reload.export_sealed_at IS NULL))
  ) THEN
    RAISE EXCEPTION 'reload evidence migration found a marker that contradicts its reload header';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM public.walrus_table_reload AS reload
    WHERE reload.exporter_generation > 0
      AND (
        (reload.start_lsn IS NOT NULL AND NOT EXISTS (
          SELECT 1
          FROM public.walrus_table_reload_marker AS marker
          WHERE marker.reload_id = reload.reload_id
            AND marker.marker_kind = 'baseline'
            AND marker.lsn = reload.start_lsn
            AND marker.schema_version = reload.schema_version
        ))
        OR
        (reload.final_lsn IS NOT NULL AND NOT EXISTS (
          SELECT 1
          FROM public.walrus_table_reload_marker AS marker
          WHERE marker.reload_id = reload.reload_id
            AND marker.marker_kind = 'end'
            AND marker.lsn = reload.final_lsn
            AND marker.schema_version = reload.schema_version
        ))
      )
  ) THEN
    RAISE EXCEPTION
      'reload evidence migration found a protocol-v2 header without its exact durable marker';
  END IF;
END
$$;

CREATE FUNCTION public.walrus_guard_table_reload_marker_evidence()
RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
  parent public.walrus_table_reload%ROWTYPE;
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    IF current_setting('walrus.manifest_fence_maintenance', true) IS DISTINCT FROM '2-delete' THEN
      RAISE EXCEPTION 'reload boundary markers are append-only durable protocol evidence'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_marker_removal';
    END IF;
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    END IF;
    RETURN NULL;
  END IF;

  IF TG_OP = 'UPDATE' THEN
    IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
      RAISE EXCEPTION 'reload boundary marker semantics are immutable'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_marker_immutable';
    END IF;
    RETURN NEW;
  END IF;

  IF current_setting('walrus.reload_marker_protocol', true) IS DISTINCT FROM '2' THEN
    RAISE EXCEPTION 'reload boundary markers may be inserted only by the protocol-v2 marker path'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_marker_insert_protocol_v2';
  END IF;

  -- BEFORE INSERT runs before ON CONFLICT decides whether this is an insert or the exact replay
  -- update below. Admit an already-identical receipt in every later parent state; the UPDATE arm
  -- still checks the complete row for equality. A changed replay never gets to use parent state as
  -- permission to rewrite the existing primary-key row.
  IF EXISTS (
    SELECT 1
    FROM public.walrus_table_reload_marker AS existing
    WHERE existing.reload_id = NEW.reload_id
      AND existing.marker_kind = NEW.marker_kind
      AND existing.lsn = NEW.lsn
      AND existing.schema_version = NEW.schema_version
  ) THEN
    RETURN NEW;
  END IF;
  IF EXISTS (
    SELECT 1
    FROM public.walrus_table_reload_marker AS existing
    WHERE existing.reload_id = NEW.reload_id
      AND existing.marker_kind = NEW.marker_kind
  ) THEN
    RAISE EXCEPTION 'reload boundary marker semantics are immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_marker_immutable';
  END IF;

  SELECT * INTO parent
  FROM public.walrus_table_reload
  WHERE reload_id = NEW.reload_id
  FOR KEY SHARE;
  IF NOT FOUND OR parent.status <> 'exporting' THEN
    RAISE EXCEPTION 'reload boundary marker requires its live exporting parent'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_marker_parent';
  END IF;

  IF NEW.marker_kind = 'baseline' THEN
    IF (parent.start_lsn IS NOT NULL AND parent.start_lsn IS DISTINCT FROM NEW.lsn)
       OR (parent.schema_version IS NOT NULL
           AND parent.schema_version IS DISTINCT FROM NEW.schema_version)
       OR parent.first_lsn IS NOT NULL
       OR parent.final_lsn IS NOT NULL
       OR parent.chunk_no <> 0
       OR parent.cursor_pk IS NOT NULL
       OR parent.export_snapshot IS NOT NULL
       OR parent.export_sealed_at IS NOT NULL THEN
      RAISE EXCEPTION 'baseline marker must establish the pristine parent F/schema exactly once'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_baseline_marker_exact';
    END IF;
  ELSIF NEW.marker_kind = 'end' THEN
    IF parent.start_lsn IS NULL
       OR parent.schema_version IS DISTINCT FROM NEW.schema_version
       OR NEW.lsn < parent.start_lsn
       OR parent.final_lsn IS NOT NULL
       OR parent.export_snapshot IS NULL
       OR parent.export_sealed_at IS NULL THEN
      RAISE EXCEPTION 'end marker must exactly bound a sealed parent export'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_end_marker_exact';
    END IF;
  ELSE
    RAISE EXCEPTION 'unknown reload boundary marker kind %', NEW.marker_kind
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_marker_kind';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_marker_evidence
BEFORE INSERT OR UPDATE OR DELETE ON public.walrus_table_reload_marker
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_table_reload_marker_evidence();

CREATE TRIGGER table_reload_marker_truncate_guard
BEFORE TRUNCATE ON public.walrus_table_reload_marker
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_table_reload_marker_evidence();

-- DDL events are permanent ordering evidence: the transformer uses both their source identity and
-- commit LSN to decide exactly where a schema transition belongs in the data history. The normal
-- replay path deliberately executes an exact no-op UPDATE through ON CONFLICT, but neither that
-- path nor an arbitrary SQL client may rewrite any semantic field (including created_at). Removal
-- is reserved for the same explicit, transaction-local maintenance path used by the other durable
-- manifest receipts.
LOCK TABLE public.walrus_ddl_manifest IN SHARE ROW EXCLUSIVE MODE;

CREATE FUNCTION public.walrus_guard_ddl_manifest_semantics()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF TG_OP = 'UPDATE' THEN
    IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
      RAISE EXCEPTION 'DDL manifest history is semantically immutable'
        USING ERRCODE = '23514', CONSTRAINT = 'ddl_manifest_semantics_immutable';
    END IF;
    RETURN NEW;
  END IF;

  IF pg_catalog.current_setting('walrus.manifest_fence_maintenance', true)
       IS DISTINCT FROM '2-delete' THEN
    RAISE EXCEPTION 'DDL manifest history is append-only and cannot be removed with %', TG_OP
      USING ERRCODE = '23514', CONSTRAINT = 'ddl_manifest_removal_guard';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NULL;
END
$$;

CREATE TRIGGER ddl_manifest_semantics_immutable
BEFORE UPDATE ON public.walrus_ddl_manifest
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_ddl_manifest_semantics();

CREATE TRIGGER ddl_manifest_delete_guard
BEFORE DELETE ON public.walrus_ddl_manifest
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_ddl_manifest_semantics();

CREATE TRIGGER ddl_manifest_truncate_guard
BEFORE TRUNCATE ON public.walrus_ddl_manifest
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_ddl_manifest_semantics();
