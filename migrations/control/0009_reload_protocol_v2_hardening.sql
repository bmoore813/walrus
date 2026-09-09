-- Durable transformer publication ownership and recovery state.
--
-- `export_complete` means the exporter made the bounded [F,H] input durable. The transformer must
-- first claim that immutable attempt as `publishing`, fenced by its table-ownership lease, before
-- it may build or publish the hidden DuckLake generation. `complete` is written only after the
-- DuckLake swap has committed and the canonical transformer checkpoint is advanced to H.

ALTER TABLE public.walrus_table_reload
  DROP CONSTRAINT walrus_table_reload_status_check;

ALTER TABLE public.walrus_table_reload
  ADD CONSTRAINT table_reload_status_check
    CHECK (status IN (
      'requested', 'exporting', 'export_complete', 'publishing', 'complete', 'failed'
    )),
  ADD COLUMN publication_nonce uuid,
  ADD COLUMN publisher_owner_pod text,
  ADD COLUMN publisher_fencing_token bigint,
  ADD COLUMN publishing_at timestamptz,
  ADD COLUMN publication_error text,
  ADD COLUMN publication_error_at timestamptz,
  ADD CONSTRAINT table_reload_publishing_identity_check
    CHECK (
      status <> 'publishing'
      OR (
        publication_nonce IS NOT NULL
        AND publisher_owner_pod IS NOT NULL
        AND publisher_fencing_token IS NOT NULL
        AND publishing_at IS NOT NULL
      )
    );

-- Exporter ownership is a fencing token, not just a friendly lease label. Snapshot identifiers
-- are persisted for audit/recovery decisions, but can only be imported while the transaction that
-- exported them is alive; a new generation must supersede a lost snapshot rather than reuse it.
ALTER TABLE public.walrus_table_reload
  ADD COLUMN exporter_generation bigint NOT NULL DEFAULT 0,
  ADD COLUMN export_snapshot text,
  ADD COLUMN export_snapshot_xmin bigint,
  ADD COLUMN export_snapshot_xmax bigint,
  ADD COLUMN export_range_count bigint,
  ADD COLUMN export_sealed_at timestamptz,
  ADD COLUMN export_file_count bigint,
  ADD COLUMN export_row_count bigint,
  ADD CONSTRAINT table_reload_exporter_generation_check CHECK (exporter_generation >= 0),
  ADD CONSTRAINT table_reload_snapshot_shape_check CHECK (
    (export_snapshot IS NULL
      AND export_snapshot_xmin IS NULL
      AND export_snapshot_xmax IS NULL
      AND export_range_count IS NULL)
    OR
    (export_snapshot IS NOT NULL
      AND export_snapshot_xmin IS NOT NULL
      AND export_snapshot_xmax IS NOT NULL
      AND export_snapshot_xmin <= export_snapshot_xmax
      AND export_range_count IS NOT NULL
      AND export_range_count > 0)
  ),
  ADD CONSTRAINT table_reload_export_seal_shape_check CHECK (
    (export_sealed_at IS NULL AND export_file_count IS NULL AND export_row_count IS NULL)
    OR
    (export_sealed_at IS NOT NULL
      AND export_file_count IS NOT NULL AND export_file_count >= 0
      AND export_row_count IS NOT NULL AND export_row_count >= 0)
  ),
  ADD CONSTRAINT table_reload_completed_export_is_sealed_check CHECK (
    status NOT IN ('export_complete', 'publishing', 'complete')
    OR exporter_generation = 0
    OR
    (export_snapshot IS NOT NULL
      AND export_sealed_at IS NOT NULL
      AND start_lsn IS NOT NULL
      AND final_lsn IS NOT NULL
      AND start_lsn <= final_lsn)
  ),
  ADD CONSTRAINT table_reload_v2_complete_identity_check CHECK (
    status <> 'complete'
    OR exporter_generation = 0
    OR (
      publication_nonce IS NOT NULL
      AND publisher_owner_pod IS NOT NULL
      AND publisher_fencing_token IS NOT NULL
      AND publishing_at IS NOT NULL
    )
  );

-- A pre-v2 exporter embeds a claim statement that leaves exporter_generation at zero. Reject its
-- transition, and reject any subsequent generation-zero mutation that tries to remain exporting.
-- A v2 adoption increments the generation before doing further work, so it is admitted.
CREATE FUNCTION public.walrus_guard_reload_exporter_protocol_v2()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status = 'exporting'
     AND (
       NEW.exporter_generation <= 0
       OR NEW.lease_holder IS NULL
       OR NEW.lease_expiry IS NULL
     ) THEN
    RAISE EXCEPTION
      'reload export requires a protocol-v2 exporter fencing generation and lease identity'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_exporter_protocol_v2';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_exporter_protocol_v2
BEFORE INSERT OR UPDATE OF status, exporter_generation, chunk_no, cursor_pk,
                           start_lsn, schema_version, export_snapshot, export_sealed_at,
                           lease_holder, lease_expiry
ON public.walrus_table_reload
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_reload_exporter_protocol_v2();

-- Crossing into `exporting`, or explicitly adopting an already-exporting lease, must mint a
-- strictly newer generation. `UPDATE OF lease_holder` fires even when a pre-v2 adopter writes the
-- same holder value, so a rolling old process cannot reuse a positive generation left behind by a
-- v2 release or adoption. Ordinary v2 renew/progress statements do not assign these acquisition
-- columns; v2 claim/adoption assigns them together with `exporter_generation + 1`.
CREATE FUNCTION public.walrus_guard_reload_exporter_acquisition_v2()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status = 'exporting'
     AND NEW.exporter_generation <= OLD.exporter_generation THEN
    RAISE EXCEPTION
      'claiming or adopting a reload export must mint a newer protocol-v2 fencing generation'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_exporter_acquisition_v2';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_exporter_acquisition_v2
BEFORE UPDATE OF status, exporter_generation, lease_holder
ON public.walrus_table_reload
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_reload_exporter_acquisition_v2();

-- Status is durable protocol evidence, not an editable label. Preserve the one-way state machine
-- while retaining the explicit pristine exporter release (`exporting -> requested`). The legacy
-- `export_complete -> complete` shape is left to the stronger completion trigger below so it
-- reports the dedicated seal/checkpoint violation.
CREATE FUNCTION public.walrus_guard_table_reload_status_transition()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF NOT (
    OLD.status = NEW.status
    OR (OLD.status = 'requested' AND NEW.status IN ('exporting', 'failed'))
    OR (
      OLD.status = 'exporting' AND NEW.status = 'requested'
      AND OLD.start_lsn IS NULL
      AND OLD.first_lsn IS NULL
      AND OLD.final_lsn IS NULL
      AND OLD.schema_version IS NULL
      AND OLD.chunk_no = 0
      AND OLD.cursor_pk IS NULL
      AND OLD.export_snapshot IS NULL
      AND OLD.export_snapshot_xmin IS NULL
      AND OLD.export_snapshot_xmax IS NULL
      AND OLD.export_range_count IS NULL
      AND OLD.export_sealed_at IS NULL
      AND OLD.export_file_count IS NULL
      AND OLD.export_row_count IS NULL
      AND NEW.lease_holder IS NULL
      AND NEW.lease_expiry IS NULL
      AND (to_jsonb(NEW) - ARRAY['status', 'lease_holder', 'lease_expiry', 'updated_at']::text[])
          IS NOT DISTINCT FROM
          (to_jsonb(OLD) - ARRAY['status', 'lease_holder', 'lease_expiry', 'updated_at']::text[])
      AND NOT EXISTS (
        SELECT 1 FROM public.walrus_table_reload_marker AS marker
        WHERE marker.reload_id = OLD.reload_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM public.walrus_table_reload_export_range AS export_range
        WHERE export_range.reload_id = OLD.reload_id
      )
      AND NOT EXISTS (
        SELECT 1 FROM public.walrus_file_manifest AS manifest
        WHERE manifest.reload_id = OLD.reload_id
      )
    )
    OR (OLD.status = 'exporting' AND NEW.status IN ('export_complete', 'failed'))
    OR (OLD.status = 'export_complete' AND NEW.status IN ('publishing', 'complete', 'failed'))
    OR (OLD.status = 'publishing' AND NEW.status = 'complete')
    OR (
      OLD.status = 'publishing' AND NEW.status = 'failed'
      AND NOT EXISTS (
        SELECT 1
        FROM public.walrus_manifest_publication_fence AS seal
        WHERE seal.epoch = OLD.epoch
          AND seal.source_schema = OLD.source_schema
          AND seal.source_table = OLD.source_table
          AND seal.sealed_reload_id = OLD.reload_id
          AND seal.sealed_publication_nonce = OLD.publication_nonce
          AND seal.sealed_through_lsn = OLD.final_lsn
      )
    )
  ) THEN
    RAISE EXCEPTION 'illegal table reload status transition % -> %', OLD.status, NEW.status
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_status_transition';
  END IF;
  IF NEW.status = 'failed'
     AND OLD.status IS DISTINCT FROM 'failed'
     AND EXISTS (
       SELECT 1 FROM public.walrus_file_manifest AS manifest
       WHERE manifest.reload_id = OLD.reload_id
     ) THEN
    RAISE EXCEPTION
      'reloads cannot become terminal while staged manifests remain'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_failure_requires_purge';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER table_reload_status_transition
BEFORE UPDATE OF status ON public.walrus_table_reload
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_table_reload_status_transition();

-- SQL is embedded in binaries, so a still-running or buggy transformer could otherwise bypass the
-- publication protocol with a direct status update. Entering `complete` is a database-attested
-- transition: the exact reload receipt must be publishing, its durable manifest seal must be at
-- H, and both canonical checkpoints must already equal H. The seal/checkpoint tables are created
-- later in this migration; PL/pgSQL resolves them when the trigger first executes, after the
-- migration transaction has committed.
CREATE FUNCTION public.walrus_guard_reload_v2_completion()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.status <> 'complete' THEN
    RETURN NEW;
  END IF;
  IF TG_OP = 'INSERT' THEN
    RAISE EXCEPTION 'reloads cannot be inserted directly in complete state'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_v2_completion_guard';
  END IF;
  IF OLD.status = 'complete' THEN
    RETURN NEW;
  END IF;
  IF OLD.status <> 'publishing'
     OR NEW.exporter_generation <= 0
     OR ROW(
       NEW.reload_id, NEW.epoch, NEW.source_schema, NEW.source_table,
       NEW.start_lsn, NEW.final_lsn, NEW.schema_version, NEW.publication_nonce,
       NEW.publisher_owner_pod, NEW.publisher_fencing_token, NEW.publishing_at
     ) IS DISTINCT FROM ROW(
       OLD.reload_id, OLD.epoch, OLD.source_schema, OLD.source_table,
       OLD.start_lsn, OLD.final_lsn, OLD.schema_version, OLD.publication_nonce,
       OLD.publisher_owner_pod, OLD.publisher_fencing_token, OLD.publishing_at
     ) THEN
    RAISE EXCEPTION
      'reload completion requires an unchanged protocol-v2 publishing receipt'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_v2_completion_guard';
  END IF;

  PERFORM 1
  FROM public.walrus_manifest_publication_fence AS seal
  JOIN public.walrus_transformer_checkpoint AS checkpoint
    ON checkpoint.epoch = NEW.epoch
   AND checkpoint.source_schema = NEW.source_schema
   AND checkpoint.source_table = NEW.source_table
  WHERE seal.epoch = NEW.epoch
    AND seal.source_schema = NEW.source_schema
    AND seal.source_table = NEW.source_table
    AND seal.sealed_reload_id = NEW.reload_id
    AND seal.sealed_publication_nonce = NEW.publication_nonce
    AND seal.sealed_through_lsn = NEW.final_lsn
    AND checkpoint.raw_appended_lsn = NEW.final_lsn
    AND checkpoint.transformed_lsn = NEW.final_lsn
  FOR SHARE OF seal, checkpoint;
  IF NOT FOUND THEN
    RAISE EXCEPTION
      'reload completion requires its exact durable seal and H checkpoints'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_v2_completion_guard';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER table_reload_v2_completion_guard
BEFORE INSERT OR UPDATE OF status ON public.walrus_table_reload
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_reload_v2_completion();

CREATE TABLE public.walrus_table_reload_export_range (
  reload_id            bigint NOT NULL
                       REFERENCES public.walrus_table_reload(reload_id) ON DELETE CASCADE,
  exporter_generation  bigint NOT NULL CHECK (exporter_generation > 0),
  range_no             bigint NOT NULL CHECK (range_no >= 0),
  full_scan            boolean NOT NULL,
  start_block          bigint,
  end_block            bigint,
  status               text NOT NULL DEFAULT 'planned'
                       CHECK (status IN ('planned', 'complete')),
  file_count           bigint,
  row_count            bigint,
  planned_at           timestamptz NOT NULL DEFAULT now(),
  completed_at         timestamptz,
  PRIMARY KEY (reload_id, range_no),
  CHECK (
    (full_scan AND start_block IS NULL AND end_block IS NULL)
    OR
    (NOT full_scan
      AND start_block IS NOT NULL AND start_block >= 0
      AND (end_block IS NULL OR end_block > start_block))
  ),
  CHECK (
    (status = 'planned' AND file_count IS NULL AND row_count IS NULL AND completed_at IS NULL)
    OR
    (status = 'complete'
      AND file_count IS NOT NULL AND file_count >= 0
      AND row_count IS NOT NULL AND row_count >= 0
      AND completed_at IS NOT NULL)
  )
);

CREATE INDEX walrus_table_reload_export_range_generation_idx
  ON public.walrus_table_reload_export_range (reload_id, exporter_generation, status, range_no);

-- The physical COPY partition is a durable receipt, not scheduler scratch space. The exporter may
-- insert the frozen plan once and turn each row from `planned` into one exact `complete` receipt;
-- every plan coordinate and every completed count is immutable. Administrative teardown uses the
-- same explicit maintenance tripwire as the other append-only publication evidence.
CREATE FUNCTION public.walrus_guard_reload_export_range_semantics()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP IN ('DELETE', 'TRUNCATE') THEN
    IF current_setting('walrus.manifest_fence_maintenance', true) IS DISTINCT FROM '2-delete' THEN
      RAISE EXCEPTION 'reload export range receipts are append-only durable protocol evidence'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_export_range_removal';
    END IF;
    IF TG_OP = 'DELETE' THEN
      RETURN OLD;
    END IF;
    RETURN NULL;
  END IF;

  IF TG_OP = 'INSERT' THEN
    IF current_setting('walrus.reload_export_plan_protocol', true) IS DISTINCT FROM '2' THEN
      RAISE EXCEPTION 'reload export ranges may be inserted only by the live frozen-plan path'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_export_range_insert_protocol';
    END IF;
    PERFORM 1
    FROM public.walrus_table_reload AS reload
    WHERE reload.reload_id = NEW.reload_id
      AND reload.status = 'exporting'
      AND reload.exporter_generation = NEW.exporter_generation
      AND reload.export_snapshot IS NOT NULL
      AND reload.export_sealed_at IS NULL
      AND NEW.range_no < reload.export_range_count
    FOR UPDATE;
    IF NOT FOUND THEN
      RAISE EXCEPTION 'reload export ranges may be inserted only by the live frozen-plan path'
        USING ERRCODE = '23514', CONSTRAINT = 'table_reload_export_range_insert_protocol';
    END IF;
    RETURN NEW;
  END IF;

  IF ROW(
       NEW.reload_id, NEW.exporter_generation, NEW.range_no, NEW.full_scan,
       NEW.start_block, NEW.end_block, NEW.planned_at
     ) IS DISTINCT FROM ROW(
       OLD.reload_id, OLD.exporter_generation, OLD.range_no, OLD.full_scan,
       OLD.start_block, OLD.end_block, OLD.planned_at
     ) THEN
    RAISE EXCEPTION 'reload export range plan semantics are immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_export_range_semantics_immutable';
  END IF;

  IF ROW(NEW.status, NEW.file_count, NEW.row_count, NEW.completed_at)
       IS NOT DISTINCT FROM
     ROW(OLD.status, OLD.file_count, OLD.row_count, OLD.completed_at) THEN
    RETURN NEW;
  END IF;

  IF OLD.status = 'planned' AND NEW.status = 'complete'
     AND current_setting('walrus.reload_export_plan_protocol', true) = '2' THEN
    PERFORM 1
    FROM public.walrus_table_reload AS reload
    WHERE reload.reload_id = OLD.reload_id
      AND reload.status = 'exporting'
      AND reload.exporter_generation = OLD.exporter_generation
      AND reload.export_snapshot IS NOT NULL
      AND reload.export_sealed_at IS NULL
    FOR UPDATE;
    IF FOUND THEN
      RETURN NEW;
    END IF;
  END IF;

  RAISE EXCEPTION 'illegal reload export range receipt transition'
    USING ERRCODE = '23514', CONSTRAINT = 'table_reload_export_range_status_transition';
END $$;

CREATE TRIGGER table_reload_export_range_semantics
BEFORE INSERT OR UPDATE OR DELETE ON public.walrus_table_reload_export_range
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_reload_export_range_semantics();

CREATE TRIGGER table_reload_export_range_truncate_guard
BEFORE TRUNCATE ON public.walrus_table_reload_export_range
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_reload_export_range_semantics();

-- A queued source request must not enter `exporting` while the transformer is reconciling or recovering
-- publication of its predecessor. Terminal rows leave this fence as before.
DROP INDEX public.walrus_table_reload_one_live;
CREATE UNIQUE INDEX walrus_table_reload_one_live
  ON public.walrus_table_reload (epoch, source_schema, source_table)
  WHERE status IN ('exporting', 'export_complete', 'publishing')
     OR (status = 'requested' AND source_request_id IS NULL);

-- This is a deliberately coordinated upgrade. Older producers did not persist an object digest,
-- and inventing one would turn corruption detection into theatre. Refuse to migrate a non-empty
-- work queue; operators must drain it with the old binaries before deploying protocol v2.
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM public.walrus_file_manifest) THEN
    RAISE EXCEPTION
      'protocol-v2 migration requires public.walrus_file_manifest to be empty; drain the old queue first';
  END IF;
  IF EXISTS (
    SELECT 1 FROM public.walrus_table_reload
    WHERE status NOT IN ('complete', 'failed')
  ) THEN
    RAISE EXCEPTION
      'protocol-v2 migration requires every pre-upgrade reload to be complete or failed';
  END IF;
  IF EXISTS (
    SELECT 1
    FROM public.walrus_table_reload AS reload
    LEFT JOIN public.walrus_transformer_checkpoint AS checkpoint
      ON checkpoint.epoch = reload.epoch
     AND checkpoint.source_schema = reload.source_schema
     AND checkpoint.source_table = reload.source_table
    WHERE reload.status = 'complete'
      AND (
        reload.final_lsn IS NULL
        OR checkpoint.epoch IS NULL
        OR checkpoint.raw_appended_lsn < reload.final_lsn
        OR checkpoint.transformed_lsn < reload.final_lsn
      )
  ) THEN
    RAISE EXCEPTION
      'protocol-v2 migration requires every completed legacy reload to have H and checkpoints through H';
  END IF;
END
$$;

ALTER TABLE public.walrus_file_manifest
  ADD COLUMN object_size bigint NOT NULL,
  ADD COLUMN sha256 bytea NOT NULL,
  ADD COLUMN stream_group_id bigint,
  ADD COLUMN stream_group_ordinal bigint,
  ADD CONSTRAINT file_manifest_kind_check
    CHECK (kind IN ('snapshot', 'stream', 'spill', 'reload')),
  ADD CONSTRAINT file_manifest_status_check
    CHECK (status IN ('ready', 'failed')),
  ADD CONSTRAINT file_manifest_row_count_check CHECK (row_count > 0),
  ADD CONSTRAINT file_manifest_object_size_check CHECK (object_size > 0),
  ADD CONSTRAINT file_manifest_sha256_check CHECK (octet_length(sha256) = 32),
  ADD CONSTRAINT file_manifest_lsn_range_check CHECK (lsn_start <= lsn_end),
  ADD CONSTRAINT file_manifest_reload_identity_check
    CHECK ((kind = 'reload') = (reload_id IS NOT NULL)),
  ADD CONSTRAINT file_manifest_stream_group_shape_check
    CHECK (
      (kind <> 'spill' AND stream_group_id IS NULL AND stream_group_ordinal IS NULL)
      OR
      (
        stream_group_id IS NOT NULL
        AND stream_group_ordinal IS NOT NULL
        AND stream_group_ordinal >= 0
        AND kind IN ('stream', 'spill')
        AND reload_id IS NULL
      )
    ),
  ADD CONSTRAINT file_manifest_s3_uri_unique UNIQUE (s3_uri),
  ADD CONSTRAINT file_manifest_group_ordinal_unique
    UNIQUE (stream_group_id, stream_group_ordinal);

-- Publication drain/seal inspects every status, not only the ready rows covered by the legacy claim
-- index. Keeping ungrouped ranges together makes both the <= H drain and the straddler attestation
-- bounded by this table's live queue slice.
CREATE INDEX walrus_file_manifest_ungrouped_range_idx
  ON public.walrus_file_manifest
    (epoch, source_schema, source_table, lsn_end, lsn_start, id)
  WHERE stream_group_id IS NULL;

-- A reload object is meaningful only for the exact table/epoch attempt that exported it. A
-- reload_id-only reference would allow an incorrectly labelled object to pass the export seal and
-- later be routed into another table. Ordinary stream rows keep reload_id NULL and therefore do
-- not participate in this composite FK.
ALTER TABLE public.walrus_table_reload
  ADD CONSTRAINT table_reload_manifest_identity_unique
    UNIQUE (reload_id, epoch, source_schema, source_table);

ALTER TABLE public.walrus_file_manifest
  ADD CONSTRAINT file_manifest_reload_identity_fk
    FOREIGN KEY (reload_id, epoch, source_schema, source_table)
    REFERENCES public.walrus_table_reload (reload_id, epoch, source_schema, source_table)
    ON DELETE RESTRICT;

-- Cross-row timing cannot be expressed as a CHECK/FK: reload objects may be inserted only while
-- the exact attempt's exported snapshot is live and unsealed. Taking a FOR UPDATE row lock
-- serializes each manifest validation with fail/seal/complete while still allowing parallel
-- workers to do their remote upload work concurrently. The seal takes the same explicit lock in a
-- first statement, then validates manifests under the next READ COMMITTED snapshot, so it both
-- waits for these inserts and prevents later ones.
-- The trigger serializes the insert with fail/seal/complete, closing the otherwise possible
-- "late object after seal" window. Status-only integrity updates do not invoke this identity
-- trigger.
CREATE FUNCTION public.walrus_guard_reload_manifest_identity()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF NEW.reload_id IS NULL THEN
    RETURN NEW;
  END IF;

  PERFORM 1
  FROM public.walrus_table_reload AS reload
  WHERE reload.reload_id = NEW.reload_id
    AND reload.epoch = NEW.epoch
    AND reload.source_schema = NEW.source_schema
    AND reload.source_table = NEW.source_table
    AND reload.status = 'exporting'
    AND reload.start_lsn = NEW.lsn_start
    AND NEW.lsn_end = NEW.lsn_start
    AND reload.schema_version = NEW.schema_version
    AND reload.export_snapshot IS NOT NULL
    AND reload.export_sealed_at IS NULL
  FOR UPDATE;

  IF NOT FOUND THEN
    RAISE EXCEPTION
      'reload manifest does not belong to a live, planned, unsealed export attempt'
      USING ERRCODE = '23514', CONSTRAINT = 'file_manifest_reload_attempt_guard';
  END IF;
  RETURN NEW;
END
$$;

CREATE TRIGGER file_manifest_reload_attempt_guard
BEFORE INSERT OR UPDATE OF reload_id, epoch, source_schema, source_table,
                           kind, lsn_start, lsn_end, schema_version
ON public.walrus_file_manifest
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_reload_manifest_identity();

-- Old transformers embed an unconditional `DELETE FROM public.walrus_file_manifest WHERE id = ANY(...)`. Make that
-- statement fail after the coordinated upgrade. Every v2 retirement path opts in transaction-
-- locally; this is a rollout compatibility fence, not an authorization boundary.
CREATE FUNCTION public.walrus_guard_manifest_delete_protocol_v2()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF current_setting('walrus.manifest_delete_protocol', true) IS DISTINCT FROM '2' THEN
    RAISE EXCEPTION
      'file manifests may be retired only by the protocol-v2 grouped/fenced path'
      USING ERRCODE = '23514', CONSTRAINT = 'file_manifest_delete_protocol_v2';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NULL;
END
$$;

CREATE TRIGGER file_manifest_delete_protocol_v2
BEFORE DELETE ON public.walrus_file_manifest
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_manifest_delete_protocol_v2();

CREATE TRIGGER file_manifest_truncate_protocol_v2
BEFORE TRUNCATE ON public.walrus_file_manifest
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_manifest_delete_protocol_v2();

-- A claimed manifest is an object attestation, not a mutable queue hint. The transformer releases the
-- control transaction while downloading and validating the object, so changing any semantic
-- field between claim and retirement could otherwise make an id-only delete acknowledge bytes
-- different from the ones it appended. Integrity handling may move `ready -> failed`; failed is
-- terminal so old work cannot be resurrected below a later reload seal. Every other column,
-- including identity and timestamps, is immutable.
CREATE FUNCTION public.walrus_guard_file_manifest_semantics()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF (to_jsonb(NEW) - 'status') IS DISTINCT FROM (to_jsonb(OLD) - 'status') THEN
    RAISE EXCEPTION 'file manifest semantics are immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'file_manifest_semantics_immutable';
  END IF;
  IF OLD.status = 'failed' AND NEW.status IS DISTINCT FROM 'failed' THEN
    RAISE EXCEPTION 'failed file manifest status is terminal'
      USING ERRCODE = '23514', CONSTRAINT = 'file_manifest_status_transition';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER file_manifest_semantics_immutable
BEFORE UPDATE ON public.walrus_file_manifest
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_file_manifest_semantics();

-- Publishers and reload cutover serialize on this table-local row. Once a reload proves its
-- manifest prefix through H is empty, the durable seal prevents a source commit at or below H
-- from becoming visible after the Duck generation has been swapped. The row remains for the slot
-- epoch so later commits can be admitted only beyond the greatest completed seal.
--
-- Give completed pre-upgrade reloads a durable receipt identity before constructing their seals.
-- The rollout precondition above proved that each has H and checkpoints through H. Backfilling the
-- greatest completed H per table closes the lost-source-ACK replay window across the upgrade.
UPDATE public.walrus_table_reload
SET publication_nonce = gen_random_uuid(),
    publisher_owner_pod = 'protocol-v2-migration',
    publisher_fencing_token = 0,
    publishing_at = COALESCE(updated_at, now())
WHERE status = 'complete' AND publication_nonce IS NULL;

ALTER TABLE public.walrus_table_reload
  DROP CONSTRAINT table_reload_v2_complete_identity_check,
  ADD CONSTRAINT table_reload_v2_complete_identity_check CHECK (
    status <> 'complete'
    OR (
      publication_nonce IS NOT NULL
      AND publisher_owner_pod IS NOT NULL
      AND publisher_fencing_token IS NOT NULL
      AND publishing_at IS NOT NULL
    )
  );

ALTER TABLE public.walrus_table_reload
  ADD CONSTRAINT table_reload_publication_seal_identity_unique
  UNIQUE (
    reload_id, epoch, source_schema, source_table, final_lsn, publication_nonce
  );

CREATE TABLE public.walrus_manifest_publication_fence (
  epoch                    bigint NOT NULL,
  source_schema            text NOT NULL,
  source_table             text NOT NULL,
  sealed_through_lsn       pg_lsn,
  sealed_reload_id         bigint,
  sealed_publication_nonce uuid,
  updated_at               timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table),
  CONSTRAINT manifest_publication_fence_reload_identity_fk
    FOREIGN KEY (
      sealed_reload_id, epoch, source_schema, source_table,
      sealed_through_lsn, sealed_publication_nonce
    )
    REFERENCES public.walrus_table_reload (
      reload_id, epoch, source_schema, source_table, final_lsn, publication_nonce
    )
    ON DELETE RESTRICT,
  CHECK (
    (sealed_through_lsn IS NULL
      AND sealed_reload_id IS NULL
      AND sealed_publication_nonce IS NULL)
    OR
    (sealed_through_lsn IS NOT NULL
      AND sealed_reload_id IS NOT NULL
      AND sealed_publication_nonce IS NOT NULL)
  )
);

-- Advisory locks put every normal multi-table publisher in deterministic table-key order before
-- it touches fence rows. Row triggers cannot block safely when a direct bulk statement chooses the
-- reverse order, so they take the same key with try-lock and raise retryable serialization failure
-- instead of entering a database deadlock cycle. Hash collisions only add harmless serialization.
CREATE FUNCTION public.walrus_manifest_publication_lock_key(bigint, text, text)
RETURNS bigint
LANGUAGE sql IMMUTABLE STRICT PARALLEL SAFE
AS $$
  SELECT hashtextextended(
    $1::text || ':' || octet_length($2)::text || ':' || $2
      || ':' || octet_length($3)::text || ':' || $3,
    1469598103934665603
  )
$$;

CREATE FUNCTION public.walrus_try_manifest_publication_lock(bigint, text, text)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  IF NOT pg_try_advisory_xact_lock(public.walrus_manifest_publication_lock_key($1, $2, $3)) THEN
    RAISE EXCEPTION
      'manifest publication table lock is busy; retry the transaction in canonical table order'
      USING ERRCODE = '40001';
  END IF;
END
$$;

CREATE FUNCTION public.walrus_guard_manifest_publication_fence_monotonic()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF TG_OP = 'UPDATE' AND (
       ROW(NEW.epoch, NEW.source_schema, NEW.source_table)
         IS DISTINCT FROM ROW(OLD.epoch, OLD.source_schema, OLD.source_table)
       OR (OLD.sealed_through_lsn IS NOT NULL
           AND (NEW.sealed_through_lsn IS NULL
                OR NEW.sealed_through_lsn < OLD.sealed_through_lsn))
     ) THEN
    RAISE EXCEPTION 'manifest publication fence identity/seal is immutable and monotonic'
      USING ERRCODE = '23514', CONSTRAINT = 'manifest_publication_fence_monotonic';
  END IF;

  -- A normal end-fence force-flush prevents one ordinary file from spanning H, but the durable
  -- cutover must attest that invariant itself. Otherwise a buggy/direct publisher could leave a
  -- pre-existing ungrouped file invisible to the <= H drain and make it claimable only after swap.
  IF NEW.sealed_through_lsn IS NOT NULL
     AND (
       TG_OP = 'INSERT'
       OR (
         TG_OP = 'UPDATE'
         AND NEW.sealed_through_lsn IS DISTINCT FROM OLD.sealed_through_lsn
       )
     )
     AND EXISTS (
       SELECT 1
       FROM public.walrus_file_manifest AS manifest
       WHERE manifest.epoch = NEW.epoch
         AND manifest.source_schema = NEW.source_schema
         AND manifest.source_table = NEW.source_table
         AND manifest.stream_group_id IS NULL
         AND manifest.lsn_start <= NEW.sealed_through_lsn
         AND manifest.lsn_end > NEW.sealed_through_lsn
     ) THEN
    RAISE EXCEPTION
      'cannot seal manifest publication while an ungrouped manifest straddles the boundary'
      USING ERRCODE = '23514', CONSTRAINT = 'manifest_publication_fence_ungrouped_straddler';
  END IF;

  IF (
       (TG_OP = 'INSERT' AND NEW.sealed_through_lsn IS NOT NULL)
       OR
       (TG_OP = 'UPDATE' AND ROW(
          NEW.sealed_through_lsn, NEW.sealed_reload_id, NEW.sealed_publication_nonce
        ) IS DISTINCT FROM ROW(
          OLD.sealed_through_lsn, OLD.sealed_reload_id, OLD.sealed_publication_nonce
        ))
     )
     AND current_setting('walrus.manifest_seal_protocol', true) IS DISTINCT FROM '2' THEN
    RAISE EXCEPTION 'manifest publication seals may be set only by the protocol-v2 seal path'
      USING ERRCODE = '23514', CONSTRAINT = 'manifest_publication_seal_protocol_v2';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER manifest_publication_fence_monotonic
BEFORE INSERT OR UPDATE ON public.walrus_manifest_publication_fence
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_manifest_publication_fence_monotonic();

CREATE FUNCTION public.walrus_guard_manifest_publication_fence_removal()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('walrus.manifest_fence_maintenance', true) IS DISTINCT FROM '2-delete' THEN
    RAISE EXCEPTION 'manifest publication fences are append-only durable protocol receipts'
      USING ERRCODE = '23514', CONSTRAINT = 'manifest_publication_fence_removal';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NULL;
END $$;

CREATE TRIGGER manifest_publication_fence_delete_guard
BEFORE DELETE ON public.walrus_manifest_publication_fence
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_manifest_publication_fence_removal();

CREATE TRIGGER manifest_publication_fence_truncate_guard
BEFORE TRUNCATE ON public.walrus_manifest_publication_fence
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_manifest_publication_fence_removal();

-- Preserve the strongest legacy H for every table. The write GUC is transaction-local and exists
-- solely as an explicit protocol tripwire; the composite FK below remains the relational proof.
WITH authorized AS MATERIALIZED (
  SELECT set_config('walrus.manifest_seal_protocol', '2', true) AS protocol
), legacy_seals AS MATERIALIZED (
  SELECT DISTINCT ON (epoch, source_schema, source_table)
         reload_id, epoch, source_schema, source_table, final_lsn, publication_nonce
  FROM public.walrus_table_reload
  WHERE status = 'complete'
  ORDER BY epoch, source_schema, source_table, final_lsn DESC, reload_id DESC
)
INSERT INTO public.walrus_manifest_publication_fence (
  epoch, source_schema, source_table,
  sealed_through_lsn, sealed_reload_id, sealed_publication_nonce
)
SELECT legacy.epoch, legacy.source_schema, legacy.source_table,
       legacy.final_lsn, legacy.reload_id, legacy.publication_nonce
FROM legacy_seals AS legacy
CROSS JOIN authorized
WHERE authorized.protocol = '2';

SELECT set_config('walrus.manifest_seal_protocol', '', true);

-- Once claimed, the reload publication identity is a permanent lost-ACK receipt. Status/error
-- fields may advance, but neither a later seal nor a newer reload may rewrite the completed proof.
CREATE FUNCTION public.walrus_guard_reload_publication_identity_immutable()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF OLD.publication_nonce IS NOT NULL AND ROW(
       NEW.epoch, NEW.source_schema, NEW.source_table, NEW.start_lsn, NEW.final_lsn,
       NEW.schema_version, NEW.publication_nonce, NEW.publishing_at
     ) IS DISTINCT FROM ROW(
       OLD.epoch, OLD.source_schema, OLD.source_table, OLD.start_lsn, OLD.final_lsn,
       OLD.schema_version, OLD.publication_nonce, OLD.publishing_at
     ) THEN
    RAISE EXCEPTION 'reload publication identity is immutable once claimed'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_publication_identity_immutable';
  END IF;
  IF OLD.publication_nonce IS NOT NULL
     AND ROW(NEW.publisher_owner_pod, NEW.publisher_fencing_token)
       IS DISTINCT FROM ROW(OLD.publisher_owner_pod, OLD.publisher_fencing_token)
     AND (
       OLD.status <> 'publishing'
       OR NEW.status <> 'publishing'
       OR current_setting('walrus.reload_publication_adopt_protocol', true)
          IS DISTINCT FROM '2'
     ) THEN
    RAISE EXCEPTION 'reload publication ownership may rotate only through fenced adoption'
      USING ERRCODE = '23514', CONSTRAINT = 'table_reload_publication_owner_transition';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER table_reload_publication_identity_immutable
BEFORE UPDATE ON public.walrus_table_reload
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_reload_publication_identity_immutable();

-- Ungrouped stream/snapshot writers use `insert_ready`, outside the multi-table StreamCommit
-- publisher. Enforce the same fence in the database so no caller can bypass cutover serialization.
-- Reload chunks are governed by their attempt/status trigger and are deliberately exempt.
CREATE FUNCTION public.walrus_guard_file_manifest_publication_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  sealed_lsn pg_lsn;
BEGIN
  IF NEW.kind = 'reload' THEN
    RETURN NEW;
  END IF;

  PERFORM public.walrus_try_manifest_publication_lock(
    NEW.epoch, NEW.source_schema, NEW.source_table
  );

  INSERT INTO public.walrus_manifest_publication_fence AS fence
    (epoch, source_schema, source_table)
  VALUES (NEW.epoch, NEW.source_schema, NEW.source_table)
  ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
    SET updated_at = fence.updated_at
  RETURNING sealed_through_lsn INTO sealed_lsn;

  IF sealed_lsn IS NOT NULL
     AND (
       NEW.lsn_end <= sealed_lsn
       OR (NEW.stream_group_id IS NULL AND NEW.lsn_start <= sealed_lsn)
     ) THEN
    RAISE EXCEPTION
      'manifest LSN range [% - %] overlaps durable reload seal % for %.%',
      NEW.lsn_start, NEW.lsn_end, sealed_lsn, NEW.source_schema, NEW.source_table
      USING ERRCODE = '23514', CONSTRAINT = 'file_manifest_publication_sealed';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER file_manifest_publication_fence
BEFORE INSERT ON public.walrus_file_manifest
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_file_manifest_publication_fence();

-- One durable receipt for each protocol-v2 commit publication (a streamed transaction, or an
-- ordinary structural DDL-only transaction). Keeping the receipt after its work has been consumed
-- makes replay after "publish committed, source ACK lost" idempotent for the slot epoch.
CREATE TABLE public.walrus_stream_txn_publication (
  id          bigserial PRIMARY KEY,
  epoch       bigint NOT NULL,
  top_xid     bigint NOT NULL CHECK (top_xid BETWEEN 0 AND 4294967295),
  commit_lsn  pg_lsn NOT NULL,
  commit_ts   text NOT NULL,
  created_at  timestamptz NOT NULL DEFAULT now(),
  -- A PostgreSQL commit record has one top-level xid. Keying the receipt by the WAL identity makes
  -- a replay that changes top_xid a semantic conflict instead of a second accepted publication.
  UNIQUE (epoch, commit_lsn),
  UNIQUE (id, epoch, top_xid, commit_lsn, commit_ts)
);

CREATE FUNCTION public.walrus_guard_stream_txn_publication_semantics()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD) THEN
    RAISE EXCEPTION 'stream transaction publication semantics are immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'stream_txn_publication_semantics_immutable';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER stream_txn_publication_semantics_immutable
BEFORE UPDATE ON public.walrus_stream_txn_publication
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_stream_txn_publication_semantics();

-- A streamed transaction can produce several Parquet objects for one table. The transformer must
-- append that complete per-table set in one DuckLake transaction; an arbitrary claim LIMIT may
-- never expose only a prefix of it.
CREATE TABLE public.walrus_stream_manifest_group (
  id              bigserial PRIMARY KEY,
  publication_id  bigint NOT NULL,
  epoch           bigint NOT NULL,
  top_xid         bigint NOT NULL CHECK (top_xid BETWEEN 0 AND 4294967295),
  source_schema   text NOT NULL,
  source_table    text NOT NULL,
  commit_lsn      pg_lsn NOT NULL,
  commit_ts       text NOT NULL,
  expected_files  bigint NOT NULL CHECK (expected_files >= 0),
  row_count       bigint NOT NULL CHECK (row_count >= 0),
  -- Final structural shape reached by this table in the streamed transaction. This can be newer
  -- than every child file when the transaction's last operation for the table is DDL.
  final_schema_version bigint NOT NULL CHECK (final_schema_version > 0),
  -- Stable semantic identity retained after randomized objects/child queue rows are gone. The
  -- producer sorts this array and excludes s3_uri/object bytes deliberately: replay must prove the
  -- same logical files, not reproduce a random object key.
  file_shape      jsonb NOT NULL CHECK (jsonb_typeof(file_shape) = 'array'),
  status          text NOT NULL DEFAULT 'ready'
                  CHECK (status IN ('ready', 'applied', 'failed', 'superseded')),
  created_at      timestamptz NOT NULL DEFAULT now(),
  applied_at      timestamptz,
  UNIQUE (publication_id, source_schema, source_table),
  UNIQUE (id, epoch, source_schema, source_table, commit_lsn),
  CONSTRAINT stream_manifest_group_publication_identity_fk
    FOREIGN KEY (publication_id, epoch, top_xid, commit_lsn, commit_ts)
    REFERENCES public.walrus_stream_txn_publication (id, epoch, top_xid, commit_lsn, commit_ts)
    ON DELETE RESTRICT,
  CHECK ((expected_files = 0) = (row_count = 0)),
  CHECK (jsonb_array_length(file_shape) = expected_files),
  CHECK ((status IN ('applied', 'superseded')) = (applied_at IS NOT NULL))
);

ALTER TABLE public.walrus_file_manifest
  ADD CONSTRAINT file_manifest_stream_group_fk
  FOREIGN KEY (stream_group_id, epoch, source_schema, source_table, lsn_end)
  REFERENCES public.walrus_stream_manifest_group
    (id, epoch, source_schema, source_table, commit_lsn)
  ON DELETE RESTRICT;

CREATE INDEX walrus_stream_manifest_group_ready_idx
  ON public.walrus_stream_manifest_group (epoch, source_schema, source_table, commit_lsn, id)
  WHERE status = 'ready';

-- Group semantics are an epoch-long replay receipt. Claims may release their read snapshot while
-- DuckDB appends, so no concurrent writer may raise/lower the schema barrier or rewrite the file
-- shape before the final locked retirement validates it.
CREATE OR REPLACE FUNCTION public.walrus_guard_stream_manifest_group_semantics()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF ROW(
       NEW.publication_id, NEW.epoch, NEW.top_xid, NEW.source_schema, NEW.source_table,
       NEW.commit_lsn, NEW.commit_ts, NEW.expected_files, NEW.row_count,
       NEW.final_schema_version, NEW.file_shape, NEW.created_at
     ) IS DISTINCT FROM ROW(
       OLD.publication_id, OLD.epoch, OLD.top_xid, OLD.source_schema, OLD.source_table,
       OLD.commit_lsn, OLD.commit_ts, OLD.expected_files, OLD.row_count,
       OLD.final_schema_version, OLD.file_shape, OLD.created_at
     ) THEN
    RAISE EXCEPTION 'stream manifest group semantics are immutable'
      USING ERRCODE = '23514', CONSTRAINT = 'stream_manifest_group_semantics_immutable';
  END IF;

  IF NOT (
    (OLD.status = NEW.status AND OLD.applied_at IS NOT DISTINCT FROM NEW.applied_at)
    OR (OLD.status = 'ready' AND NEW.status = 'failed'
        AND OLD.applied_at IS NULL AND NEW.applied_at IS NULL)
    OR (OLD.status = 'ready' AND NEW.status IN ('applied', 'superseded')
        AND OLD.applied_at IS NULL AND NEW.applied_at IS NOT NULL)
    OR (OLD.status = 'failed' AND NEW.status = 'superseded'
        AND OLD.applied_at IS NULL AND NEW.applied_at IS NOT NULL)
  ) THEN
    RAISE EXCEPTION 'illegal stream manifest group status/applied_at transition'
      USING ERRCODE = '23514', CONSTRAINT = 'stream_manifest_group_status_transition';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER stream_manifest_group_semantics_immutable
BEFORE UPDATE ON public.walrus_stream_manifest_group
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_stream_manifest_group_semantics();

-- Group parents (including zero-child schema barriers) are themselves ordered manifest work and
-- therefore must respect the same durable table seal even when no file trigger can fire.
CREATE FUNCTION public.walrus_guard_stream_manifest_group_publication_fence()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  sealed_lsn pg_lsn;
BEGIN
  -- Reactivation is rejected by the state-machine trigger as well, but retaining the publication
  -- fence on every attempted transition into ready makes a future relaxation fail closed below H.
  IF TG_OP = 'UPDATE'
     AND NOT (NEW.status = 'ready' AND OLD.status IS DISTINCT FROM 'ready') THEN
    RETURN NEW;
  END IF;

  PERFORM public.walrus_try_manifest_publication_lock(
    NEW.epoch, NEW.source_schema, NEW.source_table
  );

  INSERT INTO public.walrus_manifest_publication_fence AS fence
    (epoch, source_schema, source_table)
  VALUES (NEW.epoch, NEW.source_schema, NEW.source_table)
  ON CONFLICT (epoch, source_schema, source_table) DO UPDATE
    SET updated_at = fence.updated_at
  RETURNING sealed_through_lsn INTO sealed_lsn;

  IF sealed_lsn IS NOT NULL AND NEW.commit_lsn <= sealed_lsn THEN
    RAISE EXCEPTION
      'manifest group commit % is at or below durable reload seal % for %.%',
      NEW.commit_lsn, sealed_lsn, NEW.source_schema, NEW.source_table
      USING ERRCODE = '23514', CONSTRAINT = 'stream_manifest_group_publication_sealed';
  END IF;
  RETURN NEW;
END $$;

CREATE TRIGGER stream_manifest_group_publication_fence
BEFORE INSERT OR UPDATE OF status ON public.walrus_stream_manifest_group
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_stream_manifest_group_publication_fence();

-- Publication and group parents outlive their queue children as epoch-long replay receipts. The
-- normal protocol never removes them. Tests/explicit administrative teardown must opt in with the
-- same transaction-local maintenance tripwire used for table seals and delete children/groups in
-- foreign-key order.
CREATE FUNCTION public.walrus_guard_manifest_publication_receipt_removal()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  IF current_setting('walrus.manifest_fence_maintenance', true) IS DISTINCT FROM '2-delete' THEN
    RAISE EXCEPTION 'manifest publication receipts are append-only durable protocol evidence'
      USING ERRCODE = '23514', CONSTRAINT = 'manifest_publication_receipt_removal';
  END IF;
  IF TG_OP = 'DELETE' THEN
    RETURN OLD;
  END IF;
  RETURN NULL;
END $$;

CREATE TRIGGER stream_manifest_group_delete_guard
BEFORE DELETE ON public.walrus_stream_manifest_group
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_manifest_publication_receipt_removal();

CREATE TRIGGER stream_manifest_group_truncate_guard
BEFORE TRUNCATE ON public.walrus_stream_manifest_group
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_manifest_publication_receipt_removal();

CREATE TRIGGER stream_txn_publication_delete_guard
BEFORE DELETE ON public.walrus_stream_txn_publication
FOR EACH ROW EXECUTE FUNCTION public.walrus_guard_manifest_publication_receipt_removal();

CREATE TRIGGER stream_txn_publication_truncate_guard
BEFORE TRUNCATE ON public.walrus_stream_txn_publication
FOR EACH STATEMENT EXECUTE FUNCTION public.walrus_guard_manifest_publication_receipt_removal();

-- Object corruption is a table-level recovery state, not a poison-file skip. The first bounded
-- incident schedules a fresh full-table reconciliation; another incident before that generation
-- publishes is terminal quarantine. Keeping the row after recovery makes the last outcome visible
-- while allowing a later, independent incident to begin a fresh bounded cycle.
CREATE TABLE public.walrus_table_integrity_recovery (
  epoch               bigint NOT NULL,
  source_schema       text NOT NULL,
  source_table        text NOT NULL,
  status              text NOT NULL
                      CHECK (status IN ('retrying', 'quarantined', 'recovered')),
  attempt_count       int NOT NULL CHECK (attempt_count > 0),
  max_attempts        int NOT NULL CHECK (max_attempts >= 0),
  recovery_reload_id  bigint REFERENCES public.walrus_table_reload(reload_id) ON DELETE SET NULL,
  failed_manifest_id  bigint NOT NULL,
  failed_group_id     bigint,
  last_error          text NOT NULL,
  first_failed_at     timestamptz NOT NULL DEFAULT now(),
  updated_at          timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (epoch, source_schema, source_table),
  CHECK (status <> 'retrying' OR recovery_reload_id IS NOT NULL)
);

CREATE INDEX walrus_table_integrity_recovery_active_idx
  ON public.walrus_table_integrity_recovery (epoch, status, source_schema, source_table)
  WHERE status IN ('retrying', 'quarantined');
