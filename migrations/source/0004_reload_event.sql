-- 0004_reload_event.sql -- append-only requests and start/end fences for table reconciliation.
-- Idempotent and safe to re-run.

-- Reapply may need to repair a pre-release `targets` column. Keep trigger removal, data repair,
-- constraint validation, trigger replacement, and publication membership atomic: a failed repair
-- must roll back to the previously protected table rather than leave the append-only guard absent.
BEGIN;

CREATE TABLE IF NOT EXISTS public.walrus_reload_event (
    event_id       uuid        PRIMARY KEY,
    request_id     uuid        NOT NULL,
    reload_id      bigint,
    event_kind     text        NOT NULL
                   CHECK (event_kind IN ('request', 'start_fence', 'end_fence')),
    scope          text        NOT NULL DEFAULT 'table'
                   CHECK (scope IN ('table', 'all_published')),
    source_schema  text,
    source_table   text,
    targets        jsonb       NOT NULL DEFAULT '[]'::jsonb,
    schema_version bigint,
    wal_insert_lsn pg_lsn      NOT NULL DEFAULT pg_current_wal_insert_lsn(),
    inserted_at    timestamptz NOT NULL DEFAULT now(),
    CHECK (
      (event_kind = 'request' AND reload_id IS NULL AND schema_version IS NULL)
      OR
      (event_kind IN ('start_fence', 'end_fence') AND reload_id IS NOT NULL
       AND scope = 'table' AND source_schema IS NOT NULL AND source_table IS NOT NULL
       AND schema_version IS NOT NULL)
    ),
    CONSTRAINT reload_event_target_shape CHECK (
      (event_kind = 'request' AND scope = 'all_published'
       AND source_schema IS NULL AND source_table IS NULL)
      OR
      (scope = 'table' AND source_schema IS NOT NULL AND source_table IS NOT NULL
       AND targets = '[]'::jsonb)
    )
);

-- An older copy can already have the append-only trigger, so remove it transactionally before the
-- only repair UPDATE. The table-level ALTERs retain an exclusive lock until COMMIT; concurrent users
-- never observe an unguarded committed schema. The trigger is recreated below before COMMIT.
DROP TRIGGER IF EXISTS reload_event_append_only ON public.walrus_reload_event;

-- Source migrations are also applied by lightweight operator scripts rather than sqlx's checksum
-- ledger. Keep an already-created pre-release copy of this table additive and self-healing. `ADD
-- COLUMN IF NOT EXISTS` alone does not repair a pre-existing nullable/default-less column, so make
-- each property explicit and validate a stable, named array constraint against all historical rows.
ALTER TABLE public.walrus_reload_event
  ADD COLUMN IF NOT EXISTS targets jsonb;

ALTER TABLE public.walrus_reload_event
  ALTER COLUMN targets SET DEFAULT '[]'::jsonb;

UPDATE public.walrus_reload_event
SET targets = '[]'::jsonb
WHERE targets IS NULL
  AND NOT (event_kind = 'request' AND scope = 'all_published');

-- A historical all-published row without its frozen inventory cannot be reconstructed safely.
-- Leave it NULL so SET NOT NULL fails the migration instead of silently converting it into an
-- empty fanout that would falsely appear reconciled.

ALTER TABLE public.walrus_reload_event
  ALTER COLUMN targets SET NOT NULL;

ALTER TABLE public.walrus_reload_event
  DROP CONSTRAINT IF EXISTS reload_event_targets_array;
ALTER TABLE public.walrus_reload_event
  ADD CONSTRAINT reload_event_targets_array
  CHECK (jsonb_typeof(targets) = 'array') NOT VALID;
ALTER TABLE public.walrus_reload_event
  VALIDATE CONSTRAINT reload_event_targets_array;

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conrelid = 'public.walrus_reload_event'::regclass
      AND conname = 'reload_event_target_shape'
  ) THEN
    ALTER TABLE public.walrus_reload_event
      ADD CONSTRAINT reload_event_target_shape CHECK (
        (event_kind = 'request' AND scope = 'all_published'
         AND source_schema IS NULL AND source_table IS NULL)
        OR
        (scope = 'table' AND source_schema IS NOT NULL AND source_table IS NOT NULL
         AND targets = '[]'::jsonb)
      );
  END IF;
END $$;

-- reload_id is allocated by control Postgres and can be reused after that database is rebuilt.
-- The source request UUID is the stable namespace, so a retried boundary is unique only within
-- one request/attempt pair.
CREATE UNIQUE INDEX IF NOT EXISTS walrus_reload_event_request_attempt_phase_idx
  ON public.walrus_reload_event (request_id, reload_id, event_kind)
  WHERE reload_id IS NOT NULL;

DROP INDEX IF EXISTS public.walrus_reload_event_attempt_phase_idx;

-- This table is the durable source-side protocol log. If a row that has already crossed logical
-- decoding could be updated, deleted, or truncated, replay/idempotency would no longer have a
-- stable fact to recover from. A trigger is required in addition to privileges because the table
-- owner bypasses ordinary grants.
CREATE OR REPLACE FUNCTION public.walrus_reject_reload_event_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  RAISE EXCEPTION 'public.walrus_reload_event is append-only'
    USING ERRCODE = '55000';
END;
$$;

CREATE TRIGGER reload_event_append_only
  BEFORE UPDATE OR DELETE OR TRUNCATE ON public.walrus_reload_event
  FOR EACH STATEMENT
  EXECUTE FUNCTION public.walrus_reject_reload_event_mutation();

REVOKE UPDATE, DELETE, TRUNCATE ON public.walrus_reload_event FROM PUBLIC;

DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'walrus_pub' AND NOT puballtables)
     AND NOT EXISTS (
       SELECT 1 FROM pg_publication_tables
       WHERE pubname = 'walrus_pub' AND schemaname = 'public'
         AND tablename = 'walrus_reload_event')
  THEN
    ALTER PUBLICATION walrus_pub ADD TABLE public.walrus_reload_event;
  END IF;
END $$;

COMMIT;
