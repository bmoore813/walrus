-- 0006_unified_reconcile.sql — durable source-request and fence records for the unified
-- copy-then-WAL-catch-up protocol.
--
-- Existing operator-created reloads remain valid: their source_request_id/start_lsn are NULL and
-- their first_lsn keeps its historical meaning. Source-driven requests add a stable UUID and an
-- authoritative start fence before any data file exists, which is what makes an empty-table dump
-- a first-class reload rather than an attempt with no observable input.

ALTER TABLE public.walrus_table_reload
  ADD COLUMN source_request_id uuid,
  ADD COLUMN parent_request_id uuid,
  ADD COLUMN request_scope text NOT NULL DEFAULT 'table'
    CHECK (request_scope IN ('table', 'all_published')),
  ADD COLUMN start_lsn pg_lsn;

-- Historical direct control-plane requests predate source event UUIDs. Give every one a durable,
-- random fence namespace before the new exporter can emit into the append-only source event log.
-- This prevents a rebuilt control database from reusing a bigint reload_id and colliding with an
-- older source fence. New direct requests populate this column in request_reload.sql.
UPDATE public.walrus_table_reload
SET parent_request_id = gen_random_uuid()
WHERE source_request_id IS NULL AND parent_request_id IS NULL;

-- Keep the legacy direct-INSERT API rolling-compatible after this migration. Older clients omit
-- both UUID columns; the default gives those rows the same private fence namespace as the current
-- Rust request path. Source-WAL inserts explicitly provide source_request_id (and may explicitly
-- leave parent_request_id NULL), so the CHECK accepts either durable identity while rejecting a
-- row the exporter could never fence safely.
ALTER TABLE public.walrus_table_reload
  ALTER COLUMN parent_request_id SET DEFAULT gen_random_uuid(),
  ADD CONSTRAINT table_reload_fence_request_identity
    CHECK (source_request_id IS NOT NULL OR parent_request_id IS NOT NULL);

-- A WAL request must be durably accepted without stopping decoding just because the same table is
-- already reconciling. Source-backed `requested` rows are therefore a FIFO queue: they enter the
-- uniqueness fence only when claimed as `exporting`. Legacy direct requests retain their immediate
-- duplicate rejection, and all exporting/export_complete attempts remain mutually exclusive.
DROP INDEX public.walrus_table_reload_one_live;
CREATE UNIQUE INDEX walrus_table_reload_one_live
  ON public.walrus_table_reload (epoch, source_schema, source_table)
  WHERE status IN ('exporting', 'export_complete')
     OR (status = 'requested' AND source_request_id IS NULL);

-- One source event can fan out to many table children. Redelivery of one child maps back to the
-- same attempt, while the table coordinates keep siblings distinct. The index includes terminal
-- rows deliberately: replaying an old source event must not start a fresh export after completion.
CREATE UNIQUE INDEX walrus_table_reload_source_request_target
  ON public.walrus_table_reload (epoch, source_request_id, source_schema, source_table)
  WHERE source_request_id IS NOT NULL;

-- Marker existence is the durable control-plane fact. A baseline marker makes a zero-row dump
-- visible to the transformer; an end marker is written only after the extractor has made all target WAL
-- through its LSN durable. Both carry the frozen schema so a mismatched retry is rejected.
CREATE TABLE public.walrus_table_reload_marker (
  reload_id       bigint NOT NULL REFERENCES public.walrus_table_reload(reload_id) ON DELETE CASCADE,
  marker_kind     text NOT NULL CHECK (marker_kind IN ('baseline', 'end')),
  lsn             pg_lsn NOT NULL,
  schema_version  bigint NOT NULL,
  created_at      timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (reload_id, marker_kind)
);

CREATE INDEX walrus_table_reload_marker_order_idx
  ON public.walrus_table_reload_marker (reload_id, lsn, marker_kind);
