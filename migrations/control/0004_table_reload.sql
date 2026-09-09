-- 0004_table_reload.sql — the single-table-reload state machine (reload H4/H5/H10).
--
-- The reload's brain lives HERE, in control-pg — never in the source database. The transformer has no
-- source-DB credentials and never will (reload H4); the source-side `public.walrus_reload_signal` table
-- carries only in-band chunk-start watermarks, no state. One row per reload attempt:
--
--   requested → exporting → export_complete → complete        (`failed` terminal from the middle)
--
-- Who flips what (reload H10): an operator/`just reload` INSERTs `requested`; the extractor's reload
-- controller claims it to `exporting`, advances the chunk cursor, and flips
-- `export_complete`; the TRANSFORMER flips `complete` once `transformed_lsn >= final_lsn`.
--
-- `reload_id` is a bigserial, DELIBERATELY not the design doc's UUID: "honor only the latest
-- reload_id" (H9 restart hygiene) becomes a numeric comparison, and the id fits the transformer's
-- `_walrus_meta` k/v store (`v BIGINT`) verbatim. What a UUID key bought — duplicate-request
-- idempotency — moves to the partial unique index below, which is the stronger guarantee anyway.
--
-- There is NO 'superseded' status: a DDL-restarted attempt is `failed` with an
-- explanatory `error`, and its successor is a fresh row with `restart_count + 1`. Five statuses,
-- ever.
CREATE TABLE public.walrus_table_reload (
  reload_id      bigserial PRIMARY KEY,   -- monotonic: "latest wins" is a numeric max (H9)
  epoch          bigint      NOT NULL,    -- namespaces state like everything else (§1.8)
  source_schema  text        NOT NULL,
  source_table   text        NOT NULL,
  flavor         text        NOT NULL CHECK (flavor IN ('reload', 'resync')),
  status         text        NOT NULL DEFAULT 'requested'
                 CHECK (status IN ('requested', 'exporting', 'export_complete', 'complete', 'failed')),
  chunk_no       bigint      NOT NULL DEFAULT 0,  -- last COMPLETED chunk; 0 = none exported yet
  cursor_pk      jsonb,                   -- last exported PK bound (array ⇒ composite-safe); NULL = start
  first_lsn      pg_lsn,                  -- L₁, the reload's first chunk watermark; frozen on chunk 1
  final_lsn      pg_lsn,                  -- H, set at export_complete; completion = transformed_lsn >= H
  schema_version bigint,                  -- the ONE version this attempt exports at; frozen on chunk 1
  restart_count  int         NOT NULL DEFAULT 0,  -- DDL restarts consumed; reload_max_restarts caps this
  lease_holder   text,                    -- which extractor instance is exporting (H7)
  lease_expiry   timestamptz,             -- a live exporter keeps this in the future via renew
  error          text,                    -- why `failed` (preflight reason, DDL restart, echo timeout…)
  requested_at   timestamptz NOT NULL DEFAULT now(),
  updated_at     timestamptz NOT NULL DEFAULT now()  -- set explicitly by every UPDATE (no trigger:
                                                     -- the house has no trigger precedent, and every
                                                     -- write already goes through one Rust fn)
);

-- THE invariant (reload H5): at most one live reload per table. A duplicate request is a
-- unique-violation at INSERT time — mapped to a typed already-in-progress error in Rust — never a
-- second export. Terminal rows fall out of the index, so a new request succeeds after
-- complete/failed.
CREATE UNIQUE INDEX walrus_table_reload_one_live
  ON public.walrus_table_reload (epoch, source_schema, source_table)
  WHERE status NOT IN ('complete', 'failed');

-- Chunk files enter the transformer's EXISTING claim path (reload H8): `kind` gains a third value,
-- 'reload' (0001 documents 'snapshot' | 'stream'; there is no CHECK to extend), and each reload
-- file carries its reload_id. Stream/snapshot rows never set it — NULL for every pre-existing and
-- every non-reload row. The claim order (lsn_end, id) is untouched: chunk files sort into it
-- because their lsn_end is the chunk's echo-captured watermark L_i.
ALTER TABLE public.walrus_file_manifest ADD COLUMN reload_id bigint;

-- `fail()` purges a dead reload's staged chunk files by reload_id in the same transaction that
-- flips the status (a failed reload must leave nothing for the transformer to claim — H9). Partial
-- index: only reload files carry the id, so the purge never scans stream/snapshot rows.
CREATE INDEX walrus_file_manifest_reload_idx
  ON public.walrus_file_manifest (reload_id)
  WHERE reload_id IS NOT NULL;
