-- 0003_reload_signal.sql — the reload chunk-watermark signal table (reload §H1/H5).
-- Idempotent, re-runnable.
--
-- Exactly ONE thing about a reload travels in-band through the WAL: the chunk-start watermark,
-- because its COMMIT LSN is the datum. The extractor INSERTs one row per chunk over an ordinary SQL
-- connection and learns L_i from the row's ECHO in the replication stream —
-- never by reading this table back. All reload STATE lives in control-pg's public.walrus_table_reload;
-- this table carries watermarks only.
--
-- INSERT-ONLY: rows are never updated, so REPLICA IDENTITY DEFAULT (the PK) is all it needs, and
-- the PK gives chunk idempotency for free — a crash-redone chunk's re-INSERT is a unique-violation
-- the exporter treats as "already signalled" (H5). Rows accumulate one tiny row per chunk; pruning
-- is an operator-runbook concern, and those pruning DELETEs also flow through the
-- slot — the routing must ignore every non-insert op on this table.

CREATE TABLE IF NOT EXISTS public.walrus_reload_signal (
    reload_id      bigint      NOT NULL,   -- control-pg public.walrus_table_reload.reload_id (bigserial)
    chunk_no       bigint      NOT NULL,   -- 1-based chunk ordinal within the attempt
    -- Evaluated per-INSERT (the function is volatile — a multi-row INSERT would stamp each row
    -- individually; signals are single-row). The CROSS-CHECK, not the stamp: an insert's WAL
    -- position strictly precedes its transaction's commit record, so this is always < the echo's
    -- commit LSN. The echo-routing check asserts exactly that inequality; the chunk watermark L_i
    -- is ALWAYS the echo's commit LSN, never this column.
    wal_insert_lsn pg_lsn      NOT NULL DEFAULT pg_current_wal_insert_lsn(),
    inserted_at    timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (reload_id, chunk_no)
);

-- Publication membership — 0002's ddl_audit guard, verbatim: table-list publications need the
-- explicit add; a FOR ALL TABLES dev publication already covers it; re-running is a no-op. An
-- UNPUBLISHED signal table is the classic silent failure (the echo just never arrives), which is
-- why extractor preflight also asserts membership (reload §H11).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'walrus_pub' AND NOT puballtables)
     AND NOT EXISTS (
       SELECT 1 FROM pg_publication_tables
       WHERE pubname = 'walrus_pub' AND schemaname = 'public'
         AND tablename = 'walrus_reload_signal')
  THEN
    ALTER PUBLICATION walrus_pub ADD TABLE public.walrus_reload_signal;
  END IF;
END $$;
