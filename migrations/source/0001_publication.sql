-- 0001_publication.sql — one-time source-side setup (idempotent, re-runnable).
--
-- Applied by the operator (or by the extractor when `manage_publication = true`). It creates the walrus
-- internal tables the extractor requires to ride in the publication:
--   * public.walrus_heartbeat — the slot-liveness round-trip target;
--   * public.walrus_ddl_audit — a stub so the table exists in the publication now (the DDL-capture trigger
--     that fills it, plus its full column set, is installed by migrations/source/0002).
-- The source-side preflight asserts both are members of `walrus_pub`.

CREATE TABLE IF NOT EXISTS public.walrus_heartbeat (
    id            integer PRIMARY KEY,
    beat_seq      bigint      NOT NULL DEFAULT 0,
    ts            timestamptz NOT NULL,
    extractor_instance text
);
-- Seed the single heartbeat row (id = 1). Re-running is a no-op.
INSERT INTO public.walrus_heartbeat (id, ts) VALUES (1, now()) ON CONFLICT (id) DO NOTHING;

-- Stub: must exist to be a publication member (existence-checked by preflight); the capture trigger
-- and full column set are added by migration 0002.
CREATE TABLE IF NOT EXISTS public.walrus_ddl_audit (
    id      bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    c_lsn   pg_lsn,
    c_event text,
    c_tag   text,
    ts      timestamptz NOT NULL DEFAULT now()
);

-- Publication membership.
--   * Dev harness: `walrus_pub` is `CREATE PUBLICATION walrus_pub FOR ALL TABLES`, so the two tables
--     above are covered automatically once they exist — nothing more to do here.
--   * Table-list publication (production): the operator runs, idempotently:
--       CREATE PUBLICATION walrus_pub FOR TABLE <user tables> WITH (publish_via_partition_root = true);
--       ALTER PUBLICATION walrus_pub ADD TABLE public.walrus_heartbeat, public.walrus_ddl_audit;
--
-- Grant the extractor role write access to the heartbeat round-trip:
--   GRANT INSERT, UPDATE ON public.walrus_heartbeat TO <extractor_role>;
