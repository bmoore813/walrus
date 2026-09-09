-- 0002_registry_ddl.sql — finalize the schema_registry / ddl_manifest columns.
--
-- Migration 0001 created these two tables as stubs; here we ALTER them into their final shape rather
-- than drop-and-recreate — both are **history, never a queue**: they are never pruned because they
-- are the schema record needed to reconstruct any table at any schema_version, and a DELETE/DROP
-- here would make old-version Parquet files un-reconstructable.

-- schema_registry: add the resulting-column-set snapshot alongside the per-column descriptors.
ALTER TABLE public.walrus_schema_registry ADD COLUMN columns jsonb NOT NULL DEFAULT '[]'::jsonb;
ALTER TABLE public.walrus_schema_registry ALTER COLUMN columns DROP DEFAULT;

-- ddl_manifest: reshape the stub into the ddl_audit-derived event row (walrus-extractor.md §3.3).
-- c_lsn is the DDL's commit LSN — directly comparable to file_manifest.lsn_end / the checkpoints,
-- so the transformer crosses a schema_version boundary by applying pending DDL whose c_lsn it is about
-- to pass.
ALTER TABLE public.walrus_ddl_manifest RENAME COLUMN lsn TO c_lsn;
ALTER TABLE public.walrus_ddl_manifest DROP COLUMN change;
ALTER TABLE public.walrus_ddl_manifest
  ADD COLUMN c_event   text NOT NULL DEFAULT 'ddl_command_end', -- 'ddl_command_end' | 'sql_drop'
  ADD COLUMN c_tag     text NOT NULL DEFAULT '',                -- 'CREATE TABLE' | 'ALTER TABLE' | …
  ADD COLUMN c_rel_oid oid,                                     -- affected pg_class OID
  ADD COLUMN c_columns jsonb,                                   -- structured resulting column set
  ADD COLUMN c_dropped jsonb;                                   -- sql_drop dropped-objects payload
ALTER TABLE public.walrus_ddl_manifest ALTER COLUMN c_event DROP DEFAULT;
ALTER TABLE public.walrus_ddl_manifest ALTER COLUMN c_tag DROP DEFAULT;

ALTER INDEX public.walrus_ddl_manifest_apply_idx RENAME TO walrus_ddl_manifest_lsn_idx;
