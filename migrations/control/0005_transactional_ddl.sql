-- 0005_transactional_ddl.sql — make source DDL events replay-idempotent and retain raw SQL context.
--
-- source_audit_id is the identity PK of public.walrus_ddl_audit. The same source WAL can replay after a extractor
-- crash between committing this control row and advancing the replication slot; this key turns that
-- replay into an update of the same history row instead of a second schema-version bump.

ALTER TABLE public.walrus_ddl_manifest ADD COLUMN source_audit_id bigint;
ALTER TABLE public.walrus_ddl_manifest ADD COLUMN c_ddl_text text;

-- Pre-0005 history has no source identity. Give it a disjoint negative identity so the new NOT NULL /
-- uniqueness contract can be installed without pretending an old control id was a source audit id.
UPDATE public.walrus_ddl_manifest SET source_audit_id = -id WHERE source_audit_id IS NULL;
ALTER TABLE public.walrus_ddl_manifest ALTER COLUMN source_audit_id SET NOT NULL;

CREATE UNIQUE INDEX walrus_ddl_manifest_source_audit_idx
  ON public.walrus_ddl_manifest (epoch, source_audit_id);
