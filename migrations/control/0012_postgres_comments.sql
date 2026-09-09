-- Preserve the table-level half of each source catalog comment snapshot. Column comments travel
-- with c_columns, beside the column identity they describe. COMMENT IS NULL is represented by SQL
-- NULL here and JSON null there, so removals are durable metadata changes rather than missing data.
ALTER TABLE public.walrus_ddl_manifest ADD COLUMN c_table_comment text;
