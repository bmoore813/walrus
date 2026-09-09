CREATE TABLE IF NOT EXISTS "_walrus_meta" (k VARCHAR PRIMARY KEY, v BIGINT);
INSERT INTO "_walrus_meta" VALUES ('schema_version', {schema_version})
ON CONFLICT (k) DO NOTHING;
