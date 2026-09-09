CREATE TABLE IF NOT EXISTS "_walrus_ingested_files" (
    s3_uri VARCHAR PRIMARY KEY,
    manifest_id BIGINT NOT NULL UNIQUE,
    object_size BIGINT NOT NULL,
    sha256 VARCHAR NOT NULL,
    stream_group_id BIGINT
);
ALTER TABLE "_walrus_ingested_files" ADD COLUMN IF NOT EXISTS object_size BIGINT;
ALTER TABLE "_walrus_ingested_files" ADD COLUMN IF NOT EXISTS sha256 VARCHAR;
ALTER TABLE "_walrus_ingested_files" ADD COLUMN IF NOT EXISTS stream_group_id BIGINT;
