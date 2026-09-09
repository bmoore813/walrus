CREATE OR REPLACE VIEW "{table}_current" AS
SELECT * EXCLUDE (_applied_commit_lsn, _applied_lsn) FROM "{table}";
