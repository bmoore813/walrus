-- DuckLake variant of transform.sql. DuckLake currently permits only one matched UPDATE/DELETE
-- action per MERGE, so deletes and upserts are split while remaining in the caller's one ACID
-- transaction/snapshot.
{truncate_wipe}
CREATE OR REPLACE TEMP TABLE _batch AS
WITH winners AS (
    SELECT raw_winner.* FROM "{table}_raw" raw_winner
    WHERE raw_winner._walrus_op <> 't'
      AND raw_winner._walrus_commit_lsn >= '{after_lsn}'{truncate_bound}
    QUALIFY row_number() OVER (
        PARTITION BY {raw_pk_list}
        ORDER BY raw_winner._walrus_commit_lsn DESC, raw_winner._walrus_lsn DESC,
                 CASE WHEN raw_winner._walrus_op = 'd' THEN 0 ELSE 1 END DESC
    ) = 1
)
SELECT {resolved_select}
FROM winners s
LEFT JOIN "{table}" t ON {pk_join};

MERGE INTO "{table}" AS t
USING (SELECT * FROM _batch WHERE _walrus_op = 'd') AS s
ON {pk_join}
WHEN MATCHED AND {guard} THEN DELETE;

MERGE INTO "{table}" AS t
USING (SELECT * FROM _batch WHERE _walrus_op <> 'd') AS s
ON {pk_join}
WHEN MATCHED AND {guard} THEN UPDATE SET {set_cols}
WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals});
