-- The raw→mirror transform (transformer §5–§7, architecture §21) — the single source of truth run by both
-- the hermetic tests and Phase B. Rendered per table by `transform.rs`.
--
-- ⚠ EXTENDS architecture.md (Open Q8/Q13, transformer §7): the per-PK max-applied-`(commit_lsn, lsn)` guard.
-- The incremental MERGE would apply a window winner UNCONDITIONALLY — safe only if a winner's tuple is
-- always ≥ the tuple that last shaped the mirror row it touches. Two faces break that:
--   (A) the equal-`commit_lsn` SNAPSHOT STRADDLE — a snapshot row carries `commit_lsn = consistent_point`;
--       a strict `> :transformed_lsn` window would exclude it forever once the watermark reaches that
--       point. The relaxed `>=` low bound below re-examines it.
--   (B) a stale DELETE / re-INSERT STRADDLING THE WATERMARK — an out-of-order winner that would resurrect
--       a killed row or clobber a newer one.
-- Hidden columns `_applied_commit_lsn` / `_applied_lsn` on the mirror record the tuple that last shaped
-- each row; every mutating MERGE branch is gated on `(s.commit_lsn, s.lsn) > (t._applied_*)`, so a stale
-- winner is a no-op regardless of watermark timing. The `>=` bound and the guard work TOGETHER: `>=`
-- alone would re-apply already-applied rows every cycle; the guard makes those re-applications no-ops.
-- The periodic full-rebuild remains the safety net regardless — this only makes the
-- *incremental* path correct on its own.
--
-- Step 0 — TRUNCATE pre-step (§5.5). A `t` row carries no tuple/PK, so it can't be a MERGE branch:
-- wipe the WHOLE mirror as of the latest truncate `(Ct, Lt)` in the tail (all rows, incl. earlier
-- cycles — the source table was emptied); the window below only repopulates rows STRICTLY after that
-- tuple. `{truncate_wipe}` is empty when the tail has no truncate.
{truncate_wipe}
-- Step 1+2 — dedup to the WINNER per PK (latest by the TUPLE (commit_lsn, lsn) DESC, with a non-delete
-- preferred over its synthetic delete on an exact tie; deletes otherwise stay in and the winner's op
-- decides — the resurrection guard §5.3; the truncate boundary is the TUPLE, never the scalar
-- commit_lsn), then RESOLVE unchanged-TOAST (§5.6): pgoutput's `'u'` means "column not modified,
-- value absent" — NOT a real NULL. For each column named in the winner's `unchanged_toast` meta list,
-- substitute the last NON-sentinel value for that PK from `<table>_raw` **at or before** the winner's
-- tuple, falling back to the current mirror value LAST. A mirror-only lookup loses the value when the
-- setter and the unchanged-TOAST update land in the SAME batch (mirror has no row for that PK yet).
-- The low bound is `>= {after_lsn}` (break face A): a row AT the watermark is re-examined; the guard in
-- Step 3 makes an already-applied one a no-op.
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

-- Step 3 — collapse into the mirror. Three branches encode the intra-batch PK-churn collapse rule,
-- each MUTATING branch gated on the per-PK guard `(s.commit_lsn, s.lsn) > (t._applied_commit_lsn,
-- t._applied_lsn)` so a stale straddle winner (break face B) is a no-op:
--   MATCHED AND op='d' AND guard → DELETE (the winner is a newer tombstone)
--   MATCHED           AND guard → UPDATE (last-tuple-wins, incl. d→i; also stamps `_applied_*`)
--   NOT MATCHED AND op<>'d'     → INSERT (new key; a phantom delete for an unseen key is a no-op; the
--                                 first event for a key is always newer than the low sentinel `_applied_*`)
MERGE INTO "{table}" AS t
USING _batch AS s
ON {pk_join}
WHEN MATCHED AND s._walrus_op = 'd' AND {guard} THEN DELETE
WHEN MATCHED AND {guard} THEN UPDATE SET {set_cols}
WHEN NOT MATCHED AND s._walrus_op <> 'd' THEN INSERT ({insert_cols}) VALUES ({insert_vals});
