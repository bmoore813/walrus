//! The raw→mirror transform (transformer §5–§6) — **the correctness heart of the transformer**. One parameterized
//! SQL template ([`transform.sql`](TRANSFORM_SQL)) rendered per table: a dedup window that keeps the
//! latest change per PK (deletes stay *in* the window; the winner's `op` decides — the resurrection
//! guard §5.3), then a three-branch `MERGE INTO` that collapses intra-batch PK churn (`i→d→i`, `i→u→d`,
//! `d→i`, phantom `d`). The same template is used by the hermetic tests here and by Phase B.
//!
//! **⚠ Extends architecture.md (§7, Open Q8/Q13):** the per-PK max-applied-`(commit_lsn, lsn)` guard.
//! Each mutating MERGE branch is gated on `(s.commit_lsn, s.lsn) > (t._applied_commit_lsn, t._applied_lsn)`
//! and the window low bound is relaxed to `>=`, together closing two straddle faces — (A) the
//! equal-`commit_lsn` snapshot row and (B) a stale delete/re-insert across the watermark — while keeping
//! the mirror idempotent (the guard makes a re-applied boundary row a no-op). The full-rebuild
//! remains the safety net regardless; this makes the *incremental* path self-correcting.

use crate::duck_ext::DuckResultExt;
use crate::error::TransformerError;
use crate::table_name::{DuckTable, Mirror, Raw};
use common::{Lsn, PgRelation};
use duckdb::OptionalExt;

/// The transform template (single source of truth). Rendered by [`TransformSql::render`].
pub const TRANSFORM_SQL: &str = include_str!("../sql/duckdb/templates/transform.sql");
/// DuckLake-compatible split-MERGE form of the same transform.
pub const TRANSFORM_DUCKLAKE_SQL: &str =
    include_str!("../sql/duckdb/templates/transform_ducklake.sql");

/// Substitute every transform placeholder in one pass. `str::replace` allocates and copies the
/// whole (increasingly large) SQL string once per placeholder; the transform has eleven of them.
/// Counting against the static template first gives the final string an exact capacity, then this
/// traversal copies each literal and replacement only once.
fn render_transform_template(template: &str, replacements: &[(&str, &str)]) -> String {
    let capacity = replacements
        .iter()
        .fold(template.len(), |length, (placeholder, value)| {
            let occurrences = template.matches(placeholder).count();
            length - occurrences * placeholder.len() + occurrences * value.len()
        });
    let mut rendered = String::with_capacity(capacity);
    let mut remaining = template;

    while let Some(open) = remaining.find('{') {
        rendered.push_str(&remaining[..open]);
        let token_start = &remaining[open..];
        let Some(close) = token_start.find('}') else {
            rendered.push_str(token_start);
            return rendered;
        };
        let token_end = open + close + 1;
        let token = &remaining[open..token_end];
        let replacement = replacements
            .iter()
            .find_map(|(placeholder, value)| (*placeholder == token).then_some(*value))
            .unwrap_or(token);
        rendered.push_str(replacement);
        remaining = &remaining[token_end..];
    }
    rendered.push_str(remaining);
    rendered
}

/// The latest `TRUNCATE` tuple `(Ct, Lt)` in the un-transformed tail. The wipe boundary is the
/// **tuple**, never the scalar `commit_lsn`.
///
/// "The tail holds no truncate" is the *absence* of this value — producers and consumers carry it
/// as `Option<TruncateBoundary>` — so a half-resolved boundary (one LSN of the pair without the
/// other) cannot be constructed, and no call site has to re-check the second field.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TruncateBoundary {
    /// The TRUNCATE's commit LSN — which transaction wiped the table.
    pub ct: Lsn,
    /// The TRUNCATE's per-row LSN — the intra-transaction tiebreaker that, with `ct`, orders the
    /// wipe against rows committed by the same transaction.
    pub lt: Lsn,
}

/// One mirror column and the SQL producing its value from the winning raw row `s` (and, for an
/// unchanged-TOAST value, the current mirror `t`). `raw_value_expr` always addresses the raw emit
/// shape; the two resolved expressions differ because an incremental winner still has that raw
/// shape while a rebuild winner has already been collapsed to the mirror shape.
#[derive(Debug)]
struct MirrorCol {
    name: String,
    is_key: bool,
    raw_value_expr: String,
    incremental_value_expr: String,
    rebuild_value_expr: String,
}

/// A table's mirror-column layout for rendering the transform (order preserved). Built from the
/// crate-internal `plan::TablePlan` — the Tier-1 plan reproduces the pre-descriptor scalar SQL
/// exactly. The public constructor ([`Self::from_relation`]) takes the shape callers already hold,
/// so the plan type stays free to change with the type system it bridges.
#[derive(Debug)]
pub struct TransformSql {
    table: DuckTable<Mirror>,
    /// Frozen like the `plan::TablePlan::mirror_cols` it is derived from: the layout is
    /// settled once per Phase-B poll and only ever read back to render SQL.
    mirror: Box<[MirrorCol]>,
}

/// One logical identity component in its two physical shapes. A Tier-2 recombine is one value in
/// the mirror but several emit columns in raw, while a flat Tier-2 identity contributes several of
/// these entries. Keeping both expressions is what lets history traversal address either table.
#[derive(Debug)]
struct ToastKey {
    mirror_name: String,
    raw_value_expr: String,
}

/// Re-qualify a plan expression from its canonical raw-row alias `s` to another raw-table alias.
fn raw_expr_for(expr: &str, alias: &str) -> String {
    expr.replace("s.", &format!("{alias}."))
}

/// Render the unchanged-TOAST membership predicate against the original PostgreSQL source-column
/// name. Tier-2 emit siblings have different physical names, so testing the mirror name would miss
/// their one shared sentinel.
fn toast_listed(alias: &str, source: &str) -> String {
    // ExtractorMeta is compact JSON, and the surrounding JSON quotes keep `a` distinct from `aa`. Escape
    // LIKE metacharacters in an otherwise arbitrary PostgreSQL identifier before adding our own
    // leading/trailing wildcards.
    let json_name = serde_json::Value::String(source.to_owned()).to_string();
    let escaped_name = json_name
        .replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_");
    let pattern = format!("%{escaped_name}%");
    format!(
        "COALESCE(json_extract_string({alias}.walrus_extractor_meta, '$.unchanged_toast'), '[]') LIKE '{}' ESCAPE '\\'",
        common::sql::sql_literal(&pattern)
    )
}

/// Render the latest key-changing update edge for one key lifetime. pgoutput represents the edge as
/// a new-key `u` plus an old-key synthetic `d` at the exact same tuple. `upper_inclusive` is true
/// only for the seed lifetime, where the winner itself may be that `u`; every recursive hop is
/// strict so the edge tuple decreases and the walk must terminate.
fn toast_edge_select(
    raw_table: &str,
    keys: &[ToastKey],
    current_keys: &[String],
    upper_commit: &str,
    upper_lsn: &str,
    upper_inclusive: bool,
) -> String {
    let new_key_match = keys
        .iter()
        .zip(current_keys)
        .map(|(key, current)| {
            format!(
                "{} IS NOT DISTINCT FROM ({current})",
                raw_expr_for(&key.raw_value_expr, "u")
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let key_really_changed = keys
        .iter()
        .map(|key| {
            format!(
                "{} IS NOT DISTINCT FROM {}",
                raw_expr_for(&key.raw_value_expr, "d"),
                raw_expr_for(&key.raw_value_expr, "u")
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let old_keys = keys
        .iter()
        .enumerate()
        .map(|(index, key)| {
            format!(
                "{} AS _walrus_old_key_{index}",
                raw_expr_for(&key.raw_value_expr, "d")
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let bound = if upper_inclusive { "<=" } else { "<" };

    format!(
        "SELECT TRUE AS _walrus_edge_found, \
                u._walrus_commit_lsn AS _walrus_edge_commit_lsn, \
                u._walrus_lsn AS _walrus_edge_lsn, {old_keys} \
         FROM \"{raw_table}\" u \
         JOIN \"{raw_table}\" d \
           ON d._walrus_op = 'd' \
          AND d._walrus_commit_lsn IS NOT DISTINCT FROM u._walrus_commit_lsn \
          AND d._walrus_lsn IS NOT DISTINCT FROM u._walrus_lsn \
         WHERE u._walrus_op = 'u' AND {new_key_match} \
           AND (u._walrus_commit_lsn, u._walrus_lsn) {bound} ({upper_commit}, {upper_lsn}) \
           AND NOT ({key_really_changed}) \
         ORDER BY u._walrus_commit_lsn DESC, u._walrus_lsn DESC LIMIT 1"
    )
}

/// Resolve one mirror value. `winner_value_expr` reads the current winner (raw-shaped during the
/// incremental transform, mirror-shaped after the rebuild union); `raw_value_expr` always reads a
/// raw-shaped `s`. If a winner inherited a TOAST value through one or more key changes, the
/// recursive lineage walks the paired-delete edges back until it finds the nearest retained setter
/// or current mirror row.
fn resolved_toast_expr(
    col: &crate::plan::MirrorCol,
    winner_value_expr: &str,
    raw_value_expr: &str,
    raw_table: &str,
    mirror_table: &str,
    keys: &[ToastKey],
    winner_is_raw: bool,
) -> String {
    let Some(source) = col.toast_source.as_deref() else {
        return winner_value_expr.to_owned();
    };
    debug_assert!(!keys.is_empty(), "unchanged TOAST requires a row identity");
    if keys.is_empty() {
        return winner_value_expr.to_owned();
    }

    let qc = col.name.clone();
    let prior_value_expr = raw_expr_for(raw_value_expr, "r");
    let lineage_keys = (0..keys.len())
        .map(|index| format!("_walrus_key_{index}"))
        .collect::<Vec<_>>();
    let old_lineage_keys = (0..keys.len())
        .map(|index| format!("_walrus_old_key_{index}"))
        .collect::<Vec<_>>();
    let winner_keys = keys
        .iter()
        .map(|key| {
            if winner_is_raw {
                raw_expr_for(&key.raw_value_expr, "s")
            } else {
                format!("s.{}", key.mirror_name)
            }
        })
        .collect::<Vec<_>>();
    let seed_edge = toast_edge_select(
        raw_table,
        keys,
        &winner_keys,
        "s._walrus_commit_lsn",
        "s._walrus_lsn",
        true,
    );
    let recursive_current_keys = old_lineage_keys
        .iter()
        .map(|key| format!("lineage.{key}"))
        .collect::<Vec<_>>();
    let recursive_edge = toast_edge_select(
        raw_table,
        keys,
        &recursive_current_keys,
        "lineage._walrus_edge_commit_lsn",
        "lineage._walrus_edge_lsn",
        false,
    );
    let cte_columns = std::iter::once("_walrus_depth".to_string())
        .chain(lineage_keys.iter().cloned())
        .chain([
            "_walrus_upper_commit_lsn".to_string(),
            "_walrus_upper_lsn".to_string(),
            "_walrus_has_edge".to_string(),
            "_walrus_edge_commit_lsn".to_string(),
            "_walrus_edge_lsn".to_string(),
        ])
        .chain(old_lineage_keys.iter().cloned())
        .collect::<Vec<_>>()
        .join(", ");
    let seed_keys = winner_keys.join(", ");
    let recursive_keys = old_lineage_keys
        .iter()
        .map(|key| format!("lineage.{key}"))
        .collect::<Vec<_>>()
        .join(", ");
    let seed_old_keys = old_lineage_keys
        .iter()
        .map(|key| format!("edge.{key}"))
        .collect::<Vec<_>>()
        .join(", ");
    let recursive_old_keys = seed_old_keys.clone();
    let raw_key_match = keys
        .iter()
        .zip(&lineage_keys)
        .map(|(key, lineage_key)| {
            format!(
                "{} IS NOT DISTINCT FROM lineage.{lineage_key}",
                raw_expr_for(&key.raw_value_expr, "r")
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");
    let mirror_key_match = keys
        .iter()
        .zip(&lineage_keys)
        .map(|(key, lineage_key)| {
            format!(
                "m.{} IS NOT DISTINCT FROM lineage.{lineage_key}",
                key.mirror_name
            )
        })
        .collect::<Vec<_>>()
        .join(" AND ");

    // Every candidate value is wrapped in a one-element LIST. The envelope remains non-NULL when
    // the value it contains is a real SQL NULL, so a found NULL stops the walk instead of falling
    // through to an older setter or mirror value. The edge lower bound also confines a reused key
    // to the lifetime that led to this winner.
    format!(
        "CASE WHEN {winner} THEN COALESCE(( \
           WITH RECURSIVE _walrus_toast_lineage({cte_columns}) AS ( \
             SELECT 0, {seed_keys}, s._walrus_commit_lsn, s._walrus_lsn, \
                    edge._walrus_edge_found IS NOT NULL, \
                    edge._walrus_edge_commit_lsn, edge._walrus_edge_lsn, {seed_old_keys} \
             FROM (VALUES (TRUE)) AS seed(_walrus_dummy) \
             LEFT JOIN LATERAL ({seed_edge}) edge ON TRUE \
             UNION ALL \
             SELECT lineage._walrus_depth + 1, {recursive_keys}, \
                    lineage._walrus_edge_commit_lsn, lineage._walrus_edge_lsn, \
                    edge._walrus_edge_found IS NOT NULL, \
                    edge._walrus_edge_commit_lsn, edge._walrus_edge_lsn, {recursive_old_keys} \
             FROM _walrus_toast_lineage lineage \
             LEFT JOIN LATERAL ({recursive_edge}) edge ON TRUE \
             WHERE lineage._walrus_has_edge \
           ) \
           SELECT COALESCE(raw_value._walrus_value, mirror_value._walrus_value) \
           FROM _walrus_toast_lineage lineage \
           LEFT JOIN LATERAL ( \
             SELECT list_value({prior_value_expr}) AS _walrus_value \
             FROM \"{raw_table}\" r \
             WHERE {raw_key_match} AND r._walrus_op NOT IN ('d', 't') AND NOT ({raw}) \
               AND (r._walrus_commit_lsn, r._walrus_lsn) \
                   <= (lineage._walrus_upper_commit_lsn, lineage._walrus_upper_lsn) \
               AND (NOT lineage._walrus_has_edge OR \
                    (r._walrus_commit_lsn, r._walrus_lsn) \
                      >= (lineage._walrus_edge_commit_lsn, lineage._walrus_edge_lsn)) \
             ORDER BY r._walrus_commit_lsn DESC, r._walrus_lsn DESC LIMIT 1 \
           ) raw_value ON TRUE \
           LEFT JOIN LATERAL ( \
             SELECT list_value(m.{qc}) AS _walrus_value FROM \"{mirror_table}\" m \
             WHERE {mirror_key_match} LIMIT 1 \
           ) mirror_value ON TRUE \
           WHERE raw_value._walrus_value IS NOT NULL \
              OR mirror_value._walrus_value IS NOT NULL \
           ORDER BY lineage._walrus_depth LIMIT 1 \
         ), list_value(t.{qc}))[1] ELSE {winner_value_expr} END",
        winner = toast_listed("s", source),
        raw = toast_listed("r", source),
    )
}

impl TransformSql {
    /// Tier-1 (scalar) transform from a bare relation.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::ManifestInvariant`] when a column cannot be safely rendered as a
    /// bare lowercase snake_case SQL name.
    pub fn from_relation(rel: &PgRelation) -> Result<Self, TransformerError> {
        Ok(Self::from_plan(&crate::plan::TablePlan::tier1(rel)?))
    }

    /// The full transform from a schema [`crate::plan::TablePlan`]: each mirror column's value is
    /// precomputed as SQL over the winner `s`. Every non-key source column, including Tier-2
    /// recombines and flat siblings, resolves the source column named by `unchanged_toast`.
    #[must_use]
    pub(crate) fn from_plan(plan: &crate::plan::TablePlan) -> Self {
        use crate::plan::MirrorValue;
        let table = DuckTable::<Mirror>::new(plan.table.as_ref());
        let raw_table = table.to_raw();
        let planned_raw_value = |c: &crate::plan::MirrorCol| match &c.value {
            MirrorValue::Recombine(expr) => expr.clone(),
            MirrorValue::Passthrough => format!("s.{}", c.name),
        };
        let toast_keys = plan
            .mirror_cols
            .iter()
            .filter(|c| c.is_key)
            .map(|c| ToastKey {
                mirror_name: c.name.clone(),
                raw_value_expr: planned_raw_value(c),
            })
            .collect::<Vec<_>>();
        let mirror = plan
            .mirror_cols
            .iter()
            .map(|c| {
                let raw_value_expr = planned_raw_value(c);
                let incremental_value_expr = resolved_toast_expr(
                    c,
                    &raw_value_expr,
                    &raw_value_expr,
                    raw_table.as_str(),
                    table.as_str(),
                    &toast_keys,
                    true,
                );
                let rebuild_value_expr = resolved_toast_expr(
                    c,
                    &format!("s.{}", c.name),
                    &raw_value_expr,
                    raw_table.as_str(),
                    table.as_str(),
                    &toast_keys,
                    false,
                );
                MirrorCol {
                    name: c.name.clone(),
                    is_key: c.is_key,
                    raw_value_expr,
                    incremental_value_expr,
                    rebuild_value_expr,
                }
            })
            .collect();
        TransformSql { table, mirror }
    }

    /// The mirror's key columns in relation order. Allocates a `Vec` per call — hence `to_`; the
    /// borrowed `&str` elements still refer to `self`.
    fn to_pk_names(&self) -> Vec<&str> {
        self.mirror
            .iter()
            .filter(|c| c.is_key)
            .map(|c| c.name.as_str())
            .collect()
    }
    /// The mirror's non-key columns in relation order. Allocates a `Vec` per call — hence `to_`;
    /// the borrowed `&str` elements still refer to `self`.
    fn to_non_key_names(&self) -> Vec<&str> {
        self.mirror
            .iter()
            .filter(|c| !c.is_key)
            .map(|c| c.name.as_str())
            .collect()
    }

    /// The latest `TRUNCATE` `(Ct, Lt)` in the tail (`op='t'`, `commit_lsn >= after_lsn`), ordered by the
    /// tuple. `None` if the tail holds no truncate — every downstream predicate is then simply omitted.
    ///
    /// # Errors
    ///
    /// Returns [`TransformerError::LsnParse`] if a stored truncate commit or row LSN is malformed, or
    /// [`TransformerError::Duck`] if DuckDB rejects the boundary query. Only a genuinely missing row is
    /// treated as no truncate boundary.
    pub fn latest_truncate(
        &self,
        conn: &duckdb::Connection,
        after_lsn: Lsn,
    ) -> Result<Option<TruncateBoundary>, TransformerError> {
        let raw = self.table.to_raw();
        let sql = format!(
            "SELECT _walrus_commit_lsn, _walrus_lsn FROM \"{}\" \
             WHERE _walrus_op = 't' AND _walrus_commit_lsn >= '{}' \
             ORDER BY _walrus_commit_lsn DESC, _walrus_lsn DESC LIMIT 1",
            raw.as_str(),
            after_lsn
        );
        let row: Option<(String, String)> = conn
            .query_row(&sql, [], |r| Ok((r.get(0)?, r.get(1)?)))
            .optional()
            .duck_with(|| format!("scan truncate boundary on {}", raw.as_str()))?;
        match row {
            None => Ok(None),
            Some((ct, lt)) => Ok(Some(TruncateBoundary {
                ct: ct.parse().map_err(|source| TransformerError::LsnParse {
                    field: "Ct",
                    source,
                })?,
                lt: lt.parse().map_err(|source| TransformerError::LsnParse {
                    field: "Lt",
                    source,
                })?,
            })),
        }
    }

    /// Render the full rendered SQL (truncate wipe + dedup `CREATE TEMP TABLE _batch` + guarded `MERGE
    /// INTO`), reading the un-transformed tail (`commit_lsn >= after_lsn` — the `>=` re-examines the
    /// equal-`commit_lsn` snapshot straddle, §7 break face A; and — if the tail has a truncate — only
    /// rows STRICTLY after the `(Ct, Lt)` tuple). Composite-PK-aware.
    #[must_use]
    pub fn render(&self, after_lsn: Lsn, boundary: Option<TruncateBoundary>) -> String {
        self.render_with(TRANSFORM_SQL, after_lsn, boundary)
    }

    /// Render the semantically identical DuckLake transform using two single-action MERGEs.
    #[must_use]
    pub fn render_ducklake(&self, after_lsn: Lsn, boundary: Option<TruncateBoundary>) -> String {
        self.render_with(TRANSFORM_DUCKLAKE_SQL, after_lsn, boundary)
    }

    fn render_with(
        &self,
        template: &str,
        after_lsn: Lsn,
        boundary: Option<TruncateBoundary>,
    ) -> String {
        let table = self.table.as_str();
        let pk = self.to_pk_names();
        let non_key = self.to_non_key_names();
        let all: Vec<&str> = self.mirror.iter().map(|c| c.name.as_str()).collect();
        // Incremental winners still have the raw emit shape. A recombined Tier-2 identity therefore
        // partitions by its expression over `raw_winner`, not by a mirror-only logical name.
        let raw_pk_list = self
            .mirror
            .iter()
            .filter(|c| c.is_key)
            .map(|c| raw_expr_for(&c.raw_value_expr, "raw_winner"))
            .collect::<Vec<_>>()
            .join(", ");
        let pk_join = pk
            .iter()
            .map(|c| format!("t.{c} IS NOT DISTINCT FROM s.{c}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        // MATCHED UPDATE assigns the non-key mirror columns; if a table is all-PK, a self-assignment keeps
        // the UPDATE valid (a no-op) so `d→i` still lands via the MATCHED branch. Every UPDATE also stamps
        // the hidden `_applied_*` guard columns with the winner's tuple (§7).
        let mut set_parts: Vec<String> = if non_key.is_empty() {
            // A column-less relation has no key to self-assign either. Render the guard stamps alone
            // rather than indexing: DuckDB then rejects the degenerate statement as a classified
            // `TransformerError::Duck` instead of the apply loop panicking mid-transaction.
            pk.first()
                .map(|k| format!("{k} = s.{k}"))
                .into_iter()
                .collect()
        } else {
            non_key.iter().map(|c| format!("{c} = s.{c}")).collect()
        };
        set_parts.push("_applied_commit_lsn = s._walrus_commit_lsn".into());
        set_parts.push("_applied_lsn = s._walrus_lsn".into());
        let set_cols = set_parts.join(", ");
        // INSERT carries the mirror columns PLUS the hidden guard columns seeded from the winner's tuple.
        let mut insert_col_parts: Vec<String> = all.iter().map(|c| (*c).to_string()).collect();
        insert_col_parts.push("_applied_commit_lsn".into());
        insert_col_parts.push("_applied_lsn".into());
        let insert_cols = insert_col_parts.join(", ");
        let mut insert_val_parts: Vec<String> = all.iter().map(|c| format!("s.{c}")).collect();
        insert_val_parts.push("s._walrus_commit_lsn".into());
        insert_val_parts.push("s._walrus_lsn".into());
        let insert_vals = insert_val_parts.join(", ");
        // The per-PK max-applied guard (§7, ⚠ extends architecture.md): a MUTATING branch fires only when
        // the winner's tuple is STRICTLY newer than what last shaped the mirror row. Row-value `>` is
        // lexicographic in DuckDB — exactly the `(commit_lsn, lsn)` order; do NOT hand-decompose it.
        let guard = "(s._walrus_commit_lsn, s._walrus_lsn) > \
                     (t._applied_commit_lsn, t._applied_lsn)";

        // `_batch`'s SELECT list: each mirror column's precomputed value expression over the raw winner
        // `s` (a Tier-2 recombine or a Tier-1 TOAST-resolved passthrough), then `_walrus_op` for the MERGE
        // branches and `_walrus_commit_lsn`/`_walrus_lsn` for the guard comparison and `_applied_*` stamps.
        let mut select_parts: Vec<String> = self
            .mirror
            .iter()
            .map(|c| format!("{} AS {}", c.incremental_value_expr, c.name))
            .collect();
        select_parts.push("s._walrus_op".to_string());
        select_parts.push("s._walrus_commit_lsn".to_string());
        select_parts.push("s._walrus_lsn".to_string());
        let resolved_select = select_parts.join(", ");
        // The truncate wipe (whole mirror) + the tuple-boundary window filter — empty when no truncate.
        let (truncate_wipe, truncate_bound) = match boundary {
            Some(TruncateBoundary { ct, lt }) => (
                format!("DELETE FROM \"{table}\";"),
                format!(" AND (_walrus_commit_lsn, _walrus_lsn) > ('{ct}', '{lt}')"),
            ),
            None => (String::new(), String::new()),
        };
        let after_lsn = after_lsn.to_string();
        render_transform_template(
            template,
            &[
                ("{table}", table),
                ("{raw_pk_list}", &raw_pk_list),
                ("{pk_join}", &pk_join),
                ("{set_cols}", &set_cols),
                ("{insert_cols}", &insert_cols),
                ("{insert_vals}", &insert_vals),
                ("{after_lsn}", &after_lsn),
                ("{truncate_wipe}", &truncate_wipe),
                ("{truncate_bound}", &truncate_bound),
                ("{resolved_select}", &resolved_select),
                ("{guard}", guard),
            ],
        )
    }

    /// The table name (for compaction / prune SQL that lives outside the template).
    #[must_use]
    pub fn table(&self) -> &str {
        self.table.as_str()
    }

    /// This table's CDC log `<table>_raw`, tagged [`Raw`] so it cannot land where a mirror name
    /// belongs. [`crate::compaction::prune_raw`] `DELETE`s from it — the same statement aimed at the
    /// mirror would erase every current row — so it takes the name from the typed layer rather than
    /// re-deriving the suffix beside its own SQL.
    ///
    /// Formats a fresh name per call — hence `to_`, next to the free [`Self::table`].
    #[must_use]
    pub fn to_raw(&self) -> DuckTable<Raw> {
        self.table.to_raw()
    }

    /// Render the atomic full-rebuild: `CREATE OR REPLACE TABLE <table>` over **retained raw ∪
    /// the current mirror injected as an LSN-floor baseline**, reusing the same dedup/collapse (TRUNCATE
    /// tuple boundary, TOAST resolution, `(commit_lsn, lsn)` ranking) as the incremental path — dropping
    /// `op='d'` winners. The mirror baseline (each row tagged at its own `_applied_*` tuple, so real newer
    /// raw out-ranks it) guarantees a PK whose raw evidence was already pruned still contributes its
    /// current value. Staged through a TEMP table so the statement never reads and replaces `<table>` at
    /// once; the swap + view recreate run inside one transaction ([`crate::compaction::full_rebuild`]).
    ///
    /// The union is over the MIRROR columns (not the raw emit columns), so a Tier-2 value's
    /// recombine happens in the raw arm (the mirror baseline can't be decomposed back into emit columns);
    /// the final resolve then only TOAST-resolves the Tier-1 columns and passes the recombined Tier-2 ones
    /// through.
    #[must_use]
    pub fn render_rebuild(&self, boundary: Option<TruncateBoundary>) -> String {
        let t = self.table.as_str();
        let raw_table = self.table.to_raw();
        let pk = self.to_pk_names();
        let pk_list = pk.join(", ");
        let pk_join = pk
            .iter()
            .map(|c| format!("t.{c} IS NOT DISTINCT FROM s.{c}"))
            .collect::<Vec<_>>()
            .join(" AND ");
        // The raw arm collapses each mirror column FROM the raw emit columns (aliased `s`): a Tier-2
        // recombine, or a plain `s.col`. Unchanged-TOAST resolution happens after winner selection,
        // while its metadata and tuple are still available.
        let raw_exprs: Vec<String> = self
            .mirror
            .iter()
            .map(|c| format!("{} AS {}", c.raw_value_expr, c.name))
            .collect();
        let mirror_names = self
            .mirror
            .iter()
            .map(|c| c.name.clone())
            .collect::<Vec<_>>()
            .join(", ");
        // The union feeding the dedup: every retained raw change (op<>'t'), collapsed to mirror columns,
        // plus the current mirror as a baseline row per PK, tagged at that PK's `_applied_*` tuple with an
        // empty unchanged_toast meta. Both arms carry the same mirror-column names → `UNION ALL BY NAME`.
        let src = format!(
            "SELECT {raw}, s.walrus_extractor_meta AS walrus_extractor_meta, \
                 s._walrus_op AS _walrus_op, s._walrus_commit_lsn AS _walrus_commit_lsn, \
                 s._walrus_lsn AS _walrus_lsn FROM \"{raw_table}\" s WHERE s._walrus_op <> 't' \
             UNION ALL BY NAME \
             SELECT {mirror_names}, '{{}}' AS walrus_extractor_meta, 'i' AS _walrus_op, \
                 _applied_commit_lsn AS _walrus_commit_lsn, \
                 _applied_lsn AS _walrus_lsn FROM \"{t}\"",
            raw = raw_exprs.join(", "),
            raw_table = raw_table.as_str(),
        );
        // The truncate tuple boundary applies to the union (the mirror baseline is post-truncate by
        // construction, so it survives); empty when the retained tail holds no truncate.
        let truncate_bound = match boundary {
            Some(TruncateBoundary { ct, lt }) => {
                format!(" WHERE (_walrus_commit_lsn, _walrus_lsn) > ('{ct}', '{lt}')")
            }
            None => String::new(),
        };
        // The rebuilt row list resolves every source column's unchanged-TOAST marker over `s` (the
        // collapsed winner), the retained raw back-scan, and finally the current mirror `t`. Then the
        // `_applied_*` stamps are re-seeded from the winner's tuple for the incremental guard.
        let mut cols: Vec<String> = self
            .mirror
            .iter()
            .map(|c| format!("{} AS {}", c.rebuild_value_expr, c.name))
            .collect();
        cols.push("s._walrus_commit_lsn AS _applied_commit_lsn".to_string());
        cols.push("s._walrus_lsn AS _applied_lsn".to_string());
        let resolved = cols.join(", ");
        format!(
            "CREATE OR REPLACE TEMP TABLE \"_walrus_rebuild_{t}\" AS \
             WITH src AS ({src}), \
             winners AS (SELECT * FROM src{truncate_bound} \
                 QUALIFY row_number() OVER (PARTITION BY {pk_list} \
                    ORDER BY _walrus_commit_lsn DESC, _walrus_lsn DESC, \
                              CASE WHEN _walrus_op = 'd' THEN 0 ELSE 1 END DESC) = 1) \
             SELECT {resolved} FROM winners s LEFT JOIN \"{t}\" t ON {pk_join} \
             WHERE s._walrus_op <> 'd'; \
             DROP VIEW IF EXISTS \"{t}_current\"; \
             CREATE OR REPLACE TABLE \"{t}\" AS SELECT * FROM \"_walrus_rebuild_{t}\"; \
             DROP TABLE \"_walrus_rebuild_{t}\"; \
             {view}",
            view = crate::duck::user_view_sql(t),
        )
    }
}

/// Run the transform against `<table>_raw`, reading only `commit_lsn > after_lsn`: resolve the latest
/// truncate `(Ct, Lt)`, wipe the mirror if present, then dedup + MERGE the post-boundary tail. Phase B
/// calls this inside a DuckDB transaction so the wipe + repopulation are atomic.
///
/// # Errors
///
/// Returns [`TransformerError::LsnParse`] for a malformed truncate boundary, or [`TransformerError::Duck`] if
/// DuckDB rejects the rendered transform batch.
pub fn apply_transform(
    conn: &duckdb::Connection,
    t: &TransformSql,
    after_lsn: Lsn,
) -> Result<(), TransformerError> {
    let boundary = t.latest_truncate(conn, after_lsn)?;
    conn.execute_batch(&t.render(after_lsn, boundary))
        .duck_with(|| format!("transform {}", t.table()))
}

/// DuckLake form of [`apply_transform`], split to respect DuckLake's one matched mutation per
/// `MERGE` restriction while retaining one outer transaction.
///
/// # Errors
///
/// Returns [`TransformerError::LsnParse`] for a malformed truncate boundary, or [`TransformerError::Duck`] if
/// DuckLake rejects either rendered `MERGE`.
pub fn apply_transform_ducklake(
    conn: &duckdb::Connection,
    t: &TransformSql,
    after_lsn: Lsn,
) -> Result<(), TransformerError> {
    let boundary = t.latest_truncate(conn, after_lsn)?;
    conn.execute_batch(&t.render_ducklake(after_lsn, boundary))
        .duck_with(|| format!("transform {} in DuckLake", t.table()))
}

#[cfg(test)]
#[path = "transform_test.rs"]
mod tests;
