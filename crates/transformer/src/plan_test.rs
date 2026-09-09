use super::*;
use common::{PgColumn, PgRelation, ReplicaIdentity, Tier, TypeDescriptor, TypeMeta, oids};

/// The three flat columns an `interval` emits (months / days / microseconds).
const INTERVAL_EMIT: [&str; 3] = ["e_mo:INT32", "e_d:INT32", "e_us:INT64"];

/// The five flat siblings an `int4range` emits — DuckDB has no range type.
const RANGE_EMIT: [&str; 5] = [
    "span_lower:INT32",
    "span_upper:INT32",
    "span_lower_inc:BOOLEAN",
    "span_upper_inc:BOOLEAN",
    "span_empty:BOOLEAN",
];

fn col(name: &str, type_oid: u32, is_key: bool) -> PgColumn {
    PgColumn {
        name: name.into(),
        type_oid,
        type_modifier: -1,
        is_key,
    }
}

/// An `int4` replica-identity key column.
fn key(name: &str) -> PgColumn {
    col(name, oids::INT4, true)
}

/// A non-key `text` column.
fn text(name: &str) -> PgColumn {
    col(name, oids::TEXT, false)
}

fn relation(columns: Vec<PgColumn>) -> PgRelation {
    PgRelation {
        oid: 42,
        schema: "public".into(),
        name: "orders".into(),
        replica_identity: ReplicaIdentity::Default,
        columns,
    }
}

/// A Tier-1 `text` descriptor to spread over. Each test overrides only the fields `plan_column`
/// actually reads — `pg_type_oid`, `duckdb`, `emit` — plus `tier`, which it deliberately ignores.
fn descriptor(column: &str) -> TypeDescriptor {
    TypeDescriptor {
        column: column.into(),
        pg_type_oid: oids::TEXT,
        pg_type: "text".into(),
        tier: Tier::One,
        arrow: "Utf8".into(),
        duckdb: "VARCHAR".into(),
        emit: Vec::new(),
        recombine: None,
        meta: TypeMeta::default(),
    }
}

/// `["name:ARROW", …]` as the descriptor's owned `emit` list.
fn emit(entries: &[&str]) -> Vec<String> {
    entries.iter().map(ToString::to_string).collect()
}

fn raw_names(plan: &TablePlan) -> Vec<&str> {
    plan.raw_cols.iter().map(|c| c.name.as_str()).collect()
}

fn raw_shape(plan: &TablePlan) -> Vec<(&str, &str)> {
    plan.raw_cols
        .iter()
        .map(|c| (c.name.as_str(), c.duckdb_type.as_str()))
        .collect()
}

fn mirror_names(plan: &TablePlan) -> Vec<&str> {
    plan.mirror_cols.iter().map(|c| c.name.as_str()).collect()
}

/// `MirrorValue` has no `PartialEq` (a plan is never compared in production), so render it.
fn mirror_value(c: &MirrorCol) -> String {
    match &c.value {
        MirrorValue::Passthrough => "passthrough".to_string(),
        MirrorValue::Recombine(expr) => format!("recombine({expr})"),
    }
}

#[test]
fn a_parameterised_decimal_passes_through_as_a_slice_of_its_own_input() {
    let arrow = "DECIMAL(10,2)";

    let duck = emit_arrow_to_duck(arrow);

    assert_eq!(duck, "DECIMAL(10,2)");
    assert_eq!(duck.as_ptr(), arrow.as_ptr(), "must not allocate");
}

#[test]
fn a_nested_or_unknown_emit_type_falls_back_rather_than_failing() {
    // FIXEDBINARY is only ever a Tier-1 uuid, which takes the descriptor's `duckdb` instead;
    // anything else unrecognised must still land in a column DuckDB can hold.
    assert_eq!(emit_arrow_to_duck("FIXEDBINARY(16)"), "BLOB");
    assert_eq!(emit_arrow_to_duck("STRUCT"), "VARCHAR");
    assert_eq!(emit_arrow_to_duck("LIST"), "VARCHAR");

    // The one mapping whose DuckDB spelling is several words.
    assert_eq!(
        emit_arrow_to_duck("TIMESTAMPTZ"),
        "TIMESTAMP WITH TIME ZONE"
    );
}

#[test]
fn an_emit_entry_splits_on_its_last_colon_so_a_colon_in_the_name_survives() {
    // `rsplit_once`, not `split_once`: a quoted Postgres column name may itself contain a colon,
    // and only the type suffix is guaranteed to be last.
    let entries = vec!["odd:name:INT32".to_string()];

    let pairs = parse_emit(&entries);

    assert_eq!(pairs, vec![("odd:name", "INTEGER")]);
}

#[test]
fn a_registry_plan_rejects_malformed_emit_entries() {
    let rel = relation(vec![text("id")]);
    for entry in ["no_colon_here", ":INT64", "id:"] {
        let mut descriptor = descriptor("id");
        descriptor.emit = vec![entry.to_string()];

        assert!(matches!(
            TablePlan::from_registry(&rel, &[descriptor]),
            Err(TransformerError::ManifestInvariant { message })
                if message.contains("malformed emit entry")
        ));
    }
}

#[test]
fn a_recombine_expression_is_built_only_at_the_arity_its_type_needs() {
    // The slice patterns are the arity check: a truncated or over-long emit list must fall through
    // to `None` (a flat plan) rather than name the wrong column in the SQL.
    let three = [("m", "INTEGER"), ("d", "INTEGER"), ("us", "BIGINT")];
    let two = [("us", "BIGINT"), ("off", "INTEGER")];
    let interval = "to_months(s.m) + to_days(s.d) + to_microseconds(s.us)";
    let timetz = "make_timetz(s.us, s.off)";

    let months_days_micros = recombine_expr(INTERVAL, &three);
    let micros_offset = recombine_expr(TIMETZ, &two);

    assert_eq!(months_days_micros.as_deref(), Some(interval));
    assert_eq!(micros_offset.as_deref(), Some(timetz));

    assert!(recombine_expr(TIMETZ, &three).is_none());
    assert!(recombine_expr(INTERVAL, &three[..2]).is_none());
    assert!(recombine_expr(oids::INT4, &three).is_none());
}

#[test]
fn the_tier1_plan_makes_every_non_key_column_toast_resolvable() {
    // A key column can never carry the unchanged-TOAST sentinel — it is always sent in full — so
    // only the non-key columns are worth back-scanning the raw table for.
    let rel = relation(vec![key("id"), text("body")]);

    let plan = TablePlan::tier1(&rel).unwrap();

    let table: &str = &plan.table;
    assert_eq!(table, "orders");
    assert_eq!(
        raw_shape(&plan),
        vec![("id", "INTEGER"), ("body", "VARCHAR")]
    );
    assert_eq!(mirror_value(&plan.mirror_cols[0]), "passthrough");
    assert_eq!(mirror_value(&plan.mirror_cols[1]), "passthrough");
    assert_eq!(plan.mirror_cols[0].toast_source, None);
    assert_eq!(plan.mirror_cols[1].toast_source.as_deref(), Some("body"));
}

#[test]
fn a_column_with_no_descriptor_keeps_the_tier1_shape() {
    let rel = relation(vec![key("id"), text("body")]);
    let body = TypeDescriptor {
        emit: emit(&["body:VARCHAR"]),
        ..descriptor("body")
    };

    let plan = TablePlan::from_registry(&rel, &[body]).unwrap();

    // `id` has no registry row, so it falls back to `duck::duck_type`.
    assert_eq!(
        raw_shape(&plan),
        vec![("id", "INTEGER"), ("body", "VARCHAR")]
    );
    assert_eq!(mirror_value(&plan.mirror_cols[0]), "passthrough");
}

#[test]
fn a_tier2_interval_collapses_its_emit_columns_into_one_mirror_column() {
    let rel = relation(vec![key("id"), col("elapsed", INTERVAL, false)]);
    let elapsed = TypeDescriptor {
        pg_type_oid: INTERVAL,
        tier: Tier::Two,
        duckdb: "INTERVAL".into(),
        emit: emit(&INTERVAL_EMIT),
        ..descriptor("elapsed")
    };

    let plan = TablePlan::from_registry(&rel, &[elapsed]).unwrap();

    // Three raw columns in, one mirror column out — named for the SOURCE column, not the emits.
    assert_eq!(raw_names(&plan), vec!["id", "e_mo", "e_d", "e_us"]);
    assert_eq!(mirror_names(&plan), vec!["id", "elapsed"]);
    assert_eq!(plan.mirror_cols[1].duckdb_type, "INTERVAL");

    let recombined = mirror_value(&plan.mirror_cols[1]);
    assert!(recombined.starts_with("recombine("), "got {recombined}");
    assert_eq!(plan.mirror_cols[1].toast_source.as_deref(), Some("elapsed"));
    // This source column is non-key, so its recombined mirror value remains non-key.
    assert!(!plan.mirror_cols[1].is_key);
}

#[test]
fn tier2_replica_identity_columns_propagate_to_every_mirror_component() {
    let mut rel = relation(vec![
        key("id"),
        col("elapsed", INTERVAL, true),
        col("span", oids::INT4RANGE, true),
    ]);
    rel.replica_identity = ReplicaIdentity::Full;
    let elapsed = TypeDescriptor {
        pg_type_oid: INTERVAL,
        tier: Tier::Two,
        duckdb: "INTERVAL".into(),
        emit: emit(&INTERVAL_EMIT),
        ..descriptor("elapsed")
    };
    let span = TypeDescriptor {
        pg_type_oid: oids::INT4RANGE,
        tier: Tier::Two,
        duckdb: "IGNORED".into(),
        emit: emit(&RANGE_EMIT),
        ..descriptor("span")
    };

    let plan = TablePlan::from_registry(&rel, &[elapsed, span]).unwrap();

    let elapsed = plan
        .mirror_cols
        .iter()
        .find(|c| c.name == "elapsed")
        .unwrap();
    assert!(elapsed.is_key);
    assert_eq!(elapsed.toast_source, None);
    for sibling in plan
        .mirror_cols
        .iter()
        .filter(|c| c.name.starts_with("span_"))
    {
        assert!(sibling.is_key, "{}", sibling.name);
        assert_eq!(sibling.toast_source, None, "{}", sibling.name);
    }
}

#[test]
fn a_tier2_range_fans_out_to_flat_mirror_columns() {
    // DuckDB has no range type, so a range IS its five siblings: one mirror column per emit
    // column. They are not independent scalar sentinels: every sibling resolves against the
    // original source-column name recorded in metadata. The descriptor's `duckdb` is never read on
    // this path — the emit types are.
    let rel = relation(vec![key("id"), col("span", oids::INT4RANGE, false)]);
    let span = TypeDescriptor {
        pg_type_oid: oids::INT4RANGE,
        tier: Tier::Two,
        duckdb: "IGNORED".into(),
        emit: emit(&RANGE_EMIT),
        ..descriptor("span")
    };

    let plan = TablePlan::from_registry(&rel, &[span]).unwrap();

    assert_eq!(raw_names(&plan), mirror_names(&plan));
    assert_eq!(mirror_names(&plan).len(), 6);
    assert_eq!(raw_shape(&plan)[3], ("span_lower_inc", "BOOLEAN"));

    for c in plan.mirror_cols.iter().skip(1) {
        assert!(!c.is_key, "{}", c.name);
        assert_eq!(mirror_value(c), "passthrough", "{}", c.name);
        assert_eq!(c.toast_source.as_deref(), Some("span"), "{}", c.name);
    }
}

#[test]
fn the_dispatch_reads_the_emit_shape_not_the_declared_tier() {
    // A Tier-2 `point` stays NESTED — one struct emit column — so it must take the single-column
    // arm beside Tier-1 and Tier-3 rather than fan out. Dispatching on `d.tier` would split it
    // into siblings the extractor never wrote to the Parquet file.
    let rel = relation(vec![key("id"), col("loc", oids::POINT, false)]);
    let loc = TypeDescriptor {
        pg_type_oid: oids::POINT,
        tier: Tier::Two,
        duckdb: "STRUCT(x DOUBLE, y DOUBLE)".into(),
        emit: emit(&["loc:STRUCT"]),
        ..descriptor("loc")
    };

    let plan = TablePlan::from_registry(&rel, &[loc]).unwrap();

    assert_eq!(mirror_names(&plan), vec!["id", "loc"]);
    assert_eq!(plan.raw_cols[1].duckdb_type, "STRUCT(x DOUBLE, y DOUBLE)");
}

#[test]
fn a_numerics_precise_decimal_comes_from_the_emit_type_not_the_descriptor() {
    // The descriptor's `duckdb` for a numeric is the bare `DECIMAL`; the precision and scale live
    // only in the emit type, and dropping them would silently round every stored amount.
    let rel = relation(vec![key("id"), col("amount", oids::NUMERIC, false)]);
    let amount = TypeDescriptor {
        pg_type_oid: oids::NUMERIC,
        duckdb: "DECIMAL".into(),
        emit: emit(&["amount:DECIMAL(10,2)"]),
        ..descriptor("amount")
    };

    let plan = TablePlan::from_registry(&rel, &[amount]).unwrap();

    assert_eq!(plan.raw_cols[1].duckdb_type, "DECIMAL(10,2)");
    assert_eq!(plan.mirror_cols[1].duckdb_type, "DECIMAL(10,2)");
}

#[test]
fn registry_plan_rejects_reserved_and_duplicate_physical_names() {
    let reserved = relation(vec![key("id"), text("_walrus_op")]);
    assert!(matches!(
        TablePlan::from_registry(&reserved, &[]),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("reserved raw physical column")
    ));

    let duplicate_raw = relation(vec![
        key("id"),
        col("first", oids::INT4RANGE, false),
        col("second", oids::INT4RANGE, false),
    ]);
    let first = TypeDescriptor {
        column: "first".into(),
        pg_type_oid: oids::INT4RANGE,
        tier: Tier::Two,
        emit: emit(&["shared_lower:INT32", "first_upper:INT32"]),
        ..descriptor("first")
    };
    let second = TypeDescriptor {
        column: "second".into(),
        pg_type_oid: oids::INT4RANGE,
        tier: Tier::Two,
        emit: emit(&["shared_lower:INT32", "second_upper:INT32"]),
        ..descriptor("second")
    };
    assert!(matches!(
        TablePlan::from_registry(&duplicate_raw, &[first, second]),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("duplicate or reserved raw physical column")
    ));

    let duplicate_mirror = relation(vec![
        key("id"),
        col("elapsed", INTERVAL, false),
        col("span", oids::INT4RANGE, false),
    ]);
    let elapsed = TypeDescriptor {
        column: "elapsed".into(),
        pg_type_oid: INTERVAL,
        tier: Tier::Two,
        duckdb: "INTERVAL".into(),
        emit: emit(&INTERVAL_EMIT),
        ..descriptor("elapsed")
    };
    let span = TypeDescriptor {
        column: "span".into(),
        pg_type_oid: oids::INT4RANGE,
        tier: Tier::Two,
        emit: emit(&["elapsed:INT32", "span_upper:INT32"]),
        ..descriptor("span")
    };
    assert!(matches!(
        TablePlan::from_registry(&duplicate_mirror, &[elapsed, span]),
        Err(TransformerError::ManifestInvariant { message })
            if message.contains("duplicate or reserved mirror physical column")
    ));
}

#[test]
fn tier1_plan_rejects_source_names_that_need_quotes() {
    for name in ["CustomerID", "customer-id", "select", "authorization"] {
        let rel = relation(vec![key("id"), text(name)]);
        assert!(
            matches!(
                TablePlan::tier1(&rel),
                Err(TransformerError::ManifestInvariant { message })
                    if message.contains("invalid raw physical column")
            ),
            "{name:?}"
        );
    }
}
