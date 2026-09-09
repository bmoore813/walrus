use super::*;

#[test]
fn init_is_idempotent() {
    init();
    let first_handle = HANDLE.get().expect("init installs the recorder");
    let first = render();
    init();
    init();
    let after_handle = HANDLE.get().expect("the recorder remains installed");
    let after = render();

    assert!(
        std::ptr::eq(first_handle, after_handle),
        "repeated init must retain the same recorder handle"
    );
    assert!(!first.is_empty(), "init must make metrics renderable");
    for name in names::EXTRACTOR_ALL {
        assert!(after.contains(name), "extractor series {name} disappeared");
    }

    let source = include_str!("metrics.rs");
    assert!(
        source.contains(concat!("#[", "expect(")),
        "the sanctioned expect must use a checked lint expectation"
    );
    assert!(
        source.contains("BUG: a second global Prometheus recorder was installed"),
        "the invariant panic must carry the BUG marker"
    );
}

#[test]
fn render_lists_every_series() {
    init();
    init_table_series("public.demo");
    // Exercise a couple of helpers to prove the wired path renders too.
    set_wal_status(0);
    record_batch_flush(0.01, 4096);
    set_transform_lag("public.demo", 0);

    let text = render();
    for name in names::EXTRACTOR_ALL {
        assert!(
            text.contains(name),
            "extractor series {name} missing from /metrics"
        );
    }
    for name in names::TRANSFORMER_ALL {
        assert!(
            text.contains(name),
            "transformer series {name} missing from /metrics"
        );
    }
}
