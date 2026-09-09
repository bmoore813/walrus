-- Load generator — one large transaction: a single 200k-row upsert into `items`. With the compose
-- `logical_decoding_work_mem=64kB`, the reorder buffer spills early, so the extractor decodes this as a
-- **streamed** transaction (Stream Start/Stop frames; `walrus_extractor_spill_total` moves under a low
-- inflight ceiling). Run once (`pgbench -t 1 -c 1`).
INSERT INTO items (id, label, qty)
SELECT g, 'item_' || g, g % 100
FROM generate_series(1, 200000) AS g
ON CONFLICT (id) DO UPDATE SET label = EXCLUDED.label, qty = EXCLUDED.qty;
