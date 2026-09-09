-- Load generator — wide-text rows: upsert `orders` with a ~1 KB `note`, exercising the decoder's
-- text path (per-cell String allocation) and larger Parquet row groups end-to-end.
\set id random(1, 1000000)
\set amt random(1, 1000000)
INSERT INTO orders (id, status, amount, note)
VALUES (:id, 'wide', :amt / 100.0, repeat('x', 1024))
ON CONFLICT (id) DO UPDATE SET amount = EXCLUDED.amount, note = repeat('y', 1024);
