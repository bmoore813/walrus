-- Load generator — a weighted insert/update/delete mix on `orders` (single-PK).
-- pgbench runs this once per transaction. ~60% insert / 30% update / 10% delete over a bounded key
-- space, so keys collide and all three MERGE branches (insert/update/delete) get exercised downstream.
\set id random(1, 50000)
\set amt random(1, 1000000)
\set r random(1, 10)
\if :r <= 6
  INSERT INTO orders (id, status, amount, note)
  VALUES (:id, 'ins', :amt / 100.0, 'note ' || :id)
  ON CONFLICT (id) DO NOTHING;
\elif :r <= 9
  UPDATE orders SET status = 'upd', amount = :amt / 100.0 WHERE id = :id;
\else
  DELETE FROM orders WHERE id = :id;
\endif
