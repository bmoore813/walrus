WITH active AS MATERIALIZED (
    SELECT r.*
    FROM public.walrus_table_reload r
    WHERE r.epoch = $1
      AND r.source_schema = $2
      AND r.source_table = $3
      AND r.status IN ('export_complete', 'publishing')
    ORDER BY r.reload_id DESC
    LIMIT 1
),
locked_ownership AS MATERIALIZED (
    SELECT o.owner_pod, o.fencing_token
    FROM public.walrus_table_ownership o
    WHERE o.epoch = $1
      AND o.source_schema = $2
      AND o.source_table = $3
      AND o.owner_pod = $4
      AND o.fencing_token = $5
      AND o.lease_expiry > statement_timestamp()
    FOR UPDATE OF o
),
eligible AS MATERIALIZED (
    SELECT r.*, o.owner_pod AS claiming_owner_pod,
           o.fencing_token AS claiming_fencing_token
    FROM active r
    CROSS JOIN locked_ownership o
    WHERE r.start_lsn IS NOT NULL
      AND r.final_lsn IS NOT NULL
      AND r.schema_version IS NOT NULL
      AND r.final_lsn >= r.start_lsn
      AND (
        SELECT count(*)
        FROM public.walrus_table_reload_marker m
        WHERE m.reload_id = r.reload_id
          AND (
            (m.marker_kind = 'baseline'
             AND m.lsn = r.start_lsn
             AND m.schema_version = r.schema_version)
            OR
            (m.marker_kind = 'end'
             AND m.lsn = r.final_lsn
             AND m.schema_version = r.schema_version)
          )
      ) = 2
),
adoption_protocol AS MATERIALIZED (
    SELECT set_config('walrus.reload_publication_adopt_protocol', '2', true) AS protocol
    WHERE EXISTS (SELECT 1 FROM eligible)
),
claimed AS MATERIALIZED (
    UPDATE public.walrus_table_reload r
    SET status = 'publishing',
        publication_nonce = COALESCE(r.publication_nonce, gen_random_uuid()),
        publisher_owner_pod = e.claiming_owner_pod,
        publisher_fencing_token = e.claiming_fencing_token,
        publishing_at = COALESCE(r.publishing_at, now()),
        updated_at = now()
    FROM eligible e
    CROSS JOIN adoption_protocol protocol
    WHERE r.reload_id = e.reload_id
      AND r.status IN ('export_complete', 'publishing')
      AND protocol.protocol = '2'
    RETURNING r.reload_id, r.epoch, r.source_schema, r.source_table, r.status,
              r.start_lsn, r.final_lsn, r.schema_version,
              r.publication_nonce, r.publisher_owner_pod, r.publisher_fencing_token
), deauthorized AS MATERIALIZED (
    SELECT pg_catalog.set_config('walrus.reload_publication_adopt_protocol', '', true) AS protocol
    FROM (SELECT count(*) FROM claimed) AS operation_result
)
SELECT active.reload_id AS candidate_reload_id,
       EXISTS (SELECT 1 FROM locked_ownership) AS ownership_valid,
       EXISTS (SELECT 1 FROM eligible) AS boundaries_valid,
       claimed.reload_id, claimed.epoch, claimed.source_schema, claimed.source_table,
       claimed.status, claimed.start_lsn, claimed.final_lsn, claimed.schema_version,
       claimed.publication_nonce, claimed.publisher_owner_pod, claimed.publisher_fencing_token
FROM deauthorized
LEFT JOIN active ON true
LEFT JOIN claimed ON true
WHERE deauthorized.protocol = '';
