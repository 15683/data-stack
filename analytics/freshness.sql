-- Metabase question: Last API fetch. Empty snapshots are successful fetches too.
SELECT max(fetched_at) AS last_successful_fetch
FROM dm.ingestion_status WHERE status IN ('completed', 'empty');
