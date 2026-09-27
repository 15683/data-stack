-- Metabase question: Last 20 ingestion results. Display as a table.
SELECT fetched_at, competition, date_from, date_to, expected_count,
       processed_count, rejected_count, status
FROM dm.ingestion_status
ORDER BY fetched_at DESC LIMIT 20;
