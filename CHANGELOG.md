# Changelog

## 0.3.0

- Isolated deterministic demo and persistent local build/deploy configuration.
- Ingestion result ledger, rejected-event repair and bounded DWH verification in Airflow.
- Explicit backfill windows and season support; season-aware analytical views.
- Consumer readiness/liveness, ingestion/backup alerts and narrower ingress policies.
- Metabase session/encryption secrets, API-token rotation and scoped DWH grants.
- Safer local backup retention, restore validation and external backup export.
- Actual Helm Kafka/StatsD settings exercised in tests; opt-in full k3s acceptance.
- Separate CI and manual CD, Windows launcher checks and dependency update configuration.
- Architecture, data contract, ADRs, dashboard SQL and operational exercises.

## 0.2.0

Initial k3s migration: Airflow 3, Garage, Kafka KRaft, Python/psycopg, PostgreSQL,
Metabase and Prometheus. Fixed initial Kafka integer rendering, StatsD mapping,
Metabase Metaspace, Airflow SimpleAuth writable file and optional raw_key handling.
