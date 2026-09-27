CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS ops;
CREATE SCHEMA IF NOT EXISTS dm;

CREATE TABLE IF NOT EXISTS ods.matches (
    match_id INTEGER PRIMARY KEY,
    utc_date TIMESTAMPTZ NOT NULL,
    status TEXT,
    matchday INTEGER,
    stage TEXT,
    match_group TEXT,
    home_team_id INTEGER,
    home_team_name TEXT,
    away_team_id INTEGER,
    away_team_name TEXT,
    winner TEXT,
    score_full_time_home INTEGER,
    score_full_time_away INTEGER,
    score_half_time_home INTEGER,
    score_half_time_away INTEGER,
    competition_code TEXT,
    competition_name TEXT,
    source_updated_at TIMESTAMPTZ NOT NULL,
    fetched_at TIMESTAMPTZ NOT NULL,
    raw_object_key TEXT NOT NULL,
    load_ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS matches_utc_date_idx ON ods.matches(utc_date);
CREATE TABLE IF NOT EXISTS ops.processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS processed_events_time_idx ON ops.processed_events(processed_at);
CREATE TABLE IF NOT EXISTS ops.ingestions (
    raw_object_key TEXT PRIMARY KEY,
    fetched_at TIMESTAMPTZ NOT NULL,
    match_count INTEGER NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE OR REPLACE VIEW dm.team_goals AS
SELECT team_name, sum(goals) AS goals
FROM (
    SELECT home_team_name AS team_name, score_full_time_home AS goals
    FROM ods.matches WHERE status = 'FINISHED'
    UNION ALL
    SELECT away_team_name, score_full_time_away
    FROM ods.matches WHERE status = 'FINISHED'
) scores GROUP BY team_name;
