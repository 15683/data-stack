ALTER TABLE ods.matches ADD COLUMN IF NOT EXISTS season_id INTEGER;
ALTER TABLE ods.matches ADD COLUMN IF NOT EXISTS season_start DATE;
ALTER TABLE ods.matches ADD COLUMN IF NOT EXISTS season_end DATE;

ALTER TABLE ops.ingestions ADD COLUMN run_id TEXT;
ALTER TABLE ops.ingestions ADD COLUMN date_from DATE;
ALTER TABLE ops.ingestions ADD COLUMN date_to DATE;
ALTER TABLE ops.ingestions ADD COLUMN competition TEXT;
ALTER TABLE ops.ingestions ADD COLUMN source_mode TEXT NOT NULL DEFAULT 'live';
ALTER TABLE ops.ingestions ADD COLUMN processed_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ops.ingestions ADD COLUMN rejected_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ops.ingestions ADD COLUMN completion_received BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE ops.ingestions ADD COLUMN status TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE ops.ingestions ADD COLUMN started_at TIMESTAMPTZ NOT NULL DEFAULT now();
ALTER TABLE ops.ingestions ALTER COLUMN completed_at DROP NOT NULL;
ALTER TABLE ops.ingestions ALTER COLUMN completed_at DROP DEFAULT;
CREATE TABLE ops.event_results (
    event_id TEXT PRIMARY KEY,
    raw_object_key TEXT NOT NULL REFERENCES ops.ingestions(raw_object_key),
    outcome TEXT NOT NULL CHECK (outcome IN ('processed', 'rejected')),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX event_results_raw_key_idx ON ops.event_results(raw_object_key);

CREATE VIEW dm.ingestion_status AS
SELECT raw_object_key, run_id, date_from, date_to, competition, source_mode,
       match_count AS expected_count, processed_count, rejected_count, status,
       fetched_at, started_at, completed_at
FROM ops.ingestions;

CREATE VIEW dm.team_season_stats AS
WITH team_matches AS (
    SELECT match_id, home_team_id AS team_id, home_team_name AS team_name,
           competition_code, season_id, season_start, season_end, utc_date,
           score_full_time_home AS goals_for, score_full_time_away AS goals_against
    FROM ods.matches WHERE status = 'FINISHED'
    UNION ALL
    SELECT match_id, away_team_id, away_team_name, competition_code,
           season_id, season_start, season_end, utc_date,
           score_full_time_away, score_full_time_home
    FROM ods.matches WHERE status = 'FINISHED'
)
SELECT team_id, competition_code, season_id, season_start, season_end,
       (array_agg(team_name ORDER BY utc_date DESC, match_id DESC))[1] AS team_name,
       count(*) AS matches_played, sum(goals_for) AS goals_for,
       sum(goals_against) AS goals_against,
       count(*) FILTER (WHERE goals_for > goals_against) AS wins,
       count(*) FILTER (WHERE goals_for = goals_against) AS draws,
       count(*) FILTER (WHERE goals_for < goals_against) AS losses
FROM team_matches
GROUP BY team_id, competition_code, season_id, season_start, season_end;
