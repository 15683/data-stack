-- Metabase question: Matches and goals. Optional numeric variable: season (start year).
SELECT count(*) AS matches,
       count(*) FILTER (WHERE status='FINISHED') AS finished,
       sum(score_full_time_home + score_full_time_away)
           FILTER (WHERE status='FINISHED') AS goals
FROM ods.matches
WHERE true
[[AND extract(year FROM season_start) = {{season}}]];
