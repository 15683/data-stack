-- Metabase question: Goals by team. Display as horizontal bars.
SELECT team_name, competition_code, season_id, goals_for, goals_against, wins, draws, losses
FROM dm.team_season_stats
WHERE true
[[AND extract(year FROM season_start) = {{season}}]]
ORDER BY goals_for DESC, team_name;
