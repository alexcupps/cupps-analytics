-- Our data set (players with both CFB and NFL stats)
SELECT COUNT(DISTINCT p.player_id) AS players_with_both_stats
FROM player p
JOIN cfb_player_year_stats c ON p.player_id = c.player_id
JOIN nfl_player_year_stats n ON p.player_id = n.player_id;


-- Players who played in college but did not make the NFL at any point
SELECT COUNT(DISTINCT p.player_id) AS players_only_in_cfb
FROM player p
JOIN cfb_player_year_stats c ON p.player_id = c.player_id
LEFT JOIN nfl_player_year_stats n ON p.player_id = n.player_id
WHERE n.player_id IS NULL;


-- Players with NFL data but no CFB data in our DB (should be zero, needs to be addressed if not)
SELECT COUNT(DISTINCT p.player_id) AS players_only_in_nfl
FROM player p
JOIN nfl_player_year_stats n ON p.player_id = n.player_id
LEFT JOIN cfb_player_year_stats c ON p.player_id = c.player_id
WHERE c.player_id IS NULL;