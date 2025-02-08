UPDATE cfb_player_year_stats s
JOIN player p ON s.player_id = p.player_id
SET s.season_age = TIMESTAMPDIFF(YEAR, p.birthday, STR_TO_DATE(CONCAT(s.year, '-08-07'), '%Y-%m-%d'))
WHERE p.birthday IS NOT NULL;