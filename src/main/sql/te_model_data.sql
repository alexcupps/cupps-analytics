CREATE OR REPLACE VIEW te_model_data AS
SELECT
    p.player_id,
    p.name,
    p.position,
    p.draft_cap,
    p.production_score,
	p.size_score,
    p.cupps_score,
    p.ras,
    p.height,
    p.weight,
    
    -- College performance metrics
    ROUND(AVG(c.fppg), 2) AS avg_fppg_college,
	ROUND(MAX(c.fppg), 2) AS peak_fppg_college,
	ROUND(AVG(c.rush_yds + c.rec_yds), 2) AS avg_scrim_yds,
	ROUND(MAX(c.rush_yds + c.rec_yds), 2) AS peak_scrim_yds,
	ROUND(SUM(DISTINCT c.rush_yds) + SUM(DISTINCT c.rec_yds), 2) AS total_scrim_yds,
	ROUND(AVG(c.receptions), 2) AS avg_receptions,
	ROUND(MAX(c.receptions), 2) AS peak_receptions,
	ROUND(AVG(c.games_played), 2) AS avg_games_played,
	ROUND(MAX(c.games_played), 2) AS peak_games_played,
	ROUND(AVG(c.rush_td + c.rec_td), 2) AS avg_tds,
	ROUND(MAX(c.rush_td + c.rec_td), 2) AS peak_tds,
    ROUND(AVG(c.pff_rec_grade), 2) AS avg_pff_rec_grade,
	ROUND(MAX(c.pff_rec_grade), 2) AS peak_pff_rec_grade,
	ROUND(AVG(c.pff_run_grade), 2) AS avg_pff_run_grade,
	ROUND(MAX(c.pff_run_grade), 2) AS peak_pff_run_grade,
	ROUND(AVG(c.tprr), 2) AS avg_tprr,
    ROUND(MAX(c.tprr), 2) AS peak_tprr,
	ROUND(AVG(c.yprr), 2) AS avg_yprr,
    ROUND(MAX(c.yprr), 2) AS peak_yprr,
	ROUND(AVG(c.yac_per_rec), 2) AS avg_yac_per_rec,
	ROUND(MAX(c.yac_per_rec), 2) AS peak_yac_per_rec,    
	ROUND(AVG(c.team_yards_market_share), 2) AS avg_rec_team_yards_market_share_adj,
	ROUND(MAX(c.team_yards_market_share), 2) AS peak_rec_team_yards_market_share_adj, 
    
    -- Peak season age (the season_age of the player in the year they had peak scrimmage yards)
    (SELECT c2.season_age 
     FROM cfb_player_year_stats c2 
     WHERE c2.player_id = p.player_id
     ORDER BY (c2.rush_yds + c2.rec_yds) DESC 
     LIMIT 1) AS peak_season_age_college,
    
	-- Average Team SOS across all seasons in college for a player
    ROUND(AVG(ts.team_sos), 2) AS avg_sos,
	ROUND(MAX(ts.team_sos), 2) AS peak_sos,
	ROUND(AVG(ts.team_srs), 2) AS avg_srs,
	ROUND(MAX(ts.team_srs), 2) AS peak_srs,
	
    -- NFL performance metrics
    ROUND(AVG(n.fppg), 2) AS avg_fppg_nfl,
    ROUND(SUM(DISTINCT n.fantasy_points), 2) AS total_fantasy_points_nfl
    
FROM
    player p
LEFT JOIN
    cfb_player_year_stats c ON p.player_id = c.player_id
LEFT JOIN
    team_year_stats ts ON c.team_id = ts.team_id AND c.year = ts.year
LEFT JOIN
    nfl_player_year_stats n ON p.player_id = n.player_id

WHERE
    -- Ensure the player has NFL data and either has a draft year > 2013 or played college after 2013
    (p.draft_year > 2013
    OR (
        c.year > 2013
        AND EXISTS (
            SELECT 1
            FROM nfl_player_year_stats n_check
            WHERE n_check.player_id = p.player_id
        )
    ))
    
    -- Filter for players who have played in the NFL since 2014
    AND n.year >= 2014
    
    -- Only include players at the TE position
    AND p.position = 'TE'

GROUP BY
    p.player_id,
    p.name,
    p.position,
    p.draft_cap,
    p.production_score,
	p.size_score,
    p.cupps_score,
    p.ras,
    p.height,
    p.weight;