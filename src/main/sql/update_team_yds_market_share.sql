WITH team_stats AS (
    -- Step 1: Compute total team receiving yards & estimate team games played
    SELECT 
        team_id,
        year,
        SUM(rec_yds) AS total_team_rec_yards,
        MAX(games_played) AS estimated_team_games -- Use max games played as an estimate
    FROM cfb_player_year_stats
    GROUP BY team_id, year
),
player_adjusted AS (
    -- Step 2: Compute player's per-game receiving yards
    SELECT 
        player_id,
        team_id,
        year,
        rec_yds,
        games_played,
        rec_yds / NULLIF(games_played, 0) AS rec_yds_per_game -- Avoid division by zero
    FROM cfb_player_year_stats
)
-- Step 3: Update the player market share adjusted for games played
UPDATE cfb_player_year_stats AS p
JOIN team_stats AS t
ON p.team_id = t.team_id AND p.year = t.year
JOIN player_adjusted AS pa
ON p.player_id = pa.player_id AND p.year = pa.year
SET p.team_yards_market_share = 
    CASE 
        WHEN t.total_team_rec_yards > 0 AND pa.games_played > 0 THEN 
            (pa.rec_yds_per_game / (t.total_team_rec_yards / NULLIF(t.estimated_team_games, 0)))
        ELSE 0
    END;