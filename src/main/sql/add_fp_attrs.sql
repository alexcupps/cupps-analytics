-- Add the fantasy_points column
ALTER TABLE cfb_player_year_stats
ADD COLUMN fantasy_points FLOAT AS (
    (IFNULL(rec_yds, 0) * 0.1) +
    (IFNULL(rec_td, 0) * 6) +
    (IFNULL(rush_yds, 0) * 0.1) +
    (IFNULL(rush_td, 0) * 6) +
    (IFNULL(receptions, 0) * 1)
) STORED;

-- Add the fantasy points per game (FPPG) column
ALTER TABLE cfb_player_year_stats
ADD COLUMN fppg FLOAT AS (
    CASE 
        WHEN IFNULL(games_played, 0) > 0 THEN
            ((IFNULL(rec_yds, 0) * 0.1) +
            (IFNULL(rec_td, 0) * 6) +
            (IFNULL(rush_yds, 0) * 0.1) +
            (IFNULL(rush_td, 0) * 6) +
            (IFNULL(receptions, 0) * 1)) / games_played
        ELSE NULL
    END
) STORED;