-- Replace 'TARGET_ID' with the player_id you're comparing to
-- Retrieves the 20 most similar comps to the given player
SELECT 
    r.player_id,
	ROUND(r.peak_rec_team_yards_market_share_adj, 4) AS peak_rec_ms,
	ROUND(r.avg_tprr, 4) AS avg_tprr,
    r.weight,
    r.name,
    ROUND(r.production_score, 2) AS production_score,
    ROUND(r.size_score, 2) AS size_score,
    ROUND(r.draft_cap, 2) AS draft_cap,
    ROUND(r.cupps_score, 2) AS cupps_score,
    ROUND(f.avg_fppg_nfl, 2) AS avg_fppg_nfl,
	ROUND(
		SQRT(
			POWER(r.production_score - t.production_score, 2) +
			POWER(r.size_score - t.size_score, 2) +
			POWER(r.draft_cap - t.draft_cap, 2) +
			POWER(r.cupps_score - t.cupps_score, 1) +
			POWER((r.peak_rec_team_yards_market_share_adj * 100) - (t.peak_rec_team_yards_market_share_adj * 100), 2) +
			POWER((r.avg_tprr * 100) - (t.avg_tprr * 100), 2) +
            POWER(r.weight - t.weight, 2)
		),
		2
	) AS distance
FROM rb_model_data r
JOIN (
    SELECT 
        production_score, 
        size_score, 
        draft_cap, 
        cupps_score,
        peak_rec_team_yards_market_share_adj,
        avg_tprr,
        weight
    FROM rb_model_data
    WHERE player_id = 18529
      AND production_score IS NOT NULL
      AND size_score IS NOT NULL
      AND draft_cap IS NOT NULL
      AND peak_rec_team_yards_market_share_adj IS NOT NULL
      AND avg_tprr is NOT NULL
) t ON TRUE
LEFT JOIN (
    SELECT player_id, AVG(fppg) AS avg_fppg_nfl
    FROM nfl_player_year_stats
    GROUP BY player_id
) f ON f.player_id = r.player_id
  WHERE r.production_score IS NOT NULL
  AND r.size_score IS NOT NULL
  AND r.draft_cap IS NOT NULL
  AND r.peak_rec_team_yards_market_share_adj IS NOT NULL
  AND r.avg_tprr IS NOT NULL
  AND r.weight IS NOT NULL
ORDER BY distance ASC
LIMIT 21;