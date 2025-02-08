import math
import logging

def calculate_cupps_for_player(player_info, seasons):
    """
    Calculate the College Ultimate Player Prospect Score (CUPPS) for a given player.

    :param player_info: Tuple of (position, height, weight, birthday, draft_cap, draft_year, ras).
    :param seasons: List of tuples representing per-year college stats.
    :return: Computed CUPPS score.
    """
    if not player_info:
        return None

    position, height, weight, birthday, draft_cap, draft_year, ras = player_info

    # ✅ **Handle RAS adjustments**
    ras_weighted = 0 if ras is None else ras * (10 if ras >= 9 else 7.5 if ras >= 5 else 2 if ras >= 5 else 1)

    # ✅ **Handle Draft Capital Adjustments with Tiered Weights**
    if draft_cap is None:
        draft_cap_weighted = 0.1  # 🚨 **Undrafted players get the lowest possible positive weight**
        draft_penalty = -15  # 🚨 **Undrafted players get a huge penalty**
    elif draft_cap <= 10:
        draft_cap_weighted = 16 + (10 - draft_cap) * 0.4
        draft_penalty = 0
    elif draft_cap <= 32:
        draft_cap_weighted = 12 + (32 - draft_cap) * 0.2
        draft_penalty = 0
    elif draft_cap <= 60:
        draft_cap_weighted = 8 + (60 - draft_cap) * 0.15
        draft_penalty = 0
    elif draft_cap <= 100:
        draft_cap_weighted = 5 + (100 - draft_cap) * 0.1
        draft_penalty = 0
    elif draft_cap <= 150:
        draft_cap_weighted = 2 + (150 - draft_cap) * 0.05
        draft_penalty = -(draft_cap - 100) * 0.05  # 🏆 Moderate penalty increase
    elif draft_cap <= 200:
        draft_cap_weighted = 1 + (200 - draft_cap) * 0.025
        draft_penalty = -(draft_cap - 100) * 0.1  # 🚨 Bigger penalty for rounds 6-7
    else:
        draft_cap_weighted = max(0.5, 1 - ((draft_cap - 200) / 50))
        draft_penalty = -(draft_cap - 100) * 0.15  # 🚨 Harshest penalty for 200+

    if not seasons:
        return None  # No seasons → no score

    # ✅ **Career and peak calculations**
    total_scrim_ypg, total_fppg, total_pff_run, total_pff_rec, total_yprr, total_sos = 0, 0, 0, 0, 0, 0
    valid_seasons = 0

    # ✅ **Identify missing data issues**
    has_pff_data = False
    has_sos_data = False

    for year, games_played, scrim_ypg, fppg, pff_run, pff_rec, yprr, rec_yds, rec, rush_att, rush_yds, team_sos, season_age in seasons:
        if not games_played or games_played == 0:
            continue

        # ✅ **Identify available data**
        if pff_run or pff_rec or yprr:
            has_pff_data = True
        if team_sos is not None:
            has_sos_data = True

        # ✅ **Ignore non-offensive seasons**
        if position in ["WR", "TE"] and (rec_yds or 0) < 50 and (rec or 0) < 5:
            continue
        if position == "RB" and (rush_yds or 0) < 50 and (rush_att or 0) < 10:
            continue

        # ✅ **Age-Based Adjustments**
        age_multiplier = 1
        if season_age is not None:
            if season_age == 18:
                age_multiplier = 1.20  # +20% boost
            elif season_age == 19:
                age_multiplier = 1.15  # +15% boost
            elif season_age == 20:
                age_multiplier = 1.10  # +10% boost
            elif season_age == 21:
                age_multiplier = 0.80  # -20% penalty
            elif season_age == 22:
                age_multiplier = 0.60  # -40% penalty
            elif season_age == 23:
                age_multiplier = 0.40  # -60% penalty
            elif season_age >= 24:
                age_multiplier = 0.20  # -80% penalty

        valid_seasons += 1

        # ✅ **Track total values for averages**
        total_scrim_ypg += (scrim_ypg or 0) * age_multiplier
        total_fppg += (fppg or 0) * age_multiplier
        total_pff_run += (pff_run or 0) * age_multiplier
        total_pff_rec += (pff_rec or 0) * age_multiplier
        total_yprr += (yprr or 0) * age_multiplier
        total_sos += (team_sos or 0) * age_multiplier  # Handle NULL values properly

    if valid_seasons == 0:
        return None  # No valid offensive seasons

    # ✅ **Compute career averages**
    avg_scrim_ypg = total_scrim_ypg / valid_seasons
    avg_fppg = total_fppg / valid_seasons
    avg_pff_run = total_pff_run / valid_seasons
    avg_pff_rec = total_pff_rec / valid_seasons
    avg_yprr = total_yprr / valid_seasons
    avg_sos = total_sos / valid_seasons if has_sos_data else 0

    # ✅ **Apply SOS Factor**
    sos_multiplier = 1 + (avg_sos / 10)

    # ✅ **Adjust Weighting for Older Players Without PFF Data**
    pff_multiplier = 1 if has_pff_data else 2  

    # ✅ **Calculate CUPPS Score based on Position**
    if position == "RB":
        cupps = (
            ras_weighted +
            ((avg_scrim_ypg * 2) + (avg_scrim_ypg * 2)) * sos_multiplier * pff_multiplier +
            ((avg_fppg * 2.5)) * sos_multiplier * pff_multiplier +
            ((avg_pff_run * 2) + (avg_pff_run * 2)) * pff_multiplier +
            ((avg_pff_rec * 2) + (avg_pff_rec * 2)) * pff_multiplier +
            ((avg_yprr * 1.5)) * pff_multiplier +
            ((draft_cap_weighted * 12))
        )

    elif position == "WR":
        cupps = (
            ras_weighted +
            ((avg_scrim_ypg * 1.5) + (avg_scrim_ypg * 1.5)) * sos_multiplier * pff_multiplier +
            ((avg_fppg * 2)) * sos_multiplier * pff_multiplier +
            ((avg_pff_rec * 3) + (avg_pff_rec * 1.5)) +
            ((avg_yprr * 5)) +
            ((draft_cap_weighted * 12))
        )

    elif position == "TE":
        cupps = (
            ras_weighted +
            ((avg_scrim_ypg * 1.2) + (avg_scrim_ypg * 1.2)) * sos_multiplier * pff_multiplier +
            ((avg_fppg * 1.8)) * sos_multiplier * pff_multiplier +
            ((avg_pff_rec * 2.5) + (avg_pff_rec * 1.2)) +
            ((avg_yprr * 4)) +
            ((draft_cap_weighted * 10))
        )

    return cupps



def normalize_cupps_scores(scores):
    """ Normalize CUPPS scores so the highest value = 100 """
    max_score = max(scores.values()) if scores else 1  # Prevent divide-by-zero
    return {pid: (score / max_score) * 100 for pid, score in scores.items()}


def update_cupps_scores(db_util):
    """ 🚀 Optimized CUPPS score calculation with batch updates. """

    # **Fetch all players that have played in the NFL**
    db_util.cursor.execute("""
        SELECT DISTINCT player_id FROM nfl_player_year_stats
    """)
    players = [row[0] for row in db_util.cursor.fetchall()]

    if not players:
        logging.info("No players found to update.")
        return

    # **🚀 Bulk Fetch Player Attributes & College Stats in One Query 🚀**
    db_util.cursor.execute("""
        SELECT 
            p.player_id, p.position, p.height, p.weight, p.birthday, p.draft_cap, p.draft_year, p.ras,
            c.year, c.games_played, c.scrim_ypg, c.fppg, c.pff_run_grade, c.pff_rec_grade, c.yprr, 
            c.rec_yds, c.receptions, c.rush_att, c.rush_yds, COALESCE(t.team_sos, 0), c.season_age
        FROM player p
        JOIN cfb_player_year_stats c ON p.player_id = c.player_id
        LEFT JOIN team_year_stats t ON c.team_id = t.team_id AND c.year = t.year
        WHERE p.player_id IN ({})
    """.format(",".join(["%s"] * len(players))), players)

    player_stats = db_util.cursor.fetchall()

    # **🚀 Organize Data in a Dictionary for Faster Lookups 🚀**
    player_data = {}
    for row in player_stats:
        player_id = row[0]
        if player_id not in player_data:
            player_data[player_id] = {
                "player_info": row[1:8],  # Store player attributes
                "seasons": []
            }
        player_data[player_id]["seasons"].append(row[8:])  # Store season stats

    # **🚀 Compute CUPPS Scores 🚀**
    scores = {}
    for player_id, data in player_data.items():
        cupps_score = calculate_cupps_for_player(data["player_info"], data["seasons"])
        if cupps_score is not None:
            logging.info(f"Setting score {cupps_score} for player {player_id}")
            scores[player_id] = cupps_score

    # **🚀 Normalize Scores 🚀**
    normalized_scores = normalize_cupps_scores(scores)

    # **🚀 Batch Update Scores 🚀**
    update_values = [(score, player_id) for player_id, score in normalized_scores.items()]
    
    if update_values:
        db_util.cursor.executemany(
            "UPDATE player SET cupps_score = %s WHERE player_id = %s",
            update_values
        )
        db_util.conn.commit()
        logging.info(f"🚀 Updated CUPPS scores for {len(update_values)} players in a single batch.")
