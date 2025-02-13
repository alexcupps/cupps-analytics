import math
import logging
import numpy as np

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_global_pff_averages(db_util):
    """
    Fetches global averages for PFF-related fields to use when a player's PFF data is missing.
    """
    logging.info("🔍 Fetching global PFF averages...")
    db_util.cursor.execute("""
        SELECT 
            AVG(pff_run_grade) AS avg_pff_run,
            AVG(pff_rec_grade) AS avg_pff_rec,
            AVG(yprr) AS avg_yprr,
            AVG(tprr) as avg_tprr
        FROM cfb_player_year_stats
        WHERE pff_run_grade IS NOT NULL OR pff_rec_grade IS NOT NULL OR yprr IS NOT NULL OR tprr IS NOT NULL
    """)
    result = db_util.cursor.fetchone()

    averages = {
        "pff_run": result[0] or 60,  # Default to 60 if no data
        "pff_rec": result[1] or 60,
        "yprr": result[2] or 1.5,
        "tprr": result[3] or .1
    }
    
    logging.info(f"✅ Global PFF Averages: {averages}")
    return averages

def scale_to_100(value, max_expected):
    """ Scales a value to be out of 100 using a predefined max range. """
    return min(100, (value / max_expected) * 100)

# ✅ Compute 75th Percentile (Upper Quartile) for PFF/YPRR/TPRR values
def percentile_75(values, default):
    return float(np.percentile(values, 75)) if values else float(default)

def calculate_draft_cap_weight(draft_cap):
    """ Calculates draft capital weighting with penalties for later picks. """
    if draft_cap is None:
        return 0  # Undrafted players get no draft score

    if draft_cap <= 10:
        return 100
    elif draft_cap <= 32:
        return 90 - ((draft_cap - 10) * 1.5)
    elif draft_cap <= 64:
        return 85 - ((draft_cap - 32) * 1.2)
    elif draft_cap <= 100:
        return 70 - ((draft_cap - 60) * 1.0)
    elif draft_cap <= 150:
        return 40 - ((draft_cap - 100) * 0.8)
    elif draft_cap <= 200:
        return 20 - ((draft_cap - 150) * 0.6)
    else:
        return max(5, 10 - ((draft_cap - 200) * 0.4))  # Harshest penalty

def calculate_production_score(position, seasons, global_pff_averages):
    """ Calculates a player's production score based on counting stats and PFF grades. """
    if not seasons:
        return 0

    total_scrim_ypg, total_fppg, peak_fppg, peak_pff_run, peak_pff_rec, peak_yprr, peak_tprr = 0, 0, 0, 0, 0, 0, 0
    pff_run_values, pff_rec_values, yprr_values, tprr_values = [], [], [], []
    valid_seasons = 0

    for season in seasons:
        (year, games_played, scrim_ypg, fppg, pff_run, pff_rec, yprr, tprr,
         rec_yds, rec, rush_att, rush_yds, team_sos, season_age) = season

        if not games_played or games_played == 0:
            continue

        # ✅ Use global PFF averages if missing
        pff_run = pff_run if pff_run is not None else global_pff_averages["pff_run"]
        pff_rec = pff_rec if pff_rec is not None else global_pff_averages["pff_rec"]
        yprr = yprr if yprr is not None else global_pff_averages["yprr"]
        tprr = tprr if tprr is not None else global_pff_averages["tprr"]

        # ✅ Store PFF metrics, only doing so if a player is over a certain involvement threshold
        if rush_att > 20:
            if pff_run > peak_pff_run:
                peak_pff_run = pff_run
            pff_run_values.append(pff_run)

        if rec > 10:
            if pff_rec > peak_pff_rec:
                peak_pff_rec = pff_rec
            if yprr > peak_yprr:
                peak_yprr = yprr
            if tprr > peak_tprr:
                peak_tprr = tprr
            pff_rec_values.append(pff_rec)
            yprr_values.append(yprr)
            tprr_values.append(tprr)

        # ✅ Apply Age-Based Adjustments
        age_adjustments = {18: 1.20, 19: 1.15, 20: 1.10, 21: 0.90, 22: 0.80, 23: 0.70}
        if season_age is None:
            age_multiplier = 1  # Default multiplier if `season_age` is missing
        else:
            age_multiplier = age_adjustments.get(season_age, 0.50 if season_age >= 24 else 1)

        # ✅ Apply SOS Factor
        sos_multiplier = 1 + (math.tanh(team_sos / 15)) if team_sos is not None else 1

        # ✅ Accumulate Weighted Stats
        total_scrim_ypg += (scrim_ypg or 0) * age_multiplier * sos_multiplier
        total_fppg += (fppg or 0) * age_multiplier * sos_multiplier

        if fppg > peak_fppg:
            peak_fppg = fppg

        valid_seasons += 1

    if valid_seasons == 0:
        return 0

    pff_run_75 = percentile_75(pff_run_values, global_pff_averages["pff_run"])
    pff_rec_75 = percentile_75(pff_rec_values, global_pff_averages["pff_rec"])
    yprr_75 = percentile_75(yprr_values, global_pff_averages["yprr"])
    tprr_75 = percentile_75(tprr_values, global_pff_averages["tprr"])
    
    if position == "RB":
        raw_production_score = (
            ((total_scrim_ypg / valid_seasons) * 2) +
            ((total_fppg / valid_seasons) * 10) +
            (peak_fppg * 8) +
            (pff_run_75 * 3) +
            (pff_rec_75 * 3) +
            (peak_pff_run) +
            (peak_pff_rec) +
            (yprr_75 * 30) + 
            (tprr_75 / 0.002)
        )
        max_expected_score = 2000 

    elif position == "WR":

        raw_production_score = (
            ((total_scrim_ypg / valid_seasons) * 2) +
            ((total_fppg / valid_seasons) * 10) +
            (peak_fppg * 8) +
            (pff_rec_75 * 3) +
            (peak_pff_rec) +
            (yprr_75 * 30) + 
            (peak_yprr * 30) +
            (tprr_75 / 0.005) +
            (peak_tprr / 0.005)
        )
        max_expected_score = 1800

    elif position == "TE":
        raw_production_score = (
            ((total_scrim_ypg / valid_seasons) * 2) +
            ((total_fppg / valid_seasons) * 10) +
            (peak_fppg * 8) +
            (pff_rec_75 * 3) +
            (peak_pff_rec) +
            (yprr_75 * 30) + 
            (peak_yprr * 30) +
            (tprr_75 / 0.0025) +
            (peak_tprr / 0.0025)
        )
        max_expected_score = 1400

    else:
        logging.warn(f"Cannot calculate production score for player with position {position}")
        return
    
    scaled_score = scale_to_100(raw_production_score, max_expected_score)

    # ✅ Adjust max_expected based on observed values
    return scaled_score

def calculate_size_score(position, height, weight, ras):
    """ 
    Calculates a player's size/athleticism score using position-specific height, weight, and RAS. 
    Players who do not test for RAS will have their size score based entirely on height/weight.
    """

    if height is None or weight is None:
        return 0  # Missing critical data

    size_score = 0

    # ✅ Define position-specific ideal height & weight ranges
    size_ranges = {
        "RB": {"min_h": 67, "min_w": 190},
        "WR": {"min_h": 70, "min_w": 180},
        "TE": {"min_h": 75, "min_w": 245},
    }

    # ✅ Assign position-specific size thresholds
    if position in size_ranges:
        size_params = size_ranges[position]
        
        # 🏆 Height Scoring (0-50 points)
        if height < size_params["min_h"]:
            height_score = max(0, 50 - (size_params["min_h"] - height) * 6)  # Penalize for being too short
        else:
            height_score = 50  # Ideal range gets full points
        
        # 🏆 Weight Scoring (0-50 points)
        if weight < size_params["min_w"]:
            weight_score = max(0, 50 - (size_params["min_w"] - weight) * 3)  # Penalize for being underweight
        else:
            weight_score = 50  # Ideal range gets full points

        size_score = height_score + weight_score  # Max of 100 points from height + weight

    else:
        # Default case if position is missing or new
        size_score = (height * 0.5) + (weight * 0.2)

    # ✅ If RAS is provided, adjust weightings
    if ras is not None:
        ras_score = ras * 10  # Convert RAS to a 100-point scale
        final_score = (ras_score * 0.75) + (size_score * 0.25)  # 75% RAS, 25% Size
    else:
        final_score = size_score  # 100% based on size if no RAS

    return scale_to_100(final_score, max_expected=100)



def update_cupps_scores(db_util):
    """ 
    🚀 CUPPS (Calculated Upside Player Prospect Score) calculation with batch updates and logging. 
    """

    logging.info("🚀 Starting CUPPS score update process...")

    # ✅ Fetch players with NFL seasons since 2016
    db_util.cursor.execute("""
        SELECT DISTINCT p.player_id
        FROM player p
        JOIN nfl_player_year_stats n ON p.player_id = n.player_id
        WHERE n.year >= 2024
    """)
    players = [row[0] for row in db_util.cursor.fetchall()]

    if not players:
        logging.info("⚠️ No players found to update.")
        return

    logging.info(f"🔍 Found {len(players)} players to update.")

    # ✅ Fetch global PFF averages
    global_pff_averages = get_global_pff_averages(db_util)

    # ✅ Fetch all necessary player data in bulk
    logging.info("🔍 Fetching player and season data...")
    db_util.cursor.execute("""
        SELECT p.player_id, p.position, p.height, p.weight, p.birthday, p.draft_cap, p.draft_year, p.ras,
               c.year, c.games_played, c.scrim_ypg, c.fppg, c.pff_run_grade, c.pff_rec_grade, c.yprr, c.tprr,
               c.rec_yds, c.receptions, c.rush_att, c.rush_yds, COALESCE(t.team_sos, 0), c.season_age
        FROM player p
        JOIN cfb_player_year_stats c ON p.player_id = c.player_id
        LEFT JOIN team_year_stats t ON c.team_id = t.team_id AND c.year = t.year
        WHERE p.player_id IN ({})
        ORDER BY p.player_id, c.year
    """.format(",".join(["%s"] * len(players))), players)

    # ✅ Group season data by player_id
    player_data = {}
    for row in db_util.cursor.fetchall():
        player_id = row[0]

        if player_id not in player_data:
            player_data[player_id] = {
                "player_info": row[1:8],  # Extracts (position, height, weight, birthday, draft_cap, draft_year, ras)
                "seasons": []
            }

        player_data[player_id]["seasons"].append(row[8:])  # Append season stats

    logging.info(f"✅ Grouped season data for {len(player_data)} players.")

    update_values = []
    for player_id, data in player_data.items():
        position, height, weight, birthday, draft_cap, draft_year, ras = data["player_info"]

        # ✅ Use default values for missing data
        height = height or 72  # Default 6'0"
        weight = weight or 210  # Default 210 lbs
        ras = ras if ras is not None else 5  # Default RAS

        production_score = calculate_production_score(position, data["seasons"], global_pff_averages)
        size_score = calculate_size_score(position, height, weight, ras)
        draft_cap_weighted = calculate_draft_cap_weight(draft_cap)
        cupps_score = scale_to_100((production_score * 2) + (size_score * 1) + (draft_cap_weighted * 3), 600)

        logging.info(f"📊 Player {player_id} Scores 📊 Production: {production_score} -- Size: {size_score} -- DC: {draft_cap_weighted} -- Overall CUPPS: {cupps_score}")
        update_values.append((production_score, size_score, cupps_score, player_id))

    logging.info(f"🔄 Updating {len(update_values)} players in the database...")

    # ✅ Perform batch update
    db_util.cursor.executemany(
        "UPDATE player SET production_score = %s, size_score = %s, cupps_score = %s WHERE player_id = %s",
        update_values
    )
    db_util.conn.commit()

    logging.info(f"✅ CUPPS scores updated for {len(update_values)} players successfully!")


