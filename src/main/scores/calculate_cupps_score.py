import math
import logging
import numpy as np

# Set up logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

import logging

import logging

def get_global_pff_averages(db_util):
    """
    Fetches global PFF averages for each position (RB, WR, TE) and stores them in a dictionary.
    This prevents repeated database queries.
    """
    logging.info("🔍 Fetching NFL PFF averages for all positions...")

    position_map = {
        "RB": "rb_model_data",
        "WR": "wr_model_data",
        "TE": "te_model_data"
    }

    global_averages = {}

    for position, table_name in position_map.items():
        query = f"""
            SELECT 
                AVG(avg_pff_run_grade) AS avg_pff_run,
                AVG(avg_pff_rec_grade) AS avg_pff_rec,
                AVG(avg_yprr) AS avg_yprr,
                AVG(avg_tprr) AS avg_tprr
            FROM {table_name}
            WHERE avg_pff_run_grade IS NOT NULL 
              OR avg_pff_rec_grade IS NOT NULL 
              OR avg_yprr IS NOT NULL 
              OR avg_tprr IS NOT NULL
        """
        
        db_util.cursor.execute(query)
        result = db_util.cursor.fetchone()

        global_averages[position] = {
            "pff_run": result[0] if result and result[0] is not None else 60,  
            "pff_rec": result[1] if result and result[1] is not None else 60,
            "yprr": result[2] if result and result[2] is not None else 1.5,
            "tprr": result[3] if result and result[3] is not None else 0.1
        }

        logging.info(f"✅ Global PFF Averages for {position}: {global_averages[position]}")

    return global_averages

def get_age_multiplier(position, season_age):
    """Returns the age multiplier based on position and season age."""

    if not season_age:
        return 1
    
    # Define age adjustment multipliers for each position
    age_adjustments = {
        "RB": {18: 1.35, 19: 1.30, 20: 1.25, 21: 0.90, 22: 0.80, 23: 0.70, 24: 0.60},
        "WR": {18: 1.50, 19: 1.40, 20: 1.30, 21: 0.80, 22: 0.70, 23: 0.60, 24: 0.50},
        "TE": {18: 1.20, 19: 1.15, 20: 1.10, 21: 1, 22: 0.95, 23: 0.85, 24: 0.80},
    }

    # Get position-specific age multipliers or use default (if position not in dict)
    position_age_adjustments = age_adjustments.get(position, {})

    # Return the position-specific multiplier or a default value (1 for neutral impact)
    return position_age_adjustments.get(season_age, 0.50 if season_age >= 25 else 1)

def is_valid_season(position, touches):
    """
    Determines if a player's season is valid based on the number of touches.
    
    :param position: The player's position (e.g., 'RB', 'WR', 'TE').
    :param touches: The number of rush attempts (RB) or receptions (WR/TE) in that season.
    :return: True if the season meets the minimum threshold, False otherwise.
    """

    # ✅ Define position-specific thresholds
    touch_thresholds = {
        "RB": 20,   # Minimum rush attempts for a valid RB season
        "WR": 5,   # Minimum receptions for a valid WR season
        "TE": 5,   # Minimum receptions for a valid TE season
    }

    # ✅ Get threshold for the given position, default to 0 if not listed
    required_touches = touch_thresholds.get(position, 0)

    # ✅ Return True if touches meet or exceed the threshold, else False
    return touches >= required_touches

def scale_to_100(value, max_expected):
    """ Scales a value to be out of 100 using a predefined max range. """
    return min(100, (value / max_expected) * 100)

# ✅ Compute 75th Percentile (Upper Quartile) for PFF/YPRR/TPRR values
def percentile_75(values, default):
    return float(np.percentile(values, 75)) if values else float(default)

def weight_stats_by_age_and_sos(stat, age, sos):
    return stat * age * sos

def calculate_draft_cap_weight(draft_cap, position):
    """ Calculates draft capital weighting with penalties for later picks. """
    if draft_cap is None:
        return 0  # Undrafted players get no draft score

    if position == "RB":

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
        
    elif position == "WR":

        if draft_cap <= 5:
            return 100
        elif draft_cap <= 15:
            return 90 - ((draft_cap - 10) * 1.5)
        elif draft_cap <= 32:
            return 85 - ((draft_cap - 32) * 1.25)
        elif draft_cap <= 64:
            return 70 - ((draft_cap - 60) * 1.0)
        elif draft_cap <= 100:
            return 40 - ((draft_cap - 100) * 0.8)
        elif draft_cap <= 200:
            return 20 - ((draft_cap - 150) * 0.6)
        else:
            return max(5, 10 - ((draft_cap - 200) * 0.4))  # Harshest penalty
        
    elif position == "TE":

        if draft_cap <= 10:
            return 100
        elif draft_cap <= 32:
            return 95 - ((draft_cap - 10) * 1.2)
        elif draft_cap <= 64:
            return 90 - ((draft_cap - 32) * 0.75)
        elif draft_cap <= 100:
            return 80 - ((draft_cap - 60) * 0.65)
        elif draft_cap <= 150:
            return 60 - ((draft_cap - 100) * 0.55)
        elif draft_cap <= 200:
            return 40 - ((draft_cap - 150) * 0.45)
        else:
            return max(5, 10 - ((draft_cap - 200) * 0.4))  # Harshest penalty
        
    else:
        logging.warn(f"Cannot calculate production score for player with position {position}")
        return

def calculate_production_score(position, seasons, global_pff_averages):
    """ Calculates a player's production score based on counting stats and PFF grades. """
    if not seasons:
        return 0

    total_scrim_ypg, total_fppg, peak_fppg, peak_pff_run, peak_pff_rec, peak_yprr, peak_tprr, peak_scrim_yds, peak_rec_yds, peak_team_yards_market_share, peak_season_age = 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 
    pff_run_values, pff_rec_values, yprr_values, tprr_values = [], [], [], []
    valid_seasons = 0

    for season in seasons:
        (year, games_played, scrim_ypg, fppg, pff_run, pff_rec, yprr, tprr,
         rec_yds, rec, rush_att, rush_yds, team_sos, season_age, team_yards_market_share) = season

        if not games_played or games_played == 0:
            continue

        scrim_ypg = float(scrim_ypg or 0)
        fppg = float(fppg or 0)
        pff_run = float(pff_run or global_pff_averages[position]["pff_run"])
        pff_rec = float(pff_rec or global_pff_averages[position]["pff_rec"])
        yprr = float(yprr or global_pff_averages[position]["yprr"])
        tprr = float(tprr or global_pff_averages[position]["tprr"])
        rush_yds = float(rush_yds or 0)
        rec_yds = float(rec_yds or 0)
        rush_att = float(rush_att or 0)
        rec = float(rec or 0)
        team_sos = float(team_sos or 0)
        team_yards_market_share = float(team_yards_market_share or 0)
        scrim_yds = rush_yds + rec_yds

        # ✅ Store PFF metrics, only doing so if a player is over a certain involvement threshold
        if rush_att > 20:
            if pff_run > peak_pff_run:
                peak_pff_run = pff_run
            pff_run_values.append(pff_run)
            if scrim_yds > peak_scrim_yds:
                peak_scrim_yds = scrim_yds
            if season_age:
                peak_season_age = season_age
            else:
                peak_season_age = 21

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

            if rec_yds > peak_rec_yds:
                peak_rec_yds = rec_yds
                if season_age:
                    peak_season_age = season_age
                else:
                    peak_season_age = 21

        # ✅ Get Age-Based Adjustments
        age_multiplier = get_age_multiplier(position, season_age)

        # ✅ Get SOS Factor
        sos_multiplier = 1 + (math.tanh(team_sos / 15)) if team_sos is not None else 1

        # ✅ Accumulate Weighted Stats
        total_scrim_ypg += weight_stats_by_age_and_sos(scrim_ypg or 0, age_multiplier, sos_multiplier)
        total_fppg += weight_stats_by_age_and_sos(fppg or 0, age_multiplier, sos_multiplier)

        if weight_stats_by_age_and_sos(fppg, age_multiplier, sos_multiplier) > peak_fppg:
            peak_fppg = weight_stats_by_age_and_sos(fppg, age_multiplier, sos_multiplier)
        
        if weight_stats_by_age_and_sos(team_yards_market_share, age_multiplier, sos_multiplier) > peak_team_yards_market_share:
            peak_team_yards_market_share = weight_stats_by_age_and_sos(team_yards_market_share, age_multiplier, sos_multiplier)
        
        if is_valid_season(position, rec if position in ['TE', 'WR'] else rush_att):
            valid_seasons += 1

    if valid_seasons == 0:
        return 0

    pff_run_75 = percentile_75(pff_run_values, global_pff_averages[position]["pff_run"])
    pff_rec_75 = percentile_75(pff_rec_values, global_pff_averages[position]["pff_rec"])
    yprr_75 = percentile_75(yprr_values, global_pff_averages[position]["yprr"])
    tprr_75 = percentile_75(tprr_values, global_pff_averages[position]["tprr"])
    
    if position == "RB":
        big_szn_boost = (peak_scrim_yds - 1200) * (0.6 if peak_season_age <= 20 else 0.2) if peak_scrim_yds > 1100 else 0
        raw_production_score = (
            ((total_scrim_ypg / valid_seasons) * 2) +
            ((total_fppg / valid_seasons) * 10) +
            (peak_fppg * 8) +
            (pff_run_75 * 1.5) +
            (pff_rec_75 * 1.5) +
            (peak_pff_run * 1.5) +
            (peak_pff_rec * 1.5) +
            (yprr_75 * 30) + 
            (tprr_75 / 0.002) + 
            (big_szn_boost)
        )
        max_expected_score = 3000 

    elif position == "WR":
        # ✅ Increase Peak Receiving Yards Boost - more boost if it was in their first 3 yrs
        big_szn_boost = (peak_rec_yds - 1100) * (0.75 if peak_season_age <= 20 else 0.2) if peak_rec_yds > 1100 else 0

         # ✅ Adjusted Production Score for WRs
        raw_production_score = (
            ((total_scrim_ypg / valid_seasons)) +
            ((total_fppg / valid_seasons) * 5) +
            (peak_fppg * 5) +
            (pff_rec_75 * 2) +
            (peak_pff_rec * 4) +
            (yprr_75 * 30) +
            (peak_yprr * 60) +
            (tprr_75 / 0.0045) +
            (peak_tprr / 0.001) + 
            (peak_team_yards_market_share * 150) +
            (big_szn_boost)
        )
        max_expected_score = 2500  # Increased slightly to balance scaling

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
        max_expected_score = 1500

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
        "WR": {"min_h": 70, "min_w": 190},
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
        LEFT JOIN cfb_player_year_stats c ON p.player_id = c.player_id
        LEFT JOIN (
            -- Get the first recorded NFL year for each player
            SELECT player_id, MIN(year) AS first_nfl_year
            FROM nfl_player_year_stats
            GROUP BY player_id
        ) n ON p.player_id = n.player_id

        WHERE 
            -- Ensure the player has CFB stats
            c.player_id IS NOT NULL

            -- Ensure either drafted in 2014+ OR played in the NFL in 2014+
            AND (
                p.draft_cap IS NOT NULL AND p.draft_year >= 2014
                OR (n.first_nfl_year IS NOT NULL AND n.first_nfl_year >= 2014)
            );
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
               c.rec_yds, c.receptions, c.rush_att, c.rush_yds, COALESCE(t.team_sos, 0), c.season_age, c.team_yards_market_share
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
        draft_cap_weighted = calculate_draft_cap_weight(draft_cap, position)
        cupps_score = scale_to_100((production_score * 2.25) + (size_score * 1) + (draft_cap_weighted * 2.75), 600)

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


