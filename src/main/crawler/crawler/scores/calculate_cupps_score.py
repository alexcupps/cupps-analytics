import logging

def calculate_cupps_for_player(player_id, db_util):
    """
    Calculate the College Ultimate Player Prospect Score (CUPPS) for a given player.

    :param player_id: Player's ID.
    :param db_util: Database utility object.
    :return: Computed CUPPS score.
    """
    # Fetch player attributes
    db_util.cursor.execute("""
        SELECT position, height, weight, birthday, draft_cap, draft_year, ras
        FROM player WHERE player_id = %s
    """, (player_id,))
    player_data = db_util.cursor.fetchone()

    if not player_data:
        return None

    position, height, weight, birthday, draft_cap, draft_year, ras = player_data
    ras = ras or 5  # Default RAS to 5 if NULL
    draft_cap = draft_cap or 3  # Default draft_cap (mid-round pick if NULL)

    # Fetch player's college career stats
    db_util.cursor.execute("""
        SELECT year, games_played, scrim_ypg, fppg, pff_run_grade, pff_rec_grade, yprr
        FROM cfb_player_year_stats WHERE player_id = %s
    """, (player_id,))
    seasons = db_util.cursor.fetchall()

    if not seasons:
        logging.info(f"No seasons found for player {player_id}")
        return None  # No data, return early

    # Career and peak calculations
    total_scrim_ypg, total_fppg, total_pff_run, total_pff_rec, total_yprr = 0, 0, 0, 0, 0
    count_seasons, breakout_age = 0, None
    peak_scrim_ypg, peak_fppg, peak_pff_run, peak_pff_rec, peak_yprr = 0, 0, 0, 0, 0

    for year, games_played, scrim_ypg, fppg, pff_run, pff_rec, yprr in seasons:
        if not games_played or games_played == 0:
            continue  # Skip invalid seasons
        
        # Track total values for averages
        total_scrim_ypg += scrim_ypg or 0
        total_fppg += fppg or 0
        total_pff_run += pff_run or 0
        total_pff_rec += pff_rec or 0
        total_yprr += yprr or 0
        count_seasons += 1

        # Track peak values
        peak_scrim_ypg = max(peak_scrim_ypg, scrim_ypg or 0)
        peak_fppg = max(peak_fppg, fppg or 0)
        peak_pff_run = max(peak_pff_run, pff_run or 0)
        peak_pff_rec = max(peak_pff_rec, pff_rec or 0)
        peak_yprr = max(peak_yprr, yprr or 0)

        # Determine breakout age (first season with fppg > 10)
        if not breakout_age and fppg and fppg > 10:
            breakout_age = year - (int(birthday[:4]) if birthday else 19)  # Approximate age if birthday exists

    # Compute career averages
    avg_scrim_ypg = total_scrim_ypg / count_seasons if count_seasons else 0
    avg_fppg = total_fppg / count_seasons if count_seasons else 0
    avg_pff_run = total_pff_run / count_seasons if count_seasons else 0
    avg_pff_rec = total_pff_rec / count_seasons if count_seasons else 0
    avg_yprr = total_yprr / count_seasons if count_seasons else 0

    # Breakout Age Bonus
    breakout_bonus = 0
    if breakout_age:
        if breakout_age <= 19:
            breakout_bonus = 5
        elif breakout_age == 20:
            breakout_bonus = 3
        elif breakout_age == 21:
            breakout_bonus = 1

    # Calculate CUPPS Score based on Position
    if position == "RB":
        cupps = (
            (ras * 2.5) +
            (avg_scrim_ypg * 1.25) +
            (peak_scrim_ypg * 1.25) +
            (avg_fppg * 1.5) +
            (avg_pff_run * 2) +
            (peak_pff_run * 1) +
            (avg_pff_rec * 1) +
            (peak_pff_rec * 0.5) +
            (avg_yprr * 1.5) +
            (draft_cap * 2) +
            (breakout_bonus)
        )

    elif position == "WR":
        cupps = (
            (ras * 2.5) +
            (avg_scrim_ypg * 1.0) +
            (peak_scrim_ypg * 1.0) +
            (avg_fppg * 1.5) +
            (avg_pff_rec * 2) +
            (peak_pff_rec * 1) +
            (avg_yprr * 4) +
            (draft_cap * 2) +
            (breakout_bonus)
        )

    elif position == "TE":
        cupps = (
            (ras * 2.5) +
            (height * 0.5) +
            (avg_scrim_ypg * 1.0) +
            (peak_scrim_ypg * 1.0) +
            (avg_fppg * 1.5) +
            (avg_pff_rec * 2) +
            (peak_pff_rec * 1) +
            (avg_yprr * 4) +
            (draft_cap * 2) +
            (breakout_bonus)
        )
    else:
        return None  # No calculation for other positions

    return min(100, max(0, cupps))  # Ensure CUPPS stays within 0-100

def update_cupps_scores(db_util):
    """
    Calculate and update the CUPPS scores for players who have at least one entry in nfl_player_year_stats.
    """
    # Get all players who have played in the NFL
    db_util.cursor.execute("""
        SELECT DISTINCT p.player_id 
        FROM player p
        JOIN nfl_player_year_stats n ON p.player_id = n.player_id
    """)
    players = db_util.cursor.fetchall()

    for (player_id,) in players:
        cupps_score = calculate_cupps_for_player(player_id, db_util)
        if cupps_score is not None:
            db_util.cursor.execute("UPDATE player SET cupps_score = %s WHERE player_id = %s", (cupps_score, player_id))

    db_util.conn.commit()
    logging.info(f"Updated CUPPS scores for {len(players)} players.")