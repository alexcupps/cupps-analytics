import os
import csv
import logging
import scrapy
from ..util.db_util import DatabaseUtility
from ..util.crawler_util import *

class PFFSpider(scrapy.Spider):
    name = "pff_spider"
    start_year = 2014
    end_year = 2023

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Resolve the absolute path to the data directory
        self.data_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../../../data/pff/cfb/receiving")
        )
        logging.info(f"Resolved data directory: {self.data_dir}")

        # Initialize the database utility
        self.db_util = DatabaseUtility()

        # Missing players log file
        self.missing_players_file = os.path.abspath("pff_missing_players.csv")
        self.missing_players = []

    def start_requests(self):
        for year in range(self.start_year, self.end_year + 1):
            file_path = os.path.join(self.data_dir, f"{year}.csv")
            if not os.path.exists(file_path):
                logging.warning(f"File not found: {file_path}. Skipping.")
                continue

            # Process the file
            self.process_file(file_path, year)

        # Dummy yield to prevent 'NoneType' iterable error
        yield scrapy.Request("about:blank", dont_filter=True)

    def process_file(self, file_path, year):
        logging.info(f"Processing file: {file_path}")
        with open(file_path, mode="r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            for row in reader:
                player_name = row.get("player")
                franchise_id = row.get("franchise_id")
                position = row.get("position")
                name_like = like_name(player_name, True).lower()
                
                # Skip if position is not HB, WR, or TE
                if position not in {"HB", "WR", "TE"}:
                    logging.info(f"Skipping player {player_name} with position {position}")
                    continue

                if not player_name or not franchise_id:
                    logging.warning(f"Missing player or franchise ID in row: {row}")
                    continue

                # Calculate additional metrics
                try:
                    routes = float(row.get("routes", 0))
                    targets = float(row.get("targets", 0))
                    tprr = get_tprr(targets, routes)
                except ValueError:
                    logging.warning(f"Invalid numeric data in row: {row}")
                    continue

                # Map fields
                pff_rec_grade = row.get("grades_pass_route")
                yac_per_rec = row.get("yards_after_catch_per_reception")
                yprr = row.get("yprr")
                pff_off_grade = row.get("grades_offense")

                # Find the player in the database
                self.db_util.cursor.execute("""
                    SELECT c.player_year_id
                    FROM cfb_player_year_stats c
                    JOIN player p ON c.player_id = p.player_id
                    JOIN team t ON c.team_id = t.team_id
                    WHERE p.sr_id LIKE %s AND t.pff_id = %s AND c.year = %s
                """, (name_like, franchise_id, year))

                results = self.db_util.cursor.fetchall()
                if not results:
                    logging.warning(f"Player {name_like} (Franchise ID: {franchise_id}, Year: {year}) not found.")
                    self.missing_players.append({
                        "year": year,
                        "player": player_name,
                        "pff_id": franchise_id
                    })
                    continue

                # Update the player's stats
                player_year_id = results[0][0]
                self.update_player_stats(
                    player_year_id, pff_rec_grade, yac_per_rec, yprr, tprr, pff_off_grade
                )

    def update_player_stats(self, player_year_id, pff_rec_grade, yac_per_rec, yprr, tprr, pff_off_grade):
        try:
            self.db_util.cursor.execute("""
                UPDATE cfb_player_year_stats
                SET pff_rec_grade = %s,
                    yac_per_rec = %s,
                    yprr = %s,
                    tprr = %s,
                    pff_off_grade = %s
                WHERE player_year_id = %s
            """, (pff_rec_grade, yac_per_rec, yprr, tprr, pff_off_grade, player_year_id))
            self.db_util.conn.commit()
            logging.info(f"Updated stats for player_year_id {player_year_id}")
        except Exception as e:
            logging.error(f"Error updating stats for player_year_id {player_year_id}: {e}")

    def closed(self, reason):
        # Write missing players to a CSV file
        if self.missing_players:
            logging.info(f"Writing missing players to {self.missing_players_file}")
            with open(self.missing_players_file, mode="w", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["year", "player", "pff_id"])
                writer.writeheader()
                writer.writerows(self.missing_players)
        # Close database connection
        self.db_util.cursor.close()
        self.db_util.conn.close()
