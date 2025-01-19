import os
import csv
import logging
import scrapy
from ..util.db_util import DatabaseUtility
from ..util.crawler_util import *
import json

class PFFSpider(scrapy.Spider):
    name = "pff_spider"

    def __init__(self, table_name, data_type, start_year=None, end_year=None, *args, **kwargs):
        """
        :param table_name: Name of the table to update (e.g., cfb_player_year_stats, nfl_player_year_stats).
        :param data_type: Type of data being processed (e.g., "receiving", "rushing").
        :param start_year: Start year for processing.
        :param end_year: End year for processing.
        """
        super().__init__(*args, **kwargs)
        self.table_name = table_name
        self.data_type = data_type
        self.start_year = int(start_year)
        self.end_year = int(end_year)

        self.data_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), f"../../../data/pff/{self.table_name.split('_')[0]}/{self.data_type}")
        )
        logging.info(f"Resolved data directory: {self.data_dir}")

        # Field mapping for updates
        self.field_mapping = self.get_field_mapping(data_type)

        # Initialize database utility
        self.db_util = DatabaseUtility()

        # Missing players log file
        self.missing_players_file = os.path.abspath(f"pff_missing_{self.table_name.split('_')[0]}_{data_type}_players.csv")
        self.missing_players = []

    def get_field_mapping(self, data_type):
        """
        Define field mappings based on data type.
        """
        if data_type == "receiving":
            return {
                "grades_pass_route": "pff_rec_grade",
                "yards_after_catch_per_reception": "yac_per_rec",
                "yprr": "yprr",
                "tprr": "tprr",
                "grades_offense": "pff_off_grade",
            }
        elif data_type == "rushing":
            return {
                "yco_attempt": "yac_per_att",
                "grades_run": "pff_run_grade",
                "ypa": "ypa",
                "elusive_rating": "elu_rtg",
                "grades_offense": "pff_off_grade",
            }
        else:
            raise ValueError(f"Unsupported data type: {data_type}")

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

                # Skip irrelevant positions if applicable
                if position not in {"HB", "WR", "TE"}:
                    logging.info(f"Skipping player {player_name} with position {position}")
                    continue

                if not player_name or not franchise_id:
                    logging.warning(f"Missing player or franchise ID in row: {row}")
                    continue

                # Calculate additional metrics
                if "routes" in row and "targets" in row:
                    try:
                        routes = float(row.get("routes", 0))
                        targets = float(row.get("targets", 0))
                        row["tprr"] = get_tprr(targets, routes)
                    except ValueError:
                        logging.warning(f"Invalid numeric data in row: {row}")
                        continue

                # Map fields
                updates = {db_field: row.get(csv_field) for csv_field, db_field in self.field_mapping.items()}

                # Find the player in the database by sr_id OR exact name match
                self.db_util.cursor.execute(f"""
                    SELECT c.player_year_id
                    FROM {self.table_name} c
                    JOIN player p ON c.player_id = p.player_id
                    JOIN team t ON c.team_id = t.team_id
                    WHERE (p.sr_id LIKE %s OR p.name = %s) 
                    AND t.pff_id = %s 
                    AND c.year = %s
                """, (name_like, player_name, franchise_id, year))

                results = self.db_util.cursor.fetchall()

                # If no match, check nicknames
                if not results:
                    logging.info(f"No direct match found for {player_name}. Checking nicknames...")

                    # Safely escape player_name for JSON query
                    escaped_player_name = json.dumps(player_name)

                    # Query nicknames JSON field for matches
                    self.db_util.cursor.execute(f"""
                        SELECT c.player_year_id
                        FROM {self.table_name} c
                        JOIN player p ON c.player_id = p.player_id
                        JOIN team t ON c.team_id = t.team_id
                        WHERE JSON_CONTAINS(p.nicknames, %s) AND t.pff_id = %s AND c.year = %s
                    """, (escaped_player_name, franchise_id, year))  # Escaped safely

                    results = self.db_util.cursor.fetchall()

                # If still no results, log as missing
                if not results:
                    logging.warning(f"Player {player_name} (Franchise ID: {franchise_id}, Year: {year}) not found after checking nicknames.")
                    self.missing_players.append({
                        "year": year,
                        "player": player_name,
                        "pff_id": franchise_id
                    })
                    continue

                # Update the player's stats
                player_year_id = results[0][0]
                self.update_player_stats(player_year_id, updates)

    def update_player_stats(self, player_year_id, updates):
        """
        Update the player's stats dynamically based on the provided updates.
        """
        try:
            set_clause = ", ".join(f"{field} = %s" for field in updates.keys())
            values = list(updates.values()) + [player_year_id]
            self.db_util.cursor.execute(f"""
                UPDATE {self.table_name}
                SET {set_clause}
                WHERE player_year_id = %s
            """, values)
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
