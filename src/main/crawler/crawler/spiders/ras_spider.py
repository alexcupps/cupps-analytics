import os
import csv
import logging
import scrapy
from ....util.db_util import DatabaseUtility
from ..util.crawler_util import find_player_id

class RASSpider(scrapy.Spider):
    name = "ras_spider"

    def __init__(self, start_year=None, end_year=None, position=None, *args, **kwargs):
        """
        :param start_year: Start year for processing.
        :param end_year: End year for processing.
        :param position: Player position to filter (e.g., "rb", "wr", "te").
        """
        super().__init__(*args, **kwargs)
        self.start_year = int(start_year)
        self.end_year = int(end_year)
        self.position = position.lower()  # Normalize position input

        # Set the data directory dynamically based on the position
        self.data_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), f"../../../data/ras/{self.position}")
        )
        logging.info(f"Resolved data directory: {self.data_dir}")

        # Initialize database utility
        self.db_util = DatabaseUtility()

        # Missing players directory
        self.missing_players_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../missing_players")
        )
        os.makedirs(self.missing_players_dir, exist_ok=True)  # Ensure the directory exists

        # Missing players log file
        self.missing_players_file = os.path.join(
            self.missing_players_dir,
            f"ras_missing_players_{self.position}.csv"
        )
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
                player_name = row.get("Name")
                ras_score = row.get("RAS")

                if not ras_score:
                    logging.warning(f"No RAS score found for player {player_name}")
                    continue

                try:
                    ras_score = float(ras_score)
                except ValueError:
                    logging.warning(f"Invalid RAS score for player {player_name}: {ras_score}")
                    continue

                # Use helper function to find player_id
                player_id = find_player_id(self.db_util, player_name)

                if not player_id:
                    logging.warning(f"Player {player_name} not found.")
                    self.missing_players.append({
                        "year": year,
                        "player": player_name,
                        "ras": ras_score
                    })
                    continue

                # Update the player's RAS score
                self.update_player_ras(player_id, ras_score)

    def update_player_ras(self, player_id, ras_score):
        """
        Update the player's RAS score in the database.
        """
        try:
            self.db_util.cursor.execute(
                """
                UPDATE player
                SET ras = %s
                WHERE player_id = %s
                """,
                (ras_score, player_id)
            )
            self.db_util.conn.commit()
            logging.info(f"Updated RAS score for player_id {player_id}: {ras_score}")
        except Exception as e:
            logging.error(f"Error updating RAS score for player_id {player_id}: {e}")

    def closed(self, reason):
        # Write missing players to a CSV file
        if self.missing_players:
            logging.info(f"Writing missing players to {self.missing_players_file}")
            with open(self.missing_players_file, mode="w", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=["year", "player", "ras"])
                writer.writeheader()
                writer.writerows(self.missing_players)

        # Close database connection
        self.db_util.cursor.close()
        self.db_util.conn.close()
