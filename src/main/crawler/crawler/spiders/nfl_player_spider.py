import os
import logging
import scrapy
import csv
from ....util.db_util import DatabaseUtility
from ..util.crawler_util import *

class NFLPlayerSpider(scrapy.Spider):
    name = "nfl_player_spider"
    custom_settings = get_custom_settings()

    def __init__(self, start_year, end_year, *args, **kwargs):
        """
        :param start_year: Start year for processing.
        :param end_year: End year for processing.
        """
        super().__init__(*args, **kwargs)
        self.start_year = int(start_year)
        self.end_year = int(end_year)
        self.db_util = DatabaseUtility()

        self.missing_players_dir = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "../missing_players")
        )
        os.makedirs(self.missing_players_dir, exist_ok=True)

        # Define the path for the missing players file
        self.missing_players_file = os.path.join(
            self.missing_players_dir, "missing_nfl_players.csv"
        )
        self.missing_players = []

    def start_requests(self):
        # Get all NFL teams from the database
        self.db_util.cursor.execute("SELECT team_id, sr_name FROM team WHERE is_nfl = TRUE")
        teams = self.db_util.cursor.fetchall()

        logging.info(f"Found {len(teams)} NFL teams to process.")

        for year in range(self.start_year, self.end_year + 1):
            for team_id, sr_name in teams:
                url = f"https://www.pro-football-reference.com/teams/{sr_name}/{year}.htm"
                yield scrapy.Request(url, callback=self.parse_team_page, meta={'team_id': team_id, 'year': year})

    def parse_team_page(self, response):
        team_id = response.meta['team_id']
        year = response.meta['year']
        rows = response.xpath('//table[@id="rushing_and_receiving"]//tbody/tr')

        if not rows:
            logging.warning(f"No rushing and receiving data found for team {team_id} in year {year}.")
            return

        logging.info(f"Processing {len(rows)} players for team {team_id} in year {year}.")

        for row in rows:
            player_name = row.xpath('.//td[@data-stat="name_display"]/a/text()').get()
            player_url = row.xpath('.//td[@data-stat="name_display"]/a/@href').get()
            pos = row.xpath('.//td[@data-stat="pos"]/text()').get()

            if pos in ["RB", "WR", "TE"]:

                if not player_url:
                    logging.warning(f"No link found for player {player_name}. Skipping.")
                    continue

                player_page_url = response.urljoin(player_url)
                yield scrapy.Request(
                    player_page_url,
                    callback=self.verify_player,
                    meta={
                        'player_name': player_name,
                        'team_id': team_id,
                        'year': year,
                        'row_data': row.extract(),  # Include raw row data for processing later
                    },
                    dont_filter=True,  # Bypass deduplication
                )
            
            else:
                logging.info(f"Skipping {player_name} with position {pos}.")

    def verify_player(self, response):
        """
        Verify if the player exists in the player table using the College Stats link.
        """
        player_name = response.meta['player_name']
        team_id = response.meta['team_id']
        year = response.meta['year']
        row_data = response.meta['row_data']

        # Extract College Stats link
        college_link = response.xpath('//a[contains(text(), "College Stats")]/@href').get()
        if not college_link:
            logging.warning(f"College Stats link not found for {response.url}")
            return

        sr_id = extract_sr_id(college_link)  # Extract the sr_id from the College Stats link

        # Check if the player exists in the database
        self.db_util.cursor.execute("SELECT player_id FROM player WHERE sr_id = %s", (sr_id,))
        result = self.db_util.cursor.fetchone()
        if not result:
            logging.warning(f"Player with sr_id {sr_id} not found in DB. Adding to missing players log.")
            self.missing_players.append({
                'year': year,
                'team_id': team_id,
                'sr_id': sr_id,
                'college_link': college_link,
            })
            return

        player_id = result[0]
        logging.info(f"Player {player_id} found in DB. Saving stats...")

        # Save the player's NFL stats
        self.save_nfl_stats(player_id, team_id, year, row_data)

    def save_nfl_stats(self, player_id, team_id, year, row_data):
        """
        Save NFL stats for a player.
        """
        try:
            # Extract stats from the row data
            row_selector = scrapy.Selector(text=row_data)
            stats = {
                'games_played': row_selector.xpath('string(.//td[@data-stat="games"])').get(),
                'rec_yds': row_selector.xpath('string(.//td[@data-stat="rec_yds"])').get(),
                'rec': row_selector.xpath('string(.//td[@data-stat="rec"])').get(),
                'rush_yds': row_selector.xpath('string(.//td[@data-stat="rush_yds"])').get(),
                'rush_att': row_selector.xpath('string(.//td[@data-stat="rush_att"])').get(),
                'rush_td': row_selector.xpath('string(.//td[@data-stat="rush_td"])').get(),
                'rec_td': row_selector.xpath('string(.//td[@data-stat="rec_td"])').get(),
            }

            # Check if stats already exist for this player, team, and year
            self.db_util.cursor.execute("""
                SELECT COUNT(*)
                FROM nfl_player_year_stats
                WHERE player_id = %s AND team_id = %s AND year = %s
            """, (player_id, team_id, year))

            if self.db_util.cursor.fetchone()[0] == 0:
                # Insert the stats into the NFL player year stats table
                self.db_util.cursor.execute("""
                    INSERT INTO nfl_player_year_stats (
                        player_id, team_id, year, games_played, rec_yds, receptions, rush_yds, rush_att, rush_td, rec_td
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    player_id, team_id, year,
                    stats['games_played'],
                    stats['rec_yds'],
                    stats['rec'], 
                    stats['rush_yds'],
                    stats['rush_att'],  
                    stats['rush_td'],
                    stats['rec_td']
                ))
                self.db_util.conn.commit()
                logging.info(f"Saved NFL stats for player_id {player_id}, team_id {team_id}, year {year}.")
            else:
                logging.info(f"Stats for player_id {player_id}, team_id {team_id}, year {year} already exist. Skipping.")

        except Exception as e:
            logging.error(f"Error saving stats for player {player_id}, team {team_id}, year {year}: {e}")

    def closed(self, reason):
        # Write missing players to a CSV file
        if self.missing_players:
            logging.info(f"Writing missing players to {self.missing_players_file}")
            with open(self.missing_players_file, mode="w", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=['year', 'team_id', 'sr_id', 'college_link'])
                writer.writeheader()
                writer.writerows(self.missing_players)

        # Close database connection
        self.db_util.cursor.close()
        self.db_util.conn.close()
