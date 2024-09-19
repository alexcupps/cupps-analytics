import scrapy
import logging
from ..util.db_util import DatabaseUtility
from ..util.crawler_settings import get_custom_settings
from urllib.parse import quote_plus
import re

class PlayerSpider(scrapy.Spider):
    name = 'player_spider'
    custom_settings = get_custom_settings()

    # Define the years to iterate over
    years = list(range(2000, 2023 + 1))  # From 2000 to 2023 inclusive

    def __init__(self, *args, **kwargs):
        super(PlayerSpider, self).__init__(*args, **kwargs)
        # Initialize the database connection using the utility class
        self.db_util = DatabaseUtility()

    def start_requests(self):
        # Fetch all schools from the database
        self.db_util.cursor.execute("SELECT team_id, team_name FROM team")
        schools = self.db_util.cursor.fetchall()

        logging.info(f"Number of schools fetched: {len(schools)}")

        for year in self.years:
            for school in schools:
                team_id, team_name = school

                # Handle multiple-word schools by replacing spaces with dashes
                school_url_part = quote_plus(team_name.lower().replace(' ', '-'))
                url = f"https://www.sports-reference.com/cfb/schools/{school_url_part}/{year}.html"
                
                logging.info(f"School URL: {url}")
                
                # Send request for each school and year
                yield scrapy.Request(url, callback=self.parse_school_page, meta={'team_id': team_id, 'year': year})

    def parse_school_page(self, response):

        # The school page we're attempting to crawl may or may not be valid
        # If the school didn't exist yet in the year we are crawling, it will be invalid
        valid_page = not response.xpath('//div[@id="content"]//h1[text()="Page Not Found (404 error)"]').get()

        if valid_page:
            team_id = response.meta['team_id']
            year = response.meta['year']

            # Try to find the table in the normal HTML first
            rows = response.xpath('//table[@id="rushing_and_receiving"]//tbody/tr')

            if rows:
                logging.info(f"Found table in normal HTML. Number of rows found: {len(rows)}")
                for row in rows:
                    player_name = row.xpath('.//td[@data-stat="player"]/a/text()').get()
                    player_url = row.xpath('.//td[@data-stat="player"]/a/@href').get()
            else:
                logging.info("Table not found in normal HTML. Searching in comments...")

                # If the table is not found, search within the comments
                players_table_html = response.xpath('//comment()').re_first(r'(?s)<!--.*?(<table[^>]+id="rushing_and_receiving".*?</table>).*?-->')

                if players_table_html:
                    # Use Scrapy's HTML parser to convert the extracted table HTML string back to a selector
                    sel = scrapy.Selector(text=players_table_html)
                    rows = sel.xpath('//table//tbody/tr')

                    logging.info(f"Found table in comments. Number of rows found: {len(rows)}")

                    for row in rows:
                        player_name = row.xpath('.//td[@data-stat="player"]/a/text()').get()
                        player_url = row.xpath('.//td[@data-stat="player"]/a/@href').get()

                        # Log the player name and URL
                        # logging.info(f"Player Name: {player_name}")

                        if player_url:
                            player_page_url = response.urljoin(player_url)
                            # logging.info(f"Player URL: {player_page_url}")
                            # Go to the player page to fetch more detailed info
                            yield scrapy.Request(player_page_url, callback=self.parse_player_page, meta={
                                'player_name': player_name,
                                'team_id': team_id,
                                'year': year
                            })

                else:
                    logging.info("Table not found inside comments.")
        else:
            logging.warn(f"Invalid school page found - {response.url} - cannot crawl.")

    def parse_player_page(self, response):
        player_name = response.meta['player_name']
        team_id = response.meta['team_id']
        year = response.meta['year']

        logging.info(f"Inside parse_player_page for player {player_name} - {team_id} - {year}")

        # Extract the position from the player page and clean it
        player_position = response.xpath('normalize-space(//p[strong/text()="Position"]/text()[normalize-space()])').get().strip().replace(':', '').replace(' ', '')

        logging.info(f"Player position: {player_position}")

        # Only proceed if the player is RB, WR, or TE
        if player_position in ['RB', 'WR', 'TE']:

            # Extract the number from the player URL (e.g., '-3' from 'aj-brown-3')
            # Using this ID helps us maintain unique rows for all players in the DB
            player_url_id = int(re.search(r'-([0-9]+)\.html$', response.url).group(1))

            # Check if the player already exists in the database using the name and url_id
            self.db_util.cursor.execute("""
                SELECT player_id FROM player WHERE name = %s AND sr_id = %s
                """, (player_name, player_url_id))
    
            player_row = self.db_util.cursor.fetchone()

            if not player_row:
                # Add a new row for the player
                # Try to get their height and weight
                height_text = response.xpath('//div[@id="info"]//p/span[contains(text(), "-")]/text()').get()  # Text like "5-9"
                weight_text = response.xpath('//div[@id="info"]//p/span[contains(text(), "lb")]/text()').get()  # Text like "195lb"

                # Convert height to inches
                if height_text:
                    feet, inches = map(int, height_text.split('-'))
                    height_in_inches = feet * 12 + inches
                else:
                    height_in_inches = None

                # Convert weight to an integer by removing "lb" and stripping whitespace
                if weight_text:
                    weight = int(weight_text.replace('lb', '').strip())
                else:
                    weight = None
                # Insert player into player table
                self.db_util.cursor.execute("""
                    INSERT INTO player (name, position, height, weight, sr_id)
                    VALUES (%s, %s, %s, %s, %s)
                """, (player_name, player_position, height_in_inches, weight, player_url_id))

                # Commit and get the newly inserted player ID
                self.db_util.conn.commit()
                player_id = self.db_util.cursor.lastrowid

                logging.info(f"Added {player_name} - {player_position} - {height_in_inches} - {weight} to player table")

            else:
                logging.info(f"{player_name} already exists in the DB. Not adding a duplicate row.")
            # Parse the player's year stats
            self.parse_player_stats(response, player_id, team_id, year)

    def parse_player_stats(self, response, player_id, team_id, year):
        # Find the table that contains the player's stats (either receiving or rushing)
        table = response.xpath('//table[@id="receiving" or @id="rushing"]')
        
        # Check if the table exists
        if not table:
            logging.info(f"No stats table found for player_id: {player_id}, year: {year}")
            return
        
        # Find the specific row that matches the year we're looking for
        row = table.xpath(f'//tbody/tr[.//th[@data-stat="year_id"]/a[text()="{year}"]]')
        
        if not row:
            logging.info(f"No stats found for player_id: {player_id} in the year {year}")
            return
        
        logging.info(f"Found stats for player_id: {player_id}, year: {year}")
        
        # Extract player stats for the specific year
        player_class = row.xpath('.//td[@data-stat="class"]/text()').get(default='')
        games_played = row.xpath('.//td[@data-stat="g"]/text()').get(default=0)
        receptions = row.xpath('.//td[@data-stat="rec"]/text()').get(default=0)
        rec_yds = row.xpath('.//td[@data-stat="rec_yds"]/text()').get(default=0)
        rec_td = row.xpath('.//td[@data-stat="rec_td"]/text()').get(default=0)
        rush_att = row.xpath('.//td[@data-stat="rush_att"]/text()').get(default=0)
        rush_yds = row.xpath('.//td[@data-stat="rush_yds"]/text()').get(default=0)
        rush_td = row.xpath('.//td[@data-stat="rush_td"]/text()').get(default=0)
        
        # Insert into player_year_stats table
        self.db_util.cursor.execute("""
            INSERT INTO player_year_stats (
                player_id, team_id, year, games_played, rec_yds, receptions, 
                rush_yds, rush_att, rush_td, rec_td, class
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (player_id, team_id, year, games_played, rec_yds, receptions, rush_yds, rush_att, rush_td, rec_td, player_class))

        # Commit the transaction after inserting the stats
        self.db_util.conn.commit()

        # Log the successful insertion
        logging.info(f"Successfully added stats for player_id: {player_id}, year: {year}, team_id: {team_id}, "
                    f"class: {player_class}, games: {games_played}, receptions: {receptions}, receiving yards: {rec_yds}, "
                    f"rushing yards: {rush_yds}, rush attempts: {rush_att}, rushing touchdowns: {rush_td}, "
                    f"receiving touchdowns: {rec_td}")



    def closed(self, reason):
        # Close the database connection when the spider finishes
        self.db_util.close_connection()
