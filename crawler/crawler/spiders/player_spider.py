import scrapy
import logging
from ..util.db_util import DatabaseUtility
from ..util.crawler_util import get_custom_settings
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
        self.db_util = DatabaseUtility(dictionary=False)

    def start_requests(self):
        # Fetch all schools from the database
        self.db_util.cursor.execute("SELECT team_id, team_name, sr_name FROM team")
        schools = self.db_util.cursor.fetchall()

        logging.info(f"Number of schools fetched: {len(schools)}")

        for year in self.years:
            for school in schools:
                team_id, team_name, sr_name = school

                url = f"https://www.sports-reference.com/cfb/schools/{sr_name}/{year}.html"
                
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

            # Try the new selector first
            rows = response.xpath('//table[@data-soc-sum-table-type="RushingReceivingStandard"]//tbody/tr')

            if not rows:  # If no rows found with the new selector, fall back to the old selector
                rows = response.xpath('//table[@id="rushing_and_receiving"]//tbody/tr')

            if rows:
                logging.info(f"Found table in normal HTML. Number of rows found: {len(rows)}")
                for row in rows:
                    player_name = row.xpath('.//td[@data-stat="name_display"]/a/text()').get()
                    player_url = row.xpath('.//td[@data-stat="name_display"]/a/@href').get()
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
                logging.info("Table not found in normal HTML. Searching in comments...")

                # If the table is not found, search within the comments
                # Use the updated selector
                players_table_html = response.xpath('//comment()').re_first(
                    r'(?s)<!--.*?(<table[^>]+data-soc-sum-table-type="RushingReceivingStandard".*?</table>).*?-->'
                )

                # Fallback to the old selector if the new one returns nothing
                if not players_table_html:
                    players_table_html = response.xpath('//comment()').re_first(
                        r'(?s)<!--.*?(<table[^>]+id="rushing_and_receiving".*?</table>).*?-->'
                    )

                if players_table_html:
                    # Use Scrapy's HTML parser to convert the extracted table HTML string back to a selector
                    sel = scrapy.Selector(text=players_table_html)
                    rows = sel.xpath('//table//tbody/tr')

                    logging.info(f"Found table in comments. Number of rows found: {len(rows)}")

                    for row in rows:
                        player_name = row.xpath('.//td[@data-stat="name_display"]/a/text()').get()
                        player_url = row.xpath('.//td[@data-stat="name_display"]/a/@href').get()

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

        # Extract player position
        player_position_text = response.xpath('normalize-space(//p[strong/text()="Position"]/text()[normalize-space()])').get()
        logging.info(f"Raw Player Position: {player_position_text}")

        # Clean and split positions based on '/'
        player_positions = player_position_text.strip().replace(':', '').replace(' ', '').split('/')

        # Find the first match from the target positions
        target_positions = ['RB', 'WR', 'TE']
        player_position = next((pos for pos in player_positions if pos in target_positions), None)

        logging.info(f"Saved Player Position: {player_position} (None = non-passcatcher)")

        if player_position:
            # Extract the substring from the player URL (e.g. 'aj-brown-3')
            # Using this string helps us maintain unique rows for all players in the DB
            player_url_id = re.search(r'/cfb/players/([a-zA-Z0-9-]+)\.html$', response.url).group(1)

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
                player_id = player_row[0]  # Retrieve existing player ID
                logging.info(f"{player_name} already exists in the DB. Not adding a duplicate row.")

            # Parse the player's year stats
            logging.info(f"parsing stats for {player_name} - {player_id} - {team_id} - {year}")
            self.parse_player_stats(response, player_id, team_id)

    def parse_player_stats(self, response, player_id, team_id):
        # Find the table that contains the player's stats (either receiving or rushing)
        table = response.xpath('//table[contains(@class, "stats_table") and (contains(@id, "receiving") or contains(@id, "rushing"))]')
        
        # Check if the table exists
        if not table:
            logging.info(f"No stats table found for player_id: {player_id}, year: {year}")
            return

        # Iterate over all rows in the stats table
        rows = table.xpath('//tbody/tr')

        for row in rows:
            # Extract year from the row (important: now extracting the year from the table rows)
            row_year = row.xpath('.//th[@data-stat="year_id"]/a/text()').get()
            if not row_year:
                continue  # Skip rows without a year

            # Extract team name from the row
            team_name = row.xpath('.//td[@data-stat="team_name_abbr"]/a/text()').get(default='')

            # Fetch team ID based on team name (handle transfers)
            self.db_util.cursor.execute("""
                SELECT team_id FROM team WHERE team_name = %s
            """, (team_name,))
            team_row = self.db_util.cursor.fetchone()
            if not team_row:
                logging.info(f"Team {team_name} not found in DB for player_id: {player_id}, year: {row_year}")
                continue

            row_team_id = team_row[0]

            # Extract stats for each year
            # Using normalize-space to get text, whether it's in a <strong> or not
            player_class = row.xpath('normalize-space(.//td[@data-stat="class"])').get(default='')
            games_played = row.xpath('normalize-space(.//td[@data-stat="games"])').get(default=0)
            receptions = row.xpath('normalize-space(.//td[@data-stat="rec"])').get(default=0)
            rec_yds = row.xpath('normalize-space(.//td[@data-stat="rec_yds"])').get(default=0)
            rec_td = row.xpath('normalize-space(.//td[@data-stat="rec_td"])').get(default=0)
            rush_att = row.xpath('normalize-space(.//td[@data-stat="rush_att"])').get(default=0)
            rush_yds = row.xpath('normalize-space(.//td[@data-stat="rush_yds"])').get(default=0)
            rush_td = row.xpath('normalize-space(.//td[@data-stat="rush_td"])').get(default=0)

            # Check if stats already exist for player/year combo
            self.db_util.cursor.execute("""
                SELECT COUNT(*) FROM cfb_player_year_stats WHERE player_id = %s AND year = %s
            """, (player_id, row_year))

            if self.db_util.cursor.fetchone()[0] == 0:
                # Insert into college player_year_stats table
                self.db_util.cursor.execute("""
                    INSERT INTO cfb_player_year_stats (
                        player_id, team_id, year, class, games_played, rec_yds, receptions, 
                        rush_yds, rush_att, rush_td, rec_td
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (player_id, row_team_id, row_year, player_class, games_played, rec_yds, receptions, rush_yds, rush_att, rush_td, rec_td))

                # Commit the transaction after inserting the stats
                self.db_util.conn.commit()

                # Log the successful insertion
                logging.info(f"Successfully added stats for player_id: {player_id}, year: {row_year}, team_id: {row_team_id}, "
                            f"class: {player_class}, games: {games_played}, receptions: {receptions}, receiving yards: {rec_yds}, "
                            f"rushing yards: {rush_yds}, rush attempts: {rush_att}, rushing touchdowns: {rush_td}, "
                            f"receiving touchdowns: {rec_td}")





    def closed(self, reason):
        # Close the database connection when the spider finishes
        self.db_util.close_connection()
