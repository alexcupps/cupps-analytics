import scrapy
import logging
from ....util.db_util import DatabaseUtility
from ..util.crawler_util import *
from urllib.parse import quote_plus
import re
from datetime import datetime

class DraftSpider(scrapy.Spider):
    name = 'draft_spider'
    custom_settings = get_custom_settings()

    # Define the years to iterate over
    years = list(range(2024, 2024 + 1))  # From 2000 to 2023 inclusive

    def __init__(self, *args, **kwargs):
        super(DraftSpider, self).__init__(*args, **kwargs)
        # Initialize the database connection using the utility class
        self.db_util = DatabaseUtility(dictionary=True)

    def start_requests(self):
        # Fetch all schools from the database
        self.db_util.cursor.execute("SELECT team_id, team_name, sr_name FROM team")
        schools = self.db_util.cursor.fetchall()

        logging.info(f"Number of schools fetched: {len(schools)}")

        for year in self.years:

            url = f"https://www.pro-football-reference.com/years/{year}/draft.htm"
            logging.info(f"{year} Draft URL: {url}")
                
            # Send request for each school and year
            yield scrapy.Request(url, callback=self.parse_draft_page, meta={'year': year})

    def parse_draft_page(self, response):
        year = response.meta.get('year')
        for row in response.xpath('//table[@id="drafts"]//tbody/tr'):
            #look at the position of the player drafted
            position = row.xpath('.//td[@data-stat="pos"]/text()').get()

            if position in ['WR', 'TE', 'RB']:
                player_name = row.xpath('.//td[@data-stat="player"]//a/text()').get()
                
                pick = row.xpath('.//td[@data-stat="draft_pick"]/text()').get()
                logging.info(f"Found player: {player_name}, Pick: {pick}, Year: {year}")

                player_href = row.xpath('.//td[@data-stat="player"]//a/@href').get()
                player_url = response.urljoin(player_href)  # Construct full URL
                logging.info(f"Pro Page URL: {player_url}")

                #store the 'pick' value for this row
                #click the href to the player page
                yield scrapy.Request(
                    url=player_url,
                    callback=self.parse_pro_page,
                    meta={'player_name': player_name, 'pick': pick, 'draft_year': year}
                )    

    def parse_pro_page(self, response):
        # Extract the player data from the response
        player_name = response.meta.get('player_name')
        draft_pick = response.meta.get('pick')
        draft_year = response.meta.get('draft_year')
        logging.info(f"Inside parse_pro_page for player {player_name} - {draft_pick} - {draft_year}")

        # Extract SR ID from the College Stats link
        college_link = response.xpath('//a[contains(text(), "College Stats")]/@href').get()
        if college_link:
            sr_id = re.search(r'/cfb/players/([a-zA-Z0-9-]+)\.html$', college_link).group(1)

            # Search for the player in the database by name and SR ID
            self.db_util.cursor.execute("""
                SELECT * FROM player WHERE name = %s AND sr_id = %s
            """, (player_name, sr_id))
            player_row = self.db_util.cursor.fetchone()

            if not player_row:
                logging.info(f"Player {player_name} with SR ID {sr_id} not found in the DB. Adding new player.")
                self.db_util.cursor.execute("""
                    INSERT INTO player (name, sr_id)
                    VALUES (%s, %s)
                """, (player_name, sr_id))
                self.db_util.conn.commit()  # Commit to save changes

                # Retrieve the newly added player row
                self.db_util.cursor.execute("""
                    SELECT * FROM player WHERE name = %s AND sr_id = %s
                """, (player_name, sr_id))
                player_row = self.db_util.cursor.fetchone()

            # Set draft_cap, height, weight, and birthday fields if not already present
            player_id = player_row['player_id']
            height = player_row.get('height')
            weight = player_row.get('weight')
            height_val = None
            weight_val = None
            birthday = None
            if not height:
                height_text = response.xpath('//div[@id="info"]//p/span[contains(text(), "-")]/text()').get()
                height_val = convert_height(height_text)
            if not weight:
                weight_text = response.xpath('//div[@id="info"]//p/span[contains(text(), "lb")]/text()').get()
                weight_val = convert_weight(weight_text)
            birthdate_text = response.xpath('//span[@id="necro-birth"]/@data-birth').get()
            birthday = convert_date(birthdate_text, datetime)

            update_player(self, 
                          player_id=player_id, 
                          draft_pick=draft_pick, 
                          birthday=birthday,
                          height=height_val,
                          weight=weight_val,
                          draft_year=draft_year
                         )
            
    def closed(self, reason):
    # Close the database connection when the spider finishes
        self.db_util.close_connection()

        