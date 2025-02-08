import scrapy
import logging
from ....util.db_util import DatabaseUtility
from urllib.parse import quote_plus
from ..util.crawler_util import get_custom_settings

class SchoolYearStatsSpider(scrapy.Spider):
    name = "school_year_stats_spider"
    custom_settings = get_custom_settings()

    # Define the years to iterate over
    start_year = 2024
    end_year = 2024

    def __init__(self, *args, **kwargs):
        super(SchoolYearStatsSpider, self).__init__(*args, **kwargs)
        self.db_util = DatabaseUtility(dictionary=False)  # Initialize database utility

    def start_requests(self):
        # Fetch all CFB schools from the database
        self.db_util.cursor.execute("SELECT team_id, sr_name FROM team WHERE is_nfl = FALSE")
        schools = self.db_util.cursor.fetchall()

        logging.info(f"Number of schools fetched: {len(schools)}")

        for year in range(self.start_year, self.end_year + 1):
            for school in schools:
                team_id, sr_name = school

                # Construct the URL for the school's specific year
                url = f"https://www.sports-reference.com/cfb/schools/{quote_plus(sr_name)}/{year}.html"

                logging.info(f"Processing URL: {url}")

                # Yield a request to fetch the school page
                yield scrapy.Request(
                    url, 
                    callback=self.parse_school_page, 
                    meta={'team_id': team_id, 'year': year}
                )

    def parse_school_page(self, response):
        # Check if the page is valid
        valid_page = not response.xpath('//div[@id="content"]//h1[text()="Page Not Found (404 error)"]').get()

        if not valid_page:
            logging.warning(f"Invalid page for {response.url}, skipping.")
            return

        # Extract the necessary metadata
        team_id = response.meta['team_id']
        year = response.meta['year']

        # Extract the team SOS value
        team_sos = response.xpath('//p/a/strong[text()="SOS"]/parent::a/parent::p/text()').re_first(r':\s([-+]?\d*\.?\d+)')

        if team_sos is None:
            logging.warning(f"SOS value not found for {response.url}, skipping.")
            return

        try:
            # Check if the row already exists
            self.db_util.cursor.execute("""
                SELECT COUNT(*)
                FROM team_year_stats
                WHERE team_id = %s AND year = %s
            """, (team_id, year))
            
            exists = self.db_util.cursor.fetchone()[0] > 0

            if exists:
                # Update the existing row
                self.db_util.cursor.execute("""
                    UPDATE team_year_stats
                    SET team_sos = %s
                    WHERE team_id = %s AND year = %s
                """, (team_sos, team_id, year))
                logging.info(f"Updated team SOS for team_id {team_id} in year {year}")
            else:
                # Insert a new row
                self.db_util.cursor.execute("""
                    INSERT INTO team_year_stats (team_id, year, team_sos)
                    VALUES (%s, %s, %s)
                """, (team_id, year, team_sos))
                logging.info(f"Inserted new team SOS for team_id {team_id} in year {year}")

            # Commit the changes
            self.db_util.conn.commit()

        except Exception as e:
            logging.error(f"Error saving team year stats for team_id {team_id} in year {year}: {e}")

    def closed(self, reason):
        # Close the database connection when the spider finishes
        self.db_util.close_connection()
        logging.info("Database connection closed.")
