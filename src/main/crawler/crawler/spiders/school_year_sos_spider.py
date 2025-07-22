import scrapy
import logging
from ..util.db_util import DatabaseUtility
from urllib.parse import quote_plus
from ..util.crawler_util import get_custom_settings
from scrapy_playwright.page import PageMethod

class SchoolYearStatsSpider(scrapy.Spider):
    name = "school_year_stats_spider"

    # Add LOG_LEVEL to custom settings
    custom_settings = {
        **get_custom_settings(),
        'LOG_LEVEL': 'INFO'
    }

    start_year = 2024
    end_year = 2024

    def __init__(self, *args, **kwargs):
        super(SchoolYearStatsSpider, self).__init__(*args, **kwargs)
        self.db_util = DatabaseUtility(dictionary=False)

    def start_requests(self):
        self.db_util.cursor.execute("SELECT team_id, sr_name FROM team WHERE is_nfl = FALSE")
        schools = self.db_util.cursor.fetchall()
        logging.info(f"Number of schools fetched: {len(schools)}")

        for year in range(self.start_year, self.end_year + 1):
            for team_id, sr_name in schools:
                url = f"https://www.sports-reference.com/cfb/schools/{quote_plus(sr_name)}/{year}.html"
                logging.info(f"Processing URL: {url}")

                yield scrapy.Request(
                    url,
                    callback=self.parse_school_page,
                    meta={
                        'team_id': team_id,
                        'year': year,
                        'playwright': True,
                        'playwright_page_methods': [
                            PageMethod(
                                "route",
                                "**/*",
                                lambda route, request: route.abort()
                                if request.resource_type in ["image", "media", "font", "stylesheet", "other"]
                                else route.continue_()
                            ),
                            PageMethod("wait_for_selector", "#wrap"),
                            PageMethod("wait_for_timeout", 1000)
                        ]
                    },
                    headers={
                        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 '
                                      '(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36',
                        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9',
                        'Accept-Language': 'en-US,en;q=0.9',
                        'Referer': 'https://www.google.com/',
                        'Connection': 'keep-alive'
                    }
                )

    def parse_school_page(self, response):
        valid_page = not response.xpath('//div[@id="content"]//h1[text()="Page Not Found (404 error)"]').get()
        if not valid_page:
            logging.warning(f"Invalid page for {response.url}, skipping.")
            return

        team_id = response.meta['team_id']
        year = response.meta['year']
        team_sos = response.xpath('//p/a/strong[text()="SOS"]/parent::a/parent::p/text()').re_first(r':\s([-+]?\d*\.?\d+)')
        team_srs = response.xpath('//p/a/strong[text()="SRS"]/parent::a/parent::p/text()').re_first(r':\s([-+]?\d*\.?\d+)')

        if team_sos is None:
            logging.warning(f"SOS value not found for {response.url}, skipping.")
            return
        if team_srs is None:
            logging.warning(f"SRS value not found for {response.url}, skipping.")

        try:
            self.db_util.cursor.execute("""
                SELECT COUNT(*) FROM team_year_stats WHERE team_id = %s AND year = %s
            """, (team_id, year))
            exists = self.db_util.cursor.fetchone()[0] > 0

            if exists:
                self.db_util.cursor.execute("""
                    UPDATE team_year_stats SET team_sos = %s, team_srs = %s
                    WHERE team_id = %s AND year = %s
                """, (team_sos, team_srs, team_id, year))
                logging.info(f"Updated team SOS/SRS for team_id {team_id} in year {year}")
            else:
                self.db_util.cursor.execute("""
                    INSERT INTO team_year_stats (team_id, year, team_sos, team_srs)
                    VALUES (%s, %s, %s, %s)
                """, (team_id, year, team_sos, team_srs))
                logging.info(f"Inserted new team SOS for team_id {team_id} in year {year}")

            self.db_util.conn.commit()

            logging.info(f"✅ Finished processing: {response.url}")

        except Exception as e:
            logging.error(f"Error saving stats for team_id {team_id} in year {year}: {e}")



    def closed(self, reason):
        self.db_util.close_connection()
        logging.info("Database connection closed.")
