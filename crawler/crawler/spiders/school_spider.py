import os
import scrapy
import mysql.connector
from ..util.db_util import DatabaseUtility
from ..util.crawler_settings import get_custom_settings

class SchoolSpider(scrapy.Spider):
    name = 'school_spider'
    start_urls = ['https://www.sports-reference.com/cfb/schools/']

    custom_settings = get_custom_settings()

    def __init__(self, *args, **kwargs):
        super(SchoolSpider, self).__init__(*args, **kwargs)
        # Initialize the database connection using the utility class
        self.db_util = DatabaseUtility()

    def parse(self, response):
        # Loop through each school row in the table
        for row in response.xpath('//table[@id="schools"]/tbody/tr'):
            team_name = row.xpath('td[@data-stat="school_name"]/a/text()').get()
            team_link = row.xpath('td[@data-stat="school_name"]/a/@href').get()
            to_year = row.xpath('td[@data-stat="year_max"]/text()').get()

            # Only process schools where the "To" year is 2024 or later
            if to_year and int(to_year) >= 2024:
                if team_link:
                    team_link = response.urljoin(team_link)
                    
                    # Extract the sr_name from the URL (portion after '/schools/' and before the trailing slash)
                    sr_name = team_link.split('/cfb/schools/')[1].rstrip('/')

                    # Follow the link to the school's detail page to scrape additional details
                    yield scrapy.Request(url=team_link, callback=self.parse_school_details, meta={'team_name': team_name, 'sr_name': sr_name})

    def parse_school_details(self, response):
        team_name = response.meta['team_name']
        sr_name = response.meta['sr_name']  # Capture the sr_name from meta
        # Extract the first conference name from the 'Conferences' section, if available
        conference = response.xpath('//p[strong[text()="Conferences:"]]/a[1]/text()').get()

        # If no conference is found, set conference to an empty string
        if not conference:
            conference = ""

        # Add the school to the database with sr_name
        self.add_school(team_name, conference, sr_name)

    def add_school(self, team_name, conference, sr_name):
        try:
            sql = """
                INSERT INTO team (team_name, conference, sr_name)
                VALUES (%s, %s, %s)
            """
            self.db_util.cursor.execute(sql, (team_name, conference, sr_name))
            self.db_util.conn.commit()
            self.log(f'Successfully added {team_name} with conference {conference} and sr_name {sr_name}')
        except mysql.connector.Error as err:
            self.log(f"Error: {err}")
            self.db_util.conn.rollback()

    def closed(self, reason):
        # Close the database connection when the spider finishes
        self.db_util.close_connection()
