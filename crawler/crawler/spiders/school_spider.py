import os
import scrapy
import mysql.connector

class SchoolSpider(scrapy.Spider):
    name = 'school_spider'
    start_urls = ['https://www.sports-reference.com/cfb/schools/']

    custom_settings = {
        'DOWNLOAD_DELAY': 5,  # Wait 5 seconds between requests to avoid overloading the server
        'CONCURRENT_REQUESTS': 1,  # Only send 1 request at a time
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 3,  # Initial delay of 3 seconds between requests
        'AUTOTHROTTLE_MAX_DELAY': 60,  # Maximum delay of 60 seconds between requests
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 0.5,  # Number of requests sent in parallel
        'RETRY_ENABLED': True,  # Enable retrying on failed requests
        'RETRY_TIMES': 5,  # Retry up to 5 times for failed requests
        'RETRY_HTTP_CODES': [429],  # Retry on 429 error (rate-limited)
        'ROBOTSTXT_OBEY': True,  # Obey robots.txt rules
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',  # Set custom user-agent to mimic a browser
        'AUTOTHROTTLE_DEBUG': False,  # Disable debugging of the AutoThrottle feature
    }

    # Initialize the MySQL connection (adjust the credentials as needed)
    def __init__(self):
        self.conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        self.cursor = self.conn.cursor()

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
                    # Follow the link to the school's detail page to scrape the conference
                    yield scrapy.Request(url=team_link, callback=self.parse_school_details, meta={'team_name': team_name})

    def parse_school_details(self, response):
        team_name = response.meta['team_name']
        # Extract the first conference name from the 'Conferences' section, if available
        conference = response.xpath('//p[strong[text()="Conferences:"]]/a[1]/text()').get()

        # If no conference is found, set conference to an empty string
        if not conference:
            conference = ""

        # Add the school to the database
        self.add_school(team_name, conference)

    def add_school(self, team_name, conference):
        try:
            sql = """
                INSERT INTO team (team_name, conference)
                VALUES (%s, %s)
            """
            self.cursor.execute(sql, (team_name, conference))
            self.conn.commit()
            self.log(f'Successfully added {team_name} with conference {conference}')
        except mysql.connector.Error as err:
            self.log(f"Error: {err}")
            self.conn.rollback()

    def closed(self, reason):
        # Close the MySQL connection when the spider finishes
        self.cursor.close()
        self.conn.close()