import scrapy
import mysql.connector

class SchoolSpider(scrapy.Spider):
    name = 'school_spider'
    start_urls = ['https://www.sports-reference.com/cfb/schools/']

    # Initialize the MySQL connection (adjust the credentials as needed)
    def __init__(self):
        self.conn = mysql.connector.connect(
            host='cfl-model-data.cxy4mww2qpjg.us-east-2.rds.amazonaws.com',
            user='admin',
            password='DJeterYankees#2!',
            database='CFL'
        )
        self.cursor = self.conn.cursor()

    def parse(self, response):
        # Loop through each school row in the table
        for row in response.xpath('//table[@id="schools"]/tbody/tr'):
            team_name = row.xpath('td[@data-stat="school_name"]/a/text()').get()
            team_link = row.xpath('td[@data-stat="school_name"]/a/@href').get()

            # Follow the link to the school's detail page to scrape the conference
            if team_link:
                team_link = response.urljoin(team_link)
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