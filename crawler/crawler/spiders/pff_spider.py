import scrapy
import json
import logging
from ..util.crawler_util import get_custom_settings

class PFFSpider(scrapy.Spider):
    name = "pff_spider"
    start_urls = ['https://premium.pff.com/ncaa/positions/2015/REGPO/rushing?division=fbs&position=HB']
    api_url = "https://premium.pff.com/api/v1/facet/rushing/summary?league=ncaa&season=2015&week=0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20&division=fbs"
    custom_settings = get_custom_settings()

    def start_requests(self):
        # Add authentication cookies
        cookies = {
            "_premium_key": "SFMyNTY.g3QAAAABbQAAABZndWFyZGlhbl9kZWZhdWx0X3Rva2VubQAAAlpleUpoYkdjaU9pSklVelV4TWlJc0luUjVjQ0k2SWtwWFZDSjkuZXlKaGRXUWlPaUpRY21WdGFYVnRJaXdpWlhod0lqb3hOek16TVRFME56VTFMQ0pwWVhRaU9qRTNNek14TVRFeE5UVXNJbWx6Y3lJNklsQnlaVzFwZFcwaUxDSnFkR2tpT2lKbE9EbG1aRFF4WkMweU56TmhMVFJqWm1NdFlqSTVZeTAzTmpJd01XWmlNbUV4TTJZaUxDSnVZbVlpT2pFM016TXhNVEV4TlRRc0luQmxiU0k2ZXlKaFlXWWlPakVzSW01allXRWlPakVzSW01bWJDSTZNU3dpZFdac0lqb3hmU3dpYzNWaUlqb2llMXdpWlcxaGFXeGNJanBjSW1GcVltTjFjSEJ6UUdkdFlXbHNMbU52YlZ3aUxGd2labVZoZEhWeVpYTmNJanBiWFN4Y0ltWnBjbk4wWDI1aGJXVmNJanB1ZFd4c0xGd2liR0Z6ZEY5dVlXMWxYQ0k2Ym5Wc2JDeGNJblZwWkZ3aU9sd2lZekF3WVRGaE16UXRabUpqTWkwME56WTRMVGhoT1dZdE1UazJNVEk1WXpjM1ltRmxYQ0lzWENKMlpYSjBhV05oYkZ3aU9sd2lRMjl1YzNWdFpYSmNJbjBpTENKMGVYQWlPaUpoWTJObGMzTWlmUS5GWmxremhMQlhLTDlUQVl4UXp2NUgtVEVTSUlrdnZSM3lpdlpJMTJpbGNxUlNra3JtbHlLX2M4aDRkMVNSYXcxRVQyX01nXzJabGhHZkpoZVI4Mk9vdw.psDte6DpnOteDcFQwCSsmFRV4T093cPsaIw6Ol6X0q0",
            "c_groot_access_token": "wiKgWZKvpYMWm63aHAV13NYVauagtJOJU6IIIqganJqVCflUMz7k6M1h2WaFAsXW"
        }
        
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
        }

        for url in self.start_urls:
            yield scrapy.Request(
                url, 
                cookies=cookies, 
                headers=headers, 
                callback=self.check_login
            )

    def check_login(self, response):
        # Check if logged in
        if 'data-logged-in="true"' in response.text:
            logging.info("Successfully logged in!")

            # Now request data from the API
            headers = {
                "Authorization": f"Bearer {response.request.cookies.get('c_groot_access_token')}",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
            }
            yield scrapy.Request(
                self.api_url,
                headers=headers,
                cookies=response.request.cookies,
                callback=self.parse_api
            )
        else:
            logging.error("Login failed. Check your cookies or session setup.")

    def parse_api(self, response):
        # Parse the API response
        try:
            data = json.loads(response.text)
            logging.info("API response received successfully!")

            # Extract data from "rushing_summary"
            rushing_summary = data.get("rushing_summary", [])
            if rushing_summary:
                for player in rushing_summary:
                    logging.info(
                        f"Player: {player.get('player')}, Team: {player.get('team')}, Position: {player.get('position')}, "
                        f"Attempts: {player.get('attempts')}, Yards: {player.get('yards')}, YPA: {player.get('ypa')}, "
                        f"Touchdowns: {player.get('touchdowns')}, First Downs: {player.get('first_downs')}, "
                        f"Avoided Tackles: {player.get('avoided_tackles')}, Longest: {player.get('longest')}, "
                        f"Yards After Contact: {player.get('yards_after_contact')}, Elusive Rating: {player.get('elusive_rating')}, "
                        f"Draft Season: {player.get('draft_season')}, Grades Offense: {player.get('grades_offense')}"
                    )
            else:
                logging.warning("No rushing summary data found in API response.")
        except json.JSONDecodeError as e:
            logging.error(f"Failed to decode JSON response: {e}")
            logging.debug(response.text)
