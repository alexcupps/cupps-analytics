def get_custom_settings():
    return {
        'DOWNLOAD_DELAY': 2,  # Reduced delay (2 seconds) for more efficiency while avoiding timeouts
        'CONCURRENT_REQUESTS': 2,  # Allow 2 concurrent requests instead of 1
        'AUTOTHROTTLE_ENABLED': True,
        'AUTOTHROTTLE_START_DELAY': 2,  # Reduced initial delay for faster start
        'AUTOTHROTTLE_MAX_DELAY': 30,  # Max delay reduced to 30 seconds to avoid overly long pauses
        'AUTOTHROTTLE_TARGET_CONCURRENCY': 1.0,  # Aim for a single request at a time per second
        'RETRY_ENABLED': True,
        'RETRY_TIMES': 3,  # Reduce retries to 3 to avoid too many repeated attempts
        'RETRY_HTTP_CODES': [429, 500, 502, 503, 504],  # Include common server error codes along with 429
        'ROBOTSTXT_OBEY': True,  # Continue to obey robots.txt
        'USER_AGENT': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.36',
        'AUTOTHROTTLE_DEBUG': False,
        'COOKIES_ENABLED': False,  # Disable cookies to make fewer repetitive requests
        'DOWNLOAD_TIMEOUT': 15,  # Set a timeout of 15 seconds to avoid long-hanging requests
    }

