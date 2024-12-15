import logging

def get_custom_settings():
    return {
        'DOWNLOAD_DELAY': 2,  # Reduced delay (2 seconds) for more efficiency while avoiding timeouts
        'CONCURRENT_REQUESTS': 1,  # Allow 2 concurrent requests instead of 1
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
        'COOKIES_ENABLED': True,  # Disable cookies to make fewer repetitive requests
        'DOWNLOAD_TIMEOUT': 15,  # Set a timeout of 15 seconds to avoid long-hanging requests
    }


def update_player(self, player_id, draft_pick=None, birthday=None, height=None, weight=None, draft_year=None):
    # Start the base query and an empty list to hold set clauses and parameters
    query = "UPDATE player SET "
    set_clauses = []
    parameters = []

    # Conditionally add fields to the update query
    if draft_pick is not None:
        set_clauses.append("draft_cap = %s")
        parameters.append(draft_pick)
    if birthday is not None:
        set_clauses.append("birthday = %s")
        parameters.append(birthday)
    if height is not None:
        set_clauses.append("height = %s")
        parameters.append(height)
    if weight is not None:
        set_clauses.append("weight = %s")
        parameters.append(weight)
    if draft_year is not None:
        set_clauses.append("draft_year = %s")
        parameters.append(draft_year)

    # Ensure there's at least one field to update
    if set_clauses:
        # Join the set clauses and complete the query with a WHERE clause
        query += ", ".join(set_clauses)
        query += " WHERE player_id = %s"
        parameters.append(player_id)

        # Execute the query
        self.db_util.cursor.execute(query, parameters)
        self.db_util.conn.commit()
        logging.info(f"Updated player data for {player_id}")
    else:
        logging.info(f"No fields to update for player {player_id}")

def convert_date(date_text, datetime):
    if date_text:
        # Convert to a date object
        birthdate = datetime.strptime(date_text, "%Y-%m-%d").date()
    else:
        birthdate = None  # No date found
    return birthdate

def convert_height(height_text):
    if height_text:
        feet, inches = map(int, height_text.split('-'))
        height_in_inches = feet * 12 + inches
    else:
        height_in_inches = None

    return height_in_inches

def convert_weight(weight_text):
    if weight_text:
        weight_in_lbs = int(weight_text.replace('lb', '').strip())
    else:
        weight_in_lbs = None

    return weight_in_lbs