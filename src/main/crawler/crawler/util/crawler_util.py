import logging
import re
import json

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

def get_tprr(targets, routes):
    tprr = targets / routes if routes > 0 else None
    return tprr

def like_name(name, clean):
    if not name:
        return ""
    
    if clean:
        name = clean_name(name)

    # Split the name into words, add a '%' after each word, and join them
    return '%'.join(name.split()) + '%'

def clean_name(name):
    """
    Cleans a name by removing suffixes, extraneous punctuation, and formatting properly,
    while preserving initials and multi-initials like "C.J.".
    """
    # Remove suffixes like "Jr", "III", etc.
    cleaned_name = re.sub(r'\b(Jr|Sr|II|III|IV|V|VI|VII|VIII|IX|X)\b', '', name)

    # Replace periods with nothing, dashes with spaces, apostrophes with nothing
    cleaned_name = cleaned_name.replace('.', '').replace('-', ' ').replace('\'', '')

    # Strip leading/trailing whitespace
    return cleaned_name.strip()

def find_player_id(db_util, player_name):
    """
    Find a player's ID in the `player` table by their name or nicknames.
    :param db_util: Database utility object.
    :param player_name: Name of the player.
    :return: Matched `player_id` or None if not found.
    """
    try:
        # Generate a name_like pattern
        name_like = f"%{'%'.join(player_name.lower().split())}%"
        name_like_normalized = like_name(player_name, True)

        # Base query to find the player
        db_util.cursor.execute("""
            SELECT player_id
            FROM player
            WHERE (sr_id LIKE %s OR sr_id LIKE %s OR name = %s)
        """, (name_like, name_like_normalized, player_name))
        results = db_util.cursor.fetchall()

        # If no direct match, check nicknames
        if not results:
            logging.info(f"No direct match found for {player_name}. Checking nicknames...")
            escaped_player_name = json.dumps(player_name)
            db_util.cursor.execute("""
                SELECT player_id
                FROM player
                WHERE JSON_CONTAINS(nicknames, %s)
            """, (escaped_player_name,))
            results = db_util.cursor.fetchall()

        # Return the matched player_id if found
        return results[0][0] if results else None
    except Exception as e:
        logging.error(f"Error finding player ID for {player_name}: {e}")
        return None


def find_player_year_id(db_util, player_name, franchise_id, year, table_name):
    """
    Find a `player_year_id` for a specific year and team in the stats table.
    :param db_util: Database utility object.
    :param player_name: Name of the player.
    :param franchise_id: Team franchise ID for additional filtering.
    :param year: Year for filtering.
    :param table_name: Table name to search for the player-year mapping.
    :return: Matched `player_year_id` or None if not found.
    """
    try:
        # Generate a name_like pattern
        name_like = f"%{'%'.join(player_name.lower().split())}%"
        name_like_normalized = like_name(player_name, True)

        # Base query to find the player-year mapping
        db_util.cursor.execute(f"""
            SELECT c.player_year_id
            FROM {table_name} c
            JOIN player p ON c.player_id = p.player_id
            JOIN team t ON c.team_id = t.team_id
            WHERE (p.sr_id LIKE %s OR p.sr_id LIKE %s OR p.name = %s)
              AND t.pff_id = %s AND c.year = %s
        """, (name_like, name_like_normalized, player_name, franchise_id, year))
        results = db_util.cursor.fetchall()

        # If no direct match, check nicknames
        if not results:
            logging.info(f"No direct match found for {player_name}. Checking nicknames...")
            escaped_player_name = json.dumps(player_name)
            db_util.cursor.execute(f"""
                SELECT c.player_year_id
                FROM {table_name} c
                JOIN player p ON c.player_id = p.player_id
                JOIN team t ON c.team_id = t.team_id
                WHERE JSON_CONTAINS(p.nicknames, %s)
                  AND t.pff_id = %s AND c.year = %s
            """, (escaped_player_name, franchise_id, year))
            results = db_util.cursor.fetchall()

        # Return the matched player_year_id if found
        return results[0][0] if results else None
    except Exception as e:
        logging.error(f"Error finding player_year_id for {player_name} in year {year}: {e}")
        return None

    
def extract_sr_id(sr_url):
    return re.search(r'/cfb/players/([a-zA-Z0-9-]+)\.html$', sr_url).group(1)

