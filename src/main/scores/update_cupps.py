import logging
import sys
import os

# Add "src/main/python" to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from main.util.db_util import DatabaseUtility
from calculate_cupps_score import update_cupps_scores  # Updated function

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db_util = DatabaseUtility()  # Initialize DB connection

    # Pass in positions via command line, e.g. python run_cupps.py TE WR
    positions = sys.argv[1:] if len(sys.argv) > 1 else None

    logging.info(f"Starting CUPPS score update process for positions: {positions or 'ALL'}")
    update_cupps_scores(db_util, positions)
    logging.info("CUPPS score update process completed.")

    db_util.cursor.close()
    db_util.conn.close()
