import logging
import sys
import os

# Add "src/main/python" to sys.path
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))

from util.db_util import DatabaseUtility  # Absolute import
from calculate_cupps_score import update_cupps_scores  # Direct import

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db_util = DatabaseUtility()  # Initialize DB connection

    logging.info("Starting CUPPS score update process...")
    update_cupps_scores(db_util)
    logging.info("CUPPS score update process completed.")

    db_util.cursor.close()
    db_util.conn.close()
