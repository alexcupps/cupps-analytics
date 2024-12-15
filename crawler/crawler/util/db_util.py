import os
import mysql.connector

class DatabaseUtility:
    def __init__(self, dictionary=False):
        self.conn = mysql.connector.connect(
            host=os.getenv('DB_HOST'),
            user=os.getenv('DB_USER'),
            password=os.getenv('DB_PASSWORD'),
            database=os.getenv('DB_NAME')
        )
        self.cursor = self.conn.cursor(dictionary=dictionary)

    def close_connection(self):
        self.cursor.close()
        self.conn.close()
