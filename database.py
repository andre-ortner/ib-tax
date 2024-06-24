import logging
import configparser
import pymysql
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Read configuration
config = configparser.ConfigParser()
config.read('config.ini')

DB_CONFIG = {
    'host': config['database']['host'],
    'user': config['database']['user'],
    'password': config['database']['password'],
    'database': config['database']['database']
}

class Database:
    def __init__(self, config=DB_CONFIG):
        self.connection = pymysql.connect(**config)
        self.cursor = self.connection.cursor()

    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        self.cursor.execute(query, params)
        return pd.DataFrame(self.cursor.fetchall(), columns=[desc[0] for desc in self.cursor.description])

    def execute_update(self, query: str, params: tuple = None):
        self.cursor.execute(query, params)
        self.connection.commit()

    def close(self):
        self.cursor.close()
        self.connection.close()
