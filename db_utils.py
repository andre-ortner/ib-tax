import pymysql
import logging
import sys
import configparser
from decimal import Decimal
from typing import Tuple, Any

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("app.log"),
    logging.StreamHandler(sys.stdout)
])

def connect_to_database(config: configparser.ConfigParser) -> Tuple[pymysql.cursors.Cursor, pymysql.connections.Connection]:
    try:
        connection = pymysql.connect(
            host=config['database']['host'],
            user=config['database']['user'],
            password=config['database']['password'],
            db=config['database']['dbname']
        )
        cursor = connection.cursor(pymysql.cursors.DictCursor)
        logging.info(f"Connected to database {config['database']['dbname']}")
        return cursor, connection
    except pymysql.MySQLError as e:
        logging.error(f"Database connection failed: {e}")
        raise

def close_database(cursor: pymysql.cursors.Cursor, connection: pymysql.connections.Connection) -> None:
    cursor.close()
    connection.close()