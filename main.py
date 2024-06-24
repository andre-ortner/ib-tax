import logging, sys
import configparser
import argparse
import pymysql
from pymysql.cursors import Cursor
from pymysql.connections import Connection
from db_utils import connect_to_database, close_database
from processing import process_xml_file, process_trade_item, process_statement_item, process_tax_info_item
from tax_utils import calc_tax

# Read configuration
config = configparser.ConfigParser()
config.read('config.ini')

FILE_PATH = config['file']['path']

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', handlers=[
    logging.FileHandler("app.log"),
    logging.StreamHandler(sys.stdout)
])

# Command Line Interface (CLI)
def parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Process XML files and insert data into a database.")
    parser.add_argument('--file', type=str, help='Path to the XML file to process', default=FILE_PATH)
    return parser.parse_args()

def main() -> None:
    args = parse_arguments()
    
    cursor, connection = connect_to_database(config)
    try:        
        logging.info("Processing trades...")
        process_xml_file(args.file, cursor, connection, "trade", process_trade_item)
        
        logging.info("Processing statements...")
        process_xml_file(args.file, cursor, connection, "statement", process_statement_item)
        
        logging.info("Processing tax info...")
        process_xml_file(args.file, cursor, connection, "taxInfo", process_tax_info_item)
    
    except pymysql.MySQLError as e:
        logging.error(f"Database error: {e}")
    
    finally:
        close_database(cursor, connection)

if __name__ == "__main__":
    main()
