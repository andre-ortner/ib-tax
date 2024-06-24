import os
import logging
import argparse
import configparser
import pandas as pd
from xml.dom.minidom import parse
from datetime import datetime
import pymysql
from decimal import Decimal
from database import Database

# Read configuration
config = configparser.ConfigParser()
config.read('config.ini')

FILE_PATH = config['file']['path']

db = Database()

# Logging Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Global variable to keep track of the last date
last_date = None

# Function to parse date strings
def parse_date(date_str, date_format="%Y%m%d"):
    """
    Parse a date string into a datetime object.

    Parameters:
    date_str (str): The date string to parse.
    date_format (str): The format of the date string.

    Returns:
    datetime: The parsed datetime object.
    """
    return datetime.strptime(date_str, date_format)

# Function to calculate days to expiration
def calculate_days_to_expiration(trade):
    """
    Calculate the number of days from the trade date to the expiration date.

    Parameters:
    trade (dict): The trade data containing trade and expiration dates.

    Returns:
    int: The number of days to expiration.
    """
    if trade['expiry_date'] is None:
        return 0
    days_delta = trade['expiry_date'] - trade['trade_date']
    return days_delta.days

# Function to calculate days in trade
def calculate_days_in_trade(open_trade, trade):
    """
    Calculate the number of days between the open trade date and the current trade date.

    Parameters:
    open_trade (dict): The open trade data containing the trade date.
    trade (dict): The current trade data containing the trade date.

    Returns:
    int: The number of days in trade.
    """
    days_delta = trade['trade_date'] - open_trade['trade_date']
    return days_delta.days

# Function to check if trade times are equal
def trade_time_equal(trade, last_trade):
    """
    Check if the trade times of two trades are equal.

    Parameters:
    trade (dict): The current trade data.
    last_trade (dict): The last trade data.

    Returns:
    bool: True if trade times are equal, False otherwise.
    """
    if last_trade is None:
        return False
    return trade['ib_datetime'] == last_trade['ib_datetime']

# Function to update transaction amount and currency
def update_tx_amount_currency(tx_id, amount, currency, cursor, connection):
    """
    Update the transaction amount and currency.

    Parameters:
    tx_id (int): The transaction ID.
    amount (Decimal): The amount to update.
    currency (str): The currency to update.
    cursor (pymysql.cursors.Cursor): The database cursor.
    connection (pymysql.connections.Connection): The database connection.
    """
    cursor.execute("""
        UPDATE transactions
        SET amount = %s, currency = %s
        WHERE transaction_id = %s
    """, (amount, currency, tx_id))
    connection.commit()
    logging.info(f"Updated transaction {tx_id} with amount {amount} and currency {currency}")

# Function to update a transaction field
def update_tx_field(tx_id, field_name, field_value, cursor, connection):
    """
    Update a field in the transaction record.

    Parameters:
    tx_id (int): The transaction ID.
    field_name (str): The name of the field to update.
    field_value (any): The value to update the field with.
    cursor (pymysql.cursors.Cursor): The database cursor.
    connection (pymysql.connections.Connection): The database connection.
    """
    query = f"UPDATE transactions SET {field_name} = %s WHERE transaction_id = %s"
    cursor.execute(query, (field_value, tx_id))
    connection.commit()
    logging.info(f"Updated transaction {tx_id} field {field_name} with value {field_value}")

# Validation functions
def validate_trade_data(trade_data):
    """
    Validate trade data before insertion into the database.

    Parameters:
    trade_data (dict): The trade data to validate.

    Returns:
    bool: True if valid, False otherwise.
    """
    required_fields = ["transaction_id", "account_id", "trade_date", "trade_datetime"]
    for field in required_fields:
        if field not in trade_data or not trade_data[field]:
            return False
    return True

def validate_statement_data(statement_data):
    """
    Validate statement data before insertion into the database.

    Parameters:
    statement_data (dict): The statement data to validate.

    Returns:
    bool: True if valid, False otherwise.
    """
    required_fields = ["transaction_id", "account_id", "date", "amount", "currency"]
    for field in required_fields:
        if field not in statement_data or not statement_data[field]:
            return False
    return True

def validate_tax_info_data(tax_info_data):
    """
    Validate tax info data before insertion into the database.

    Parameters:
    tax_info_data (dict): The tax info data to validate.

    Returns:
    bool: True if valid, False otherwise.
    """
    required_fields = ["transaction_id", "account_id", "trade_date", "ib_datetime", "quantity", "price", "ib_commission", "amount", "currency"]
    for field in required_fields:
        if field not in tax_info_data or not tax_info_data[field]:
            return False
    return True

# Function to process trade items and insert into database
def process_trade_item(item, cursor, connection):
    """
    Process a trade item and insert the data into the database.

    Parameters:
    item (xml.dom.minidom.Element): The trade item from the XML file.
    cursor (pymysql.cursors.Cursor): The database cursor.
    connection (pymysql.connections.Connection): The database connection.
    """
    trade_data = {
        "activity_description": item.getAttribute("activityDescription"),
        "activity_code": item.getAttribute("activityCode"),
        "transaction_id": item.getAttribute("transactionID"),
        "trade_id": int(item.getAttribute("tradeID") or 0),
        "account_id": item.getAttribute("accountId"),
        "conid": item.getAttribute("conid"),
        "description": item.getAttribute("description"),
        "symbol": item.getAttribute("symbol"),
        "trade_date": parse_date(item.getAttribute("tradeDate")),
        "trade_datetime": parse_date(item.getAttribute("dateTime"), "%Y%m%d;%H%M%S"),
        "quantity": item.getAttribute("quantity"),
        "price": item.getAttribute("price"),
        "ib_commission": item.getAttribute("ibCommission"),
        "action": item.getAttribute("action"),
        "fx_rate_to_base": item.getAttribute("fxRateToBase"),
        "capital_gains_pnl": item.getAttribute("capitalGainsPnl"),
        "fx_pnl": item.getAttribute("fxPnl"),
        "open_close": item.getAttribute("openClose"),
        "notes": item.getAttribute("notes")
    }

    if validate_trade_data(trade_data):
        cursor.execute("""
            INSERT INTO trades (
                activity_description, activity_code, transaction_id, trade_id, account_id, conid, 
                description, symbol, trade_date, trade_datetime, quantity, price, ib_commission, 
                action, fx_rate_to_base, capital_gains_pnl, fx_pnl, open_close, notes
            ) VALUES (
                %(activity_description)s, %(activity_code)s, %(transaction_id)s, %(trade_id)s, 
                %(account_id)s, %(conid)s, %(description)s, %(symbol)s, %(trade_date)s, 
                %(trade_datetime)s, %(quantity)s, %(price)s, %(ib_commission)s, %(action)s, 
                %(fx_rate_to_base)s, %(capital_gains_pnl)s, %(fx_pnl)s, %(open_close)s, %(notes)s
            )
        """, trade_data)
        connection.commit()
        logging.info(f"Inserted trade data: {trade_data}")
    else:
        logging.warning(f"Invalid trade data: {trade_data}")

# Function to process statement items and insert into database
def process_statement_item(item, cursor, connection):
    """
    Process a statement item and insert the data into the database.

    Parameters:
    item (xml.dom.minidom.Element): The statement item from the XML file.
    cursor (pymysql.cursors.Cursor): The database cursor.
    connection (pymysql.connections.Connection): The database connection.
    """
    global last_date
    
    statement_data = {
        "transaction_id": int(item.getAttribute("transactionID") or 0),
        "account_id": item.getAttribute("accountId"),
        "settle_date": item.getAttribute("settleDate"),
        "date": item.getAttribute("date"),
        "amount": float(item.getAttribute("amount")),
        "currency": item.getAttribute("currency"),
        "level_of_detail": item.getAttribute("levelOfDetail"),
        "balance": round(float(item.getAttribute("balance")), 2),
        "description": item.getAttribute("description")
    }

    if statement_data["settle_date"] == "MULTI":
        statement_data["date"] = last_date
    else:
        last_date = statement_data["date"]
    
    statement_data["date"] = parse_date(statement_data["date"])

    if validate_statement_data(statement_data):
        cursor.execute("""
            INSERT INTO statements (
                transaction_id, account_id, settle_date, date, amount, currency, 
                level_of_detail, balance, description
            ) VALUES (
                %(transaction_id)s, %(account_id)s, %(settle_date)s, %(date)s, 
                %(amount)s, %(currency)s, %(level_of_detail)s, %(balance)s, %(description)s
            )
        """, statement_data)
        connection.commit()
        logging.info(f"Inserted statement data: {statement_data}")
    else:
        logging.warning(f"Invalid statement data: {statement_data}")

# Function to process tax info items and insert into database
def process_tax_info_item(item, cursor, connection):
    """
    Process a tax info item and insert the data into the database.

    Parameters:
    item (xml.dom.minidom.Element): The tax info item from the XML file.
    cursor (pymysql.cursors.Cursor): The database cursor.
    connection (pymysql.connections.Connection): The database connection.
    """
    tax_info_data = {
        "transaction_id": int(item.getAttribute("transactionID") or 0),
        "account_id": item.getAttribute("accountId"),
        "trade_date": parse_date(item.getAttribute("tradeDate")),
        "expiry_date": parse_date(item.getAttribute("expiryDate")) if item.getAttribute("expiryDate") else None,
        "ib_datetime": parse_date(item.getAttribute("ibDateTime"), "%Y%m%d;%H%M%S"),
        "quantity": int(item.getAttribute("quantity")),
        "price": Decimal(item.getAttribute("price")),
        "ib_commission": Decimal(item.getAttribute("ibCommission")),
        "amount": Decimal(item.getAttribute("amount")),
        "currency": item.getAttribute("currency"),
        "days_to_expiration": None,
        "days_in_trade": None
    }

    # Calculate additional fields
    tax_info_data["days_to_expiration"] = calculate_days_to_expiration(tax_info_data)

    if validate_tax_info_data(tax_info_data):
        cursor.execute("""
            INSERT INTO tax_info (
                transaction_id, account_id, trade_date, expiry_date, ib_datetime, 
                quantity, price, ib_commission, amount, currency, days_to_expiration, days_in_trade
            ) VALUES (
                %(transaction_id)s, %(account_id)s, %(trade_date)s, %(expiry_date)s, %(ib_datetime)s, 
                %(quantity)s, %(price)s, %(ib_commission)s, %(amount)s, %(currency)s, %(days_to_expiration)s, %(days_in_trade)s
            )
        """, tax_info_data)
        connection.commit()
        logging.info(f"Inserted tax info data: {tax_info_data}")
    else:
        logging.warning(f"Invalid tax info data: {tax_info_data}")

# Function to read and process the XML file in batches
def process_xml_file(file_path, db_cursor, db_connection, item_tag, process_function):
    """
    Read and process the XML file in batches.

    Parameters:
    file_path (str): The path to the XML file.
    db_cursor (pymysql.cursors.Cursor): The database cursor.
    db_connection (pymysql.connections.Connection): The database connection.
    item_tag (str): The XML tag to process.
    process_function (function): The function to process each XML item.
    """
    try:
        dom_tree = parse(file_path)
        items = dom_tree.getElementsByTagName(item_tag)
        batch_size = 100
        batch = []

        for item in items:
            batch.append(item)
            if len(batch) == batch_size:
                for batch_item in batch:
                    process_function(batch_item, db_cursor, db_connection)
                batch = []

        # Process remaining items
        for batch_item in batch:
            process_function(batch_item, db_cursor, db_connection)
    except Exception as e:
        logging.error(f"Error processing XML file: {e}")

# Command Line Interface (CLI)
def parse_arguments():
    """
    Parse command line arguments.

    Returns:
    argparse.Namespace: The parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Process XML files and insert data into a database.")
    parser.add_argument('--file', type=str, help='Path to the XML file to process', default=FILE_PATH)
    return parser.parse_args()

# Main function to run the script
def main():
    """
    Main function to run the script.
    """
    args = parse_arguments()

    try:        
        logging.info("Processing trades...")
        process_xml_file(args.file, db.cursor, db.connection, "trade", process_trade_item)
        
        logging.info("Processing statements...")
        process_xml_file(args.file, db.cursor, db.connection, "statement", process_statement_item)
        
        logging.info("Processing tax info...")
        process_xml_file(args.file, db.cursor, db.connection, "taxInfo", process_tax_info_item)
        
    except pymysql.MySQLError as e:
        logging.error(f"Database error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    main()
