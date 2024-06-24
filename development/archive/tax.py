import logging
import configparser
from datetime import datetime
from decimal import Decimal
import pymysql
import pandas as pd
from typing import Optional

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
FILE_PATH = config['file']['path']

class Database:
    def __init__(self, config):
        self.connection = pymysql.connect(**config)
        self.cursor = self.connection.cursor()

    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        self.cursor.execute(query, params)
        return pd.DataFrame(self.cursor.fetchall(), columns=[desc[0] for desc in self.cursor.description])

    def execute_update(self, query: str, params: tuple = None):
        self.cursor.execute(query, params)
        self.connection.commit()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cursor.close()
        self.connection.close()

# Utility functions
def calculate_days_to_expiration(expiry_date, trade_date) -> int:
    if pd.isna(expiry_date):
        return 0
    days_delta = expiry_date - trade_date
    return days_delta.days

def calculate_days_in_trade(open_trade_date, trade_date) -> int:
    days_delta = trade_date - open_trade_date
    return days_delta.days

def trade_time_equal(ib_datetime, last_ib_datetime) -> bool:
    return ib_datetime == last_ib_datetime

def calculate_combo(row, last_trade):
    if trade_time_equal(row['IBDateTime'], last_trade['IBDateTime']):
        combo_id = f"Combo-{row['id']}"
        if row['quantity'] > 0:
            prefix = "BullPS" if row['strike'] < last_trade['strike'] else "BullCS"
        else:
            prefix = "BearPS" if row['strike'] > last_trade['strike'] else "BearCS"
        combo = f"{prefix}-{combo_id}"
        return combo
    return ""

def fetch_joined_data(cursor) -> pd.DataFrame:
    sql = """
    SELECT 
        s.*, 
        t.symbol, t.description AS trade_description, t.conid, t.amount AS trade_amount, t.quantity, 
        t.transactionID AS trade_transactionID, t.buySell, t.assetCategory, t.IBDateTime, t.strike,
        t.expiryDate, t.tradeDate, t.openClose, t.putCall, t.fifoPnlRealized, t.fxRateToBase, t.action
    FROM statements s
    LEFT JOIN trades t ON s.transactionID = t.transactionID
    """
    return cursor.execute_query(sql)

def fetch_statement(cursor, transaction_id: int) -> Optional[pd.Series]:
    sql = "SELECT * FROM statements WHERE transactionID = %s"
    statement = cursor.execute_query(sql, (transaction_id,))
    if statement.empty:
        logging.info(f"No statement found for Transaction ID: {transaction_id}")
        return None
    return statement.iloc[0]

def fetch_open_transaction_id(cursor, conid: int) -> Optional[int]:
    sql = "SELECT MIN(transactionID) AS transactionID FROM openPositions WHERE conid = %s"
    result = cursor.execute_query(sql, (conid,))
    if result.empty or pd.isna(result.loc[0, 'transactionID']):
        return None
    return int(result.loc[0, 'transactionID'])

def delete_open_trade(cursor, transaction_id: int):
    sql = "DELETE FROM openPositions WHERE transactionID = %s"
    cursor.execute_update(sql, (transaction_id,))

def update_open_trade(cursor, open_trade: pd.Series, open_statement: pd.Series, statement: pd.Series, trade: pd.Series):
    new_quantity = open_trade['quantity'] + trade['quantity']
    new_amount = open_statement['amount'] + statement['amount']
    transaction_id = open_trade['transactionID']
    sql = f"UPDATE openPositions SET quantity = %s, amount = %s WHERE transactionID = %s"
    cursor.execute_update(sql, (new_quantity, new_amount, transaction_id))

def update_statement_field(cursor, statement_id: int, value: str, field: str):
    sql = f"UPDATE statements SET {field} = %s WHERE transactionID = %s"
    cursor.execute_update(sql, (value, statement_id))

def get_open_positions_count(cursor, conid: int) -> int:
    sql = "SELECT COUNT(*) AS count FROM openPositions WHERE conid = %s"
    result = cursor.execute_query(sql, (conid,))
    return int(result.loc[0, 'count'])

def update_open_positions(cursor, data: pd.DataFrame, last_trade: Optional[pd.Series]):
    for idx, row in data.iterrows():
        if row['openClose'] == "Open":
            insert_open_trade(cursor, row, last_trade)
        elif row['openClose'] == "Close":
            open_transaction_id = fetch_open_transaction_id(cursor, row['conid'])
            if open_transaction_id is None:
                logging.info("No open position found, nothing to close")
                continue
            open_statement = fetch_statement(cursor, open_transaction_id)
            open_trade = data[data['transactionID'] == open_transaction_id].iloc[0]
            insert_closed_trade(cursor, open_statement, row, open_trade)
            if row['quantity'] >= -open_trade['quantity']:
                delete_open_trade(cursor, open_trade['transactionID'])
            else:
                update_open_trade(cursor, open_trade, open_statement, row)
        else:
            logging.error(f"Invalid openClose value: {row['openClose']}")
            raise ValueError("Invalid openClose value")

def insert_open_trade(cursor, row, last_trade):
    combo = calculate_combo(row, last_trade)
    days_to_expiration = calculate_days_to_expiration(row['expiryDate'], row['tradeDate'])
    sql = (
        "INSERT INTO openPositions (symbol, description, conid, amount, quantity, transactionID, buySell, assetCategory, combo, daysToExpiration) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    values = (  row['symbol'], row['trade_description'], row['conid'], row['trade_amount'], row['quantity'], row['trade_transactionID'],
                row['buySell'], row['assetCategory'], combo, days_to_expiration)
    cursor.execute_update(sql, values)
    info = f"Open-Trade {row['trade_description']} with total price {row['trade_amount']} and quantity: {row['quantity']} inserted, Combo: {combo}"
    logging.info(info)
    update_statement_field(cursor, row['transactionID'], info, "opInfo")

def insert_closed_trade(cursor, open_statement, close_statement, open_trade):
    days_to_expiration = calculate_days_to_expiration(open_trade['expiryDate'], open_trade['tradeDate'])
    days_in_trade = calculate_days_in_trade(open_trade['tradeDate'], close_statement['tradeDate'])
    close_result = (open_statement['amount'] / close_statement['quantity']) * close_statement['quantity'] + close_statement['trade_amount']
    comment = f"{open_trade['assetCategory']} {open_trade['buySell']} --> {close_statement['buySell']} {open_statement['amount']} {close_statement['trade_amount']} Result: {close_result}"
    sql = (
        "INSERT INTO closedPositions (transactionID, symbol, description, conid, assetCategory, "
        "openTransactionID, openBuySell, openDate, openAmount, daysToExpiration, openQuantity, "
        "closeDate, daysInTrade, closeAmount, closeQuantity, closeBuySell, closeResult, comment) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    values = (
        close_statement['trade_transactionID'], close_statement['symbol'], close_statement['trade_description'], close_statement['conid'], close_statement['assetCategory'],
        open_trade['transactionID'], open_trade['buySell'], open_trade['tradeDate'], open_statement['amount'], days_to_expiration, open_trade['quantity'],
        close_statement['tradeDate'], days_in_trade, close_statement['trade_amount'], close_statement['quantity'], close_statement['buySell'], close_result, comment
    )
    cursor.execute_update(sql, values)
    info = f"Close-Trade {close_statement['trade_description']} with total price {close_statement['trade_amount']} and quantity: {close_statement['quantity']} inserted"
    logging.info(info)
    update_statement_field(cursor, close_statement['transactionID'], info, "opInfo")

def tx_insert_tax_statement(cursor, row):
    if row['openClose'] == "Open":
        tax_fifo_result = row['trade_amount']
    else:
        tax_fifo_result = tax_calculate_fifo_result(cursor, row)
    tx_year = row['datum'].year
    comment = f"in: txInsertTax Statement {row['openClose']} {row['trade_description']} with total price {row['trade_amount']} and quantity: {row['quantity']} inserted FiFo {tax_fifo_result}"
    sql = (
        "INSERT INTO taxStatements (transactionID, symbol, underlyingSymbol, conid, description, activityDescription, assetCategory, "
        "openClose, tradeDate, taxYear, buySell, putCall, baseAmount, baseCurrency, baseBalance, fifoPnlRealized, "
        "taxFiFoResult, quantity, comment, record, fxRateToBase, action) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    values = (
        row['transactionID'], row['symbol'], row['underlyingSymbol'], row['conid'], row['trade_description'], row['activityDescription'], row['assetCategory'],
        row['openClose'], row['tradeDate'], tx_year, row['buySell'], row['putCall'], row['trade_amount'], row['IBcurrency'],
        row['balance'], row['fifoPnlRealized'], tax_fifo_result, row['quantity'], comment, row.name, row['fxRateToBase'], row['action']
    )
    cursor.execute_update(sql, values)
    update_statement_field(cursor, row['transactionID'], f"+ 530 - Tx inserted with result: {tax_fifo_result} According to fifoPnlRealized {row['fifoPnlRealized']} Success or not?", "txInfo")

def tx_update_tax_statement(cursor, row):
    sql_field = None
    if tx_is_opt_open_trade(row):
        sql_field = f"taxAmountStillhalterGewinn = {row['taxFiFoResult']}"
    elif row['openClose'] == "Open":
        return
    elif row['assetCategory'] == "OPT":
        if row['quantity'] > 0:
            if row['fifoPnlRealized'] > 0.0:
                sql_field = f"taxAmountStillhalterGewinn = {row['taxFiFoResult']}"
            else:
                sql_field = f"taxAmountStillhalterverlust = {row['taxFiFoResult']}"
        elif row['fifoPnlRealized'] > 0.0:
            sql_field = f"taxAmountTerminGewinn = {row['taxFiFoResult']}"
        else:
            sql_field = f"taxAmountTerminVerlust = {row['taxFiFoResult']}"
    elif row['assetCategory'] == "STK":
        if row['taxFiFoResult'] > 0.0:
            sql_field = f"taxAmountAktienGewinn = {row['taxFiFoResult']}"
        else:
            sql_field = f"taxAmountAktienverlust = {row['taxFiFoResult']}"
    if sql_field:
        sql = f"UPDATE taxStatements SET {sql_field} WHERE transactionID = {row['transactionID']}"
        cursor.execute_update(sql)

def tx_is_opt_open_trade(row) -> bool:
    return row['openClose'] == "Open" and row['buySell'] == "SELL" and row['assetCategory'] == "OPT"

def tax_calculate_fifo_result(cursor, row) -> Decimal:
    if row['assetCategory'] == "STK" or (row['assetCategory'] == "OPT" and row['action'] == "STC"):
        return row['fifoPnlRealized']
    return row['trade_amount']

# Main function to process statements
def main():
    base_currency = "BaseCurrency"
    max_statements = 100  # Set a sensible limit for max statements to process
    start_index = 0  # Starting index for processing

    with Database(DB_CONFIG) as db:
        data = fetch_joined_data(db)
        
        # Filter and process the DataFrame
        data = data[start_index:max_statements]

        last_trade = None

        for idx, row in data.iterrows():
            logging.info(f"Processing statement {idx}: ID: {row['id']} - {row['activityDescription']}")

            if row['levelOfDetail'] != base_currency:
                continue

            open_positions_count = get_open_positions_count(db, row['conid'])
            logging.info(f"Open positions count for conid {row['conid']}: {open_positions_count}")

            tx_insert_tax_statement(db, row)
            update_open_positions(db, data, last_trade)

            last_trade = row.copy()

    logging.info(f"Finished processing {len(data)} statements")

if __name__ == "__main__":
    main()
