import logging
from decimal import Decimal, DivisionByZero
import pandas as pd
from typing import Optional, Tuple, Any
import pymysql
from pymysql.cursors import Cursor
from helper import calculate_days_to_expiration, calculate_days_in_trade, trade_time_equal

def update_tx_amount_currency(tx_id: int, amount: Decimal, currency: str, cursor: pymysql.cursors.Cursor, connection: pymysql.connections.Connection) -> None:
    try:
        cursor.execute("""
            UPDATE transactions
            SET amount = %s, currency = %s
            WHERE transaction_id = %s
        """, (amount, currency, tx_id))
        connection.commit()
        logging.info(f"Updated transaction {tx_id} with amount {amount} and currency {currency}")
    except pymysql.MySQLError as e:
        logging.error(f"Failed to update transaction {tx_id}: {e}")

def update_tx_field(tx_id: int, field_name: str, field_value: Any, cursor: pymysql.cursors.Cursor, connection: pymysql.connections.Connection) -> None:
    try:
        query = f"UPDATE transactions SET {field_name} = %s WHERE transaction_id = %s"
        cursor.execute(query, (field_value, tx_id))
        connection.commit()
        logging.info(f"Updated transaction {tx_id} field {field_name} with value {field_value}")
    except pymysql.MySQLError as e:
        logging.error(f"Failed to update transaction {tx_id} field {field_name}: {e}")

def calculate_combo(row: pd.Series, last_trade: pd.Series) -> str:
    if trade_time_equal(row['IBDateTime'], last_trade['IBDateTime']):
        combo_id = f"Combo-{row['id']}"
        if row['quantity'] > 0:
            prefix = "BullPS" if row['strike'] < last_trade['strike'] else "BullCS"
        else:
            prefix = "BearPS" if row['strike'] > last_trade['strike'] else "BearCS"
        combo = f"{prefix}-{combo_id}"
        return combo
    return ""

def fetch_joined_data(cursor: Cursor) -> pd.DataFrame:
    sql = """
    SELECT 
        s.*, 
        t.symbol, t.description AS trade_description, t.conid, t.amount AS trade_amount, t.quantity, 
        t.transactionID AS trade_transactionID, t.buySell, t.assetCategory, t.IBDateTime, t.strike,
        t.expiryDate, t.tradeDate, t.openClose, t.putCall, t.fifoPnlRealized, t.fxRateToBase, t.action
    FROM statements s
    LEFT JOIN trades t ON s.transactionID = t.transactionID
    """
    cursor.execute(sql)
    result = cursor.fetchall()
    return pd.DataFrame(result)

def fetch_statement(cursor: Cursor, transaction_id: int) -> Optional[pd.Series]:
    sql = "SELECT * FROM statements WHERE transactionID = %s"
    cursor.execute(sql, (transaction_id,))
    statement = cursor.fetchone()
    if not statement:
        logging.info(f"No statement found for Transaction ID: {transaction_id}")
        return None
    return pd.Series(statement)

def fetch_open_transaction_id(cursor: Cursor, conid: int) -> Optional[int]:
    sql = "SELECT MIN(transactionID) AS transactionID FROM openPositions WHERE conid = %s"
    cursor.execute(sql, (conid,))
    result = cursor.fetchone()
    if not result or not result['transactionID']:
        return None
    return int(result['transactionID'])

def delete_open_trade(cursor: Cursor, transaction_id: int) -> None:
    sql = "DELETE FROM openPositions WHERE transactionID = %s"
    cursor.execute(sql, (transaction_id,))

def update_open_trade(cursor: Cursor, open_trade: pd.Series, open_statement: pd.Series, statement: pd.Series, trade: pd.Series) -> None:
    new_quantity = open_trade['quantity'] + trade['quantity']
    new_amount = open_statement['amount'] + statement['amount']
    transaction_id = open_trade['transactionID']
    sql = f"UPDATE openPositions SET quantity = %s, amount = %s WHERE transactionID = %s"
    cursor.execute(sql, (new_quantity, new_amount, transaction_id))

def update_statement_field(cursor: Cursor, statement_id: int, value: str, field: str) -> None:
    sql = f"UPDATE statements SET {field} = %s WHERE transactionID = %s"
    cursor.execute(sql, (value, statement_id))

def get_open_positions_count(cursor: Cursor, conid: int) -> int:
    sql = "SELECT COUNT(*) AS count FROM openPositions WHERE conid = %s"
    cursor.execute(sql, (conid,))
    result = cursor.fetchone()
    return int(result['count'])

def update_open_positions(cursor: Cursor, data: pd.DataFrame, last_trade: Optional[pd.Series]) -> None:
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

def insert_open_trade(cursor: Cursor, row: pd.Series, last_trade: Optional[pd.Series]) -> None:
    combo = calculate_combo(row, last_trade)
    days_to_expiration = calculate_days_to_expiration(row['expiryDate'], row['tradeDate'])
    sql = (
        "INSERT INTO openPositions (symbol, description, conid, amount, quantity, transactionID, buySell, assetCategory, combo, daysToExpiration) "
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )
    values = (  row['symbol'], row['trade_description'], row['conid'], row['trade_amount'], row['quantity'], row['trade_transactionID'],
                row['buySell'], row['assetCategory'], combo, days_to_expiration)
    cursor.execute(sql, values)
    info = f"Open-Trade {row['trade_description']} with total price {row['trade_amount']} and quantity: {row['quantity']} inserted, Combo: {combo}"
    logging.info(info)
    update_statement_field(cursor, row['transactionID'], info, "opInfo")

def insert_closed_trade(cursor: Cursor, open_statement: pd.Series, close_statement: pd.Series, open_trade: pd.Series) -> None:
    try:
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
        cursor.execute(sql, values)
        info = f"Close-Trade {close_statement['trade_description']} with total price {close_statement['trade_amount']} and quantity: {close_statement['quantity']} inserted"
        logging.info(info)
        update_statement_field(cursor, close_statement['transactionID'], info, "opInfo")
    except DivisionByZero as e:
        logging.error(f"Division by zero error while inserting closed trade: {e}")
    except Exception as e:
        logging.error(f"Unexpected error while inserting closed trade: {e}")

def tx_insert_tax_statement(cursor: Cursor, row: pd.Series) -> None:
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
    cursor.execute(sql, values)
    update_statement_field(cursor, row['transactionID'], f"+ 530 - Tx inserted with result: {tax_fifo_result} According to fifoPnlRealized {row['fifoPnlRealized']} Success or not?", "txInfo")

def tx_update_tax_statement(cursor: Cursor, row: pd.Series) -> None:
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
        cursor.execute(sql)

def tx_is_opt_open_trade(row: pd.Series) -> bool:
    return row['openClose'] == "Open" and row['buySell'] == "SELL" and row['assetCategory'] == "OPT"

def tax_calculate_fifo_result(cursor: Cursor, row: pd.Series) -> Decimal:
    if row['assetCategory'] == "STK" or (row['assetCategory'] == "OPT" and row['action'] == "STC"):
        return row['fifoPnlRealized']
    return row['trade_amount']

def calc_tax(cursor: Cursor) -> None:
    base_currency = "BaseCurrency"
    max_statements = 100  # Set a sensible limit for max statements to process
    start_index = 0  # Starting index for processing

    # Filter and process the DataFrame
    data = fetch_joined_data(cursor)
    data = data[start_index:max_statements]

    last_trade = None

    for idx, row in data.iterrows():
        logging.info(f"Processing statement {idx}: ID: {row['id']} - {row['activityDescription']}")

        if row['levelOfDetail'] != base_currency:
            continue

        open_positions_count = get_open_positions_count(cursor, row['conid'])
        logging.info(f"Open positions count for conid {row['conid']}: {open_positions_count}")

        tx_insert_tax_statement(cursor, row)
        last_trade = row.copy()
        update_open_positions(cursor, data, last_trade)

    logging.info(f"Finished tax calculation of {len(data)} statements")