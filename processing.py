import logging
import pymysql
from xml.dom.minidom import parse, Element
from decimal import Decimal
from pymysql.cursors import Cursor
from pymysql.connections import Connection
from typing import Optional
from helper import parse_date, calculate_days_to_expiration
from validation import validate_trade_data, validate_statement_data, validate_tax_info_data
from tax_utils import fetch_joined_data, update_open_positions

last_date: Optional[str] = None

def process_trade_item(item: Element, cursor: Cursor, connection: Connection) -> None:
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
        try:
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
        except pymysql.MySQLError as e:
            logging.error(f"Failed to insert trade data: {e}")
    else:
        logging.warning(f"Invalid trade data: {trade_data}")

def process_statement_item(item: Element, cursor: Cursor, connection: Connection) -> None:
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
        try:
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
        except pymysql.MySQLError as e:
            logging.error(f"Failed to insert statement data: {e}")
    else:
        logging.warning(f"Invalid statement data: {statement_data}")

def process_tax_info_item(item: Element, cursor: Cursor, connection: Connection) -> None:
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

    tax_info_data["days_to_expiration"] = calculate_days_to_expiration(tax_info_data['expiry_date'], tax_info_data['trade_date'])

    if validate_tax_info_data(tax_info_data):
        try:
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
        except pymysql.MySQLError as e:
            logging.error(f"Failed to insert tax info data: {e}")
    else:
        logging.warning(f"Invalid tax info data: {tax_info_data}")

def process_xml_file(file_path: str, db_cursor: Cursor, db_connection: Connection, item_tag: str, process_function: callable) -> None:
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

        for batch_item in batch:
            process_function(batch_item, db_cursor, db_connection)
        
        data = fetch_joined_data(db_cursor)
        update_open_positions(db_cursor, data, None)

    except Exception as e:
        logging.error(f"Error processing XML file: {e}")
