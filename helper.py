from datetime import datetime
import pandas as pd

def parse_date(date_str: str, date_format: str = "%Y%m%d") -> datetime:
    return datetime.strptime(date_str, date_format)

def calculate_days_to_expiration(expiry_date: datetime, trade_date: datetime) -> int:
    if pd.isna(expiry_date):
        return 0
    days_delta = expiry_date - trade_date
    return days_delta.days

def calculate_days_in_trade(open_trade_date: datetime, trade_date: datetime) -> int:
    days_delta = trade_date - open_trade_date
    return days_delta.days

def trade_time_equal(ib_datetime: datetime, last_ib_datetime: datetime) -> bool:
    return ib_datetime == last_ib_datetime
