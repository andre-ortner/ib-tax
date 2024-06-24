from typing import Dict, Any

def validate_trade_data(trade_data: Dict[str, Any]) -> bool:
    required_fields = ["transaction_id", "account_id", "trade_date", "trade_datetime"]
    for field in required_fields:
        if field not in trade_data or not trade_data[field]:
            return False
    return True

def validate_statement_data(statement_data: Dict[str, Any]) -> bool:
    required_fields = ["transaction_id", "account_id", "date", "amount", "currency"]
    for field in required_fields:
        if field not in statement_data or not statement_data[field]:
            return False
    return True

def validate_tax_info_data(tax_info_data: Dict[str, Any]) -> bool:
    required_fields = ["transaction_id", "account_id", "trade_date", "ib_datetime", "quantity", "price", "ib_commission", "amount", "currency"]
    for field in required_fields:
        if field not in tax_info_data or not tax_info_data[field]:
            return False
    return True
