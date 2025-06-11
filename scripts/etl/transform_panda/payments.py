import pandas as pd
from datetime import datetime

def safe_to_timestamp_multi_formats(date_series: pd.Series) -> pd.Series:
    formats = [
        "%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d-%m-%Y", 
        "%m/%d/%Y", "%d/%m/%Y", "%Y/%m/%d",
        "%B %d %Y", "%b %d %Y"
    ]
    
    clean_series = date_series.str.replace(r'(?i)(st|nd|rd|th)', '', regex=True)
    clean_series = clean_series.str.replace(r' +', ' ', regex=True).str.strip()
    
    result = pd.Series(index=date_series.index, dtype='datetime64[ns]')
    
    for fmt in formats:
        mask = result.isna()
        try:
            result.loc[mask] = pd.to_datetime(
                clean_series.loc[mask], format=fmt, errors='coerce'
            )
        except:
            continue
            
    return result

# --- ASIA ---
def transform_payments_asia(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    rate = exchange_rates.get("JPY/USD", 0.0068)
    return pd.DataFrame({
        'payment_id': df['payment_id'],
        'order_id': df['order_id'],
        'payment_method': df['method'],
        'payment_status': df['status'],
        'payment_amount_usd': df['amount_jpy'] * rate,
        'payment_timestamp': safe_to_timestamp_multi_formats(df['payment_time']),
        'transaction_id': df['transaction_ref'],
        'gateway_response': df['gateway_resp'],
        'created_at': safe_to_timestamp_multi_formats(df['created']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_payments_eu(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    rate = exchange_rates.get("EUR/USD", 1.08)
    return pd.DataFrame({
        'payment_id': df['payment_id'],
        'order_id': df['order_id'],
        'payment_method': df['payment_method'],
        'payment_status': df['payment_status'],
        'payment_amount_usd': df['payment_amount_eur'] * rate,
        'payment_timestamp': safe_to_timestamp_multi_formats(df['payment_timestamp']),
        'transaction_id': df['transaction_id'],
        'gateway_response': df['gateway_response'],
        'psd2_compliant': df['psd2_compliant'],
        'created_at': safe_to_timestamp_multi_formats(df['created_at']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- US ---
def transform_payments_us(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'payment_id': df['payment_id'],
        'order_id': df['order_id'],
        'payment_method': df['payment_method'],
        'payment_status': df['payment_status'],
        'payment_amount_usd': df['payment_amount_usd'],
        'payment_timestamp': safe_to_timestamp_multi_formats(df['payment_date']),
        'transaction_id': df['transaction_id'],
        'gateway_response': df['gateway_response'],
        'created_at': safe_to_timestamp_multi_formats(df['created_at']),
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_payments(df: pd.DataFrame, region: str, exchange_rates: dict) -> pd.DataFrame:
    if region == "asia":
        return transform_payments_asia(df, exchange_rates)
    elif region == "eu":
        return transform_payments_eu(df, exchange_rates)
    elif region == "us":
        return transform_payments_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")