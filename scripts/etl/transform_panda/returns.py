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
def transform_returns_asia(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    rate = exchange_rates.get("JPY/USD", 0.0068)
    return pd.DataFrame({
        'return_id': df['return_id'],
        'order_id': df['order_id'],
        'return_reason': df['reason'],
        'return_status': df['status'],
        'return_timestamp': safe_to_timestamp_multi_formats(df['return_time']),
        'refund_amount': df['refund_jpy'] * rate,
        'refund_timestamp': df['refund_time'],
        'created_at': safe_to_timestamp_multi_formats(df['created']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_returns_eu(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    rate = exchange_rates.get("EUR/USD", 1.08)
    return pd.DataFrame({
        'return_id': df['return_id'],
        'order_id': df['order_id'],
        'return_reason': df['return_reason'],
        'return_status': df['return_status'],
        'return_timestamp': df['return_timestamp'],
        'refund_amount': df['refund_amount_eur'] * rate,
        'refund_timestamp': df['refund_timestamp'],
        'created_at': df['created_at'],
        'cooling_off_period_days': df['cooling_off_period_days'],
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- US ---
def transform_returns_us(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'return_id': df['return_id'],
        'order_id': df['order_id'],
        'return_reason': df['return_reason'],
        'return_status': df['return_status'],
        'return_timestamp': df['return_date'],
        'refund_amount': df['refund_amount_usd'],
        'refund_timestamp': df['refund_date'],
        'created_at': df['created_at'],
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_returns(df: pd.DataFrame, region: str, exchange_rates: dict) -> pd.DataFrame:
    if region == "asia":
        return transform_returns_asia(df, exchange_rates)
    elif region == "eu":
        return transform_returns_eu(df, exchange_rates)
    elif region == "us":
        return transform_returns_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")