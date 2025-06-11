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

def normalize_is_abandoned(flag_series: pd.Series) -> pd.Series:
    return flag_series.str.lower().replace({
        '1': True, 'true': True, 'yes': True, 'y': True,
        '0': False, 'false': False, 'no': False, 'n': False
    })

# --- ASIA ---
def transform_cart_asia(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'cart_id': df['cart_id'],
        'customer_id': df['cust_id'],
        'session_id': df['session_id'],
        'created_at': safe_to_timestamp_multi_formats(df['created']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated']),
        'abandoned_at': safe_to_timestamp_multi_formats(df['abandoned']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_cart_eu(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'cart_id': df['cart_id'],
        'customer_id': df['customer_id'],
        'session_id': df['session_id'],
        'created_at': safe_to_timestamp_multi_formats(df['created_at']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated_at']),
        'abandoned_at': safe_to_timestamp_multi_formats(df['abandoned_at']),
        'is_abandoned': normalize_is_abandoned(df['is_abandoned']),
        '_region': 'eu',
        '_source': df['_source']
    })

# --- US ---
def transform_cart_us(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'cart_id': df['cart_id'],
        'customer_id': df['customer_id'],
        'session_id': df['session_id'],
        'created_at': safe_to_timestamp_multi_formats(df['created_at']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated_at']),
        'abandoned_at': safe_to_timestamp_multi_formats(df['abandoned_at']),
        'is_abandoned': df['is_abandoned'].astype(bool),
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_shopping_cart(df: pd.DataFrame, region: str) -> pd.DataFrame:
    if region == "asia":
        return transform_cart_asia(df)
    elif region == "eu":
        return transform_cart_eu(df)
    elif region == "us":
        return transform_cart_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")