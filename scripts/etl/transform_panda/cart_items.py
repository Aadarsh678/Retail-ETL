import pandas as pd
from datetime import datetime
from typing import Dict, Union

def safe_to_timestamp_multi_formats(date_series: pd.Series) -> pd.Series:
    """Convert date strings to timestamps using multiple possible formats"""
    formats = [
        "%Y-%m-%d %H:%M:%S",  # yyyy-MM-dd HH:mm:ss
        "%Y-%m-%d",            # yyyy-MM-dd
        "%d-%m-%Y",            # dd-MM-yyyy
        "%m/%d/%Y",            # MM/dd/yyyy
        "%d/%m/%Y",            # dd/MM/yyyy
        "%Y/%m/%d",            # yyyy/MM/dd
        "%B %d %Y",            # March 5 2024
        "%b %d %Y",            # Mar 5 2024
    ]
    
    # Clean the strings first
    clean_series = date_series.str.replace(r'(?i)(st|nd|rd|th)', '', regex=True)
    clean_series = clean_series.str.replace(r' +', ' ', regex=True).str.strip()
    
    result = pd.Series(index=date_series.index, dtype='datetime64[ns]')
    
    for fmt in formats:
        mask = result.isna()
        try:
            result.loc[mask] = pd.to_datetime(
                clean_series.loc[mask], 
                format=fmt, 
                errors='coerce'
            )
        except:
            continue
            
    return result

# --- ASIA ---
def transform_cart_items_asia(df: pd.DataFrame, exchange_rates: Dict[str, float]) -> pd.DataFrame:
    rate = exchange_rates.get("JPY/USD", 0.0068)
    return pd.DataFrame({
        'cart_item_id': df['cart_item_id'],
        'cart_id': df['cart_id'],
        'variant_id': df['variant_id'],
        'quantity': df['qty'],
        'unit_price_usd': df['unit_price_jpy'] * rate,
        'added_at': safe_to_timestamp_multi_formats(df['added']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_cart_items_eu(df: pd.DataFrame, exchange_rates: Dict[str, float]) -> pd.DataFrame:
    rate = exchange_rates.get("EUR/USD", 1.08)
    return pd.DataFrame({
        'cart_item_id': df['cart_item_id'],
        'cart_id': df['cart_id'],
        'variant_id': df['variant_id'],
        'quantity': df['quantity'],
        'unit_price_usd': df['unit_price_eur'] * rate,
        'added_at': safe_to_timestamp_multi_formats(df['added_at']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated_at']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- US ---
def transform_cart_items_us(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'cart_item_id': df['cart_item_id'],
        'cart_id': df['cart_id'],
        'variant_id': df['variant_id'],
        'quantity': df['quantity'],
        'unit_price_usd': df['unit_price_usd'],
        'added_at': safe_to_timestamp_multi_formats(df['added_at']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated_at']),
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_cart_items(df: pd.DataFrame, region: str, exchange_rates: Dict[str, float]) -> pd.DataFrame:
    """Main transformation function that routes to region-specific logic"""
    if region == "asia":
        return transform_cart_items_asia(df, exchange_rates)
    elif region == "eu":
        return transform_cart_items_eu(df, exchange_rates)
    elif region == "us":
        return transform_cart_items_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")

# Example usage:
# df = pd.read_parquet("/path/to/parquet")
# exchange_rates = {"JPY/USD": 0.0068, "EUR/USD": 1.08}
# transformed_df = transform_cart_items(df, "asia", exchange_rates)