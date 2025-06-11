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
def transform_wishlists_asia(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'wishlist_id': df['wishlist_id'],
        'customer_id': df['cust_id'],
        'product_id': df['product_id'],
        'added_at': safe_to_timestamp_multi_formats(df['added']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_wishlists_eu(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'wishlist_id': df['wishlist_id'],
        'customer_id': df['customer_id'],
        'product_id': df['product_id'],
        'added_at': df['added_at'],
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- US ---
def transform_wishlists_us(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'wishlist_id': df['wishlist_id'],
        'customer_id': df['customer_id'],
        'product_id': df['product_id'],
        'added_at': df['added_at'],
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_whishlist(df: pd.DataFrame, region: str) -> pd.DataFrame:
    if region == "asia":
        return transform_wishlists_asia(df)
    elif region == "eu":
        return transform_wishlists_eu(df)
    elif region == "us":
        return transform_wishlists_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")