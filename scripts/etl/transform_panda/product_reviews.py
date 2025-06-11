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

def normalize_active(flag_series: pd.Series) -> pd.Series:
    return flag_series.str.lower().replace({
        'y': True, 'yes': True, 'n': False, 'no': False
    })

# --- ASIA ---
def transform_product_reviews_asia(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'review_id': df['review_id'],
        'product_id': df['product_id'],
        'customer_id': df['cust_id'],
        'order_id': df['order_id'],
        'rating': df['rating'],
        'review_title': df['title'],
        'review_text': df['review_text'],
        'is_verified_purchase': normalize_active(df['verified']),
        'helpful_votes': df['helpful_count'],
        'created_at': safe_to_timestamp_multi_formats(df['created']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_product_reviews_eu(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'review_id': df['review_id'],
        'product_id': df['product_id'],
        'customer_id': df['customer_id'],
        'order_id': df['order_id'],
        'rating': df['rating'],
        'review_title': df['review_title'],
        'review_text': df['review_text'],
        'is_verified_purchase': df['is_verified_purchase'],
        'helpful_votes': df['helpful_votes'],
        'moderation_status': df['moderation_status'],
        'created_at': df['created_at'],
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- US ---
def transform_product_reviews_us(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'review_id': df['review_id'],
        'product_id': df['product_id'],
        'customer_id': df['customer_id'],
        'order_id': df['order_id'],
        'rating': df['rating'],
        'review_title': df['review_title'],
        'review_text': df['review_text'],
        'is_verified_purchase': df['is_verified_purchase'],
        'helpful_votes': df['helpful_votes'],
        'created_at': df['created_at'],
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_product_reviews(df: pd.DataFrame, region: str) -> pd.DataFrame:
    if region == "asia":
        return transform_product_reviews_asia(df)
    elif region == "eu":
        return transform_product_reviews_eu(df)
    elif region == "us":
        return transform_product_reviews_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")