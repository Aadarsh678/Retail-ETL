import pandas as pd
import re
from datetime import datetime

def normalize_weight(region: str, weight_series: pd.Series, precision: int = 2) -> pd.Series:
    if region == "asia":
        return (weight_series / 1000).round(precision)
    elif region == "eu":
        return weight_series.round(precision)
    elif region == "us":
        return (weight_series * 0.453592).round(precision)
    else:
        return weight_series

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

def normalize_name(name_series: pd.Series) -> pd.Series:
    return name_series.str.strip().str.title()

def parse_dimensions_inches(dim_series: pd.Series) -> tuple:
    length = dim_series.str.extract(r'([0-9.]+)', expand=False).astype(float)
    width = dim_series.str.extract(r'[xX]\s*([0-9.]+)', expand=False).astype(float)
    height = dim_series.str.extract(r'[xX]\s*[0-9.]+\s*[xX]\s*([0-9.]+)', expand=False).astype(float)
    return length, width, height

# --- ASIA ---
def transform_asia_products(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    rate = exchange_rates.get("JPY/USD", 0.0068)
    
    # Parse dimensions from size_info
    length = df['size_info'].str.extract(r'([0-9.]+)mm')[0].astype(float) / 10
    width = df['size_info'].str.extract(r'x\s*([0-9.]+)mm')[0].astype(float) / 10
    height = df['size_info'].str.extract(r'x\s*[0-9.]+mm\s*x\s*([0-9.]+)mm')[0].astype(float) / 10
    
    return pd.DataFrame({
        'product_id': df['product_id'],
        'product_sku': df['item_code'].str.strip(),
        'product_name': normalize_name(df['name'].str.strip()),
        'product_description': df['description'],
        'category_id': df['cat_id'],
        'base_usd': df['price_jpy'] * rate,
        'cost_usd': df['cost_jpy'] * rate,
        'weight_kg': normalize_weight("asia", df['weight_g']),
        'length_cm': length,
        'width_cm': width,
        'height_cm': height,
        'is_active': df['active_status'].str.lower() == 'active',
        'created_at': safe_to_timestamp_multi_formats(df['create_date']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_eu_products(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    rate = exchange_rates.get("EUR/USD", 1.08)
    return pd.DataFrame({
        'product_id': df['product_id'],
        'product_sku': df['product_sku'],
        'product_name': normalize_name(df['product_name']),
        'product_description': df['product_description'],
        'category_id': df['category_id'],
        'base_usd': df['base_price_eur'] * rate,
        'cost_usd': df['cost_price_eur'] * rate,
        'vat_rate': df['vat_rate'],
        'weight_kg': normalize_weight("eu", df['weight_kg']),
        'length_cm': df['length_cm'],
        'width_cm': df['width_cm'],
        'height_cm': df['height_cm'],
        'energy_rating': df['energy_rating'],
        'is_active': df['is_active'] == 1,
        'created_at': safe_to_timestamp_multi_formats(df['created_at']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated_at']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- US ---
def transform_us_products(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    length, width, height = parse_dimensions_inches(df['dimensions_inches'])
    
    return pd.DataFrame({
        'product_id': df['product_id'],
        'product_sku': df['product_code'],
        'product_name': normalize_name(df['product_name']),
        'product_description': df['product_description'],
        'category_id': df['category_id'],
        'base_usd': df['base_price_usd'],
        'cost_usd': df['cost_price_usd'],
        'weight_kg': normalize_weight("us", df['weight_lbs']),
        'length_cm': length,
        'width_cm': width,
        'height_cm': height,
        'is_active': df['is_active'],
        'created_at': safe_to_timestamp_multi_formats(df['created_at']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated_at']),
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_products(df: pd.DataFrame, region: str, exchange_rates: dict) -> pd.DataFrame:
    if region == "asia":
        return transform_asia_products(df, exchange_rates)
    elif region == "eu":
        return transform_eu_products(df, exchange_rates)
    elif region == "us":
        return transform_us_products(df, exchange_rates)
    else:
        raise ValueError(f"Unsupported region: {region}")