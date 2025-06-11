import pandas as pd
from datetime import datetime

def normalize_is_active(region: str, status_series: pd.Series) -> pd.Series:
    if region == "asia":
        return status_series.str.lower().replace({
            'active': True,
            'inactive': False,
            'discontinued': False
        })
    elif region == "eu":
        return status_series == 1
    elif region == "us":
        return status_series.astype(bool)
    else:
        return pd.Series([None] * len(status_series))

def normalize_created_at(region: str, date_series: pd.Series) -> pd.Series:
    if region == "asia":
        return pd.to_datetime(date_series, errors='coerce')
    else:
        return date_series

# --- ASIA ---
def transform_product_variants_asia(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    rate = exchange_rates.get("JPY/USD", 0.0068)
    return pd.DataFrame({
        'variant_id': df['variant_id'],
        'product_id': df['product_id'],
        'product_sku': df['sku'],
        'variant_name': df['variant_name'],
        'variant_type': df['variant_type'],
        'variant_value': df['variant_value'],
        'price_diff_usd': df['price_diff_jpy'] * rate,
        'is_active': normalize_is_active("asia", df['status']),
        'created_at': normalize_created_at("asia", df['created']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_product_variants_eu(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    rate = exchange_rates.get("EUR/USD", 1.08)
    return pd.DataFrame({
        'variant_id': df['variant_id'],
        'product_id': df['product_id'],
        'product_sku': df['sku'],
        'variant_name': df['variant_name'],
        'variant_type': df['variant_type'],
        'variant_value': df['variant_value'],
        'price_diff_usd': df['price_adjustment_eur'] * rate,
        'is_active_eu': df['is_active'],
        'is_active': normalize_is_active("eu", df['is_active']),
        'created_at': normalize_created_at("eu", df['created_at']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- US ---
def transform_product_variants_us(df: pd.DataFrame, exchange_rates: dict) -> pd.DataFrame:
    return pd.DataFrame({
        'variant_id': df['variant_id'],
        'product_id': df['product_id'],
        'product_sku': df['sku'],
        'variant_name': df['variant_name'],
        'variant_type': df['variant_type'],
        'variant_value': df['variant_value'],
        'price_diff_usd': df['price_adjustment_usd'],
        'is_active_us': df['is_active'],
        'is_active': normalize_is_active("us", df['is_active']),
        'created_at': normalize_created_at("us", df['created_at']),
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_product_variants(df: pd.DataFrame, region: str, exchange_rates: dict) -> pd.DataFrame:
    if region == "asia":
        return transform_product_variants_asia(df, exchange_rates)
    elif region == "eu":
        return transform_product_variants_eu(df, exchange_rates)
    elif region == "us":
        return transform_product_variants_us(df, exchange_rates)
    else:
        raise ValueError(f"Unsupported region: {region}")