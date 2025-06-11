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
def transform_shipments_asia(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'shipment_id': df['shipment_id'],
        'order_id': df['order_id'],
        'tracking_number': df['tracking_no'],
        'carrier': df['carrier'],
        'shipping_method': df['method'],
        'shipped_timestamp': safe_to_timestamp_multi_formats(df['shipped_time']),
        'estimated_delivery_date': safe_to_timestamp_multi_formats(df['est_delivery']),
        'actual_delivery_timestamp': safe_to_timestamp_multi_formats(df['actual_delivery']),
        'shipment_status': df['status'],
        'created_at': safe_to_timestamp_multi_formats(df['created']),
        'updated_at': safe_to_timestamp_multi_formats(df['updated']),
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- EU ---
def transform_shipments_eu(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'shipment_id': df['shipment_id'],
        'order_id': df['order_id'],
        'tracking_number': df['tracking_number'],
        'carrier': df['carrier'],
        'shipping_method': df['shipping_method'],
        'shipped_timestamp': df['shipped_timestamp'],
        'estimated_delivery_date': df['estimated_delivery_date'],
        'actual_delivery_timestamp': df['actual_delivery_timestamp'],
        'shipment_status': df['shipment_status'],
        'created_at': df['created_at'],
        'updated_at': df['updated_at'],
        '_region': df['_region'],
        '_source': df['_source']
    })

# --- US ---
def transform_shipments_us(df: pd.DataFrame) -> pd.DataFrame:
    return pd.DataFrame({
        'shipment_id': df['shipment_id'],
        'order_id': df['order_id'],
        'tracking_number': df['tracking_number'],
        'carrier': df['carrier'],
        'shipping_method': df['shipping_method'],
        'shipped_timestamp': df['shipped_date'],
        'estimated_delivery_date': df['estimated_delivery_date'],
        'actual_delivery_timestamp': df['actual_delivery_date'],
        'shipment_status': df['shipment_status'],
        'created_at': df['created_at'],
        'updated_at': df['updated_at'],
        '_region': df['_region'],
        '_source': df['_source']
    })

def transform_shipments(df: pd.DataFrame, region: str) -> pd.DataFrame:
    if region == "asia":
        return transform_shipments_asia(df)
    elif region == "eu":
        return transform_shipments_eu(df)
    elif region == "us":
        return transform_shipments_us(df)
    else:
        raise ValueError(f"Unsupported region: {region}")