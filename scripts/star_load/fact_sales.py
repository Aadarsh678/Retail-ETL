def fact_sales_sql() -> str:
    return """
MERGE INTO fact_sales AS target
USING (
    WITH sales_data AS (
        SELECT 
            o.order_id,
            o.order_reference,
            o.source_region,
            oi.order_item_id,
            oi.variant_id,
            oi.quantity,
            oi.unit_price_usd,
            oi.total_price_usd,
            o.discount_amount_usd,
            o.tax_amount_usd,
            o.shipping_amount_usd,
            CAST(o.order_timestamp AS DATE) as order_date,
            o.customer_id,
            pv.product_id,
            c.country_code,
            o.campaign_id,
            p.payment_method,
            CASE 
                WHEN s.shipped_timestamp IS NOT NULL AND o.order_timestamp IS NOT NULL
                THEN DATEDIFF('day', o.order_timestamp, s.shipped_timestamp)
                ELSE NULL
            END as days_to_ship,
            CASE WHEN r.return_id IS NOT NULL THEN TRUE ELSE FALSE END as is_returned
        FROM ods.orders o
        JOIN ods.order_items oi 
            ON o.order_id = oi.order_id AND o.source_region = oi.source_region
        JOIN ods.customers c 
            ON o.customer_id = c.customer_id AND o.source_region = c.source_region
        JOIN ods.product_variants pv 
            ON oi.variant_id = pv.variant_id AND oi.source_region = pv.source_region
        LEFT JOIN ods.payments p 
            ON o.order_id = p.order_id AND o.source_region = p.source_region
        LEFT JOIN ods.shipments s 
            ON o.order_id = s.order_id AND o.source_region = s.source_region
        LEFT JOIN ods.returns r 
            ON o.order_id = r.order_id AND o.source_region = r.source_region
        WHERE o.order_status NOT IN ('cancelled', 'pending_payment')
    ),
    dimension_keys AS (
        SELECT 
            sd.*,
            dt.time_key AS order_date_key,
            dc.customer_key,
            dp.product_key,
            dg.geography_key,
            dmc.campaign_key,
            dpm.payment_method_key,
            dp.cost_usd,
            (sd.unit_price_usd - COALESCE(dp.cost_usd, 0)) * sd.quantity AS gross_profit_usd
        FROM sales_data sd
        JOIN dim_time dt 
            ON TO_NUMBER(TO_CHAR(sd.order_date, 'YYYYMMDD')) = dt.time_key
        JOIN dim_customer dc 
            ON sd.customer_id = dc.customer_id AND sd.source_region = dc.source_region
        JOIN dim_product dp 
            ON sd.product_id = dp.product_id AND sd.source_region = dp.source_region
        JOIN dim_geography dg 
            ON sd.country_code = dg.country_code
        LEFT JOIN dim_marketing_campaign dmc 
            ON sd.campaign_id = dmc.campaign_id AND sd.source_region = dmc.source_region
        LEFT JOIN dim_payment_method dpm 
            ON sd.payment_method = dpm.payment_method
    ),
    first_purchases AS (
        SELECT 
            customer_id,
            source_region,
            MIN(order_date) AS first_purchase_date
        FROM sales_data
        GROUP BY customer_id, source_region
    )
    SELECT 
        dk.order_date_key,
        dk.customer_key,
        dk.product_key,
        dk.geography_key,
        dk.campaign_key,
        dk.payment_method_key,
        dk.order_id,
        dk.order_reference,
        dk.source_region,
        dk.quantity,
        dk.unit_price_usd,
        dk.total_price_usd,
        dk.cost_usd,
        dk.gross_profit_usd,
        ROUND(dk.discount_amount_usd * (dk.total_price_usd / SUM(dk.total_price_usd) OVER (PARTITION BY dk.order_id, dk.source_region)), 2) AS discount_amount_usd,
        ROUND(dk.tax_amount_usd * (dk.total_price_usd / SUM(dk.total_price_usd) OVER (PARTITION BY dk.order_id, dk.source_region)), 2) AS tax_amount_usd,
        ROUND(dk.shipping_amount_usd * (dk.total_price_usd / SUM(dk.total_price_usd) OVER (PARTITION BY dk.order_id, dk.source_region)), 2) AS shipping_amount_usd,
        CASE WHEN dk.order_date = fp.first_purchase_date THEN TRUE ELSE FALSE END AS is_first_purchase,
        dk.is_returned,
        dk.days_to_ship
    FROM dimension_keys dk
    LEFT JOIN first_purchases fp 
        ON dk.customer_id = fp.customer_id AND dk.source_region = fp.source_region
) AS source

ON target.order_id = source.order_id
AND target.order_date_key = source.order_date_key
AND target.customer_key = source.customer_key
AND target.product_key = source.product_key
AND target.source_region = source.source_region

WHEN MATCHED THEN UPDATE SET
    order_reference = source.order_reference,
    quantity = source.quantity,
    unit_price_usd = source.unit_price_usd,
    total_price_usd = source.total_price_usd,
    cost_usd = source.cost_usd,
    gross_profit_usd = source.gross_profit_usd,
    discount_amount_usd = source.discount_amount_usd,
    tax_amount_usd = source.tax_amount_usd,
    shipping_amount_usd = source.shipping_amount_usd,
    campaign_key = source.campaign_key,
    payment_method_key = source.payment_method_key,
    is_first_purchase = source.is_first_purchase,
    is_returned = source.is_returned,
    days_to_ship = source.days_to_ship,
    geography_key = source.geography_key,
    created_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN INSERT (
    order_date_key,
    customer_key,
    product_key,
    geography_key,
    campaign_key,
    payment_method_key,
    order_id,
    order_reference,
    source_region,
    quantity,
    unit_price_usd,
    total_price_usd,
    cost_usd,
    gross_profit_usd,
    discount_amount_usd,
    tax_amount_usd,
    shipping_amount_usd,
    is_first_purchase,
    is_returned,
    days_to_ship,
    created_at
) VALUES (
    source.order_date_key,
    source.customer_key,
    source.product_key,
    source.geography_key,
    source.campaign_key,
    source.payment_method_key,
    source.order_id,
    source.order_reference,
    source.source_region,
    source.quantity,
    source.unit_price_usd,
    source.total_price_usd,
    source.cost_usd,
    source.gross_profit_usd,
    source.discount_amount_usd,
    source.tax_amount_usd,
    source.shipping_amount_usd,
    source.is_first_purchase,
    source.is_returned,
    source.days_to_ship,
    CURRENT_TIMESTAMP()
);
"""
