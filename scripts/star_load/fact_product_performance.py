def fact_product_performance_sql() -> str:
    return """
MERGE INTO fact_product_performance AS target
USING (
    -- Aggregate product performance data from multiple sources
    WITH sales_data AS (
        -- Get sales metrics by product, date, and region
        SELECT 
            DATE(o.order_timestamp) as record_date,
            pv.product_id,
            o.source_region,
            c.country_code,
            SUM(oi.quantity) as units_sold,
            SUM(oi.total_price_usd) as revenue_usd,
            SUM(oi.quantity * COALESCE(p.cost_usd, 0)) as cost_usd,
            SUM(oi.total_price_usd - (oi.quantity * COALESCE(p.cost_usd, 0))) as gross_profit_usd
        FROM ods.orders o
        JOIN ods.order_items oi 
            ON o.order_id = oi.order_id AND o.source_region = oi.source_region
        JOIN ods.product_variants pv 
            ON oi.variant_id = pv.variant_id AND oi.source_region = pv.source_region
        JOIN ods.products p 
            ON pv.product_id = p.product_id AND pv.source_region = p.source_region
        JOIN ods.customers c 
            ON o.customer_id = c.customer_id AND o.source_region = c.source_region
        WHERE o.order_status NOT IN ('cancelled', 'pending_payment')
        GROUP BY 
            DATE(o.order_timestamp),
            pv.product_id,
            o.source_region,
            c.country_code
    ),
    
    return_data AS (
        -- Get return metrics by product, date, and region
        SELECT 
            DATE(r.return_timestamp) as record_date,
            pv.product_id,
            r.source_region,
            c.country_code,
            COUNT(*) as returns,
            SUM(oi.total_price_usd) as return_value_usd
        FROM ods.returns r
        JOIN ods.orders o 
            ON r.order_id = o.order_id AND r.source_region = o.source_region
        JOIN ods.order_items oi 
            ON o.order_id = oi.order_id AND o.source_region = oi.source_region
        JOIN ods.product_variants pv 
            ON oi.variant_id = pv.variant_id AND oi.source_region = pv.source_region
        JOIN ods.customers c 
            ON o.customer_id = c.customer_id AND o.source_region = c.source_region
        WHERE r.return_timestamp IS NOT NULL
        GROUP BY 
            DATE(r.return_timestamp),
            pv.product_id,
            r.source_region,
            c.country_code
    ),
    
    review_data AS (
        -- Get review metrics by product, date, and region
        SELECT 
            DATE(pr.created_at) as record_date,
            pr.product_id,
            pr.source_region,
            c.country_code,
            COUNT(*) as reviews_count,
            AVG(CAST(pr.rating AS DECIMAL(3,2))) as average_rating
        FROM ods.product_reviews pr
        JOIN ods.customers c 
            ON pr.customer_id = c.customer_id AND pr.source_region = c.source_region
        WHERE pr.rating IS NOT NULL
        GROUP BY 
            DATE(pr.created_at),
            pr.product_id,
            pr.source_region,
            c.country_code
    ),
    
    inventory_data AS (
        -- Get inventory metrics by product, date, and region
        -- Note: Using updated_at as the date field since record_date doesn't exist
        SELECT 
            DATE(i.updated_at) as record_date,
            pv.product_id,
            i.source_region,
            -- Map source_region to country_code for geography lookup
            CASE 
                WHEN i.source_region = 'US' THEN 'US'
                WHEN i.source_region = 'EU' THEN 'DE'  -- Default to Germany for EU
                WHEN i.source_region = 'ASIA' THEN 'CN'  -- Default to China for ASIA
                ELSE 'US'
            END as country_code,
            
            -- Calculate basic inventory metrics
            AVG(i.quantity_available) as avg_inventory,
            SUM(CASE WHEN i.quantity_available = 0 THEN 1 ELSE 0 END) as stockout_days,
            
            -- Simplified inventory turns calculation
            -- Note: Without sales velocity data in inventory table, we'll calculate this differently
            0 as inventory_turns  -- Will be calculated later from sales data
        FROM ods.inventory i
        JOIN ods.product_variants pv 
            ON i.variant_id = pv.variant_id AND i.source_region = pv.source_region
        GROUP BY 
            DATE(i.updated_at),
            pv.product_id,
            i.source_region
    ),
    
    -- Create a comprehensive date/product/region combination
    all_combinations AS (
        SELECT DISTINCT record_date, product_id, source_region, country_code 
        FROM sales_data
        UNION
        SELECT DISTINCT record_date, product_id, source_region, country_code 
        FROM return_data
        UNION
        SELECT DISTINCT record_date, product_id, source_region, country_code 
        FROM review_data
        UNION
        SELECT DISTINCT record_date, product_id, source_region, country_code 
        FROM inventory_data
    ),
    
    -- Combine all metrics
    combined_data AS (
        SELECT 
            ac.record_date,
            ac.product_id,
            ac.source_region,
            ac.country_code,
            
            -- Sales metrics (with defaults for missing data)
            COALESCE(s.units_sold, 0) as units_sold,
            COALESCE(s.revenue_usd, 0) as revenue_usd,
            COALESCE(s.cost_usd, 0) as cost_usd,
            COALESCE(s.gross_profit_usd, 0) as gross_profit_usd,
            
            -- Return metrics
            COALESCE(r.returns, 0) as returns,
            COALESCE(r.return_value_usd, 0) as return_value_usd,
            
            -- Review metrics
            COALESCE(rv.reviews_count, 0) as reviews_count,
            rv.average_rating,
            
            -- Inventory metrics
            COALESCE(i.stockout_days, 0) as stockout_days,
            -- Calculate inventory turns from sales and average inventory
            CASE 
                WHEN COALESCE(i.avg_inventory, 0) > 0 
                THEN ROUND(COALESCE(s.units_sold, 0) / i.avg_inventory, 4)
                ELSE 0 
            END as inventory_turns
            
        FROM all_combinations ac
        LEFT JOIN sales_data s
            ON ac.record_date = s.record_date 
            AND ac.product_id = s.product_id
            AND ac.source_region = s.source_region
            AND ac.country_code = s.country_code
        LEFT JOIN return_data r
            ON ac.record_date = r.record_date 
            AND ac.product_id = r.product_id
            AND ac.source_region = r.source_region
            AND ac.country_code = r.country_code
        LEFT JOIN review_data rv
            ON ac.record_date = rv.record_date 
            AND ac.product_id = rv.product_id
            AND ac.source_region = rv.source_region
            AND ac.country_code = rv.country_code
        LEFT JOIN inventory_data i
            ON ac.record_date = i.record_date 
            AND ac.product_id = i.product_id
            AND ac.source_region = i.source_region
            AND ac.country_code = i.country_code
    ),
    
    -- Get dimension keys and calculate derived metrics
    final_data AS (
        SELECT
            dt.time_key as date_key,
            dp.product_key,
            dg.geography_key,
            cd.units_sold,
            cd.revenue_usd,
            cd.cost_usd,
            cd.gross_profit_usd,
            cd.returns,
            cd.return_value_usd,
            cd.reviews_count,
            cd.average_rating,
            cd.inventory_turns,
            cd.stockout_days,
            
            -- Calculate derived metrics
            CASE 
                WHEN cd.units_sold > 0 THEN ROUND(cd.returns * 1.0 / cd.units_sold, 4)
                ELSE 0 
            END as return_rate,
            
            CASE 
                WHEN cd.revenue_usd > 0 THEN ROUND(cd.gross_profit_usd / cd.revenue_usd, 4)
                ELSE 0 
            END as profit_margin,
            
            cd.source_region
        FROM combined_data cd
        JOIN dim_time dt 
            ON TO_NUMBER(TO_CHAR(cd.record_date, 'YYYYMMDD')) = dt.time_key
        JOIN dim_product dp 
            ON cd.product_id = dp.product_id AND cd.source_region = dp.source_region
        JOIN dim_geography dg 
            ON cd.country_code = dg.country_code
        WHERE cd.record_date IS NOT NULL
    )
    
    SELECT * FROM final_data
) AS source

ON target.date_key = source.date_key
AND target.product_key = source.product_key
AND target.geography_key = source.geography_key
AND target.source_region = source.source_region

WHEN MATCHED THEN UPDATE SET
    units_sold = source.units_sold,
    revenue_usd = source.revenue_usd,
    cost_usd = source.cost_usd,
    gross_profit_usd = source.gross_profit_usd,
    returns = source.returns,
    return_value_usd = source.return_value_usd,
    reviews_count = source.reviews_count,
    average_rating = source.average_rating,
    inventory_turns = source.inventory_turns,
    stockout_days = source.stockout_days,
    return_rate = source.return_rate,
    profit_margin = source.profit_margin,
    created_at = CURRENT_TIMESTAMP

WHEN NOT MATCHED THEN INSERT (
    date_key,
    product_key,
    geography_key,
    units_sold,
    revenue_usd,
    cost_usd,
    gross_profit_usd,
    returns,
    return_value_usd,
    reviews_count,
    average_rating,
    inventory_turns,
    stockout_days,
    return_rate,
    profit_margin,
    source_region,
    created_at
) VALUES (
    source.date_key,
    source.product_key,
    source.geography_key,
    source.units_sold,
    source.revenue_usd,
    source.cost_usd,
    source.gross_profit_usd,
    source.returns,
    source.return_value_usd,
    source.reviews_count,
    source.average_rating,
    source.inventory_turns,
    source.stockout_days,
    source.return_rate,
    source.profit_margin,
    source.source_region,
    CURRENT_TIMESTAMP
);
"""