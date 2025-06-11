def dim_customer_sql() -> str:
    return """
MERGE INTO star.dim_customer AS target
USING (
    SELECT *
    FROM (
        SELECT 
            c.customer_id,
            c.source_region,
            CASE 
                WHEN c.source_region = 'EU' THEN c.email
                ELSE c.email  -- Replace with HASH(c.email) if you want to hash for US/ASIA
            END AS email_hash,
            c.first_name,
            c.last_name,
            CASE 
                WHEN c.first_name IS NOT NULL AND c.last_name IS NOT NULL 
                    THEN c.first_name || ' ' || c.last_name
                WHEN c.first_name IS NOT NULL 
                    THEN c.first_name
                WHEN c.last_name IS NOT NULL 
                    THEN c.last_name
                ELSE 'Unknown Customer'
            END AS full_name,
            c.gender,
            c.birth_date,
            CASE 
                WHEN c.birth_date IS NULL THEN 'Unknown'
                ELSE
                    CASE 
                        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE) < 18 THEN 'Under 18'
                        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE) < 25 THEN '18-24'
                        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE) < 35 THEN '25-34'
                        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE) < 45 THEN '35-44'
                        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE) < 55 THEN '45-54'
                        WHEN DATEDIFF(year, c.birth_date, CURRENT_DATE) < 65 THEN '55-64'
                        ELSE '65+'
                    END
            END AS age_group,
            c.country_code,
            g.country_name,
            COALESCE(c.customer_segment, 'Standard') AS customer_segment,
            COALESCE(c.acquisition_channel, 'Direct') AS acquisition_channel,
            DATE(c.registration_date) AS registration_date,
            COALESCE(order_stats.total_spent, 0) AS customer_lifetime_value_usd,
            COALESCE(order_stats.order_count, 0) AS total_orders,
            c.is_active,
            CASE WHEN c.source_region = 'EU' THEN TRUE ELSE FALSE END AS gdpr_compliant,
            CURRENT_TIMESTAMP AS updated_at,
            ROW_NUMBER() OVER (PARTITION BY c.customer_id, c.source_region ORDER BY CURRENT_TIMESTAMP DESC) AS row_num
        FROM ods.customers c
        LEFT JOIN dim_geography g 
            ON c.country_code = g.country_code
        LEFT JOIN (
            SELECT 
                customer_id,
                source_region,
                SUM(total_amount_usd) AS total_spent,
                COUNT(DISTINCT order_id) AS order_count
            FROM ods.orders 
            WHERE order_status NOT IN ('cancelled', 'refunded')
            GROUP BY customer_id, source_region
        ) order_stats 
            ON c.customer_id = order_stats.customer_id 
            AND c.source_region = order_stats.source_region
    )
    WHERE row_num = 1
) AS src
ON target.customer_id = src.customer_id AND target.source_region = src.source_region

-- Update existing records
WHEN MATCHED THEN UPDATE SET
    target.email_hash = src.email_hash,
    target.first_name = src.first_name,
    target.last_name = src.last_name,
    target.full_name = src.full_name,
    target.gender = src.gender,
    target.birth_date = src.birth_date,
    target.age_group = src.age_group,
    target.country_code = src.country_code,
    target.country_name = src.country_name,
    target.customer_segment = src.customer_segment,
    target.acquisition_channel = src.acquisition_channel,
    target.registration_date = src.registration_date,
    target.customer_lifetime_value_usd = src.customer_lifetime_value_usd,
    target.total_orders = src.total_orders,
    target.is_active = src.is_active,
    target.gdpr_compliant = src.gdpr_compliant,
    target.updated_at = src.updated_at

-- Insert new records
WHEN NOT MATCHED THEN INSERT (
    customer_id,
    source_region,
    email_hash,
    first_name,
    last_name,
    full_name,
    gender,
    birth_date,
    age_group,
    country_code,
    country_name,
    customer_segment,
    acquisition_channel,
    registration_date,
    customer_lifetime_value_usd,
    total_orders,
    is_active,
    gdpr_compliant,
    updated_at
) VALUES (
    src.customer_id,
    src.source_region,
    src.email_hash,
    src.first_name,
    src.last_name,
    src.full_name,
    src.gender,
    src.birth_date,
    src.age_group,
    src.country_code,
    src.country_name,
    src.customer_segment,
    src.acquisition_channel,
    src.registration_date,
    src.customer_lifetime_value_usd,
    src.total_orders,
    src.is_active,
    src.gdpr_compliant,
    src.updated_at
);
"""
