def fact_customer_sql() -> str:
    return """
MERGE INTO fact_customer_behavior AS target
USING (
    WITH customer_keys AS (
        SELECT 
            customer_key,
            customer_id,
            source_region
        FROM dim_customer
    ),
    geo_keys AS (
        SELECT 
            geography_key,
            country_code
        FROM dim_geography
    ),
    time_keys AS (
        SELECT 
            time_key,
            date_actual
        FROM dim_time
        WHERE date_actual BETWEEN DATE('2023-01-01') AND CURRENT_DATE()
    ),
    cart_activity AS (
        SELECT 
            customer_id,
            source_region,
            DATE(created_at) AS activity_date,
            COUNT(*) AS additions,
            SUM(CASE WHEN is_abandoned THEN 1 ELSE 0 END) AS abandonments
        FROM ods.shopping_carts
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id, source_region, DATE(created_at)
    ),
    wishlist_activity AS (
        SELECT 
            customer_id,
            source_region,
            DATE(added_at) AS activity_date,
            COUNT(*) AS additions
        FROM ods.wishlists
        GROUP BY customer_id, source_region, DATE(added_at)
    ),
    review_activity AS (
        SELECT 
            customer_id,
            source_region,
            DATE(created_at) AS activity_date,
            COUNT(*) AS reviews
        FROM ods.product_reviews
        GROUP BY customer_id, source_region, DATE(created_at)
    )
    
    SELECT
        t.time_key AS date_key,
        ck.customer_key,
        gk.geography_key,
        COALESCE(ca.additions, 0) AS cart_additions,
        COALESCE(ca.abandonments, 0) AS cart_abandonments,
        COALESCE(wa.additions, 0) AS wishlist_additions,
        COALESCE(ra.reviews, 0) AS reviews_written,
        CASE 
            WHEN COALESCE(ca.additions, 0) > 0 
            THEN COALESCE(ca.abandonments, 0) * 1.0 / COALESCE(ca.additions, 1)
            ELSE 0 
        END AS cart_abandonment_rate,
        (
            COALESCE(ca.additions, 0) * 0.5 +
            COALESCE(wa.additions, 0) * 0.25 +
            COALESCE(ra.reviews, 0) * 0.25
        ) AS engagement_score,
        c.source_region
    FROM ods.customers c
    JOIN customer_keys ck ON c.customer_id = ck.customer_id AND c.source_region = ck.source_region
    JOIN geo_keys gk ON c.country_code = gk.country_code
    CROSS JOIN time_keys t
    LEFT JOIN cart_activity ca ON c.customer_id = ca.customer_id 
                               AND c.source_region = ca.source_region
                               AND t.date_actual = ca.activity_date
    LEFT JOIN wishlist_activity wa ON c.customer_id = wa.customer_id 
                                   AND c.source_region = wa.source_region
                                   AND t.date_actual = wa.activity_date
    LEFT JOIN review_activity ra ON c.customer_id = ra.customer_id 
                                 AND c.source_region = ra.source_region
                                 AND t.date_actual = ra.activity_date
    WHERE ca.additions IS NOT NULL 
       OR wa.additions IS NOT NULL 
       OR ra.reviews IS NOT NULL
) AS source

ON target.date_key = source.date_key
   AND target.customer_key = source.customer_key
   AND target.geography_key = source.geography_key

WHEN MATCHED THEN UPDATE SET
    cart_additions = source.cart_additions,
    cart_abandonments = source.cart_abandonments,
    wishlist_additions = source.wishlist_additions,
    reviews_written = source.reviews_written,
    cart_abandonment_rate = source.cart_abandonment_rate,
    engagement_score = source.engagement_score,
    source_region = source.source_region

WHEN NOT MATCHED THEN INSERT (
    date_key,
    customer_key,
    geography_key,
    cart_additions,
    cart_abandonments,
    wishlist_additions,
    reviews_written,
    cart_abandonment_rate,
    engagement_score,
    source_region
) VALUES (
    source.date_key,
    source.customer_key,
    source.geography_key,
    source.cart_additions,
    source.cart_abandonments,
    source.wishlist_additions,
    source.reviews_written,
    source.cart_abandonment_rate,
    source.engagement_score,
    source.source_region
);
"""
